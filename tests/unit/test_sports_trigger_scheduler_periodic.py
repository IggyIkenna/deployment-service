"""Unit tests for sports_trigger_scheduler periodic-tier dispatch.

Covers:
  - _build_cli_cmd shape for per-fixture (today/today) and rolling-window paths.
  - PeriodicTierState round-trip (GCS-backed, via InMemoryStorageClient).
  - _check_discovery: first run, cadence-elapsed, cadence-not-elapsed,
    rolling-window math, force-overwrite flag propagation.
  - _check_reference: window_condition=transfer_window_open gating + per-league
    --league arg; window_condition=season_boundary tolerance; run_always:true.
  - Dry-run mode: no dispatch, no state persist.
  - Cloud Run backend parity: periodic + per-fixture share the `_backend=cloud`
    stub path (no duplicate stub).

No real subprocess dispatch — `_dispatch_local` is monkeypatched to a fake that
records invocations. This follows the shard-level failure-isolation guarantee
— tests do not touch the network, GCS, or subprocess.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest

from deployment_service.deployments_registry import InMemoryStorageClient
from deployment_service.sports_trigger_scheduler import SportsTriggerScheduler
from deployment_service.sports_trigger_state import DEFAULT_STATE_KEY, PeriodicTierState

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_DISCOVERY_CONFIG: dict[str, Any] = {
    "discovery": {
        "frequency_hours": 6,
        "rolling_window": {
            "lookback_days": 1,
            "lookahead_days": 7,
            "force_overwrite": True,
        },
        "services": [
            {
                "service": "instruments-service",
                "operation": "instruments",
                "category": "SPORTS",
                "args": {"--sports-entity": "FIXTURES"},
                "description": "Refresh fixtures",
            },
        ],
    },
    "reference": {
        "frequency_hours": 24,
        "services": [
            {
                "service": "instruments-service",
                "operation": "instruments",
                "category": "SPORTS",
                "args": {"--sports-entity": "INJURIES"},
                "description": "Refresh injuries",
                "run_always": True,
            },
            {
                "service": "instruments-service",
                "operation": "instruments",
                "category": "SPORTS",
                "args": {"--sports-entity": "TRANSFERS"},
                "description": "Transfers gated on window",
                "run_always": False,
                "window_condition": "transfer_window_open",
            },
        ],
    },
}


@pytest.fixture
def in_memory_state() -> PeriodicTierState:
    """GCS-free periodic state backed by the in-memory storage adapter."""
    storage = InMemoryStorageClient()
    return PeriodicTierState(bucket="unit-test-bucket", storage=storage)


@pytest.fixture
def scheduler(in_memory_state: PeriodicTierState) -> SportsTriggerScheduler:
    """Scheduler with in-memory state and empty config."""
    sched = SportsTriggerScheduler.__new__(SportsTriggerScheduler)
    sched._config_path = "configs/sports-trigger-tiers.yaml"
    sched._poll_interval = 300
    sched._dry_run = False
    sched._backend = "local"
    sched._workspace_root = ""
    # TriggerState import-safe — instantiate manually to avoid class lookups.
    from deployment_service.sports_trigger_scheduler import TriggerState

    sched._state = TriggerState()
    sched._config = _DISCOVERY_CONFIG
    sched._running = False
    sched._periodic_state = in_memory_state
    return sched


# ---------------------------------------------------------------------------
# _build_cli_cmd
# ---------------------------------------------------------------------------


def test_build_cli_cmd_per_fixture(scheduler: SportsTriggerScheduler) -> None:
    cmd = scheduler._build_cli_cmd(
        service="instruments-service",
        operation="instruments",
        asset_group="SPORTS",
        start_date="2026-04-21",
        end_date="2026-04-21",
        extra_args={"--sports-entity": "LINEUPS"},
        force=False,
    )
    assert cmd.startswith("python -m instruments_service")
    assert "--operation instruments" in cmd
    assert "--mode batch" in cmd
    assert "--asset-group SPORTS" in cmd
    assert "--start-date 2026-04-21" in cmd
    assert "--end-date 2026-04-21" in cmd
    assert "--run-tag live" in cmd
    assert "--sports-entity LINEUPS" in cmd
    assert "--force" not in cmd


def test_build_cli_cmd_rolling_window_emits_raw_flags(scheduler: SportsTriggerScheduler) -> None:
    """Rolling-window path emits raw ``--lookback-days``/``--lookahead-days``/``--force-window``.

    Mutually exclusive with ``--start-date``/``--end-date``: the
    instruments-service CLI contract (``70517b2``) rejects both shapes
    mixed, and the periodic dispatcher relies on CLI-side date resolution.
    """
    cmd = scheduler._build_cli_cmd(
        service="instruments-service",
        operation="instruments",
        asset_group="SPORTS",
        extra_args={"--sports-entity": "FIXTURES"},
        rolling_window=(1, 7, True),
    )
    assert "--lookback-days 1" in cmd
    assert "--lookahead-days 7" in cmd
    assert "--force-window" in cmd
    assert "--start-date" not in cmd
    assert "--end-date" not in cmd
    assert "--force " not in cmd and not cmd.endswith("--force")


def test_build_cli_cmd_rolling_window_without_force(scheduler: SportsTriggerScheduler) -> None:
    """``force_overwrite: false`` yields lookback/lookahead with no ``--force-window``."""
    cmd = scheduler._build_cli_cmd(
        service="instruments-service",
        operation="instruments",
        asset_group="SPORTS",
        extra_args={},
        rolling_window=(2, 14, False),
    )
    assert "--lookback-days 2" in cmd
    assert "--lookahead-days 14" in cmd
    assert "--force-window" not in cmd


# ---------------------------------------------------------------------------
# PeriodicTierState round-trip
# ---------------------------------------------------------------------------


def test_periodic_tier_state_roundtrip() -> None:
    storage = InMemoryStorageClient()
    state_a = PeriodicTierState(bucket="b", storage=storage)
    assert state_a.get_last_run("discovery") is None

    when = datetime(2026, 4, 21, 12, 0, tzinfo=UTC)
    state_a.set_last_run("discovery", when)

    # Second instance reads prior persisted state (simulating restart).
    state_b = PeriodicTierState(bucket="b", storage=storage)
    got = state_b.get_last_run("discovery")
    assert got == when
    assert state_b.snapshot() == {"discovery": when.isoformat()}


def test_periodic_tier_state_missing_blob_starts_fresh() -> None:
    storage = InMemoryStorageClient()
    state = PeriodicTierState(bucket="b", storage=storage)
    assert state.snapshot() == {}
    # Key should NOT be written until set_last_run is called.
    assert storage.list_keys("b", DEFAULT_STATE_KEY[:5]) == []


# ---------------------------------------------------------------------------
# _check_discovery
# ---------------------------------------------------------------------------


def test_check_discovery_fires_first_run(scheduler: SportsTriggerScheduler) -> None:
    """No prior state → discovery fires with raw rolling flags + force-window.

    Passes the raw ``(lookback, lookahead, force_window)`` triple straight
    through to instruments-service's ``--lookback-days``/``--lookahead-days``/
    ``--force-window`` CLI (``70517b2``), which owns the date math.
    """
    captured: list[dict[str, Any]] = []

    def fake_dispatch_local(*, cmd: str, service: str, trigger_name: str, fixture_id: str) -> bool:
        captured.append({"cmd": cmd, "service": service, "trigger": trigger_name, "id": fixture_id})
        return True

    scheduler._dispatch_local = fake_dispatch_local  # type: ignore[method-assign]
    fired = scheduler._check_discovery()

    assert fired == 1
    assert len(captured) == 1
    cmd = captured[0]["cmd"]
    assert "--lookback-days 1" in cmd
    assert "--lookahead-days 7" in cmd
    assert "--force-window" in cmd
    # Mutually exclusive — no pre-computed dates leak through.
    assert "--start-date" not in cmd
    assert "--end-date" not in cmd
    # Last-run persisted.
    assert scheduler._periodic_state is not None
    assert scheduler._periodic_state.get_last_run("discovery") is not None


def test_check_discovery_skips_when_cadence_not_elapsed(
    scheduler: SportsTriggerScheduler,
) -> None:
    """Firing within frequency_hours must be a no-op."""
    # Mark last_run = just now → cadence (6h) cannot have elapsed.
    assert scheduler._periodic_state is not None
    scheduler._periodic_state.set_last_run("discovery", datetime.now(UTC))

    captured: list[str] = []
    scheduler._dispatch_local = lambda **kw: captured.append(kw["cmd"]) or True  # type: ignore[method-assign]

    fired = scheduler._check_discovery()
    assert fired == 0
    assert captured == []


def test_check_discovery_fires_when_cadence_elapsed(
    scheduler: SportsTriggerScheduler,
) -> None:
    """last_run older than frequency_hours → fire again."""
    assert scheduler._periodic_state is not None
    stale = datetime.now(UTC) - timedelta(hours=12)  # 2× frequency (6h)
    scheduler._periodic_state.set_last_run("discovery", stale)

    captured: list[str] = []
    scheduler._dispatch_local = lambda **kw: captured.append(kw["cmd"]) or True  # type: ignore[method-assign]

    fired = scheduler._check_discovery()
    assert fired == 1
    assert len(captured) == 1


def test_check_discovery_rolling_window_flags_match_config(
    scheduler: SportsTriggerScheduler,
) -> None:
    """Raw rolling flags reflect lookback/lookahead config exactly.

    No date math in the scheduler — instruments-service resolves
    ``--lookback-days N --lookahead-days M`` at CLI parse time. Scheduler
    is responsible only for forwarding the intent.
    """
    captured: list[str] = []
    scheduler._dispatch_local = lambda **kw: captured.append(kw["cmd"]) or True  # type: ignore[method-assign]

    scheduler._check_discovery()

    assert "--lookback-days 1" in captured[0]
    assert "--lookahead-days 7" in captured[0]
    # Mutually exclusive with explicit dates — none leak through.
    assert "--start-date" not in captured[0]
    assert "--end-date" not in captured[0]


def test_check_discovery_dry_run_does_not_persist(
    scheduler: SportsTriggerScheduler,
) -> None:
    scheduler._dry_run = True
    captured: list[str] = []
    scheduler._dispatch_local = lambda **kw: captured.append(kw["cmd"]) or True  # type: ignore[method-assign]

    fired = scheduler._check_discovery()
    # Dry-run reports what WOULD fire but doesn't dispatch or persist.
    assert fired == 1
    assert captured == []  # no real dispatch
    assert scheduler._periodic_state is not None
    assert scheduler._periodic_state.get_last_run("discovery") is None


# ---------------------------------------------------------------------------
# _check_reference
# ---------------------------------------------------------------------------


def test_check_reference_run_always_fires(scheduler: SportsTriggerScheduler) -> None:
    """run_always: true → service always fires when cadence elapsed."""
    captured: list[str] = []
    scheduler._dispatch_local = lambda **kw: captured.append(kw["cmd"]) or True  # type: ignore[method-assign]

    # First run → cadence trivially elapsed.
    fired = scheduler._check_reference()
    # INJURIES (run_always) always fires. TRANSFERS gated on windows.
    cmds = " ".join(captured)
    assert "--sports-entity INJURIES" in cmds
    assert fired >= 1


def test_check_reference_transfer_window_gating(
    scheduler: SportsTriggerScheduler,
) -> None:
    """During an open EPL transfer window, TRANSFERS fires scoped by --league."""
    captured: list[str] = []
    scheduler._dispatch_local = lambda **kw: captured.append(kw["cmd"]) or True  # type: ignore[method-assign]

    # Monkey-patch is_transfer_window_open used in the gating helper to return
    # True for EPL on the scheduler's chosen date. We do this by replacing the
    # module-level reference that _gate_by_transfer_window uses.
    import deployment_service.sports_trigger_periodic as mod

    # Pretend a window is open only for EPL.
    with patch.object(mod, "is_transfer_window_open", lambda lid, d: lid == "EPL"):
        scheduler._check_reference()

    transfers_cmds = [c for c in captured if "--sports-entity TRANSFERS" in c]
    assert transfers_cmds, "Expected TRANSFERS to fire during an open window"
    assert "--league EPL" in transfers_cmds[0]


def test_check_reference_transfer_window_closed_skips(
    scheduler: SportsTriggerScheduler,
) -> None:
    """With all windows closed, TRANSFERS is skipped but INJURIES still fires."""
    captured: list[str] = []
    scheduler._dispatch_local = lambda **kw: captured.append(kw["cmd"]) or True  # type: ignore[method-assign]

    import deployment_service.sports_trigger_periodic as mod

    with patch.object(mod, "is_transfer_window_open", lambda lid, d: False):
        scheduler._check_reference()

    assert any("--sports-entity INJURIES" in c for c in captured)
    assert not any("--sports-entity TRANSFERS" in c for c in captured)


def test_check_reference_season_boundary_gating() -> None:
    """window_condition=season_boundary fires when today == a league season boundary."""
    storage = InMemoryStorageClient()
    state = PeriodicTierState(bucket="b", storage=storage)

    config: dict[str, Any] = {
        "reference": {
            "frequency_hours": 24,
            "services": [
                {
                    "service": "instruments-service",
                    "operation": "instruments",
                    "category": "SPORTS",
                    "args": {"--sports-entity": "TEAMS"},
                    "description": "Teams roster",
                    "run_always": False,
                    "window_condition": "season_boundary",
                },
            ],
        },
    }
    sched = SportsTriggerScheduler.__new__(SportsTriggerScheduler)
    from deployment_service.sports_trigger_scheduler import TriggerState

    sched._config_path = ""
    sched._poll_interval = 300
    sched._dry_run = False
    sched._backend = "local"
    sched._workspace_root = ""
    sched._state = TriggerState()
    sched._config = config
    sched._running = False
    sched._periodic_state = state

    captured: list[str] = []
    sched._dispatch_local = lambda **kw: captured.append(kw["cmd"]) or True  # type: ignore[method-assign]

    import deployment_service.sports_trigger_periodic as mod

    today = datetime.now(UTC).date()
    # Force every league's season_start to match today → boundary triggers.
    with (
        patch.object(mod, "get_season_start", lambda lid, yr: today),
        patch.object(mod, "get_season_end", lambda lid, yr: today + timedelta(days=365)),
    ):
        sched._check_reference()

    assert any("--sports-entity TEAMS" in c for c in captured), captured
    # League scope gets populated by the gate.
    assert any("--league" in c for c in captured)


def test_check_reference_season_boundary_no_trigger_when_far_from_boundary() -> None:
    """Service is skipped when today is outside season-boundary tolerance."""
    storage = InMemoryStorageClient()
    state = PeriodicTierState(bucket="b", storage=storage)
    config: dict[str, Any] = {
        "reference": {
            "frequency_hours": 24,
            "services": [
                {
                    "service": "instruments-service",
                    "operation": "instruments",
                    "category": "SPORTS",
                    "args": {"--sports-entity": "TEAMS"},
                    "description": "Teams roster",
                    "run_always": False,
                    "window_condition": "season_boundary",
                },
            ],
        },
    }
    sched = SportsTriggerScheduler.__new__(SportsTriggerScheduler)
    from deployment_service.sports_trigger_scheduler import TriggerState

    sched._config_path = ""
    sched._poll_interval = 300
    sched._dry_run = False
    sched._backend = "local"
    sched._workspace_root = ""
    sched._state = TriggerState()
    sched._config = config
    sched._running = False
    sched._periodic_state = state

    captured: list[str] = []
    sched._dispatch_local = lambda **kw: captured.append(kw["cmd"]) or True  # type: ignore[method-assign]

    import deployment_service.sports_trigger_periodic as mod

    far = date(2000, 1, 1)
    with (
        patch.object(mod, "get_season_start", lambda lid, yr: far),
        patch.object(mod, "get_season_end", lambda lid, yr: far + timedelta(days=365)),
    ):
        fired = sched._check_reference()

    assert fired == 0
    assert captured == []


# ---------------------------------------------------------------------------
# Cloud Run backend parity
# ---------------------------------------------------------------------------


def test_cloud_backend_routes_periodic_dispatch_through_cloud_branch(
    scheduler: SportsTriggerScheduler,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Switching to cloud backend routes periodic dispatch through the cloud path.

    When no cloud_run_job_name is configured, the dispatcher warns and skips
    rather than silently falling back to local.  This verifies the cloud code
    path is reached for periodic tiers, not the local subprocess branch.
    """
    scheduler._backend = "cloud"

    import logging

    with caplog.at_level(logging.WARNING):
        scheduler._check_discovery()

    warns = [r.message for r in caplog.records if "cloud_run_job_name" in r.message or "cloud_run_config" in r.message]
    assert warns, "periodic dispatch must reach the cloud dispatch branch"


# ---------------------------------------------------------------------------
# get_upcoming_fixtures — blob.name fix
# ---------------------------------------------------------------------------


def test_get_upcoming_fixtures_returns_fixtures_within_horizon() -> None:
    """Regression: blob.name — not str(blob) — must be used to match .parquet path.

    BlobMetadata.__str__ returns the dataclass repr, which never ends with
    ".parquet". Using str(blob).endswith(".parquet") would silently skip every
    blob, giving "Found 0 upcoming fixtures" on every poll. This test verifies
    blob.name is used for the filter and the download path.
    """
    import io
    from unittest.mock import MagicMock

    import pandas as pd
    from unified_trading_library import BlobMetadata

    kickoff = (datetime.now(UTC) + timedelta(hours=24)).isoformat()
    df = pd.DataFrame(
        [
            {
                "fixture_id": "fx-001",
                "league_id": "EPL",
                "kickoff_utc": kickoff,
                "home_team": "Arsenal",
                "away_team": "Chelsea",
            }
        ]
    )
    buf = io.BytesIO()
    df.to_parquet(buf)
    parquet_bytes = buf.getvalue()

    blob = BlobMetadata(
        name="sports_reference/fixtures/day=2026-06-01/fixtures.parquet",
        bucket="test-bucket",
        size=len(parquet_bytes),
        last_modified=None,
    )

    mock_storage = MagicMock()
    mock_storage.list_blobs.return_value = [blob]
    mock_storage.download_bytes.return_value = parquet_bytes

    sched = SportsTriggerScheduler.__new__(SportsTriggerScheduler)

    with (
        patch("deployment_service.sports_trigger_state.get_storage_client", return_value=mock_storage),
        patch("deployment_service.sports_trigger_state.DeploymentConfig") as mock_cfg,
        patch("deployment_service.sports_trigger_state.get_bucket_name", return_value="test-bucket"),
    ):
        mock_cfg.return_value.project_id = "test-project"
        fixtures = sched.get_upcoming_fixtures(horizon_hours=48)

    assert len(fixtures) >= 1
    assert any(f["fixture_id"] == "fx-001" for f in fixtures)


# ---------------------------------------------------------------------------
# Cloud dispatch — args must never carry a second "python -m <module>" prefix
# ---------------------------------------------------------------------------
#
# Regression coverage for the 2026-07-08 fix
# (unified-trading-pm/plans/active/issues/
# sports_trigger_scheduler_cloud_dispatch_broken_2026_07_08.md): the real,
# provisioned Cloud Run Job targets (verified via `gcloud run jobs describe
# uts-prod-instruments-service-t1-recon`) have ``command: None`` — the image's
# own ENTRYPOINT already resolves to the module invocation — so
# ``CloudRunBackend.deploy_shard``'s ``args=`` override must be CLI flags
# only. Before this fix, `_dispatch_services`'s inline cloud branch passed
# the FULL ``shlex.split(cmd)`` (including "python -m <module>") straight
# through, which would have broken every real cloud dispatch even after the
# CLI wiring bug was fixed.


def test_strip_python_module_prefix_drops_python_dash_m_module() -> None:
    """`_build_cli_cmd` always emits "python -m <module> <flags...>"."""
    cmd = "python -m instruments_service --operation instruments --mode batch --asset-group SPORTS"
    args = SportsTriggerScheduler._strip_python_module_prefix(cmd)
    assert args == ["--operation", "instruments", "--mode", "batch", "--asset-group", "SPORTS"]
    assert "python" not in args
    assert "-m" not in args
    assert "instruments_service" not in args


def test_strip_python_module_prefix_short_command_returned_as_is() -> None:
    """Fewer than 4 tokens (malformed/edge input) — nothing to strip, return unchanged."""
    assert SportsTriggerScheduler._strip_python_module_prefix("python -m mod") == ["python", "-m", "mod"]


def test_cloud_backend_dispatch_strips_python_module_prefix_before_deploy_shard(
    scheduler: SportsTriggerScheduler,
) -> None:
    """`_dispatch_services`'s cloud branch must pass stripped args to `deploy_shard`.

    Configures a real `cloud_run_job_name` (unlike the existing
    `test_cloud_backend_routes_periodic_dispatch_through_cloud_branch`, which
    intentionally has none and only asserts the warning path is reached) and
    a fake `CloudRunBackend` so the actual `deploy_shard` call — the one that
    would hit the real Cloud Run Jobs API in production — can be inspected.
    """
    import copy
    from unittest.mock import MagicMock

    scheduler._backend = "cloud"
    scheduler._cloud_run_config = {
        "project_id": "test-project",
        "region": "asia-northeast1",
        "service_account_email": "unified-trading-sa@test-project.iam.gserviceaccount.com",
    }
    # Deep-copy — `_DISCOVERY_CONFIG` is a module-level dict shared across
    # every test using the `scheduler` fixture; mutating it in place would
    # leak `cloud_run_job_name` into unrelated tests.
    scheduler._config = copy.deepcopy(scheduler._config)
    scheduler._config["reference"]["services"][0]["cloud_run_job_name"] = "uts-prod-instruments-service-t1-recon"

    fake_backend = MagicMock()
    scheduler._get_cloud_run_backend = lambda job_name: fake_backend  # type: ignore[method-assign]

    fired = scheduler._check_reference()

    assert fired == 1  # INJURIES has run_always: true
    fake_backend.deploy_shard.assert_called_once()
    call_kwargs = fake_backend.deploy_shard.call_args.kwargs
    args = call_kwargs["args"]
    assert "python" not in args, f"cloud dispatch args must not re-include the interpreter: {args}"
    assert "-m" not in args, f"cloud dispatch args must not re-include -m: {args}"
    assert "instruments_service" not in args, f"cloud dispatch args must not re-include the module: {args}"
    assert "--sports-entity" in args
    assert "INJURIES" in args
    assert "--operation" in args and "instruments" in args
