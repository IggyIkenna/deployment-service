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


def test_cloud_backend_routes_periodic_dispatch_via_dispatch_cloud(
    scheduler: SportsTriggerScheduler,
) -> None:
    """Switching to cloud backend routes periodic dispatch through _dispatch_cloud."""
    scheduler._backend = "cloud"

    calls: list[dict[str, object]] = []

    def fake_dispatch_cloud(cmd: str, service: str, trigger_name: str, dispatch_id: str) -> bool:
        calls.append({"cmd": cmd, "service": service, "trigger_name": trigger_name})
        return True

    with patch.object(scheduler, "_dispatch_cloud", side_effect=fake_dispatch_cloud):
        fired = scheduler._check_discovery()

    assert fired > 0, "cloud backend must dispatch periodic tiers"
    assert calls, "_dispatch_cloud must be called for each service"
    assert all(c["service"] == "instruments-service" for c in calls)
