"""Unit tests for the data-pipeline fleet monitors CLI + remaining branches.

Exercises the cli.py pure helpers + the ``main`` dispatch (in --dry-run with
mocked storage/compute clients) and the residual branches in the sweep/meta/
escalation modules — credential-free + block-network safe.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest
from unified_trading_library import (
    ACTIVE_PREFIX,
    ARCHIVE_PREFIX,
    DEFAULT_BUCKET,
    DeploymentRegistryEntry,
    DeploymentsRegistry,
    InMemoryStorageClient,
)

from deployment_service.data_pipeline_monitors import (
    _gcs,
    cli,
    escalation,
    exit_code_fleet_monitor,
    heartbeat_stall_watcher,
    meta_targets,
    meta_watchers,
)
from deployment_service.data_pipeline_monitors.launcher_registry import LAUNCHER_FOR_VM_PREFIX
from tests.unit.test_data_pipeline_monitors import LOG_BUCKET, FakeStorage


# ── cli pure helpers ─────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("vm", "expected"),
    [
        ("mtds-backfill-defi-2025", "defi"),
        ("cefi-mr-binance-2025", "cefi"),
        ("tradfi-bf-es-2025", "tradfi"),
        ("sports-full-sweep-2025", "sports"),
        ("prediction-live-1", "prediction"),
        ("vm-zombie-watchdog-2025", "unknown"),
    ],
)
def test_asset_group_for_vm(vm, expected):
    assert cli._asset_group_for_vm(vm) == expected


@pytest.mark.parametrize(
    ("vm", "is_data"),
    [
        ("tm-backfill-20260622-211407", True),
        ("fs-backfill-20260622", True),
        ("mtds-live-cefi-okx-trades-2026", True),
        ("tradfi-bf-cme-ohlcv-1m-es-2025", True),
        ("prediction-live-kalshi-trades", True),
        # af-backfill-*/af-audit-* (API-Football launcher naming) carry no
        # asset_group substring in their name and were missing from
        # _DATA_VM_PREFIXES entirely — silently invisible to the exit-code
        # sweep's PREEMPTED/exit_nonzero classification, so RelaunchPreemptedVm
        # never fired on preemption (af_backfill_preemption_auto_recovery_not_firing_2026_08_04.md).
        ("af-backfill-20260804-002608", True),
        ("af-audit-20260804-000001", True),
        # infra VMs are NOT data VMs — must be skipped so they never false-alert.
        ("vm-zombie-watchdog-20260528-212634", False),
        ("agent-orchestrator-vm-1", False),
    ],
)
def test_is_data_vm_filters_infra(vm, is_data):
    assert cli._is_data_vm(vm) is is_data


def test_data_vm_prefixes_cover_every_relaunchable_launcher() -> None:
    """Every genuinely auto-relaunchable prefix must be visible to ``_is_data_vm``.

    Guards the whole bug class af_backfill_preemption_auto_recovery_not_firing_2026_08_04.md
    found (not just the two prefixes it happened to catch): a VM prefix with a real
    (non-None) ``launcher_registry.LAUNCHER_FOR_VM_PREFIX`` entry IS a genuinely
    relaunchable data-pipeline VM, so ``cli._is_data_vm`` must return True for it — a
    False here means ``exit_code_fleet_monitor.sweep()`` would never even consider that
    VM for PREEMPTED classification, so ``RelaunchPreemptedVm`` could never fire on
    preemption, regardless of budget/wiring/cadence being otherwise correct. Checking
    the bare prefix (not a fabricated example name) is deliberate — a runtime VM name
    can accidentally satisfy the ASSET_GROUPS substring check for SOME of a launcher's
    invocations (e.g. feat-orph- embeds ASSET_GROUP_ABBREV literally for cefi/defi/
    sports) while missing it for others (tradfi->tfi, prediction->pred, no
    --asset-group->gl) — the prefix itself is the only invocation-independent check.
    A None launcher is excluded: those are documented non-relaunch targets (one-off
    audits, reserved/read-only/live-service prefixes) that exit_code_fleet_monitor
    is not expected to auto-recover.
    """
    missing = sorted(
        prefix
        for prefix, launcher in LAUNCHER_FOR_VM_PREFIX.items()
        if launcher is not None and not cli._is_data_vm(prefix)
    )
    assert not missing, (
        "LAUNCHER_FOR_VM_PREFIX prefixes with a real launcher but invisible to "
        "cli._is_data_vm() — add each to vm_classification.DATA_VM_PREFIXES (or an "
        "ASSET_GROUPS substring already covers it, in which case this list is stale):\n" + "\n".join(missing)
    )


def test_minutes_since_missing_is_zero():
    assert cli._minutes_since(None) == 0.0
    assert cli._minutes_since("not-a-date") == 0.0


def test_minutes_since_past_timestamp():
    from datetime import UTC, datetime, timedelta

    ts = (datetime.now(UTC) - timedelta(minutes=30)).isoformat()
    assert 29 <= cli._minutes_since(ts) <= 31


def test_utcnow_iso_is_parseable():
    from datetime import datetime

    assert datetime.fromisoformat(cli._utcnow_iso()) is not None


def test_first_seen_roundtrip():
    storage = FakeStorage({})
    assert cli._load_first_seen(storage, LOG_BUCKET) == {}
    cli._write_first_seen(storage, LOG_BUCKET, {"vm-a": "2026-06-22T00:00:00+00:00"})
    assert cli._load_first_seen(storage, LOG_BUCKET) == {"vm-a": "2026-06-22T00:00:00+00:00"}


def test_first_seen_ignores_non_string_values():
    storage = FakeStorage({(LOG_BUCKET, cli._FIRST_SEEN_BLOB): (json.dumps({"vm-a": "x", "vm-b": 5}).encode(), 0.0)})
    assert cli._load_first_seen(storage, LOG_BUCKET) == {"vm-a": "x"}


def test_first_seen_handles_corrupt_json():
    storage = FakeStorage({(LOG_BUCKET, cli._FIRST_SEEN_BLOB): (b"{not json", 0.0)})
    assert cli._load_first_seen(storage, LOG_BUCKET) == {}


def test_make_captured_reader_unknown_ag_returns_zero():
    storage = FakeStorage({})
    reader = cli._make_captured_reader(storage)
    # unknown AG → no shard bucket → 0
    assert reader("vm-zombie-watchdog-2025") == 0


def test_shard_bucket_for_vm_unknown_is_none():
    assert cli._shard_bucket_for_vm("vm-zombie-watchdog-2025") is None


# ── _make_shard_backed_ag_fn ──────────────────────────────────────────────────
def test_shard_backed_ag_fn_known_name_no_io():
    """VM with an AG segment in its name resolves via name-match — zero GCS calls."""
    storage = FakeStorage({})
    fn = cli._make_shard_backed_ag_fn(storage)
    assert fn("sports-full-sweep-2025") == "sports"
    assert fn("cefi-mr-binance-2025") == "cefi"
    # blob_exists was never called (FakeStorage has no uploaded entries; if it were
    # probed it would still return the name-match result — we just confirm no
    # "unknown" leaks through for named VMs)


def test_shard_backed_ag_fn_probes_gcs_when_name_unknown(monkeypatch):
    """Generic instruments VM with no AG in name → GCS shard probe returns the AG."""
    vm = "instruments-backfill-20260622-211407"
    shard_path = cli._PER_VM_SHARD.format(vm=vm)
    sports_bucket = "market-data-tick-sports-prd-x"

    def _resolve(**kw):
        if kw.get("asset_group") == "sports":
            return sports_bucket
        raise RuntimeError("no bucket for other AGs in this test")

    monkeypatch.setattr(cli, "resolve_bucket_name", _resolve)
    storage = FakeStorage({(sports_bucket, shard_path): (b"x", 5.0)})
    fn = cli._make_shard_backed_ag_fn(storage)
    assert fn(vm) == "sports"


def test_shard_backed_ag_fn_multi_when_multiple_shards(monkeypatch):
    """Shard found in two AG buckets → returns 'multi' (shouldn't happen but safe)."""
    vm = "instruments-backfill-20260622-000001"
    shard_path = cli._PER_VM_SHARD.format(vm=vm)
    cefi_bucket = "market-data-tick-cefi-prd-x"
    defi_bucket = "market-data-tick-defi-prd-x"

    def _resolve(**kw):
        ag = kw.get("asset_group")
        if ag == "cefi":
            return cefi_bucket
        if ag == "defi":
            return defi_bucket
        raise RuntimeError("no bucket")

    monkeypatch.setattr(cli, "resolve_bucket_name", _resolve)
    storage = FakeStorage(
        {
            (cefi_bucket, shard_path): (b"x", 5.0),
            (defi_bucket, shard_path): (b"x", 5.0),
        }
    )
    fn = cli._make_shard_backed_ag_fn(storage)
    assert fn(vm) == "multi"


def test_shard_backed_ag_fn_unknown_when_no_shard(monkeypatch):
    """Generic VM with no shard in any AG bucket → 'unknown'."""
    vm = "instruments-backfill-20260622-999999"

    def _resolve(**_kw):
        raise RuntimeError("no bucket")

    monkeypatch.setattr(cli, "resolve_bucket_name", _resolve)
    storage = FakeStorage({})
    fn = cli._make_shard_backed_ag_fn(storage)
    assert fn(vm) == "unknown"


# ── cli.main dispatch (dry-run, mocked clients) ──────────────────────────────
@contextmanager
def _noop_lifecycle(service_name: str):  # noqa: ARG001
    yield


def _stub_main_cloud(monkeypatch) -> None:
    """Stub PubSub / lifecycle wiring so main() tests stay credential-free."""
    monkeypatch.setattr(cli, "setup_events", lambda **_kw: None)
    monkeypatch.setattr(cli, "run_lifecycle", _noop_lifecycle)


def test_main_exit_code_mode_dry_run(monkeypatch):
    storage = FakeStorage({})
    _stub_main_cloud(monkeypatch)
    monkeypatch.setattr(cli, "get_storage_client", lambda: storage)
    monkeypatch.setattr(cli, "_list_running_vms", lambda: [("cefi-mr-2025", "asia-northeast1-c")])
    monkeypatch.setattr(cli, "resolve_bucket_name", lambda **_kw: "market-data-tick-cefi-prd-x")
    rc = cli.main(["--mode", "exit-code", "--dry-run"])
    assert rc == 0


def _make_registry_entry(**overrides: object) -> DeploymentRegistryEntry:
    base: dict[str, object] = {
        "deployment_id": "dep-gone-vm-1",
        "vm_name": "cefi-migration-gone-20260701-000000",
        "asset_group": "CEFI",
        "task": "canonical-migration",
        "mode": "dry",
        "start_date": "2026-07-01",
        "end_date": "2026-07-01",
        "status": "running",
        "started_at": "2026-07-01T00:00:00Z",
        "last_heartbeat_at": "2026-07-01T00:00:00Z",
        "completed_at": None,
        "exit_code": None,
        "rows_in": 0,
        "rows_out": 0,
        "rows_error": 0,
        "events_emitted": 1,
        "log_uri": "gs://bucket/vm-logs/x/run.log",
    }
    base.update(overrides)
    return DeploymentRegistryEntry(**base)  # type: ignore[arg-type]


def test_main_exit_code_mode_reaps_gone_vm_registry_entry(monkeypatch):
    """cefi_satellite_ao_dispatch_batch1-017: DeploymentsRegistry.reap_stale() is wired
    into cli.py's exit-code sweep (previously implemented + unit-tested but never
    called) — a deployments/active/*.json entry whose VM is confirmed gone (not in the
    running-VM census, heartbeat older than the 5-min race-tolerance grace) gets
    archived after one real (non-dry-run) --mode exit-code run."""
    storage = FakeStorage({})
    _stub_main_cloud(monkeypatch)
    monkeypatch.setattr(cli, "get_storage_client", lambda: storage)
    # The running census does NOT include the registry entry's VM — it's confirmed gone.
    monkeypatch.setattr(cli, "_list_running_vms", lambda: [("cefi-mr-2025", "asia-northeast1-c")])
    monkeypatch.setattr(cli, "resolve_bucket_name", lambda **_kw: "market-data-tick-cefi-prd-x")

    reg_storage = InMemoryStorageClient()
    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET, storage=reg_storage)
    old_hb = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    registry.register(_make_registry_entry(last_heartbeat_at=old_hb))
    monkeypatch.setattr(cli, "DeploymentsRegistry", lambda *_a, **_kw: registry)

    rc = cli.main(["--mode", "exit-code"])
    assert rc == 0

    archive_keys = reg_storage.list_keys(DEFAULT_BUCKET, ARCHIVE_PREFIX)
    assert len(archive_keys) == 1
    stored = json.loads(reg_storage.download_string(DEFAULT_BUCKET, archive_keys[0]))
    assert stored["status"] == "failed"
    assert stored["extras"]["reap_reason"] == "vm_not_running"


def test_main_exit_code_mode_census_unavailable_no_false_vm_not_running_reap(monkeypatch):
    """cefi_satellite_ao_dispatch_batch4-003 / Finding 3: a transient GCE list-API
    failure/timeout made _list_running_vms() collapse to an empty list which the sweep
    passed as running_vm_names={} — _reap_reason then classified EVERY stale-heartbeat
    active entry as vm_not_running, falsely reaping a genuinely-RUNNING VM at
    2026-07-31T05:46:53Z. Fix: _list_running_vms() returns None on failure and the
    sweep passes running_vm_names=None, so the reaper falls back to heartbeat-age-only
    (a 1h-stale heartbeat < the 6h max_age must NOT be reaped while the census is
    unavailable)."""
    storage = FakeStorage({})
    _stub_main_cloud(monkeypatch)
    monkeypatch.setattr(cli, "get_storage_client", lambda: storage)
    # Census UNAVAILABLE (the API failed / timed out this tick) — not an empty census.
    monkeypatch.setattr(cli, "_list_running_vms", lambda: None)
    monkeypatch.setattr(cli, "resolve_bucket_name", lambda **_kw: "market-data-tick-cefi-prd-x")

    reg_storage = InMemoryStorageClient()
    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET, storage=reg_storage)
    old_hb = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    registry.register(_make_registry_entry(last_heartbeat_at=old_hb))
    monkeypatch.setattr(cli, "DeploymentsRegistry", lambda *_a, **_kw: registry)

    rc = cli.main(["--mode", "exit-code"])
    assert rc == 0

    # 1h-stale heartbeat < 6h max_age + census unavailable → NOT reaped (no false
    # vm_not_running on a still-RUNNING VM).
    active_keys = reg_storage.list_keys(DEFAULT_BUCKET, ACTIVE_PREFIX)
    assert len(active_keys) == 1


def _preempted_result(name: str) -> exit_code_fleet_monitor.TerminationResult:
    return exit_code_fleet_monitor.TerminationResult(
        vm_name=name,
        verdict=exit_code_fleet_monitor.TerminationVerdict.PREEMPTED,
        exit_code=None,
        captured_before=0,
        captured_after=0,
        preempted=True,
    )


def test_main_exit_code_mode_storm_resweeps_then_stops(monkeypatch):
    """asia_northeast1_c_spot_preemption_storm_2026_08_04.md todo 3: sweep()'s
    prior-vs-running census diff can never see a VM whose entire lifetime fits
    inside one tick. cli.main's exit-code mode now re-sweeps at a short interval
    while a pass observes >= _STORM_PREEMPTION_THRESHOLD PREEMPTED verdicts
    (storm evidence), bounded by _STORM_MAX_RESWEEPS total passes."""
    storage = FakeStorage({})
    _stub_main_cloud(monkeypatch)
    monkeypatch.setattr(cli, "get_storage_client", lambda: storage)
    monkeypatch.setattr(cli, "_list_running_vms", lambda: [])
    monkeypatch.setattr(cli, "resolve_bucket_name", lambda **_kw: "market-data-tick-cefi-prd-x")

    reg_storage = InMemoryStorageClient()
    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET, storage=reg_storage)
    monkeypatch.setattr(cli, "DeploymentsRegistry", lambda *_a, **_kw: registry)

    call_count = {"n": 0}

    def _fake_sweep(**_kwargs):
        call_count["n"] += 1
        # Every pass looks like an active storm (>= threshold PREEMPTED) — the
        # loop must still stop at _STORM_MAX_RESWEEPS, never run unbounded.
        return [
            _preempted_result(f"af-backfill-{call_count['n']}-a"),
            _preempted_result(f"af-backfill-{call_count['n']}-b"),
        ]

    monkeypatch.setattr(exit_code_fleet_monitor, "sweep", _fake_sweep)
    sleeps: list[float] = []
    monkeypatch.setattr(cli.time, "sleep", lambda s: sleeps.append(s))

    rc = cli.main(["--mode", "exit-code"])
    assert rc == 0
    assert call_count["n"] == cli._STORM_MAX_RESWEEPS
    assert sleeps == [cli._STORM_RESWEEP_INTERVAL_SECS] * (cli._STORM_MAX_RESWEEPS - 1)


def test_main_exit_code_mode_no_storm_sweeps_once(monkeypatch):
    """Below-threshold PREEMPTED count (the common, non-storm case) must sweep
    exactly once — no behavior change from before this fix."""
    storage = FakeStorage({})
    _stub_main_cloud(monkeypatch)
    monkeypatch.setattr(cli, "get_storage_client", lambda: storage)
    monkeypatch.setattr(cli, "_list_running_vms", lambda: [])
    monkeypatch.setattr(cli, "resolve_bucket_name", lambda **_kw: "market-data-tick-cefi-prd-x")

    reg_storage = InMemoryStorageClient()
    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET, storage=reg_storage)
    monkeypatch.setattr(cli, "DeploymentsRegistry", lambda *_a, **_kw: registry)

    call_count = {"n": 0}

    def _fake_sweep(**_kwargs):
        call_count["n"] += 1
        return [_preempted_result("af-backfill-only-one")]

    monkeypatch.setattr(exit_code_fleet_monitor, "sweep", _fake_sweep)
    sleeps: list[float] = []
    monkeypatch.setattr(cli.time, "sleep", lambda s: sleeps.append(s))

    rc = cli.main(["--mode", "exit-code"])
    assert rc == 0
    assert call_count["n"] == 1
    assert sleeps == []


def test_main_exit_code_mode_dry_run_never_resweeps(monkeypatch):
    """dry_run must stay single-pass even amid apparent storm evidence — matches
    sweep()'s own 'classify + return, emit/persist nothing' dry_run contract."""
    storage = FakeStorage({})
    _stub_main_cloud(monkeypatch)
    monkeypatch.setattr(cli, "get_storage_client", lambda: storage)
    monkeypatch.setattr(cli, "_list_running_vms", lambda: [])
    monkeypatch.setattr(cli, "resolve_bucket_name", lambda **_kw: "market-data-tick-cefi-prd-x")

    call_count = {"n": 0}

    def _fake_sweep(**_kwargs):
        call_count["n"] += 1
        return [_preempted_result("af-backfill-x"), _preempted_result("af-backfill-y")]

    monkeypatch.setattr(exit_code_fleet_monitor, "sweep", _fake_sweep)
    sleeps: list[float] = []
    monkeypatch.setattr(cli.time, "sleep", lambda s: sleeps.append(s))

    rc = cli.main(["--mode", "exit-code", "--dry-run"])
    assert rc == 0
    assert call_count["n"] == 1
    assert sleeps == []


def test_main_heartbeat_mode_dry_run(monkeypatch):
    storage = FakeStorage({})
    _stub_main_cloud(monkeypatch)
    monkeypatch.setattr(cli, "get_storage_client", lambda: storage)
    monkeypatch.setattr(cli, "_list_running_vms", lambda: [("mtds-live-defi-2025", "asia-northeast1-c")])
    monkeypatch.setattr(cli, "resolve_bucket_name", lambda **_kw: "market-data-tick-defi-prd-x")
    rc = cli.main(["--mode", "heartbeat", "--dry-run"])
    assert rc == 0


def test_main_meta_mode_dry_run(monkeypatch):
    storage = FakeStorage({})
    _stub_main_cloud(monkeypatch)
    monkeypatch.setattr(cli, "get_storage_client", lambda: storage)
    monkeypatch.setattr(cli, "resolve_bucket_name", lambda **_kw: "bucket-x")
    # KEY #2/DP-WATCHER-003 (2026-07-15, google-cloud-scheduler landed as a real
    # dependency): stub the scheduler-client factories so this dry-run test stays
    # credential-free — a REAL CloudSchedulerClient() otherwise tries a live RPC
    # that pytest-socket blocks, and the GAPIC client's default retry policy stalls
    # past the pytest-timeout window instead of failing fast.
    monkeypatch.setattr(cli, "_make_scheduler_state_reader", lambda: lambda _job: None)
    monkeypatch.setattr(
        cli.consolidator_scheduler_watcher,
        "make_consolidator_scheduler_lister",
        lambda project_id, scheduler_rpc_timeout_secs=10.0: lambda: [],
    )
    # KEY #4 execution-history reader (google-cloud-run, a pre-existing real
    # dependency) has the identical real-client shape and was never stubbed here
    # either — stub it too so this test doesn't attempt a live Cloud Run RPC.
    monkeypatch.setattr(cli, "_make_execution_history_reader", lambda: lambda _job: None)
    # DP-WATCHER-005 consolidator OOM reader (google-cloud-run, same shape as
    # execution-history reader) — stub so this test doesn't attempt live RPCs.
    monkeypatch.setattr(
        cli.consolidator_oom_watcher,
        "make_consolidator_execution_oom_reader",
        lambda project_id, env_prefix="", location="asia-northeast1": lambda _ags: {},
    )
    # DP-VM-013 generic Cloud Run Job reader — stub so this test doesn't attempt live RPCs.
    monkeypatch.setattr(
        cli.cloud_run_job_failure_watcher,
        "make_cloud_run_job_execution_reader",
        lambda project_id, env_prefix="", location="asia-northeast1": lambda _job: None,
    )
    rc = cli.main(["--mode", "meta", "--dry-run"])
    assert rc == 0


def test_catalogue_targets_built(monkeypatch):
    # KEY #3 (2026-06-23): the catalogue artifact is {DEPLOYMENT_ENV}/catalog.parquet
    # in the env-SHORT instruments-store bucket — NOT _catalogue/instrument_catalogue.parquet
    # (the old wrong path made the probe read age=None → false "missing").
    monkeypatch.setattr(meta_targets, "get_environment", lambda: "prod")
    monkeypatch.setattr(meta_targets, "resolve_bucket_name", lambda **_kw: "instruments-store-x")
    targets = cli._catalogue_targets()
    assert len(targets) == len(cli.ASSET_GROUPS)
    assert all(t.blob_path == "prod/catalog.parquet" for t in targets)


def test_catalogue_targets_skips_on_resolve_error(monkeypatch):
    def _raise(**_kw):
        raise RuntimeError("no bucket")

    monkeypatch.setattr(meta_targets, "resolve_bucket_name", _raise)
    assert cli._catalogue_targets() == []


# ── residual branch coverage ─────────────────────────────────────────────────
def test_gcs_blob_age_missing_metadata_is_none():
    storage = FakeStorage({})
    assert _gcs.blob_age_minutes(storage, LOG_BUCKET, "no/such/blob") is None


def test_gcs_read_text_missing_is_none():
    storage = FakeStorage({})
    assert _gcs.read_text(storage, LOG_BUCKET, "no/such/blob") is None


def test_gcs_run_log_shows_stall_true():
    vm = "sports-x"
    storage = FakeStorage(
        {(LOG_BUCKET, _gcs.RUN_LOG_BLOB.format(vm=vm)): (b"reason=WORKER_STALLED stalled_for=2000\n", 0.0)}
    )
    assert _gcs.run_log_shows_stall(storage, LOG_BUCKET, vm) is True


def test_gcs_run_log_shows_stall_false_when_clean():
    vm = "sports-y"
    storage = FakeStorage({(LOG_BUCKET, _gcs.RUN_LOG_BLOB.format(vm=vm)): (b"all good rc=0\n", 0.0)})
    assert _gcs.run_log_shows_stall(storage, LOG_BUCKET, vm) is False


def test_exit_code_invalid_status_blob_falls_back_to_log():
    vm = "vm-z"
    storage = FakeStorage(
        {
            (LOG_BUCKET, _gcs.EXIT_STATUS_BLOB.format(vm=vm)): (b"garbage\n", 0.0),
            (LOG_BUCKET, _gcs.RUN_LOG_BLOB.format(vm=vm)): (b"command exited rc=2\n", 0.0),
        }
    )
    assert _gcs.read_terminal_exit_code(storage, LOG_BUCKET, vm) == 2


def test_load_census_corrupt_is_empty():
    storage = FakeStorage({(LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB): (b"{bad", 0.0)})
    assert exit_code_fleet_monitor.load_census(storage, LOG_BUCKET) == {}


def test_load_census_non_dict_vms_is_empty():
    storage = FakeStorage(
        {(LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB): (json.dumps({"vms": [1, 2]}).encode(), 0.0)}
    )
    assert exit_code_fleet_monitor.load_census(storage, LOG_BUCKET) == {}


def test_write_census_persists():
    storage = FakeStorage({})
    exit_code_fleet_monitor.write_census(storage, LOG_BUCKET, {"vm-a": 5})
    assert (LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB) in storage.uploaded


def test_heartbeat_sweep_too_young_skips(monkeypatch):
    vm = "mtds-live-defi-new"
    storage = FakeStorage({})  # no heartbeat blob
    emitted: list = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    results = heartbeat_stall_watcher.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[(vm, "z")],
        vm_age_reader=lambda _n, _z: 1.0,  # younger than grace
        captured_reader=lambda _vm: 0,
        asset_group_for_vm=lambda _vm: "defi",
    )
    assert results[0].verdict is heartbeat_stall_watcher.LivenessVerdict.TOO_YOUNG
    assert emitted == []


def test_heartbeat_sweep_event_loop_starved_emits(monkeypatch):
    vm = "mtds-live-defi-old"
    storage = FakeStorage({})  # no heartbeat blob, VM is old
    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    results = heartbeat_stall_watcher.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[(vm, "z")],
        vm_age_reader=lambda _n, _z: 120.0,
        captured_reader=lambda _vm: 0,
        # captured FLAT (0 == prior 0) — load-bearing: a no-instrumentation VM whose
        # captured count CLIMBS is alive (operator 2026-06-24), so a genuine starve
        # needs total silence INCLUDING no capture progress.
        prior_captured={vm: 0},
        asset_group_for_vm=lambda _vm: "defi",
    )
    assert results[0].verdict is heartbeat_stall_watcher.LivenessVerdict.EVENT_LOOP_STARVED
    assert any(e[0] == "DP_EVENT_LOOP_STARVED" for e in emitted)


def test_meta_cron_did_not_fire_emits(monkeypatch):
    bucket = "market-data-tick-cefi-prd-x"
    storage = FakeStorage({})  # missing index → cron did not fire
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    target = meta_watchers.FreshnessTarget(
        bucket=bucket,
        blob_path="_index/availability_index.parquet",
        max_age_min=180.0,
        label="manifest-consolidator-cefi",
    )
    results = meta_watchers.check_cron_fired(storage_client=storage, targets=[target])
    assert results[0].stale is True
    assert "DP_CRON_DID_NOT_FIRE" in emitted


def test_meta_catalogue_fresh_no_alert(monkeypatch):
    bucket = "instruments-store-defi-prd-x"
    storage = FakeStorage({(bucket, "_catalogue/instrument_catalogue.parquet"): (b"x", 60.0)})
    emitted: list = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    target = meta_watchers.FreshnessTarget(
        bucket=bucket,
        blob_path="_catalogue/instrument_catalogue.parquet",
        max_age_min=meta_watchers.DEFAULT_CATALOGUE_MAX_AGE_MIN,
        label="defi",
    )
    results = meta_watchers.check_catalogue_freshness(storage_client=storage, targets=[target])
    assert results[0].stale is False
    assert emitted == []


# ── _list_running_vms timeout (DP-WATCHER-002 root-cause fix) ────────────────


def test_list_running_vms_returns_none_on_timeout(monkeypatch):
    """_list_running_vms must return None within _LIST_VMS_TIMEOUT_SECS even when
    aggregated_list_instances blocks indefinitely — the sentinel write must still
    fire so the Cloud Run Job finishes SUCCEEDED and no DP_CRON_DID_NOT_FIRE alert
    is raised. ``None`` (not []) signals "census unavailable" so the reap_stale()
    wiring passes running_vm_names=None instead of an over-reaping empty set
    (cefi_content_apply_memory_freeze_recurs_post_fix_and_registry_false_reap_2026_07_31.md
    Finding 3)."""
    import threading

    hang_event = threading.Event()

    class _HangingClient:
        def aggregated_list_instances(self, project_id, filter_str):  # noqa: ARG002
            hang_event.wait()  # blocks until the test releases it
            return []

    monkeypatch.setattr(cli, "get_compute_engine_client", lambda **_kw: _HangingClient())
    monkeypatch.setattr(cli, "_project_id", lambda: "test-project")
    # Patch the timeout to a tiny value so the test is fast.
    monkeypatch.setattr(cli, "_LIST_VMS_TIMEOUT_SECS", 0.1)
    try:
        result = cli._list_running_vms()
    finally:
        hang_event.set()  # release the blocked thread so it doesn't leak

    assert result is None


def test_list_running_vms_returns_results_normally(monkeypatch):
    """_list_running_vms returns real results when the API responds promptly."""

    class _FastClient:
        def aggregated_list_instances(self, project_id, filter_str):  # noqa: ARG002
            return [
                {"name": "mtds-live-cefi-2025", "status": "RUNNING", "zone": "asia-northeast1-c"},
                {"name": "mtds-stopped-1", "status": "TERMINATED", "zone": "asia-northeast1-c"},
            ]

    monkeypatch.setattr(cli, "get_compute_engine_client", lambda **_kw: _FastClient())
    monkeypatch.setattr(cli, "_project_id", lambda: "test-project")
    result = cli._list_running_vms()
    assert result == [("mtds-live-cefi-2025", "asia-northeast1-c")]


def test_escalation_dry_run_emits_nothing(monkeypatch):
    emitted: list = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    finding = escalation.PipelineFinding(
        event="DP_VM_STALL",
        severity="WARN",
        tier=escalation.EscalationTier.AUTO_RECOVER,
        summary="x",
    )
    result = escalation.route_finding(finding, dry_run=True)
    assert result["emitted"] is False
    assert emitted == []
