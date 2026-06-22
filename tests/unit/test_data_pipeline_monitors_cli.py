"""Unit tests for the data-pipeline fleet monitors CLI + remaining branches.

Exercises the cli.py pure helpers + the ``main`` dispatch (in --dry-run with
mocked storage/compute clients) and the residual branches in the sweep/meta/
escalation modules — credential-free + block-network safe.
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import pytest

from deployment_service.data_pipeline_monitors import (
    _gcs,
    cli,
    escalation,
    exit_code_fleet_monitor,
    heartbeat_stall_watcher,
    meta_watchers,
)
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
    rc = cli.main(["--mode", "meta", "--dry-run"])
    assert rc == 0


def test_catalogue_targets_built(monkeypatch):
    monkeypatch.setattr(cli, "resolve_bucket_name", lambda **_kw: "instruments-store-x")
    targets = cli._catalogue_targets()
    assert len(targets) == len(cli.ASSET_GROUPS)
    assert all(t.blob_path == "_catalogue/instrument_catalogue.parquet" for t in targets)


def test_catalogue_targets_skips_on_resolve_error(monkeypatch):
    def _raise(**_kw):
        raise RuntimeError("no bucket")

    monkeypatch.setattr(cli, "resolve_bucket_name", _raise)
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
