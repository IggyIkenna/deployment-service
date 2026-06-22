"""Unit tests for the data-pipeline fleet monitors (Wave 4a).

Credential-free + block-network safe: GCS/compute I/O is injected via a fake
``StorageClient`` + stub readers. Covers:
  - _gcs.read_terminal_exit_code (EXIT_STATUS blob + run.log rc= fallback)
  - exit_code_fleet_monitor: 137 run.log → DP_VM_EXIT_NONZERO;
    flat-captured exit-0 → DP_VM_GONE_NO_CAPTURE; clean exit-0 + climb → CLEAN
  - heartbeat_stall_watcher: stale heartbeat → DP_VM_STALL; no heartbeat →
    DP_EVENT_LOOP_STARVED; fresh + climbing → ALIVE; too-young → skip
  - escalation: each tier routes (auto_recover / file_issue writes PM-clone doc /
    page_operator) + always emits the DP_* event
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from deployment_service.data_pipeline_monitors import (
    _gcs,
    escalation,
    exit_code_fleet_monitor,
    heartbeat_stall_watcher,
    meta_watchers,
)
from deployment_service.data_pipeline_monitors.escalation import (
    EscalationTier,
    PipelineFinding,
)


# ── fakes ────────────────────────────────────────────────────────────────────
class _FakeBlobMeta:
    def __init__(self, last_modified: str | None):
        self.last_modified = last_modified


class FakeStorage:
    """Minimal in-memory StorageClient stand-in.

    blobs: {(bucket, path): (bytes, age_minutes)}  — age None ⇒ no metadata.
    """

    def __init__(self, blobs: dict[tuple[str, str], tuple[bytes, float | None]] | None = None):
        self.blobs = blobs or {}
        self.uploaded: dict[tuple[str, str], bytes] = {}

    def blob_exists(self, bucket: str, path: str) -> bool:
        return (bucket, path) in self.blobs

    def download_bytes(self, bucket: str, path: str) -> bytes:
        return self.blobs[(bucket, path)][0]

    def get_blob_metadata(self, bucket: str, path: str):
        entry = self.blobs.get((bucket, path))
        if entry is None:
            return None
        _data, age_min = entry
        if age_min is None:
            return _FakeBlobMeta(None)
        ts = (datetime.now(UTC) - timedelta(minutes=age_min)).isoformat()
        return _FakeBlobMeta(ts)

    def upload_bytes(self, bucket: str, path: str, data: bytes, content_type: str | None = None) -> str:
        self.uploaded[(bucket, path)] = data
        self.blobs[(bucket, path)] = (data, 0.0)
        return f"gs://{bucket}/{path}"


LOG_BUCKET = "deployment-scripts-test-project"


def _exit_status_blob(vm: str) -> str:
    return _gcs.EXIT_STATUS_BLOB.format(vm=vm)


def _run_log_blob(vm: str) -> str:
    return _gcs.RUN_LOG_BLOB.format(vm=vm)


def _heartbeat_blob(vm: str) -> str:
    return _gcs.HEARTBEAT_BLOB.format(vm=vm)


# ── _gcs.read_terminal_exit_code ─────────────────────────────────────────────
def test_exit_code_from_exit_status_blob():
    vm = "sports-full-sweep-2025"
    storage = FakeStorage({(LOG_BUCKET, _exit_status_blob(vm)): (b"137\n", 0.0)})
    assert _gcs.read_terminal_exit_code(storage, LOG_BUCKET, vm) == 137


def test_exit_code_fallback_to_run_log_rc():
    vm = "cefi-mr-2025"
    log = b"[vm-exec] starting\n[vm-exec] command exited rc=0\n[vm-exec] final\n"
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (log, 0.0)})
    assert _gcs.read_terminal_exit_code(storage, LOG_BUCKET, vm) == 0


def test_exit_code_none_when_no_durable_signal():
    storage = FakeStorage({})
    assert _gcs.read_terminal_exit_code(storage, LOG_BUCKET, "gone-vm") is None


# ── exit_code_fleet_monitor classification ───────────────────────────────────
def test_classify_137_is_exit_nonzero():
    res = exit_code_fleet_monitor.classify_terminated_vm("vm", exit_code=137, captured_before=0, captured_after=0)
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.EXIT_NONZERO


def test_classify_clean_when_exit0_and_climb():
    res = exit_code_fleet_monitor.classify_terminated_vm("vm", exit_code=0, captured_before=10, captured_after=200)
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.CLEAN


def test_classify_gone_no_capture_when_exit0_flat():
    res = exit_code_fleet_monitor.classify_terminated_vm("vm", exit_code=0, captured_before=50, captured_after=50)
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.GONE_NO_CAPTURE


def test_classify_unknown_exit_flat_is_failsafe_gone_no_capture():
    # No durable exit code AND no captured climb ⇒ fail-safe to GONE_NO_CAPTURE
    # (never infer success from "the VM is gone").
    res = exit_code_fleet_monitor.classify_terminated_vm("vm", exit_code=None, captured_before=5, captured_after=5)
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.GONE_NO_CAPTURE


# ── exit_code_fleet_monitor.sweep end-to-end ────────────────────────────────
def test_sweep_emits_exit_nonzero_on_137_run_log(monkeypatch):
    vm = "sports-full-sweep-2025"
    # Prior census has the VM (so it counts as terminated this tick).
    census = json.dumps({"vms": {vm: 0}}).encode()
    storage = FakeStorage(
        {
            (LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB): (census, 0.0),
            # run.log records a stall-kill rc=137 OOM
            (LOG_BUCKET, _run_log_blob(vm)): (b"[vm-exec] command exited rc=137\n", 0.0),
        }
    )
    emitted: list[tuple[str, str, dict]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity, details or {})),
    )
    results = exit_code_fleet_monitor.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[],  # VM is gone now
        captured_reader=lambda _vm: 0,
        asset_group_for_vm=lambda _vm: "sports",
    )
    assert len(results) == 1
    assert results[0].verdict is exit_code_fleet_monitor.TerminationVerdict.EXIT_NONZERO
    assert any(e[0] == "DP_VM_EXIT_NONZERO" and e[1] == "CRITICAL" for e in emitted)


def test_sweep_emits_gone_no_capture_on_flat_captured(monkeypatch):
    vm = "mtds-backfill-defi-2025"
    census = json.dumps({"vms": {vm: 100}}).encode()
    storage = FakeStorage(
        {
            (LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB): (census, 0.0),
            (LOG_BUCKET, _exit_status_blob(vm)): (b"0\n", 0.0),  # clean exit
        }
    )
    emitted: list[tuple[str, str, dict]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity, details or {})),
    )
    results = exit_code_fleet_monitor.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[],
        captured_reader=lambda _vm: 100,  # FLAT — no climb
        asset_group_for_vm=lambda _vm: "defi",
    )
    assert results[0].verdict is exit_code_fleet_monitor.TerminationVerdict.GONE_NO_CAPTURE
    assert any(e[0] == "DP_VM_GONE_NO_CAPTURE" and e[1] == "CRITICAL" for e in emitted)


def test_sweep_clean_run_emits_nothing(monkeypatch):
    vm = "cefi-mr-2025"
    census = json.dumps({"vms": {vm: 10}}).encode()
    storage = FakeStorage(
        {
            (LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB): (census, 0.0),
            (LOG_BUCKET, _exit_status_blob(vm)): (b"0\n", 0.0),
        }
    )
    emitted: list = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    results = exit_code_fleet_monitor.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[],
        captured_reader=lambda _vm: 500,  # climbed 10 -> 500
        asset_group_for_vm=lambda _vm: "cefi",
    )
    assert results[0].verdict is exit_code_fleet_monitor.TerminationVerdict.CLEAN
    assert emitted == []


# ── heartbeat_stall_watcher ─────────────────────────────────────────────────
def test_classify_liveness_stall_on_stale_heartbeat():
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "vm", vm_age_min=120, heartbeat_age_min=40, captured_flat=False, stall_minutes=15
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.STALL


def test_classify_liveness_event_loop_starved_when_no_heartbeat():
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "vm", vm_age_min=120, heartbeat_age_min=None, captured_flat=False
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.EVENT_LOOP_STARVED


def test_classify_liveness_alive_when_fresh_and_progressing():
    res = heartbeat_stall_watcher.classify_vm_liveness("vm", vm_age_min=120, heartbeat_age_min=2, captured_flat=False)
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.ALIVE


def test_classify_liveness_too_young_skips():
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "vm", vm_age_min=3, heartbeat_age_min=None, captured_flat=False, grace_minutes=10
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.TOO_YOUNG


def _pipeline_hb_runlog(vm: str, *, marker_age_min: float) -> bytes:
    """A run.log whose freshest PIPELINE_HEARTBEAT marker is ``marker_age_min`` old."""
    ts = (datetime.now(UTC) - timedelta(minutes=marker_age_min)).strftime("%Y-%m-%dT%H:%M:%S")
    return (
        f"2026-06-22T00:00:00 INFO start\n"
        f"PIPELINE_HEARTBEAT vm={vm} ag=defi task=mtds-backfill source=vm-life-emitter ts={ts}\n"
    ).encode()


def test_heartbeat_sweep_emits_stall(monkeypatch):
    # A LIVE VM (no run.log progress signal) whose PIPELINE_HEARTBEAT marker is
    # stale → STALL. The worker-heartbeat marker (run.log), NOT the infra sidecar,
    # is the liveness signal now (BUG2 fix).
    vm = "mtds-live-defi-2025"
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (_pipeline_hb_runlog(vm, marker_age_min=40.0), None)})
    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    results = heartbeat_stall_watcher.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[(vm, "asia-northeast1-c")],
        vm_age_reader=lambda _n, _z: 120.0,
        captured_reader=lambda _vm: 0,
        asset_group_for_vm=lambda _vm: "defi",
        stall_minutes=15,
    )
    assert results[0].verdict is heartbeat_stall_watcher.LivenessVerdict.STALL
    assert any(e[0] == "DP_VM_STALL" and e[1] == "WARN" for e in emitted)


def test_silent_vm_with_fresh_infra_sidecar_still_alerts(monkeypatch):
    """THE BUG2 KEYSTONE regression (operator's 'zero alerts in 1.5h' symptom).

    A running data VM with a FRESH generic infra ``vm-heartbeat`` sidecar blob but
    NO PIPELINE_HEARTBEAT worker marker (the data worker died / never launched /
    its heartbeat timer is broken) MUST alert ``DP_EVENT_LOOP_STARVED`` — the
    watcher must NOT be fooled into ALIVE by the always-fresh infra sidecar.
    """
    vm = "mtds-live-sports-odds-api-trades-2026"
    fresh_epoch = int(datetime.now(UTC).timestamp()) - 30  # infra sidecar wrote 30s ago
    storage = FakeStorage(
        {
            # Fresh INFRA sidecar — the old, wrong liveness signal.
            (LOG_BUCKET, _heartbeat_blob(vm)): (f"{fresh_epoch}\n-1\nstarting".encode(), None),
            # run.log exists but carries NO PIPELINE_HEARTBEAT worker marker.
            (LOG_BUCKET, _run_log_blob(vm)): (b"2026-06-22T00:00:00 INFO boot\n", None),
        }
    )
    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    results = heartbeat_stall_watcher.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[(vm, "asia-northeast1-c")],
        vm_age_reader=lambda _n, _z: 120.0,  # well past grace
        captured_reader=lambda _vm: 0,
        asset_group_for_vm=lambda _vm: "sports",
        stall_minutes=15,
    )
    assert results[0].verdict is heartbeat_stall_watcher.LivenessVerdict.EVENT_LOOP_STARVED
    assert any(e[0] == "DP_EVENT_LOOP_STARVED" and e[1] == "WARN" for e in emitted)


def test_healthy_vm_with_fresh_pipeline_marker_reads_alive(monkeypatch):
    """A VM emitting a FRESH PIPELINE_HEARTBEAT marker reads ALIVE (no false alert)."""
    vm = "mtds-live-sports-odds-api-trades-2026"
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (_pipeline_hb_runlog(vm, marker_age_min=0.5), None)})
    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    results = heartbeat_stall_watcher.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[(vm, "asia-northeast1-c")],
        vm_age_reader=lambda _n, _z: 120.0,
        captured_reader=lambda _vm: 0,
        asset_group_for_vm=lambda _vm: "sports",
        stall_minutes=15,
    )
    assert results[0].verdict is heartbeat_stall_watcher.LivenessVerdict.ALIVE
    assert not emitted


# ── _gcs.pipeline_heartbeat_age_minutes (BUG2: worker marker, not infra sidecar) ─
def test_pipeline_heartbeat_age_from_marker_tail():
    vm = "tm-backfill-2026"
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (_pipeline_hb_runlog(vm, marker_age_min=0.4), None)})
    age = _gcs.pipeline_heartbeat_age_minutes(storage, LOG_BUCKET, vm)
    assert age is not None and 0.0 <= age < 2.0


def test_pipeline_heartbeat_age_none_when_no_marker():
    """run.log present but NO PIPELINE_HEARTBEAT marker ⇒ None (silent worker)."""
    vm = "tm-backfill-2026"
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (b"2026-06-22T00:00:00 INFO only-boot-line\n", None)})
    assert _gcs.pipeline_heartbeat_age_minutes(storage, LOG_BUCKET, vm) is None


def test_pipeline_heartbeat_age_none_when_log_absent():
    assert _gcs.pipeline_heartbeat_age_minutes(FakeStorage({}), LOG_BUCKET, "vm-x") is None


# ── _gcs content-epoch heartbeat age (BUG2: last_modified is bare on the bucket) ─
def test_heartbeat_blob_age_uses_content_epoch_when_metadata_bare():
    """The real bug: get_blob_metadata().last_modified is None for every blob, so
    age MUST come from the heartbeat blob's first-line Unix epoch (the sidecar
    writes `<epoch>\\n<rc>\\n<status>`). A FRESH epoch ⇒ a small (non-None) age."""
    vm = "tm-backfill-2026"
    fresh_epoch = int(datetime.now(UTC).timestamp()) - 30  # 30s ago
    # age_min=None ⇒ FakeStorage returns _FakeBlobMeta(None) (bare metadata).
    storage = FakeStorage({(LOG_BUCKET, _heartbeat_blob(vm)): (f"{fresh_epoch}\n-1\nrunning".encode(), None)})
    age = _gcs.heartbeat_blob_age_minutes(storage, LOG_BUCKET, vm)
    assert age is not None and 0.0 <= age < 2.0  # ~0.5 min, NOT None → no false starve


def test_heartbeat_blob_age_none_when_blob_absent():
    """No heartbeat blob at all ⇒ None (genuine total silence — EVENT_LOOP_STARVED)."""
    assert _gcs.heartbeat_blob_age_minutes(FakeStorage({}), LOG_BUCKET, "vm-x") is None


def test_run_log_age_from_embedded_timestamp_tail():
    """run.log freshness derives from the LAST embedded `YYYY-MM-DD HH:MM:SS` line
    (last_modified is bare), so a frozen log reads as STALE-by-content."""
    vm = "tradfi-bf-cme-2025"
    old = (datetime.now(UTC) - timedelta(minutes=90)).strftime("%Y-%m-%d %H:%M:%S")
    log = f"2026-06-22 10:00:00,000 INFO start\n{old},123 INFO last line\n".encode()
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (log, None)})
    age = _gcs.run_log_age_minutes(storage, LOG_BUCKET, vm)
    assert age is not None and age > 60.0  # ~90 min frozen


# ── classifier: run.log hang signal + corroborated-flat + live-vs-backfill ──────
def test_classify_stall_on_frozen_runlog_even_when_heartbeat_fresh():
    """Hung-process class: heartbeat fresh but run.log frozen past the generous
    threshold ⇒ STALL (the bash heartbeat ticks but the worker made no progress)."""
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "tradfi-bf-vm",
        vm_age_min=120,
        heartbeat_age_min=1.0,
        captured_flat=False,
        run_log_age_min=60.0,
        run_log_stall_minutes=45.0,
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.STALL


def test_classify_alive_when_heartbeat_fresh_and_runlog_recent():
    """Fresh heartbeat + recently-advancing run.log ⇒ ALIVE (no false stall)."""
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "tradfi-bf-vm",
        vm_age_min=120,
        heartbeat_age_min=1.0,
        captured_flat=False,
        run_log_age_min=3.0,
        run_log_stall_minutes=45.0,
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.ALIVE


def test_classify_captured_flat_alone_does_not_stall():
    """A live-capture VM's per-VM shard count legitimately holds flat between
    ticks — captured_flat alone (no corroborating stale log) must NOT stall."""
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "mtds-live-cefi-vm",
        vm_age_min=120,
        heartbeat_age_min=1.0,
        captured_flat=True,
        run_log_age_min=None,  # live VMs pass None (run.log signal not applied)
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.ALIVE


def test_classify_captured_flat_with_stale_log_does_stall():
    """Captured flat AND run.log also stopped advancing ⇒ corroborated stall."""
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "tradfi-bf-vm",
        vm_age_min=120,
        heartbeat_age_min=1.0,
        captured_flat=True,
        run_log_age_min=20.0,
        stall_minutes=10.0,
        run_log_stall_minutes=45.0,
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.STALL


def test_is_backfill_vm_classification():
    assert heartbeat_stall_watcher._is_backfill_vm("tm-backfill-20260622-211407")
    assert heartbeat_stall_watcher._is_backfill_vm("tradfi-bf-cme-ohlcv-1m-rb-2025")
    assert heartbeat_stall_watcher._is_backfill_vm("fs-backfill-20260622")
    # live-capture VMs are NOT backfill (run.log signal must not apply to them).
    assert not heartbeat_stall_watcher._is_backfill_vm("mtds-live-cefi-okx-trades-2026")
    assert not heartbeat_stall_watcher._is_backfill_vm("prediction-live-kalshi-trades")


def test_live_vm_with_quiet_runlog_reads_alive_in_sweep(monkeypatch):
    """End-to-end of the false-positive fix: a LIVE VM with a FRESH PIPELINE_HEARTBEAT
    marker but a long-quiet DATA run.log line (296 min, like the real mtds-live-deribit)
    must read ALIVE. The run.log progress signal is NOT applied to live-capture VMs, and
    the worker-heartbeat marker (emitted by the VM-life emitter) is fresh → ALIVE."""
    vm = "mtds-live-cefi-deribit-trades-2026"
    fresh_marker_ts = (datetime.now(UTC) - timedelta(seconds=45)).strftime("%Y-%m-%dT%H:%M:%S")
    old_log_line = (datetime.now(UTC) - timedelta(minutes=296)).strftime("%Y-%m-%d %H:%M:%S")
    storage = FakeStorage(
        {
            (LOG_BUCKET, _run_log_blob(vm)): (
                (
                    f"{old_log_line},000 INFO connected\n"
                    f"PIPELINE_HEARTBEAT vm={vm} ag=cefi source=vm-life-emitter ts={fresh_marker_ts}\n"
                ).encode(),
                None,
            ),
        }
    )
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    results = heartbeat_stall_watcher.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[(vm, "asia-northeast1-c")],
        vm_age_reader=lambda _n, _z: 300.0,
        captured_reader=lambda _vm: 0,
        asset_group_for_vm=lambda _vm: "cefi",
    )
    assert results[0].verdict is heartbeat_stall_watcher.LivenessVerdict.ALIVE
    assert not emitted  # no false DP_VM_STALL / DP_EVENT_LOOP_STARVED


# ── escalation hop ───────────────────────────────────────────────────────────
def test_route_page_operator_emits_event(monkeypatch):
    emitted: list[tuple[str, str, dict]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity, details or {})),
    )
    finding = PipelineFinding(
        event="DP_VM_EXIT_NONZERO",
        severity="CRITICAL",
        tier=EscalationTier.PAGE_OPERATOR,
        summary="vm crashed",
        details={"vm_name": "x"},
        registry_id="DP-VM-001",
    )
    result = escalation.route_finding(finding)
    assert result["emitted"] is True
    assert result["tier"] == "page_operator"
    assert emitted[0][0] == "DP_VM_EXIT_NONZERO"
    assert emitted[0][2]["escalation_tier"] == "page_operator"


def test_route_auto_recover_records_flag(monkeypatch):
    captured: list[dict] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: captured.append(details or {}),
    )
    finding = PipelineFinding(
        event="DP_VM_STALL",
        severity="WARN",
        tier=EscalationTier.AUTO_RECOVER,
        summary="stall",
    )
    escalation.route_finding(finding, auto_recover_ran=True)
    assert captured[0]["auto_recover_ran"] is True


def test_route_file_issue_writes_pm_doc(tmp_path, monkeypatch):
    # PM clone with the issues dir + orchestrator inbox present.
    pm = tmp_path / "unified-trading-pm"
    (pm / "plans" / "active" / "issues").mkdir(parents=True)
    (pm / "harsh_orchestrator").mkdir(parents=True)
    (pm / "harsh_orchestrator" / "_agent_pings.md").write_text("# pings\n", encoding="utf-8")
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: None,
    )
    finding = PipelineFinding(
        event="DP_EVENT_LOOP_STARVED",
        severity="WARN",
        tier=EscalationTier.FILE_ISSUE,
        summary="VM xyz event loop starved",
        details={"vm_name": "xyz"},
        registry_id="DP-VM-004",
    )
    result = escalation.route_finding(finding, pm_repo_path=str(pm))
    assert result["issue_path"] is not None
    written = list((pm / "plans" / "active" / "issues").glob("*.md"))
    assert len(written) == 1
    body = written[0].read_text()
    assert "DP_EVENT_LOOP_STARVED" in body
    assert result["inbox_pinged"] is True


def test_route_file_issue_no_pm_clone_defers(monkeypatch):
    details_seen: list[dict] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: details_seen.append(details or {}),
    )
    finding = PipelineFinding(
        event="DP_DIVERGENT_EMPTY",
        severity="WARN",
        tier=EscalationTier.FILE_ISSUE,
        summary="divergence",
    )
    # nonexistent PM path → defer, but still emit
    result = escalation.route_finding(finding, pm_repo_path="/nonexistent/pm")
    assert result["emitted"] is True
    assert details_seen[0].get("file_issue_deferred") == "no_pm_clone_on_disk"


# ── meta_watchers freshness probe ───────────────────────────────────────────
def test_catalogue_stale_emits_critical(monkeypatch):
    bucket = "instruments-store-defi-prd-test-project"
    # blob present but 30h old (budget 24h) → stale
    storage = FakeStorage({(bucket, "_catalogue/instrument_catalogue.parquet"): (b"x", 30 * 60.0)})
    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    target = meta_watchers.FreshnessTarget(
        bucket=bucket,
        blob_path="_catalogue/instrument_catalogue.parquet",
        max_age_min=meta_watchers.DEFAULT_CATALOGUE_MAX_AGE_MIN,
        label="defi",
    )
    results = meta_watchers.check_catalogue_freshness(storage_client=storage, targets=[target])
    assert results[0].stale is True
    assert any(e[0] == "DP_CATALOG_NOT_RUNNING" and e[1] == "CRITICAL" for e in emitted)


def test_zombie_watchdog_down_when_census_missing(monkeypatch):
    storage = FakeStorage({})  # no census blob
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    result = meta_watchers.check_zombie_watchdog_alive(storage_client=storage, log_bucket=LOG_BUCKET)
    assert result.stale is True
    assert "DP_ZOMBIE_WATCHDOG_DOWN" in emitted


def test_zombie_watchdog_alive_when_fresh(monkeypatch):
    storage = FakeStorage({(LOG_BUCKET, meta_watchers.WATCHDOG_CENSUS_BLOB): (b"{}", 3.0)})
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    result = meta_watchers.check_zombie_watchdog_alive(storage_client=storage, log_bucket=LOG_BUCKET)
    assert result.stale is False
    assert emitted == []
