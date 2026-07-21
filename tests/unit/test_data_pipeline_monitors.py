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
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from deployment_service.data_pipeline_monitors import (
    _gcs,
    consolidator_scheduler_watcher,
    escalation,
    exit_code_fleet_monitor,
    heartbeat_stall_watcher,
    live_stream_watcher,
    meta_watchers,
    stale_image_watcher,
)
from deployment_service.data_pipeline_monitors import (
    renag_tracker as renag_tracker_module,
)
from deployment_service.data_pipeline_monitors.escalation import (
    EscalationTier,
    PipelineFinding,
)

# Module-level import (NOT inside a test function) — a test that monkeypatches
# ``scripts.recovery.relaunch_backfill_vm.RelaunchPreemptedVm`` must close over
# THIS original class reference, never re-import the name inside the patched
# function (that re-import would resolve to the monkeypatched fake itself →
# infinite recursion).
from scripts.recovery.relaunch_backfill_vm import RelaunchPreemptedVm as _RelaunchPreemptedVm


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


def test_exit_code_none_for_running_sentinel_not_misread_as_success():
    """lc_log_upload_trap_block (launcher_common.sh) stamps EXIT_STATUS="RUNNING"
    at VM-startup, before the EXIT trap that writes the real rc is installed —
    a same-named relaunch's mid-run signal, in place until either the trap
    overwrites it with a real rc or a whole-unit SIGKILL leaves it stuck. A
    non-integer sentinel must never be misread as the successful rc=0 a STALE
    prior run could otherwise leave behind (the exact false-success bug this
    guards against — features_sports_unbounded_memory_early_history_dates
    issue doc, 2026-07-13)."""
    vm = "fss-backfill-vm-4"
    storage = FakeStorage({(LOG_BUCKET, _exit_status_blob(vm)): (b"RUNNING\n", 0.0)})
    assert _gcs.read_terminal_exit_code(storage, LOG_BUCKET, vm) is None


# ── _gcs.read_progress_checkpoint (SPOT preemption resume checkpoint) ─────────


def _progress_blob(vm: str) -> str:
    return _gcs.PROGRESS_BLOB.format(vm=vm)


def test_read_progress_checkpoint_monotonic():
    vm = "cefi-bf-1"
    blob = json.dumps({"last_completed_date": "2026-02-15", "monotonic": True, "vm_name": vm}).encode()
    storage = FakeStorage({(LOG_BUCKET, _progress_blob(vm)): (blob, 0.0)})
    assert _gcs.read_progress_checkpoint(storage, LOG_BUCKET, vm) == {
        "last_completed_date": "2026-02-15",
        "monotonic": "true",
    }


def test_read_progress_checkpoint_non_monotonic():
    vm = "mtds-bf-1"
    blob = json.dumps({"last_completed_date": "2026-02-15", "monotonic": False}).encode()
    storage = FakeStorage({(LOG_BUCKET, _progress_blob(vm)): (blob, 0.0)})
    got = _gcs.read_progress_checkpoint(storage, LOG_BUCKET, vm)
    assert got is not None and got["monotonic"] == "false"


def test_read_progress_checkpoint_absent_monotonic_fails_safe_false():
    """A checkpoint missing the monotonic flag fails safe → 'false' (no skip-ahead)."""
    vm = "old-bf-1"
    blob = json.dumps({"last_completed_date": "2026-02-15"}).encode()
    storage = FakeStorage({(LOG_BUCKET, _progress_blob(vm)): (blob, 0.0)})
    got = _gcs.read_progress_checkpoint(storage, LOG_BUCKET, vm)
    assert got is not None and got["monotonic"] == "false"


def test_read_progress_checkpoint_missing_blob_is_none():
    assert _gcs.read_progress_checkpoint(FakeStorage({}), LOG_BUCKET, "gone-vm") is None


def test_read_progress_checkpoint_malformed_json_is_none():
    vm = "bad-json"
    storage = FakeStorage({(LOG_BUCKET, _progress_blob(vm)): (b"{not json", 0.0)})
    assert _gcs.read_progress_checkpoint(storage, LOG_BUCKET, vm) is None


def test_read_progress_checkpoint_invalid_date_is_none():
    vm = "bad-date"
    blob = json.dumps({"last_completed_date": "not-a-date", "monotonic": True}).encode()
    storage = FakeStorage({(LOG_BUCKET, _progress_blob(vm)): (blob, 0.0)})
    assert _gcs.read_progress_checkpoint(storage, LOG_BUCKET, vm) is None


def test_read_progress_checkpoint_non_dict_is_none():
    vm = "list-blob"
    storage = FakeStorage({(LOG_BUCKET, _progress_blob(vm)): (b'["a", "b"]', 0.0)})
    assert _gcs.read_progress_checkpoint(storage, LOG_BUCKET, vm) is None


# ── _gcs.recent_log_summary ──────────────────────────────────────────────────
def test_recent_log_summary_counts_error_lines_and_last_line():
    vm = "defi-recursive-backfill-2026"
    log = (
        b"2026-07-09 10:00:00 INFO starting\n"
        b"2026-07-09 10:00:05 ERROR connection refused\n"
        b"2026-07-09 10:00:10 INFO retrying\n"
        b"2026-07-09 10:00:15 WARN slow response\n"
        b"2026-07-09 10:00:20 INFO done rows_out=42\n"
    )
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (log, 0.0)})
    summary = _gcs.recent_log_summary(storage, LOG_BUCKET, vm)
    assert summary.recent_error_count == 2  # ERROR line + WARN line (both match _ERROR_LINE_RE)
    assert summary.last_log_line == "2026-07-09 10:00:20 INFO done rows_out=42"


def test_recent_log_summary_missing_log_is_honest_empty():
    storage = FakeStorage({})
    summary = _gcs.recent_log_summary(storage, LOG_BUCKET, "gone-vm")
    assert summary.recent_error_count == 0
    assert summary.last_log_line is None


def test_recent_log_summary_ignores_trailing_blank_lines():
    vm = "cefi-mr-2026"
    log = b"2026-07-09 10:00:00 INFO started\n2026-07-09 10:00:01 INFO progressing rows_out=1\n\n\n"
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (log, 0.0)})
    summary = _gcs.recent_log_summary(storage, LOG_BUCKET, vm)
    assert summary.last_log_line == "2026-07-09 10:00:01 INFO progressing rows_out=1"
    assert summary.recent_error_count == 0


def test_recent_log_summary_respects_tail_lines_window():
    vm = "cefi-mr-2027"
    # 3 old ERROR lines outside a tail_lines=2 window, 1 clean line inside it.
    log = b"\n".join([b"ERROR old failure %d" % i for i in range(3)] + [b"INFO clean tail line"])
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (log, 0.0)})
    summary = _gcs.recent_log_summary(storage, LOG_BUCKET, vm, tail_lines=1)
    assert summary.recent_error_count == 0
    assert summary.last_log_line == "INFO clean tail line"


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


# ── PARTIAL_UNCONFIRMED (DP-VM-008) — the premature-kill-vs-CLEAN ambiguity ───
# exit_code_fleet_monitor_clean_misclassifies_premature_kill_2026_07_21.md: a VM
# force-deleted mid-run with real partial progress (no durable exit marker EVER
# written) was indistinguishable from a genuinely-finished run — silently
# resolved to CLEAN, no alert, no relaunch, ~22h of total silence.


def test_classify_partial_unconfirmed_when_exit_none_and_climb():
    # The exact incident shape: no durable exit marker (force-deleted mid-run,
    # PREEMPTED marker never written) but real partial progress landed.
    res = exit_code_fleet_monitor.classify_terminated_vm(
        "af-backfill-20260719-180520", exit_code=None, captured_before=0, captured_after=340
    )
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.PARTIAL_UNCONFIRMED


def test_classify_clean_still_requires_confirmed_exit0_not_just_climb():
    # A confirmed exit_code=0 (a durable terminal marker WAS observed) stays
    # CLEAN — only the exit_code IS None case is downgraded to unconfirmed.
    res = exit_code_fleet_monitor.classify_terminated_vm(
        "af-backfill-clean-run", exit_code=0, captured_before=0, captured_after=340
    )
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.CLEAN


def test_classify_preempted_overrides_exit_nonzero():
    # A spot preemption sends SIGTERM → non-zero exit, but PREEMPTED flag takes
    # priority so no DP_VM_EXIT_NONZERO false-fires.
    res = exit_code_fleet_monitor.classify_terminated_vm(
        "sports-backfill-vm-001", exit_code=1, captured_before=0, captured_after=0, preempted=True
    )
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.PREEMPTED
    assert res.preempted is True


def test_classify_preempted_overrides_gone_no_capture():
    # A preempted VM with flat captured must NOT produce GONE_NO_CAPTURE CRITICAL.
    res = exit_code_fleet_monitor.classify_terminated_vm(
        "af-backfill-vm-001", exit_code=0, captured_before=50, captured_after=50, preempted=True
    )
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.PREEMPTED


def test_is_vm_preempted_true_when_blob_exists():
    storage = FakeStorage({(LOG_BUCKET, _gcs.PREEMPTED_BLOB.format(vm="af-backfill-001")): (b"preempted", 0.0)})
    assert _gcs.is_vm_preempted(storage, LOG_BUCKET, "af-backfill-001") is True


def test_is_vm_preempted_false_when_blob_absent():
    storage = FakeStorage({})
    assert _gcs.is_vm_preempted(storage, LOG_BUCKET, "af-backfill-001") is False


def _patch_dp_log_event(monkeypatch) -> list[tuple[str, str, dict]]:
    """Patch log_event in BOTH modules a PREEMPTED sweep can call it from:
    escalation's own trailing emit AND the relaunch_preempted_vm actuator's
    internal emits (success INFO / failure CRITICAL) — same shape as
    test_dp_recovery_actuators.py's ``_patch_log_event`` helper.
    """
    emitted: list[tuple[str, str, dict]] = []

    def _capture(event, severity="INFO", details=None):
        emitted.append((event, severity, details or {}))

    monkeypatch.setattr("deployment_service.data_pipeline_monitors.escalation.log_event", _capture)
    monkeypatch.setattr("scripts.recovery.relaunch_backfill_vm.log_event", _capture)
    return emitted


def test_sweep_preempted_vm_relaunches_successfully_emits_no_critical(tmp_path: Path, monkeypatch):
    # A spot preemption with a resolvable launcher that relaunches SUCCESSFULLY
    # produces the PREEMPTED verdict and NO CRITICAL alert (SPOT reclaim +
    # auto-recovery is the benign, routine case — Fix 1/2 close the prior gap
    # where this VM's relaunch was CLAIMED but never actually happened).
    vm = "cefi-queue-heavy-binancefutu-x15-20260716-075338"
    census = json.dumps({"vms": {vm: 50}}).encode()
    storage = FakeStorage(
        {
            (LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB): (census, 0.0),
            (LOG_BUCKET, _gcs.EXIT_STATUS_BLOB.format(vm=vm)): (b"1\n", 0.0),  # non-zero from SIGTERM
            # PREEMPTED signal blob written by the VM's shutdown-script
            (LOG_BUCKET, _gcs.PREEMPTED_BLOB.format(vm=vm)): (b"preempted", 0.0),
            # LAUNCH_PARAMS.json captured at VM-creation time (lc_write_launch_params)
            (LOG_BUCKET, _gcs.LAUNCH_PARAMS_BLOB.format(vm=vm)): (
                json.dumps(
                    {
                        "launcher": "launch-cefi-sharded-backfill.sh",
                        "env": {"VENUES": "BINANCE-FUTURES", "START_DATE": "2026-02-01"},
                    }
                ).encode(),
                0.0,
            ),
        }
    )
    emitted = _patch_dp_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def fake_run_launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        launched.append((name, dict(env)))
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    def fake_preempted_actuator(*_a, **_k):  # noqa: ANN002, ANN003
        return _RelaunchPreemptedVm(
            budget_dir=tmp_path, now=lambda: datetime(2026, 7, 16, tzinfo=UTC), run_launcher=fake_run_launcher
        )

    monkeypatch.setattr("scripts.recovery.relaunch_backfill_vm.RelaunchPreemptedVm", fake_preempted_actuator)
    results = exit_code_fleet_monitor.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[],
        captured_reader=lambda _vm: 50,  # flat — would otherwise trigger GONE_NO_CAPTURE
        asset_group_for_vm=lambda _vm: "cefi",
        launcher_for_vm=lambda _vm: "launch-cefi-sharded-backfill.sh",
    )
    assert results[0].verdict is exit_code_fleet_monitor.TerminationVerdict.PREEMPTED
    assert not any(e[1] == "CRITICAL" for e in emitted), f"unexpected CRITICAL alert on preemption: {emitted}"
    # The relaunch replayed the EXACT captured env — never a blind relaunch.
    assert launched == [("launch-cefi-sharded-backfill.sh", {"VENUES": "BINANCE-FUTURES", "START_DATE": "2026-02-01"})]


def test_sweep_preempted_vm_no_launcher_emits_critical_no_relaunch(monkeypatch):
    # Fix 2 (belt-and-braces): when NOTHING will relaunch a preempted VM (no
    # resolvable launcher binding), the sweep must NOT silently vanish it — a
    # CRITICAL DP_VM_PREEMPTED_NO_RELAUNCH fires instead of the old misleading
    # INFO-only "SPOT relaunch (benign, no alert)" log.
    vm = "some-unregistered-preemptible-vm-1"
    census = json.dumps({"vms": {vm: 50}}).encode()
    storage = FakeStorage(
        {
            (LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB): (census, 0.0),
            (LOG_BUCKET, _gcs.PREEMPTED_BLOB.format(vm=vm)): (b"preempted", 0.0),
        }
    )
    emitted = _patch_dp_log_event(monkeypatch)
    results = exit_code_fleet_monitor.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[],
        captured_reader=lambda _vm: 50,
        asset_group_for_vm=lambda _vm: "cefi",
        launcher_for_vm=lambda _vm: "",  # no resolvable launcher
    )
    assert results[0].verdict is exit_code_fleet_monitor.TerminationVerdict.PREEMPTED
    assert any(e[0] == "DP_VM_PREEMPTED_NO_RELAUNCH" and e[1] == "CRITICAL" for e in emitted), (
        f"expected a CRITICAL DP_VM_PREEMPTED_NO_RELAUNCH when no relaunch can happen: {emitted}"
    )


def test_sweep_partial_unconfirmed_vm_relaunches_successfully_emits_warn_not_critical(tmp_path: Path, monkeypatch):
    # The reproduction of exit_code_fleet_monitor_clean_misclassifies_premature_kill_2026_07_21.md:
    # NO EXIT_STATUS blob, NO rc= line in run.log, NO PREEMPTED marker — but
    # captured climbed (real partial progress before a force-delete). Before the
    # fix this silently resolved CLEAN; now it must fire WARN (not CRITICAL) and
    # dispatch the SAME checkpoint-resume relaunch as PREEMPTED.
    vm = "af-backfill-20260719-180520"
    census = json.dumps({"vms": {vm: 0}}).encode()
    storage = FakeStorage(
        {
            (LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB): (census, 0.0),
            (LOG_BUCKET, _gcs.LAUNCH_PARAMS_BLOB.format(vm=vm)): (
                json.dumps(
                    {
                        "launcher": "launch-apifootball-backfill.sh",
                        "env": {"START_DATE": "2015-01-01", "END_DATE": "2026-07-19"},
                    }
                ).encode(),
                0.0,
            ),
        }
    )
    emitted = _patch_dp_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def fake_run_launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        launched.append((name, dict(env)))
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    def fake_preempted_actuator(*_a, **_k):  # noqa: ANN002, ANN003
        return _RelaunchPreemptedVm(
            budget_dir=tmp_path, now=lambda: datetime(2026, 7, 21, tzinfo=UTC), run_launcher=fake_run_launcher
        )

    monkeypatch.setattr("scripts.recovery.relaunch_backfill_vm.RelaunchPreemptedVm", fake_preempted_actuator)
    results = exit_code_fleet_monitor.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[],
        captured_reader=lambda _vm: 340,  # climbed from 0 — real partial progress
        asset_group_for_vm=lambda _vm: "sports",
        launcher_for_vm=lambda _vm: "launch-apifootball-backfill.sh",
    )
    assert results[0].verdict is exit_code_fleet_monitor.TerminationVerdict.PARTIAL_UNCONFIRMED
    assert not any(e[1] == "CRITICAL" for e in emitted), f"unexpected CRITICAL alert: {emitted}"
    assert any(e[0] == "DP_VM_PARTIAL_UNCONFIRMED" and e[1] == "WARN" for e in emitted), (
        f"expected a WARN DP_VM_PARTIAL_UNCONFIRMED — this ambiguous case must never be silent: {emitted}"
    )
    # The relaunch replayed the captured launch env — never a blind relaunch.
    assert launched == [("launch-apifootball-backfill.sh", {"START_DATE": "2015-01-01", "END_DATE": "2026-07-19"})]


def test_sweep_partial_unconfirmed_vm_no_launcher_emits_critical_no_relaunch(monkeypatch):
    # Belt-and-braces (mirrors the PREEMPTED no-launcher case): if nothing CAN
    # relaunch a PARTIAL_UNCONFIRMED VM, that must page — never a silent no-op.
    vm = "some-unregistered-backfill-vm-1"
    census = json.dumps({"vms": {vm: 0}}).encode()
    storage = FakeStorage({(LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB): (census, 0.0)})
    emitted = _patch_dp_log_event(monkeypatch)
    results = exit_code_fleet_monitor.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[],
        captured_reader=lambda _vm: 340,
        asset_group_for_vm=lambda _vm: "sports",
        launcher_for_vm=lambda _vm: "",  # no resolvable launcher
    )
    assert results[0].verdict is exit_code_fleet_monitor.TerminationVerdict.PARTIAL_UNCONFIRMED
    assert any(e[0] == "DP_VM_PREEMPTED_NO_RELAUNCH" and e[1] == "CRITICAL" for e in emitted), (
        f"expected a CRITICAL no-relaunch page when nothing can relaunch: {emitted}"
    )


def test_read_launch_params_round_trip():
    vm = "cefi-queue-heavy-x-1"
    payload = json.dumps(
        {
            "launcher": "launch-cefi-sharded-backfill.sh",
            "env": {"VENUES": "BINANCE-FUTURES", "START_DATE": "2026-02-01"},
        }
    ).encode()
    storage = FakeStorage({(LOG_BUCKET, _gcs.LAUNCH_PARAMS_BLOB.format(vm=vm)): (payload, 0.0)})
    out = _gcs.read_launch_params(storage, LOG_BUCKET, vm)
    assert out == {"VENUES": "BINANCE-FUTURES", "START_DATE": "2026-02-01"}


def test_read_launch_params_absent_returns_none():
    storage = FakeStorage({})
    assert _gcs.read_launch_params(storage, LOG_BUCKET, "cefi-queue-heavy-x-1") is None


def test_read_launch_params_malformed_returns_none():
    vm = "cefi-queue-heavy-x-1"
    storage = FakeStorage({(LOG_BUCKET, _gcs.LAUNCH_PARAMS_BLOB.format(vm=vm)): (b"not json", 0.0)})
    assert _gcs.read_launch_params(storage, LOG_BUCKET, vm) is None


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
    # Genuine starve: no heartbeat, no run.log AND captured FLAT (no progress by ANY
    # signal). captured_flat=True is load-bearing — a CLIMBING captured count proves
    # the worker is alive without instrumentation (see the next test).
    res = heartbeat_stall_watcher.classify_vm_liveness("vm", vm_age_min=120, heartbeat_age_min=None, captured_flat=True)
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.EVENT_LOOP_STARVED


def test_classify_liveness_no_instrumentation_but_capturing_is_alive():
    # operator 2026-06-24: cefi-extended-2025-resume (pre-05:03-sidecar-rollout tarball)
    # emits no sidecar + no run.log + no PIPELINE_HEARTBEAT, but its captured count
    # CLIMBS (it's capturing a small DEX venue) → alive, NOT EVENT_LOOP_STARVED.
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "cefi-extended-2025-resume", vm_age_min=600, heartbeat_age_min=None, captured_flat=False
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.ALIVE


def test_classify_liveness_slow_fetch_fresh_pipeline_heartbeat_not_stall():
    # operator 2026-06-24: cefi-deribit-2025-light grinds through ONE huge deribit
    # OPTIONS/options_chain fetch (8-min/date) → its last *progress* line (run_log_age)
    # legitimately exceeds 90m, but the 60s PIPELINE_HEARTBEAT marker is FRESH (worker
    # loop alive) + sidecar fresh → ALIVE, NOT a false hung-worker STALL.
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "cefi-deribit-2025-light",
        vm_age_min=300,
        heartbeat_age_min=1.0,
        captured_flat=True,
        run_log_age_min=120.0,
        pipeline_heartbeat_age_min=1.0,
        run_log_stall_minutes=90.0,
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.ALIVE


def test_classify_liveness_genuinely_hung_worker_still_stalls():
    # The gate must NOT mask a real hang: sidecar fresh (host alive) but BOTH the
    # progress line AND the PIPELINE_HEARTBEAT frozen past the bound → hung worker → STALL.
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "cefi-deribit-2025-light",
        vm_age_min=300,
        heartbeat_age_min=1.0,
        captured_flat=True,
        run_log_age_min=120.0,
        pipeline_heartbeat_age_min=120.0,
        run_log_stall_minutes=90.0,
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.STALL


def test_classify_liveness_alive_when_fresh_and_progressing():
    res = heartbeat_stall_watcher.classify_vm_liveness("vm", vm_age_min=120, heartbeat_age_min=2, captured_flat=False)
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.ALIVE


def test_classify_liveness_too_young_skips():
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "vm", vm_age_min=3, heartbeat_age_min=None, captured_flat=False, grace_minutes=10
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.TOO_YOUNG


def test_classify_liveness_fresh_shard_overrides_stale_heartbeat():
    # INCIDENT 2026-06-23: a tradfi-bf VM captured 114k rows + heartbeat on-box every
    # 60s, but its GCS-tee'd run.log lagged 42m so the PIPELINE_HEARTBEAT marker read
    # stale → false DP_VM_STALL. The AUTHORITATIVE per-VM manifest-shard mtime (fresh,
    # the worker writes it directly to GCS as it captures) must OVERRIDE the stale
    # heartbeat → ALIVE.
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "tradfi-bf-cme-ohlcv-1m-cl-2025",
        vm_age_min=120,
        heartbeat_age_min=42,
        captured_flat=False,
        progress_age_min=1.0,
        stall_minutes=10,
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.ALIVE


def test_classify_liveness_stall_when_shard_also_stale():
    # Fail-safe: no shard signal (None) → the heartbeat-staleness STALL still fires,
    # so a VM that genuinely never writes a shard is still caught.
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "tradfi-bf-cme-ohlcv-1m-cl-2025",
        vm_age_min=120,
        heartbeat_age_min=42,
        captured_flat=False,
        progress_age_min=None,
        stall_minutes=10,
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.STALL


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


def test_stall_finding_carries_relaunch_launcher(monkeypatch):
    """When launcher_for_vm is wired, the DP_VM_STALL finding carries relaunch_launcher.

    That binding is what lets the relaunch_stalled_vm auto_recover actuator
    re-launch the watchdog-killed VM (instead of falling through to file_issue).
    """
    vm = "tradfi-bf-cme-2026"
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (_pipeline_hb_runlog(vm, marker_age_min=40.0), None)})
    captured_details: list[dict] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: captured_details.append(details or {}),
    )
    # mute the best-effort dispatch + the file_issue PM-clone write so the test is hermetic
    monkeypatch.setattr(
        escalation, "_dispatch_to_orchestrator", lambda _f, _p: {"dispatched": False, "reason": "muted"}
    )
    monkeypatch.setattr(escalation, "_resolve_pm_path", lambda _p: None)
    heartbeat_stall_watcher.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[(vm, "asia-northeast1-c")],
        vm_age_reader=lambda _n, _z: 120.0,
        captured_reader=lambda _vm: 0,
        asset_group_for_vm=lambda _vm: "tradfi",
        launcher_for_vm=lambda _vm: "launch-tradfi-bf-cme.sh",
        stall_minutes=15,
    )
    stall_details = [d for d in captured_details if d.get("relaunch_launcher")]
    assert stall_details, "DP_VM_STALL finding should carry the relaunch_launcher binding"
    assert stall_details[0]["relaunch_launcher"] == "launch-tradfi-bf-cme.sh"


def _stall_result(vm: str, *, hb_age: float | None, run_log_age: float | None = None):
    return heartbeat_stall_watcher.LivenessResult(
        vm_name=vm,
        verdict=heartbeat_stall_watcher.LivenessVerdict.STALL,
        heartbeat_age_min=hb_age,
        captured_flat=False,
        run_log_age_min=run_log_age,
    )


def test_should_auto_kill_backfill_stale_past_threshold():
    res = _stall_result("tradfi-bf-cme-2026", hb_age=50.0)
    assert heartbeat_stall_watcher.should_auto_kill(res, is_backfill=True, umbrella="batch", kill_minutes=45.0)


def test_should_auto_kill_false_when_stall_fresh():
    # Stalled but only 20m — under the 45m kill bound → alert/relaunch window, no kill.
    res = _stall_result("tradfi-bf-cme-2026", hb_age=20.0)
    assert not heartbeat_stall_watcher.should_auto_kill(res, is_backfill=True, umbrella="batch", kill_minutes=45.0)


def test_should_auto_kill_false_for_live_vm():
    # A LONG_LIVED_LIVE producer is NEVER reaped, even past the threshold.
    res = _stall_result("mtds-live-defi-2026", hb_age=999.0)
    assert not heartbeat_stall_watcher.should_auto_kill(res, is_backfill=False, umbrella="live", kill_minutes=45.0)
    # defence-in-depth: even if the backfill heuristic mis-fires, umbrella=live blocks the kill
    assert not heartbeat_stall_watcher.should_auto_kill(res, is_backfill=True, umbrella="live", kill_minutes=45.0)


def test_should_auto_kill_false_when_not_stall():
    alive = heartbeat_stall_watcher.LivenessResult(
        vm_name="tradfi-bf-cme-2026",
        verdict=heartbeat_stall_watcher.LivenessVerdict.ALIVE,
        heartbeat_age_min=2.0,
        captured_flat=False,
    )
    assert not heartbeat_stall_watcher.should_auto_kill(alive, is_backfill=True, umbrella="batch")


# ── EXPLICIT progress-guard (operator 2026-06-23): a PROGRESSING VM is NEVER
# reaped — keyed on measured progress vs SLA, not live-status. Prevents the
# zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23 incident.
def test_is_vm_progressing_fresh_heartbeat():
    # A fresh heartbeat within the kill window = the worker process-tree is alive.
    res = _stall_result("tradfi-bf-cme-2026", hb_age=5.0)
    assert heartbeat_stall_watcher.is_vm_progressing(res, kill_minutes=45.0)


def test_is_vm_progressing_advancing_run_log():
    # No heartbeat blob but the run.log advanced recently = measured forward progress.
    res = _stall_result("tradfi-bf-cme-2026", hb_age=None, run_log_age=3.0)
    assert heartbeat_stall_watcher.is_vm_progressing(res, kill_minutes=45.0)


def test_is_vm_progressing_fresh_shard_blocks_kill():
    # Defence-in-depth (lagging-GCS-tee class, incident 2026-06-23): a fresh per-VM
    # manifest shard = the worker is capturing right now → never reap, even if the
    # heartbeat + run.log signals lag past the kill bound.
    res = heartbeat_stall_watcher.LivenessResult(
        vm_name="tradfi-bf-cme-ohlcv-1m-cl-2025",
        verdict=heartbeat_stall_watcher.LivenessVerdict.STALL,
        heartbeat_age_min=99.0,
        captured_flat=False,
        run_log_age_min=99.0,
        progress_age_min=2.0,
    )
    assert heartbeat_stall_watcher.is_vm_progressing(res, kill_minutes=45.0)
    assert not heartbeat_stall_watcher.should_auto_kill(res, is_backfill=True, umbrella="batch", kill_minutes=45.0)


def test_is_vm_progressing_false_when_both_signals_stale():
    # Heartbeat AND run.log both past the kill window = no recent progress.
    res = _stall_result("tradfi-bf-cme-2026", hb_age=60.0, run_log_age=70.0)
    assert not heartbeat_stall_watcher.is_vm_progressing(res, kill_minutes=45.0)


def test_is_vm_progressing_false_when_no_signals():
    # No measured signal at all = not "fresh" (fail toward NOT-progressing so a
    # genuinely-silent VM stays reapable — the guard only ever BLOCKS a kill).
    res = _stall_result("tradfi-bf-cme-2026", hb_age=None, run_log_age=None)
    assert not heartbeat_stall_watcher.is_vm_progressing(res, kill_minutes=45.0)


def test_progressing_vm_is_never_reaped():
    """THE invariant (operator 2026-06-23): a VM doing real work within its SLA is
    NEVER auto-killed — even if some other signal looks stale. Defence-in-depth
    over the STALL verdict; the explicit guard makes it independently provable."""
    # Construct a STALL-verdict result (worst case for the reaper) that nonetheless
    # carries a FRESH heartbeat — the progress-guard must veto the kill. (In live
    # operation classify would never emit STALL with a fresh heartbeat, but the
    # guard must hold even if a future classify change regresses that ordering.)
    progressing = heartbeat_stall_watcher.LivenessResult(
        vm_name="tradfi-bf-cme-2026",
        verdict=heartbeat_stall_watcher.LivenessVerdict.STALL,
        heartbeat_age_min=3.0,  # FRESH — within the 45m window → progressing
        captured_flat=False,
        run_log_age_min=None,
    )
    assert heartbeat_stall_watcher.is_vm_progressing(progressing, kill_minutes=45.0)
    assert not heartbeat_stall_watcher.should_auto_kill(
        progressing, is_backfill=True, umbrella="batch", kill_minutes=45.0
    )
    # Same VM but run.log still advancing (heartbeat absent) → still never reaped.
    progressing_log = _stall_result("tradfi-bf-cme-2026", hb_age=None, run_log_age=4.0)
    assert not heartbeat_stall_watcher.should_auto_kill(
        progressing_log, is_backfill=True, umbrella="batch", kill_minutes=45.0
    )


def test_sweep_auto_kills_stalled_backfill_vm(monkeypatch):
    """A stalled backfill VM past kill_minutes is DELETED so the wave-launcher reclaims its slot."""
    vm = "tradfi-bf-cme-2026"
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (_pipeline_hb_runlog(vm, marker_age_min=60.0), None)})
    killed: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: None,
    )
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.heartbeat_stall_watcher.log_event",
        lambda event, severity="INFO", details=None: None,
    )
    monkeypatch.setattr(escalation, "_dispatch_to_orchestrator", lambda _f, _p: {"dispatched": False})
    monkeypatch.setattr(escalation, "_resolve_pm_path", lambda _p: None)

    def _killer(name: str, zone: str) -> bool:
        killed.append((name, zone))
        return True

    heartbeat_stall_watcher.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[(vm, "asia-northeast1-c")],
        vm_age_reader=lambda _n, _z: 120.0,
        captured_reader=lambda _vm: 0,
        asset_group_for_vm=lambda _vm: "tradfi",
        umbrella_for_vm=lambda _vm: "batch",
        vm_killer=_killer,
        stall_minutes=15,
        kill_minutes=45.0,
    )
    assert killed == [(vm, "asia-northeast1-c")], "stalled backfill VM past kill_minutes should be auto-killed"


def test_sweep_does_not_kill_live_vm(monkeypatch):
    """A LONG_LIVED_LIVE producer is never auto-killed even when its heartbeat is stale."""
    vm = "mtds-live-defi-2026"
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (_pipeline_hb_runlog(vm, marker_age_min=300.0), None)})
    killed: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: None,
    )
    monkeypatch.setattr(escalation, "_dispatch_to_orchestrator", lambda _f, _p: {"dispatched": False})
    monkeypatch.setattr(escalation, "_resolve_pm_path", lambda _p: None)
    heartbeat_stall_watcher.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[(vm, "asia-northeast1-c")],
        vm_age_reader=lambda _n, _z: 120.0,
        captured_reader=lambda _vm: 0,
        asset_group_for_vm=lambda _vm: "defi",
        umbrella_for_vm=lambda _vm: "live",
        vm_killer=lambda name, _z: killed.append(name) or True,
        stall_minutes=15,
        kill_minutes=45.0,
    )
    assert killed == [], "a live producer must never be auto-killed"


def test_sweep_kill_cap_blocks_runaway(monkeypatch):
    """Per-sweep kill cap prevents a runaway from reaping the whole fleet."""
    vms = [(f"tradfi-bf-cme-{i}", "asia-northeast1-c") for i in range(4)]
    storage = FakeStorage(
        {(LOG_BUCKET, _run_log_blob(vm)): (_pipeline_hb_runlog(vm, marker_age_min=60.0), None) for vm, _ in vms}
    )
    killed: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: None,
    )
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.heartbeat_stall_watcher.log_event",
        lambda event, severity="INFO", details=None: None,
    )
    monkeypatch.setattr(escalation, "_dispatch_to_orchestrator", lambda _f, _p: {"dispatched": False})
    monkeypatch.setattr(escalation, "_resolve_pm_path", lambda _p: None)
    heartbeat_stall_watcher.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=vms,
        vm_age_reader=lambda _n, _z: 120.0,
        captured_reader=lambda _vm: 0,
        asset_group_for_vm=lambda _vm: "tradfi",
        umbrella_for_vm=lambda _vm: "batch",
        vm_killer=lambda name, _z: killed.append(name) or True,
        stall_minutes=15,
        kill_minutes=45.0,
        kill_cap_per_sweep=2,
    )
    assert len(killed) == 2, "kill cap should bound deletions per sweep (runaway-guard)"


def test_silent_vm_with_fresh_infra_sidecar_still_alerts(monkeypatch):
    """THE BUG2 KEYSTONE regression (operator's 'zero alerts in 1.5h' symptom).

    A running data VM with a FRESH generic infra ``vm-heartbeat`` sidecar blob but
    NO PIPELINE_HEARTBEAT worker marker (the data worker died / never launched /
    its heartbeat timer is broken) MUST still ALERT — the watcher must NOT be
    fooled into ALIVE by the always-fresh infra sidecar. Here the run.log is
    FROZEN (boot line ~stale, no marker) → the heartbeat-absent fallback (2026-06-23)
    classifies it ``STALL`` (dead worker, frozen log) rather than
    ``EVENT_LOOP_STARVED`` (reserved for TOTAL silence — no run.log at all). Both
    alert to Slack; the keystone property is "still alerts, not fooled".
    """
    vm = "mtds-live-sports-odds-api-trades-2026"
    fresh_epoch = int(datetime.now(UTC).timestamp()) - 30  # infra sidecar wrote 30s ago
    storage = FakeStorage(
        {
            # Fresh INFRA sidecar — the old, wrong liveness signal.
            (LOG_BUCKET, _heartbeat_blob(vm)): (f"{fresh_epoch}\n-1\nstarting".encode(), None),
            # run.log exists but carries NO PIPELINE_HEARTBEAT worker marker, and
            # its last embedded timestamp is FROZEN well past the run-log stall
            # bound (a stale 2020 boot line) → heartbeat-absent + frozen-log = STALL.
            (LOG_BUCKET, _run_log_blob(vm)): (b"2020-01-01T00:00:00 INFO boot\n", None),
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
    assert results[0].verdict is heartbeat_stall_watcher.LivenessVerdict.STALL
    assert any(e[0] == "DP_VM_STALL" and e[1] == "WARN" for e in emitted)


def test_no_marker_but_fresh_run_log_reads_alive(monkeypatch):
    """Transition-safety fallback (2026-06-23): a healthy PRE-heartbeat-tarball VM.

    ~30 of 41 live VMs predate the PIPELINE_HEARTBEAT tarball and emit NO worker
    marker though their worker is alive and writing. Keying solely on the marker
    flagged all of them EVENT_LOOP_STARVED (29 false alerts). With the run.log
    PROGRESS fallback, a missing marker + a FRESH/advancing run.log reads ALIVE —
    no false alert — while a frozen log still STALLs (see the keystone test above).
    """
    vm = "mtds-live-cefi-deribit-trades-2026"
    fresh_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S")
    storage = FakeStorage(
        {
            # No PIPELINE_HEARTBEAT marker, but the run.log advanced seconds ago.
            (LOG_BUCKET, _run_log_blob(vm)): (f"{fresh_ts} INFO captured shard\n".encode(), None),
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
        vm_age_reader=lambda _n, _z: 120.0,
        captured_reader=lambda _vm: 0,
        asset_group_for_vm=lambda _vm: "cefi",
        stall_minutes=15,
    )
    assert results[0].verdict is heartbeat_stall_watcher.LivenessVerdict.ALIVE
    assert emitted == []  # no false alert for a healthy old-tarball VM


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


def test_heartbeat_blob_write_epoch_returns_raw_epoch():
    """Unlike heartbeat_blob_age_minutes (age vs now), this returns the write
    INSTANT itself — the post-mortem reliability check needs age vs an arbitrary
    kill time, not vs analysis time."""
    vm = "af-backfill-20260717-151237"
    epoch = 1752830264  # arbitrary fixed instant
    storage = FakeStorage({(LOG_BUCKET, _heartbeat_blob(vm)): (f"{epoch}\n-1\nrunning".encode(), None)})
    assert _gcs.heartbeat_blob_write_epoch(storage, LOG_BUCKET, vm) == float(epoch)


def test_heartbeat_blob_write_epoch_none_when_blob_absent():
    assert _gcs.heartbeat_blob_write_epoch(FakeStorage({}), LOG_BUCKET, "vm-x") is None


def test_heartbeat_blob_write_epoch_none_on_non_epoch_first_line():
    vm = "vm-y"
    storage = FakeStorage({(LOG_BUCKET, _heartbeat_blob(vm)): (b"{}\n", None)})
    assert _gcs.heartbeat_blob_write_epoch(storage, LOG_BUCKET, vm) is None


def test_run_log_age_from_embedded_timestamp_tail():
    """run.log freshness derives from the LAST embedded `YYYY-MM-DD HH:MM:SS` line
    (last_modified is bare), so a frozen log reads as STALE-by-content."""
    vm = "tradfi-bf-cme-2025"
    old = (datetime.now(UTC) - timedelta(minutes=90)).strftime("%Y-%m-%d %H:%M:%S")
    log = f"2026-06-22 10:00:00,000 INFO start\n{old},123 INFO last line\n".encode()
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (log, None)})
    age = _gcs.run_log_age_minutes(storage, LOG_BUCKET, vm)
    assert age is not None and age > 60.0  # ~90 min frozen


# ── _gcs.run_log_signals (OOM fix: single download for both ages) ─────────────
def test_run_log_signals_both_ages_from_single_read():
    """run_log_signals extracts both pipeline-heartbeat age and log-mtime age from
    ONE run.log download — the OOM-fix that prevents double-download per VM sweep."""
    vm = "tm-backfill-2026"
    old = (datetime.now(UTC) - timedelta(minutes=50)).strftime("%Y-%m-%d %H:%M:%S")
    log = _pipeline_hb_runlog(vm, marker_age_min=0.4) + f"\n{old},000 INFO progress\n".encode()
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (log, None)})
    sig = _gcs.run_log_signals(storage, LOG_BUCKET, vm)
    assert sig.pipeline_heartbeat_age_min is not None and sig.pipeline_heartbeat_age_min < 2.0
    assert sig.run_log_age_min is not None and sig.run_log_age_min > 40.0


def test_run_log_signals_absent_log_returns_none_pair():
    """Missing run.log ⇒ both ages are None (no false-fresh verdict)."""
    sig = _gcs.run_log_signals(FakeStorage({}), LOG_BUCKET, "vm-x")
    assert sig.pipeline_heartbeat_age_min is None
    assert sig.run_log_age_min is None


def test_run_log_signals_no_marker_but_has_timestamp():
    """Log with no PIPELINE_HEARTBEAT marker ⇒ hb_age=None, log_age reflects last ts."""
    vm = "tradfi-bf-cme-2025"
    old = (datetime.now(UTC) - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
    log = f"{old},000 INFO only-progress\n".encode()
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (log, None)})
    sig = _gcs.run_log_signals(storage, LOG_BUCKET, vm)
    assert sig.pipeline_heartbeat_age_min is None
    assert sig.run_log_age_min is not None and 20.0 <= sig.run_log_age_min < 40.0


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
    """Captured flat AND run.log frozen past the GENEROUS bound ⇒ corroborated stall.

    The run.log must exceed run_log_stall_minutes (not the tight stall_minutes) so a
    healthy-host VM whose tee merely lags never false-flags (REVISED 2026-06-24)."""
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "tradfi-bf-vm",
        vm_age_min=120,
        heartbeat_age_min=1.0,  # fresh sidecar
        captured_flat=True,
        run_log_age_min=95.0,  # past the generous 90m bound
        stall_minutes=10.0,
        run_log_stall_minutes=90.0,
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.STALL


def test_classify_captured_flat_with_merely_lagging_log_is_alive():
    """Fresh sidecar + captured flat + run.log lagging WITHIN the generous bound ⇒
    ALIVE — the false-DP_VM_STALL-flood killer (a healthy-slow VM whose tee lags)."""
    res = heartbeat_stall_watcher.classify_vm_liveness(
        "tradfi-bf-vm",
        vm_age_min=120,
        heartbeat_age_min=2.0,  # fresh sidecar = host alive
        captured_flat=True,
        run_log_age_min=60.0,  # laggy tee, but < 90m bound
        stall_minutes=10.0,
        run_log_stall_minutes=90.0,
    )
    assert res.verdict is heartbeat_stall_watcher.LivenessVerdict.ALIVE


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


# ── DP_VM_STALL self-heal actuator (Fix 1) ──────────────────────────────────
def _silence_dispatch_and_emit(monkeypatch):
    """Mute log_event + the best-effort GH dispatch so the escalation tests are hermetic."""
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: None,
    )
    monkeypatch.setattr(
        escalation, "_dispatch_to_orchestrator", lambda _f, _p: {"dispatched": False, "reason": "muted"}
    )


def test_route_stall_invokes_relaunch_actuator(monkeypatch):
    """DP_VM_STALL auto_recover → relaunch_stalled_vm actuator is wired + invoked."""
    _silence_dispatch_and_emit(monkeypatch)
    # the DP_VM_STALL event has a wired actuator (was missing → fell through to file_issue)
    actuator = escalation._DP_RECOVERY_ACTIONS.get("DP_VM_STALL")
    assert actuator is not None
    finding = PipelineFinding(
        event="DP_VM_STALL",
        severity="WARN",
        tier=EscalationTier.AUTO_RECOVER,
        summary="VM tradfi-bf-cme stalled — heartbeat 12m stale",
        details={"vm_name": "tradfi-bf-cme-20260623", "relaunch_launcher": "launch-tradfi-bf-cme.sh"},
        registry_id="DP-VM-003",
    )
    # the actuator routes to relaunch_stalled_vm; DRY_RUN proves the relaunch plan
    outcome = actuator(finding, dry_run=True)
    assert outcome["actuator"] == "relaunch_stalled_vm"
    assert outcome["recovered"] is True  # DRY_RUN counts as recovered
    assert outcome["result"]["status"] == "DRY_RUN"
    # routed end-to-end (dry_run) — stays auto_recover, no fall-through to file_issue
    result = escalation.route_finding(finding, dry_run=True)
    assert result["effective_tier"] == "auto_recover"


def test_stall_no_launcher_falls_through_to_file_issue(monkeypatch, tmp_path):
    """A DP_VM_STALL with no relaunch_launcher → actuator not recovered → file_issue."""
    pm = tmp_path / "unified-trading-pm"
    (pm / "plans" / "active" / "issues").mkdir(parents=True)
    _silence_dispatch_and_emit(monkeypatch)
    finding = PipelineFinding(
        event="DP_VM_STALL",
        severity="WARN",
        tier=EscalationTier.AUTO_RECOVER,
        summary="VM mystery-vm stalled — heartbeat 30m stale",
        details={"vm_name": "mystery-vm"},  # no relaunch_launcher binding
        registry_id="DP-VM-003",
    )
    result = escalation.route_finding(finding, pm_repo_path=str(pm))
    assert result["effective_tier"] == "file_issue"
    assert result["issue_path"] is not None


# ── actionable issue doc (Fix 2) ────────────────────────────────────────────
def test_filed_issue_is_actionable(monkeypatch, tmp_path):
    """The filed issue carries a `- [ ]` todo + assigned_vm + names the right repo."""
    pm = tmp_path / "unified-trading-pm"
    (pm / "plans" / "active" / "issues").mkdir(parents=True)
    _silence_dispatch_and_emit(monkeypatch)
    # a VM-lifecycle finding → deployment-service
    finding = PipelineFinding(
        event="DP_EVENT_LOOP_STARVED",
        severity="WARN",
        tier=EscalationTier.FILE_ISSUE,
        summary="VM silent-vm emitting NO PIPELINE_HEARTBEAT",
        details={"vm_name": "silent-vm"},
        registry_id="DP-VM-004",
    )
    result = escalation.route_finding(finding, pm_repo_path=str(pm))
    body = (
        tmp_path / "unified-trading-pm" / "plans" / "active" / "issues" / str(result["issue_path"]).split("/")[-1]
    ).read_text()
    assert "assigned_vm: vm-cross-cutting" in body
    assert "parent_epic: observability_master" in body
    assert "- [ ] [CODE] P1." in body
    assert "deployment-service" in body  # VM-lifecycle → deployment-service
    assert "SUB_AGENT_MANDATORY_RULES.md" in body


def test_filed_issue_routes_data_finding_to_mtds(monkeypatch, tmp_path):
    """A data-correctness finding (not VM-lifecycle) → market-tick-data-service."""
    pm = tmp_path / "unified-trading-pm"
    (pm / "plans" / "active" / "issues").mkdir(parents=True)
    _silence_dispatch_and_emit(monkeypatch)
    finding = PipelineFinding(
        event="DP_DIVERGENT_EMPTY",
        severity="WARN",
        tier=EscalationTier.FILE_ISSUE,
        summary="5 defi cells oracle-expects-but-empty",
        details={"asset_group": "defi"},
        registry_id="DP-MANIFEST-002",
    )
    result = escalation.route_finding(finding, pm_repo_path=str(pm))
    body = (
        tmp_path / "unified-trading-pm" / "plans" / "active" / "issues" / str(result["issue_path"]).split("/")[-1]
    ).read_text()
    assert "market-tick-data-service" in body
    assert "- [ ] [CODE] P1." in body


# ── fast CI-parity dispatch (Fix 3) ─────────────────────────────────────────
def test_critical_attempts_dispatch(monkeypatch):
    """A CRITICAL (page_operator) finding ALSO fires the best-effort repository_dispatch."""
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: None,
    )
    seen: list[str] = []
    monkeypatch.setattr(
        escalation,
        "_dispatch_to_orchestrator",
        lambda _f, _p: (seen.append("called"), {"dispatched": True, "reason": "http_204"})[1],
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
    assert seen == ["called"]
    assert result["dispatch"]["dispatched"] is True


def test_dispatch_is_non_raising_without_gh_token(monkeypatch):
    """The REAL _dispatch_to_orchestrator never raises — no GH token → graceful skip."""
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )

    # get_secret_client raising (no SM access / token-less Cloud Run Job) must be
    # swallowed → {dispatched: False, reason: no_gh_token}, NOT a crash.
    def _no_secret_client():
        raise RuntimeError("no SM access")

    monkeypatch.setattr(escalation, "get_secret_client", _no_secret_client)
    finding = PipelineFinding(
        event="DP_VM_EXIT_NONZERO",
        severity="CRITICAL",
        tier=EscalationTier.PAGE_OPERATOR,
        summary="vm crashed",
        details={"vm_name": "x"},
    )
    out = escalation._dispatch_to_orchestrator(finding, None)
    assert out["dispatched"] is False
    assert out["reason"] == "no_gh_token"
    # and route_finding completes + emits despite the dispatch skip
    res = escalation.route_finding(finding)
    assert res["emitted"] is True
    assert "DP_VM_EXIT_NONZERO" in emitted


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


# ── Fix 2: consecutive-miss gate (transient-blip suppression) ────────────────
def _capture_emits(monkeypatch) -> list[tuple[str, str]]:
    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    return emitted


def test_catalogue_consecutive_miss_suppresses_first_then_pages_second(monkeypatch):
    # A SINGLE stale sweep (a transient GCS-read blip under heavy backfill) must NOT
    # page; only a SUSTAINED stale condition (>= min_consecutive sweeps) does.
    bucket = "instruments-store-defi-prd-test-project"
    blob = "_catalogue/instrument_catalogue.parquet"
    storage = FakeStorage({(bucket, blob): (b"x", 30 * 60.0)})  # 30h old, budget 24h → stale
    emitted = _capture_emits(monkeypatch)
    target = meta_watchers.FreshnessTarget(
        bucket=bucket, blob_path=blob, max_age_min=meta_watchers.DEFAULT_CATALOGUE_MAX_AGE_MIN, label="defi"
    )
    # Sweep 1 — first miss → suppressed.
    t1 = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_catalogue_freshness(
        storage_client=storage, targets=[target], miss_tracker=t1, min_consecutive=2
    )
    t1.persist()
    assert not any(e[0] == "DP_CATALOG_NOT_RUNNING" for e in emitted)
    # Sweep 2 — second consecutive miss (counter reloaded from GCS) → pages CRITICAL.
    t2 = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_catalogue_freshness(
        storage_client=storage, targets=[target], miss_tracker=t2, min_consecutive=2
    )
    t2.persist()
    assert any(e[0] == "DP_CATALOG_NOT_RUNNING" and e[1] == "CRITICAL" for e in emitted)


def test_catalogue_fresh_resets_miss_counter(monkeypatch):
    # A miss followed by a fresh probe resets the counter — a recovered-then-stale-again
    # cycle starts counting from 0, so only a CONSECUTIVE run pages.
    bucket = "instruments-store-defi-prd-test-project"
    blob = "_catalogue/instrument_catalogue.parquet"
    storage = FakeStorage({(bucket, blob): (b"x", 30 * 60.0)})  # stale
    _capture_emits(monkeypatch)
    target = meta_watchers.FreshnessTarget(
        bucket=bucket, blob_path=blob, max_age_min=meta_watchers.DEFAULT_CATALOGUE_MAX_AGE_MIN, label="defi"
    )
    t1 = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_catalogue_freshness(
        storage_client=storage, targets=[target], miss_tracker=t1, min_consecutive=2
    )
    t1.persist()
    assert t1._counts[meta_watchers._catalogue_miss_key(target)] == 1  # one miss recorded
    # Now fresh — the reloaded counter (1) must reset to 0/absent on a fresh probe.
    storage.blobs[(bucket, blob)] = (b"x", 1.0)
    t2 = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    assert t2._counts[meta_watchers._catalogue_miss_key(target)] == 1  # loaded the prior miss
    meta_watchers.check_catalogue_freshness(
        storage_client=storage, targets=[target], miss_tracker=t2, min_consecutive=2
    )
    assert meta_watchers._catalogue_miss_key(target) not in t2._counts  # reset on fresh


def test_cron_consecutive_miss_suppresses_first_then_pages_second(monkeypatch):
    blob = "vm-census/some-cron-last-run.json"
    storage = FakeStorage({(LOG_BUCKET, blob): (b"{}", 999.0)})  # well past budget → stale
    emitted = _capture_emits(monkeypatch)
    target = meta_watchers.FreshnessTarget(bucket=LOG_BUCKET, blob_path=blob, max_age_min=10.0, label="some-cron")
    t1 = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_cron_fired(storage_client=storage, targets=[target], miss_tracker=t1, min_consecutive=2)
    t1.persist()
    assert not any(e[0] == "DP_CRON_DID_NOT_FIRE" for e in emitted)
    t2 = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_cron_fired(storage_client=storage, targets=[target], miss_tracker=t2, min_consecutive=2)
    t2.persist()
    assert any(e[0] == "DP_CRON_DID_NOT_FIRE" and e[1] == "CRITICAL" for e in emitted)


def test_cron_paused_resets_miss_counter(monkeypatch):
    # A suppressed-by-design (PAUSED scheduler) probe is NOT a real miss → resets the
    # counter, so an un-pause-then-stale doesn't inherit stale misses.
    blob = "vm-census/some-cron-last-run.json"
    storage = FakeStorage({(LOG_BUCKET, blob): (b"{}", 999.0)})  # stale
    _capture_emits(monkeypatch)
    target = meta_watchers.FreshnessTarget(
        bucket=LOG_BUCKET, blob_path=blob, max_age_min=10.0, label="some-cron", scheduler_job="job-x"
    )
    t = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    t.register(meta_watchers._cron_miss_key(target), stale=True)  # pre-seed a miss
    meta_watchers.check_cron_fired(
        storage_client=storage,
        targets=[target],
        scheduler_state_reader=lambda _j: "PAUSED",
        miss_tracker=t,
        min_consecutive=2,
    )
    assert meta_watchers._cron_miss_key(target) not in t._counts  # paused → reset


def test_misstracker_load_register_persist_roundtrip():
    storage = FakeStorage({})
    t = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    assert t.register("k", stale=True) == 1
    assert t.register("k", stale=True) == 2
    assert t.register("k", stale=False) == 0  # reset
    t.persist()
    t2 = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    assert t2._counts == {}  # 'k' was reset before persist


def test_catalogue_no_tracker_pages_immediately(monkeypatch):
    # Back-compat: with no miss_tracker (existing call sites / tests), a stale probe
    # pages on the FIRST sweep, as before.
    bucket = "instruments-store-defi-prd-test-project"
    blob = "_catalogue/instrument_catalogue.parquet"
    storage = FakeStorage({(bucket, blob): (b"x", 30 * 60.0)})
    emitted = _capture_emits(monkeypatch)
    target = meta_watchers.FreshnessTarget(
        bucket=bucket, blob_path=blob, max_age_min=meta_watchers.DEFAULT_CATALOGUE_MAX_AGE_MIN, label="defi"
    )
    meta_watchers.check_catalogue_freshness(storage_client=storage, targets=[target])
    assert any(e[0] == "DP_CATALOG_NOT_RUNNING" and e[1] == "CRITICAL" for e in emitted)


# ── DP-FETCH-009: high attempted_failed manifest cells ──────────────────────
_MD_BUCKET = "market-data-sports-prd-test-project"
_AVAIL_INDEX = meta_watchers.AVAILABILITY_INDEX_BLOB


def _index_parquet(rows: list[tuple[str, str]]) -> bytes:
    """Build a consolidated _index parquet from (data_type, capture_status) rows."""
    import io

    import pandas as pd

    df = pd.DataFrame(rows, columns=["data_type", "capture_status"])
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _af_target(bucket: str = _MD_BUCKET, label: str = "sports") -> meta_watchers.FreshnessTarget:
    return meta_watchers.FreshnessTarget(bucket=bucket, blob_path=_AVAIL_INDEX, max_age_min=0.0, label=label)


def test_high_attempted_failed_pages_on_abs_threshold(monkeypatch):
    # A backfill that exited 0 / captured climbed but wrote >ABS_THRESHOLD failed
    # cells for one data_type → CRITICAL page (the invisible-failure gap closed).
    n_failed = meta_watchers.ATTEMPTED_FAILED_ABS_THRESHOLD + 5
    rows = [("trades", "captured")] * 100 + [("trades", "attempted_failed")] * n_failed
    storage = FakeStorage({(_MD_BUCKET, _AVAIL_INDEX): (_index_parquet(rows), 0.0)})
    emitted = _capture_emits(monkeypatch)
    cells = meta_watchers.check_high_attempted_failed(storage_client=storage, targets=[_af_target()])
    assert any(c.data_type == "trades" and c.high for c in cells)
    assert any(e[0] == "DP_RUN_MOSTLY_EMPTY" and e[1] == "CRITICAL" for e in emitted)


def test_high_attempted_failed_pages_on_ratio_threshold(monkeypatch):
    # Small corpus: below the abs floor but ratio crosses (and count >= the
    # MIN_ATTEMPTED_FAILED_FOR_RATIO micro-cell guard) → pages.
    n_failed = meta_watchers.MIN_ATTEMPTED_FAILED_FOR_RATIO + 10  # >= guard, < abs floor
    rows = [("ohlcv", "captured")] * 20 + [("ohlcv", "attempted_failed")] * n_failed
    storage = FakeStorage({(_MD_BUCKET, _AVAIL_INDEX): (_index_parquet(rows), 0.0)})
    emitted = _capture_emits(monkeypatch)
    cells = meta_watchers.check_high_attempted_failed(storage_client=storage, targets=[_af_target()])
    cell = next(c for c in cells if c.data_type == "ohlcv")
    assert cell.attempted_failed < meta_watchers.ATTEMPTED_FAILED_ABS_THRESHOLD  # abs floor NOT crossed
    assert cell.ratio >= meta_watchers.ATTEMPTED_FAILED_RATIO_THRESHOLD
    assert cell.high
    assert any(e[0] == "DP_RUN_MOSTLY_EMPTY" and e[1] == "CRITICAL" for e in emitted)


def test_high_attempted_failed_low_failure_does_not_page(monkeypatch):
    # A healthy cell (a few failures in a big captured corpus, ratio tiny) must NOT page.
    rows = [("trades", "captured")] * 1000 + [("trades", "attempted_failed")] * 5
    storage = FakeStorage({(_MD_BUCKET, _AVAIL_INDEX): (_index_parquet(rows), 0.0)})
    emitted = _capture_emits(monkeypatch)
    cells = meta_watchers.check_high_attempted_failed(storage_client=storage, targets=[_af_target()])
    assert not any(c.high for c in cells)
    assert not any(e[0] == "DP_RUN_MOSTLY_EMPTY" for e in emitted)


def test_high_attempted_failed_ratio_guard_ignores_micro_cell(monkeypatch):
    # 1-of-2 failed = 50% ratio but below the MIN_ATTEMPTED_FAILED_FOR_RATIO guard →
    # NOT high (ratio path requires a meaningful count to avoid micro-cell noise).
    rows = [("oi", "captured"), ("oi", "attempted_failed")]
    storage = FakeStorage({(_MD_BUCKET, _AVAIL_INDEX): (_index_parquet(rows), 0.0)})
    emitted = _capture_emits(monkeypatch)
    cells = meta_watchers.check_high_attempted_failed(storage_client=storage, targets=[_af_target()])
    cell = next(c for c in cells if c.data_type == "oi")
    assert cell.ratio == 0.5 and not cell.high
    assert not any(e[0] == "DP_RUN_MOSTLY_EMPTY" for e in emitted)


def test_high_attempted_failed_consecutive_miss_suppresses_first_then_pages_second(monkeypatch):
    # A SINGLE high sweep (a transient consolidator blip) must NOT page; only a
    # SUSTAINED high condition (>= min_consecutive sweeps) does. Mirrors the catalogue gate.
    n_failed = meta_watchers.ATTEMPTED_FAILED_ABS_THRESHOLD + 5
    rows = [("trades", "captured")] * 100 + [("trades", "attempted_failed")] * n_failed
    storage = FakeStorage({(_MD_BUCKET, _AVAIL_INDEX): (_index_parquet(rows), 0.0)})
    emitted = _capture_emits(monkeypatch)
    # Sweep 1 — first high → suppressed.
    t1 = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_high_attempted_failed(
        storage_client=storage, targets=[_af_target()], miss_tracker=t1, min_consecutive=2
    )
    t1.persist()
    assert not any(e[0] == "DP_RUN_MOSTLY_EMPTY" for e in emitted)
    # Sweep 2 — second consecutive high (counter reloaded from GCS) → pages CRITICAL.
    t2 = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_high_attempted_failed(
        storage_client=storage, targets=[_af_target()], miss_tracker=t2, min_consecutive=2
    )
    t2.persist()
    assert any(e[0] == "DP_RUN_MOSTLY_EMPTY" and e[1] == "CRITICAL" for e in emitted)


def test_high_attempted_failed_no_tracker_pages_immediately(monkeypatch):
    # Back-compat: with no miss_tracker, a high cell pages on the FIRST sweep.
    n_failed = meta_watchers.ATTEMPTED_FAILED_ABS_THRESHOLD + 5
    rows = [("trades", "attempted_failed")] * n_failed
    storage = FakeStorage({(_MD_BUCKET, _AVAIL_INDEX): (_index_parquet(rows), 0.0)})
    emitted = _capture_emits(monkeypatch)
    meta_watchers.check_high_attempted_failed(storage_client=storage, targets=[_af_target()])
    assert any(e[0] == "DP_RUN_MOSTLY_EMPTY" and e[1] == "CRITICAL" for e in emitted)


def test_high_attempted_failed_missing_index_no_page(monkeypatch):
    # An absent _index is the catalogue/cron probes' job, not this one's — no page,
    # no crash (read returns no cells).
    storage = FakeStorage({})  # no index blob
    emitted = _capture_emits(monkeypatch)
    cells = meta_watchers.check_high_attempted_failed(storage_client=storage, targets=[_af_target()])
    assert cells == []
    assert not any(e[0] == "DP_RUN_MOSTLY_EMPTY" for e in emitted)


def test_high_attempted_failed_alert_key_matches_miss_key():
    # The RESOLVED-bookend identity (_alert_key) MUST equal the miss-counter key so
    # the counter, emitted-set and bookend all agree on ONE identity per (ag, dt) cell.
    finding = PipelineFinding(
        event="DP_RUN_MOSTLY_EMPTY",
        severity="CRITICAL",
        tier=meta_watchers.EscalationTier.PAGE_OPERATOR,
        summary="x",
        details={"asset_group": meta_watchers._high_attempted_failed_cell_label("sports", "trades")},
        registry_id="DP-FETCH-009",
    )
    assert meta_watchers.alert_key(finding) == meta_watchers._high_attempted_failed_miss_key("sports", "trades")


# ── DP-FETCH-009 re-nag cooldown (2026-07-15, defense-in-depth) ──────────────
# Source-side fix for the DP_RUN_MOSTLY_EMPTY duplicate-alert spam (see
# plans/active/issues/dp_run_mostly_empty_no_recurring_dedup_2026_07_15.md): past
# onset, a still-HIGH cell must NOT re-page every */15 sweep — only after
# ``DEFAULT_RENAG_COOLDOWN_SECONDS`` has elapsed since the LAST actual alert.


def test_renagtracker_should_emit_record_clear_semantics():
    # Low-level contract test (mirrors test_misstracker_load_register_persist_roundtrip):
    # never-seen key → emit; within cooldown → suppressed; cooldown elapsed → emit
    # again; cleared key → treated as fresh (never-seen) again.
    storage = FakeStorage({})
    t = renag_tracker_module.RenagTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    t0 = datetime(2026, 7, 15, 12, 0, 0, tzinfo=UTC)
    assert t.should_emit("k", cooldown_seconds=1800.0, now=t0) is True  # first-ever
    t.record("k", now=t0)
    assert t.should_emit("k", cooldown_seconds=1800.0, now=t0 + timedelta(seconds=100)) is False
    assert t.should_emit("k", cooldown_seconds=1800.0, now=t0 + timedelta(seconds=1800)) is True
    t.persist()
    t2 = renag_tracker_module.RenagTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    assert t2.should_emit("k", cooldown_seconds=1800.0, now=t0 + timedelta(seconds=100)) is False  # round-tripped
    t2.clear("k")
    assert t2.should_emit("k", cooldown_seconds=1800.0, now=t0 + timedelta(seconds=100)) is True  # cleared → fresh


def test_high_attempted_failed_renag_first_emission_after_onset_fires_immediately(monkeypatch):
    # (a) Onset (MissTracker, min_consecutive=2) crosses on sweep 2 — the renag
    # tracker has NEVER recorded this cell's key before, so its first-ever alert
    # fires on that SAME sweep (no extra cooldown wait stacked on top of onset).
    n_failed = meta_watchers.ATTEMPTED_FAILED_ABS_THRESHOLD + 5
    rows = [("trades", "captured")] * 100 + [("trades", "attempted_failed")] * n_failed
    storage = FakeStorage({(_MD_BUCKET, _AVAIL_INDEX): (_index_parquet(rows), 0.0)})
    emitted = _capture_emits(monkeypatch)
    meta_watchers.reset_emitted_tracker()
    # Sweep 1 — below onset threshold → suppressed (both miss + renag agree: no page).
    miss1 = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    renag1 = renag_tracker_module.RenagTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_high_attempted_failed(
        storage_client=storage,
        targets=[_af_target()],
        miss_tracker=miss1,
        min_consecutive=2,
        renag_tracker=renag1,
    )
    miss1.persist()
    renag1.persist()
    assert not any(e[0] == "DP_RUN_MOSTLY_EMPTY" for e in emitted)

    # Sweep 2 — onset crosses (2nd consecutive HIGH) → fires immediately (renag
    # tracker's key was never recorded, so should_emit is True on first sight).
    meta_watchers.reset_emitted_tracker()
    miss2 = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    renag2 = renag_tracker_module.RenagTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_high_attempted_failed(
        storage_client=storage,
        targets=[_af_target()],
        miss_tracker=miss2,
        min_consecutive=2,
        renag_tracker=renag2,
    )
    miss2.persist()
    renag2.persist()
    assert any(e[0] == "DP_RUN_MOSTLY_EMPTY" and e[1] == "CRITICAL" for e in emitted)
    meta_watchers.reset_emitted_tracker()


def test_high_attempted_failed_renag_suppresses_within_cooldown(monkeypatch):
    # (b) A second sweep with the cell still HIGH, well within the cooldown window,
    # must NOT re-emit — but the cell must still be reported as genuinely high (the
    # suppression is a Slack-page decision, not a blindness to the condition).
    n_failed = meta_watchers.ATTEMPTED_FAILED_ABS_THRESHOLD + 5
    rows = [("trades", "captured")] * 100 + [("trades", "attempted_failed")] * n_failed
    storage = FakeStorage({(_MD_BUCKET, _AVAIL_INDEX): (_index_parquet(rows), 0.0)})
    emitted = _capture_emits(monkeypatch)
    key = meta_watchers._high_attempted_failed_miss_key("sports", "trades")

    # Sweep 1 — no miss_tracker (fires on first HIGH) + fresh renag_tracker → pages.
    meta_watchers.reset_emitted_tracker()
    renag1 = renag_tracker_module.RenagTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_high_attempted_failed(storage_client=storage, targets=[_af_target()], renag_tracker=renag1)
    renag1.persist()
    assert any(e[0] == "DP_RUN_MOSTLY_EMPTY" for e in emitted)
    emitted.clear()

    # Sweep 2 — same cell still HIGH, default 1800s cooldown has NOT elapsed → suppressed.
    meta_watchers.reset_emitted_tracker()
    renag2 = renag_tracker_module.RenagTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    cells = meta_watchers.check_high_attempted_failed(
        storage_client=storage, targets=[_af_target()], renag_tracker=renag2
    )
    renag2.persist()
    assert any(c.data_type == "trades" and c.high for c in cells)  # condition genuinely still HIGH
    assert not any(e[0] == "DP_RUN_MOSTLY_EMPTY" for e in emitted)  # but the page is suppressed
    # Still marked ACTIVE this sweep (cooldown-suppressed != resolved) — reconcile_resolved
    # must NOT post a false RESOLVED bookend while the cell is genuinely still failing.
    assert key in meta_watchers._EMITTED_THIS_SWEEP
    meta_watchers.reset_emitted_tracker()


def test_high_attempted_failed_renag_reemits_after_cooldown_elapses(monkeypatch):
    # (c) Once the cooldown has genuinely elapsed since the LAST actual alert, the
    # still-HIGH cell re-emits (CRITICAL still pages — this is re-nag, not permanent
    # silence).
    n_failed = meta_watchers.ATTEMPTED_FAILED_ABS_THRESHOLD + 5
    rows = [("trades", "captured")] * 100 + [("trades", "attempted_failed")] * n_failed
    storage = FakeStorage({(_MD_BUCKET, _AVAIL_INDEX): (_index_parquet(rows), 0.0)})
    emitted = _capture_emits(monkeypatch)
    key = meta_watchers._high_attempted_failed_miss_key("sports", "trades")

    # Sweep 1 — first-ever emission.
    meta_watchers.reset_emitted_tracker()
    renag1 = renag_tracker_module.RenagTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_high_attempted_failed(storage_client=storage, targets=[_af_target()], renag_tracker=renag1)
    renag1.persist()
    assert any(e[0] == "DP_RUN_MOSTLY_EMPTY" for e in emitted)
    emitted.clear()

    # Back-date the persisted last_alerted_at past the cooldown (simulates a sweep
    # that runs after DEFAULT_RENAG_COOLDOWN_SECONDS have genuinely elapsed,
    # without a real sleep in the test).
    stale_ts = datetime.now(UTC).timestamp() - (renag_tracker_module.DEFAULT_RENAG_COOLDOWN_SECONDS + 60.0)
    storage.blobs[(LOG_BUCKET, renag_tracker_module.DP_RENAG_TIMESTAMPS_BLOB)] = (
        json.dumps({key: stale_ts}).encode(),
        0.0,
    )

    meta_watchers.reset_emitted_tracker()
    renag2 = renag_tracker_module.RenagTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_high_attempted_failed(storage_client=storage, targets=[_af_target()], renag_tracker=renag2)
    renag2.persist()
    assert any(e[0] == "DP_RUN_MOSTLY_EMPTY" and e[1] == "CRITICAL" for e in emitted)
    meta_watchers.reset_emitted_tracker()


def test_high_attempted_failed_renag_cleared_on_resolve_allows_immediate_fresh_onset(monkeypatch):
    # (d) After reconcile_resolved posts the RESOLVED bookend for a cell that clears,
    # a LATER fresh onset on the SAME cell must fire immediately — not blocked by
    # stale re-nag state left over from the prior incident.
    n_failed = meta_watchers.ATTEMPTED_FAILED_ABS_THRESHOLD + 5
    high_rows = [("trades", "captured")] * 100 + [("trades", "attempted_failed")] * n_failed
    clean_rows = [("trades", "captured")] * 100
    storage = FakeStorage({(_MD_BUCKET, _AVAIL_INDEX): (_index_parquet(high_rows), 0.0)})
    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.meta_watchers.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    key = meta_watchers._high_attempted_failed_miss_key("sports", "trades")

    # Sweep 1 — cell HIGH, first-ever emission; reconcile_resolved records it ACTIVE.
    meta_watchers.reset_emitted_tracker()
    renag1 = renag_tracker_module.RenagTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_high_attempted_failed(storage_client=storage, targets=[_af_target()], renag_tracker=renag1)
    meta_watchers.reconcile_resolved(storage_client=storage, log_bucket=LOG_BUCKET, renag_tracker=renag1)
    renag1.persist()
    assert any(e[0] == "DP_RUN_MOSTLY_EMPTY" and e[1] == "CRITICAL" for e in emitted)
    emitted.clear()

    # Sweep 2 — the condition genuinely CLEARS (index rewritten with only captured
    # rows) → not high → not emitted → reconcile_resolved sees the key drop out →
    # posts the RESOLVED bookend AND clears the renag_tracker's last_alerted_at.
    storage.blobs[(_MD_BUCKET, _AVAIL_INDEX)] = (_index_parquet(clean_rows), 0.0)
    meta_watchers.reset_emitted_tracker()
    renag2 = renag_tracker_module.RenagTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_high_attempted_failed(storage_client=storage, targets=[_af_target()], renag_tracker=renag2)
    resolved = meta_watchers.reconcile_resolved(storage_client=storage, log_bucket=LOG_BUCKET, renag_tracker=renag2)
    renag2.persist()
    assert key in resolved
    assert any(e[0] == "DP_RUN_MOSTLY_EMPTY" and e[1] == "INFO" for e in emitted)  # RESOLVED bookend
    emitted.clear()

    # Sweep 3 — a FRESH onset on the SAME cell moments later. Despite the prior
    # incident having alerted very recently (well within the cooldown), the cleared
    # renag state means this is treated as a first-ever emission → fires immediately.
    storage.blobs[(_MD_BUCKET, _AVAIL_INDEX)] = (_index_parquet(high_rows), 0.0)
    meta_watchers.reset_emitted_tracker()
    renag3 = renag_tracker_module.RenagTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    meta_watchers.check_high_attempted_failed(storage_client=storage, targets=[_af_target()], renag_tracker=renag3)
    renag3.persist()
    assert any(e[0] == "DP_RUN_MOSTLY_EMPTY" and e[1] == "CRITICAL" for e in emitted)
    meta_watchers.reset_emitted_tracker()


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


# ── DP-WATCHER-003: a non-`-legacy-` consolidator scheduler is PAUSED ────────
def test_consolidator_scheduler_paused_pages_for_non_legacy_job(monkeypatch):
    emitted = _capture_emits(monkeypatch)
    paused = consolidator_scheduler_watcher.check_consolidator_scheduler_paused(
        scheduler_job_lister=lambda: ["uts-prod-manifest-consolidator-instruments-sports-cron"],
        scheduler_state_reader=lambda _job: "PAUSED",
    )
    assert paused == ["uts-prod-manifest-consolidator-instruments-sports-cron"]
    assert any(e[0] == "DP_CONSOLIDATOR_SCHEDULER_PAUSED" and e[1] == "CRITICAL" for e in emitted)


def test_consolidator_scheduler_paused_skips_legacy_job(monkeypatch):
    # -legacy- jobs are deliberately deprecated/paused — never page.
    emitted = _capture_emits(monkeypatch)
    paused = consolidator_scheduler_watcher.check_consolidator_scheduler_paused(
        scheduler_job_lister=lambda: ["uts-prod-manifest-consolidator-market-data-tradfi-legacy-cron"],
        scheduler_state_reader=lambda _job: "PAUSED",
    )
    assert paused == []
    assert emitted == []


def test_consolidator_scheduler_paused_skips_enabled_job(monkeypatch):
    emitted = _capture_emits(monkeypatch)
    paused = consolidator_scheduler_watcher.check_consolidator_scheduler_paused(
        scheduler_job_lister=lambda: ["uts-prod-manifest-consolidator-market-data-sports-cron"],
        scheduler_state_reader=lambda _job: "ENABLED",
    )
    assert paused == []
    assert emitted == []


def test_consolidator_scheduler_paused_empty_lister_no_page(monkeypatch):
    # A listing failure (empty list) checks nothing this sweep — never invents jobs.
    emitted = _capture_emits(monkeypatch)
    paused = consolidator_scheduler_watcher.check_consolidator_scheduler_paused(
        scheduler_job_lister=lambda: [],
        scheduler_state_reader=lambda _job: "PAUSED",
    )
    assert paused == []
    assert emitted == []


# ── KEY #1: DP_VM_GONE_NO_CAPTURE is now run.log-reason-aware ─────────────────
def test_no_capture_reason_progress_when_rows_written():
    # "Wrote N rows" → the writer's own shard climbed; consolidated merely lags.
    log = "2026-06-23 12:00:01 INFO Wrote 1392841 rows to gs://market-data-tick-cefi-prd/.../x.parquet"
    assert _gcs.classify_no_capture_reason(log) is _gcs.NoCaptureReason.PROGRESS


def test_no_capture_reason_honest_absence_settled_market():
    log = "2026-06-23 12:00:01 INFO kalshi-bulk 2025-09-01: 0 trades in corpus (settled)"
    assert _gcs.classify_no_capture_reason(log) is _gcs.NoCaptureReason.HONEST_ABSENCE


def test_no_capture_reason_honest_absence_enrichment_already_complete():
    log = "2026-06-23 INFO Skipping LST rates for 2025-09-01 — all expected sentinels already captured"
    assert _gcs.classify_no_capture_reason(log) is _gcs.NoCaptureReason.HONEST_ABSENCE


def test_no_capture_reason_mtds_idempotent_preflight_skip():
    # A resumed/idempotent backfill VM re-runs a (venue, date) already fully captured
    # and the MTDS pre-flight skips re-fetching (venue_fetch.py:248). captured 0->0 is
    # benign already-done, NOT a silent zero — must NOT false-positive DP_VM_GONE_NO_CAPTURE
    # (operator 2026-06-24: the bybit-2021-heavy false positive on the 171-VM backfill).
    log = "2026-06-24 09:00:01 INFO Pre-flight: venue=BYBIT date=2021-12-31 — all requested data_types fully covered (atoms ⊆ captured), skipping"
    assert _gcs.classify_no_capture_reason(log) is _gcs.NoCaptureReason.HONEST_ABSENCE


def test_no_capture_reason_progress_when_records_written():
    # The DeFi oracle-prices writer logs "Wrote N … RECORDS" (not "rows"); a flat captured
    # count there is consolidated lag, NOT a silent zero (operator 2026-06-24:
    # defi-fwd-oracle-prices-poll false positive — wrote 4 oracle price records, classified SILENT).
    log = "2026-06-24 14:45:13 INFO Wrote 4 oracle price records for 2026-06-24 to gs://market-data-tick-defi-prd/.../oracle_prices.parquet"
    assert _gcs.classify_no_capture_reason(log) is _gcs.NoCaptureReason.PROGRESS


def test_no_capture_reason_honest_absence_sentinel_fan_out():
    # The VM honestly enumerated its universe and recorded expected-but-absent SENTINELS
    # (Tier-3 per-instrument sentinel fan-out, captured=0). A silent zero never reaches the
    # fan-out (operator 2026-06-24: cefi-bybit-2021-light false positive).
    log = "2026-06-24 14:55:47 INFO Tier-3 per-instrument sentinel fan-out: venue=BYBIT dt=trades date=2021-12-31 rows=50 (expected_instruments=50 captured=0)"
    assert _gcs.classify_no_capture_reason(log) is _gcs.NoCaptureReason.HONEST_ABSENCE


def test_no_capture_reason_honest_absence_arb_detector_no_pairs():
    # A cross-venue arb DETECTOR (service-only / manifest-exempt) ran fine and matched no
    # pairs → rows_written=0 is the correct empty result, not a silent capture failure
    # (operator 2026-06-24: prediction-arb-detector false positive).
    log = (
        "2026-06-24 14:56:10 INFO Prediction XV: no cross-venue pairs matched for 2026-06-22\n"
        "2026-06-24 14:56:10 INFO ARB_DETECT_TICK tick=6 two_way_on_both=0 pure=0 quotable=0 executable=0 rows_written=0"
    )
    assert _gcs.classify_no_capture_reason(log) is _gcs.NoCaptureReason.HONEST_ABSENCE


def test_no_capture_reason_still_silent_on_genuine_zero():
    # Guard: the broadened patterns must NOT mask a genuine silent zero (auth fail / 0-universe
    # with no write + no honest-absence + no rate-limit signal).
    log = "2026-06-24 INFO authenticating…\n2026-06-24 ERROR 401 Unauthorized\n2026-06-24 INFO Batch complete: 0 results collected"
    assert _gcs.classify_no_capture_reason(log) is _gcs.NoCaptureReason.SILENT


def test_no_capture_reason_rate_limited_beats_absence():
    # A 429 throttle wins precedence so it's never mistaken for benign absence.
    log = "2026-06-23 WARNING HTTP 429 = Rate limited. Wait and retry.\n0 trades returned"
    assert _gcs.classify_no_capture_reason(log) is _gcs.NoCaptureReason.RATE_LIMITED


def test_no_capture_reason_silent_when_no_signal_or_empty():
    assert _gcs.classify_no_capture_reason("2026-06-23 INFO connecting...\ndone") is _gcs.NoCaptureReason.SILENT
    assert _gcs.classify_no_capture_reason(None) is _gcs.NoCaptureReason.SILENT
    assert _gcs.classify_no_capture_reason("") is _gcs.NoCaptureReason.SILENT


def test_no_capture_reason_honest_absence_empty_confirmed_writes():
    # operator 2026-06-27: sports-ref-v3-1 backfilled zero-fixture 2022 dates and the writer
    # recorded 4-state honest-absence rows (empty_confirmed), NOT captured rows → flat captured
    # 0→0 is honest absence. It false-fired DP_SOURCE_RATE_LIMITED and, post-regex-tighten, must
    # land on HONEST_ABSENCE (benign), never GONE_NO_CAPTURE.
    log = (
        "2026-06-27 07:21:06 INFO Zero-fixture fast path: wrote empty_confirmed for 4 "
        "fixture-dependent entities on date=2022-05-31\n"
        "2026-06-27 07:23:11 INFO ManifestWriter: per-VM shard updated (198 total entries, 198 new)"
    )
    assert _gcs.classify_no_capture_reason(log) is _gcs.NoCaptureReason.HONEST_ABSENCE


def test_no_capture_reason_honest_absence_expected_unattempted_seeding():
    # operator 2026-06-27: instr-backfill-tradfi-ice seeded expected_unattempted sentinels for the
    # target universe → captured flat is honest absence, not a silent zero.
    log = "2026-06-27 07:58:04 INFO EU seeding: wrote expected_unattempted for 6 target-universe venue cells on date=2026-06-25"
    assert _gcs.classify_no_capture_reason(log) is _gcs.NoCaptureReason.HONEST_ABSENCE


def test_no_capture_reason_benign_rate_limit_config_is_not_throttled():
    # REGRESSION (2026-06-27 false-positive flood): a clean run that merely MENTIONS rate-limiting
    # in a config/telemetry line — or echoes the event name DP_SOURCE_RATE_LIMITED — must NOT be
    # classified RATE_LIMITED. The old bare-substring + self-referential pattern tripped on both.
    benign_config = (
        "2026-06-27 07:20:00 INFO rate limiter configured: 300 requests per minute\n"
        "2026-06-27 07:21:06 INFO Sports reference: 0 injuries returned by API\n"
        "PIPELINE_HEARTBEAT vm=sports-ref-v3-1 ag=SPORTS task=instruments-backfill\n"
        "2026-06-27 07:23:11 INFO Zero-fixture fast path: wrote empty_confirmed for 4 entities"
    )
    # Matches honest-absence (empty_confirmed), NOT rate-limited.
    assert _gcs.classify_no_capture_reason(benign_config) is _gcs.NoCaptureReason.HONEST_ABSENCE
    # The self-referential event name alone must not self-trigger RATE_LIMITED.
    self_ref = "2026-06-27 INFO prior alert was [DP_SOURCE_RATE_LIMITED]; nothing wrong now\ndone"
    assert _gcs.classify_no_capture_reason(self_ref) is _gcs.NoCaptureReason.SILENT


def test_no_capture_reason_genuine_throttle_still_rate_limited():
    # Guard: a REAL throttle signal must still classify RATE_LIMITED after the tighten.
    assert (
        _gcs.classify_no_capture_reason("2026-06-27 WARNING API-Football: Too many requests (429)")
        is _gcs.NoCaptureReason.RATE_LIMITED
    )
    assert (
        _gcs.classify_no_capture_reason("2026-06-27 ERROR subgraph quota exceeded for the day")
        is _gcs.NoCaptureReason.RATE_LIMITED
    )
    assert (
        _gcs.classify_no_capture_reason("2026-06-27 WARNING rate limit exceeded, backing off 30s")
        is _gcs.NoCaptureReason.RATE_LIMITED
    )


def test_classify_flat_with_progress_is_expected_no_capture():
    res = exit_code_fleet_monitor.classify_terminated_vm(
        "vm",
        exit_code=0,
        captured_before=50,
        captured_after=50,
        no_capture_reason=_gcs.NoCaptureReason.PROGRESS,
    )
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.EXPECTED_NO_CAPTURE


def test_classify_flat_with_honest_absence_is_expected_no_capture():
    res = exit_code_fleet_monitor.classify_terminated_vm(
        "vm",
        exit_code=0,
        captured_before=0,
        captured_after=0,
        no_capture_reason=_gcs.NoCaptureReason.HONEST_ABSENCE,
    )
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.EXPECTED_NO_CAPTURE


def test_classify_flat_with_rate_limit_is_rate_limited():
    res = exit_code_fleet_monitor.classify_terminated_vm(
        "vm",
        exit_code=0,
        captured_before=10,
        captured_after=10,
        no_capture_reason=_gcs.NoCaptureReason.RATE_LIMITED,
    )
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.RATE_LIMITED


def test_classify_flat_silent_still_gone_no_capture():
    res = exit_code_fleet_monitor.classify_terminated_vm(
        "vm",
        exit_code=0,
        captured_before=5,
        captured_after=5,
        no_capture_reason=_gcs.NoCaptureReason.SILENT,
    )
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.GONE_NO_CAPTURE


# ── KEY #5: LIVE-VM exemption from DP_VM_GONE_NO_CAPTURE (2026-06-27) ─────────
# Incident: 17 CRITICAL false fires when 16 mtds-live-cefi-* VMs were deleted
# during intentional consolidation.  Flat captured on a live VM is the INSTRUMENT
# COUNT (~15, stable by design), NOT the batch instrument-days counter.


def test_classify_live_vm_flat_exit0_is_expected_no_capture():
    # LIVE VM + exit 0 + flat captured (instrument count stable) → NOT GONE_NO_CAPTURE.
    res = exit_code_fleet_monitor.classify_terminated_vm(
        "mtds-live-cefi-binance-001",
        exit_code=0,
        captured_before=15,
        captured_after=15,
        is_live_vm=True,
    )
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.EXPECTED_NO_CAPTURE


def test_classify_live_vm_flat_exit_none_is_expected_no_capture():
    # LIVE VM + no durable exit code + flat captured → also benign (not a silent zero).
    res = exit_code_fleet_monitor.classify_terminated_vm(
        "mtds-live-cefi-okx-001",
        exit_code=None,
        captured_before=0,
        captured_after=0,
        is_live_vm=True,
    )
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.EXPECTED_NO_CAPTURE


def test_classify_live_vm_nonzero_exit_still_alerts():
    # LIVE VM + exit != 0 → EXIT_NONZERO (crashes still page, live exemption never masks OOM/error).
    res = exit_code_fleet_monitor.classify_terminated_vm(
        "mtds-live-cefi-bybit-001",
        exit_code=1,
        captured_before=15,
        captured_after=15,
        is_live_vm=True,
    )
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.EXIT_NONZERO


def test_classify_batch_vm_flat_silent_still_gone_no_capture_unchanged():
    # BATCH VM behaviour UNCHANGED: flat captured + SILENT reason → GONE_NO_CAPTURE.
    res = exit_code_fleet_monitor.classify_terminated_vm(
        "mtds-backfill-cefi-2025",
        exit_code=0,
        captured_before=50,
        captured_after=50,
        is_live_vm=False,
    )
    assert res.verdict is exit_code_fleet_monitor.TerminationVerdict.GONE_NO_CAPTURE


def test_sweep_live_vm_flat_captured_no_gone_no_capture_alert(monkeypatch):
    # End-to-end: a terminated live VM with flat captured must NOT fire DP_VM_GONE_NO_CAPTURE.
    # Reproduces the 2026-06-27 false-alarm: 16 mtds-live-cefi-* deleted during consolidation.
    vm = "mtds-live-cefi-binance-001"
    census = json.dumps({"vms": {vm: 15}}).encode()
    storage = FakeStorage(
        {
            (LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB): (census, 0.0),
            (LOG_BUCKET, _exit_status_blob(vm)): (b"0\n", 0.0),
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
        captured_reader=lambda _vm: 15,  # FLAT instrument count — stable by design for a live VM
        asset_group_for_vm=lambda _vm: "cefi",
        umbrella_for_vm=lambda _vm: "live",  # the live resolver that should gate the exemption
    )
    assert results[0].verdict is exit_code_fleet_monitor.TerminationVerdict.EXPECTED_NO_CAPTURE
    assert not any(e[0] == "DP_VM_GONE_NO_CAPTURE" for e in emitted), (
        f"DP_VM_GONE_NO_CAPTURE must NOT fire for a LIVE VM with flat instrument count: {emitted}"
    )


def test_sweep_live_vm_nonzero_exit_still_emits_exit_nonzero(monkeypatch):
    # A crashed live VM (exit 1) still fires EXIT_NONZERO — the exemption never masks crashes.
    vm = "mtds-live-cefi-okx-001"
    census = json.dumps({"vms": {vm: 15}}).encode()
    storage = FakeStorage(
        {
            (LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB): (census, 0.0),
            (LOG_BUCKET, _exit_status_blob(vm)): (b"1\n", 0.0),
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
        captured_reader=lambda _vm: 15,
        asset_group_for_vm=lambda _vm: "cefi",
        umbrella_for_vm=lambda _vm: "live",
    )
    assert results[0].verdict is exit_code_fleet_monitor.TerminationVerdict.EXIT_NONZERO
    assert any(e[0] == "DP_VM_EXIT_NONZERO" and e[1] == "CRITICAL" for e in emitted)


def test_sweep_shard_wrote_rows_suppresses_gone_no_capture(monkeypatch):
    # cefi-hyperliquid wrote 1.39M rows to its shard yet consolidated read flat
    # 6391→6391 — the run.log "Wrote N rows" reclassifies it as benign (no alert).
    vm = "cefi-hyperliquid-2025"
    census = json.dumps({"vms": {vm: 6391}}).encode()
    storage = FakeStorage(
        {
            (LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB): (census, 0.0),
            (LOG_BUCKET, _exit_status_blob(vm)): (b"0\n", 0.0),
            (LOG_BUCKET, _run_log_blob(vm)): (b"2026-06-23 INFO Wrote 1392841 rows to gs://...\n", 0.0),
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
        captured_reader=lambda _vm: 6391,  # FLAT consolidated count
        asset_group_for_vm=lambda _vm: "cefi",
    )
    assert results[0].verdict is exit_code_fleet_monitor.TerminationVerdict.EXPECTED_NO_CAPTURE
    assert not any(e[0] == "DP_VM_GONE_NO_CAPTURE" for e in emitted)


def test_sweep_rate_limit_emits_source_rate_limited_warn(monkeypatch):
    vm = "sports-injuries-2025"
    census = json.dumps({"vms": {vm: 290}}).encode()
    storage = FakeStorage(
        {
            (LOG_BUCKET, exit_code_fleet_monitor.CENSUS_BLOB): (census, 0.0),
            (LOG_BUCKET, _exit_status_blob(vm)): (b"0\n", 0.0),
            (LOG_BUCKET, _run_log_blob(vm)): (b"2026-06-23 WARNING api_football Too many requests\n", 0.0),
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
        captured_reader=lambda _vm: 290,  # FLAT
        asset_group_for_vm=lambda _vm: "sports",
    )
    assert results[0].verdict is exit_code_fleet_monitor.TerminationVerdict.RATE_LIMITED
    assert any(e[0] == "DP_SOURCE_RATE_LIMITED" and e[1] == "WARN" for e in emitted)
    # NOT escalated as a CRITICAL silent-zero.
    assert not any(e[0] == "DP_VM_GONE_NO_CAPTURE" for e in emitted)


# ── KEY #2: DP_CRON_DID_NOT_FIRE is pause-aware ──────────────────────────────
def _consolidator_target() -> meta_watchers.FreshnessTarget:
    return meta_watchers.FreshnessTarget(
        bucket="market-data-tick-sports-prd-x",
        blob_path="_index/availability_index.parquet",
        max_age_min=180.0,
        label="manifest-consolidator-sports",
        scheduler_job="uts-prd-manifest-consolidator-market-data-sports-cron",
    )


def test_cron_paused_scheduler_suppresses_alert(monkeypatch):
    # Stale artifact (240m > 180m budget) BUT the scheduler is PAUSED-by-design.
    storage = FakeStorage({("market-data-tick-sports-prd-x", "_index/availability_index.parquet"): (b"x", 240.0)})
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    results = meta_watchers.check_cron_fired(
        storage_client=storage,
        targets=[_consolidator_target()],
        scheduler_state_reader=lambda _job: "PAUSED",
    )
    assert results[0].stale is True  # still STALE...
    assert "DP_CRON_DID_NOT_FIRE" not in emitted  # ...but SUPPRESSED (paused by design)


def test_cron_enabled_but_stale_still_alerts(monkeypatch):
    storage = FakeStorage({("market-data-tick-sports-prd-x", "_index/availability_index.parquet"): (b"x", 240.0)})
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    meta_watchers.check_cron_fired(
        storage_client=storage,
        targets=[_consolidator_target()],
        scheduler_state_reader=lambda _job: "ENABLED",
    )
    assert "DP_CRON_DID_NOT_FIRE" in emitted  # an ENABLED-but-stale cron is a real failure


def test_cron_unknown_state_fails_safe_on(monkeypatch):
    # None (job not found / API error) ⇒ do NOT suppress (a missing scheduler alerts).
    storage = FakeStorage({("market-data-tick-sports-prd-x", "_index/availability_index.parquet"): (b"x", 240.0)})
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    meta_watchers.check_cron_fired(
        storage_client=storage,
        targets=[_consolidator_target()],
        scheduler_state_reader=lambda _job: None,
    )
    assert "DP_CRON_DID_NOT_FIRE" in emitted


# ── KEY #4: DP_CRON_DID_NOT_FIRE cross-checks the REAL Cloud Run execution history ──
def _monitor_target() -> meta_watchers.FreshnessTarget:
    # A stale monitor sentinel (40m > 10m budget) backed by a Cloud Run Job stem.
    return meta_watchers.FreshnessTarget(
        bucket="deployment-scripts-prd",
        blob_path="vm-census/exit-code-last-run.json",
        max_age_min=10.0,
        label="dp-exit-code-monitor",
        cloud_run_job="dp-exit-code-monitor",
    )


def test_cron_stale_sentinel_suppressed_when_execution_recent(monkeypatch):
    """The dp-exit-code-monitor false positive: stale sentinel + recent SUCCEEDED execution → no alert."""
    storage = FakeStorage({("deployment-scripts-prd", "vm-census/exit-code-last-run.json"): (b"{}", 40.0)})
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    results = meta_watchers.check_cron_fired(
        storage_client=storage,
        targets=[_monitor_target()],
        # Job last SUCCEEDED 3m ago — well within the 10m budget → the cron IS firing.
        execution_history_reader=lambda _job: 3.0,
    )
    assert results[0].stale is True  # the sentinel is still stale...
    assert "DP_CRON_DID_NOT_FIRE" not in emitted  # ...but the REAL execution history suppresses the false alert


def test_cron_stale_sentinel_alerts_when_execution_also_stale(monkeypatch):
    """A genuinely-dead job (no recent SUCCEEDED execution) still alerts."""
    storage = FakeStorage({("deployment-scripts-prd", "vm-census/exit-code-last-run.json"): (b"{}", 40.0)})
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    meta_watchers.check_cron_fired(
        storage_client=storage,
        targets=[_monitor_target()],
        execution_history_reader=lambda _job: 90.0,  # last success 90m ago (> 10m budget) — genuinely stopped
    )
    assert "DP_CRON_DID_NOT_FIRE" in emitted


def test_consolidator_cloud_run_job_wired_for_key4():
    """The consolidator watcher must name its Cloud Run Job (KEY #4) — without it a transiently
    stale _index can't be cross-checked against the job's real execution history (the 2026-06-24
    manifest-consolidator-tradfi false-positive)."""
    from deployment_service.data_pipeline_monitors import cli

    job = cli._consolidator_cloud_run_job("tradfi")
    assert job  # non-empty → KEY #4 wired
    assert job == cli._consolidator_scheduler_job("tradfi").removesuffix("-cron")
    assert "manifest-consolidator-market-data-tradfi" in job


def test_consolidator_stale_index_suppressed_when_execution_recent(monkeypatch):
    """Regression: a transiently-stale _index + a recent SUCCEEDED consolidator execution must NOT
    page DP_CRON_DID_NOT_FIRE (the manifest-consolidator-tradfi false-positive, 2026-06-24)."""
    from deployment_service.data_pipeline_monitors import cli

    bucket = "market-data-tick-tradfi-prd"
    storage = FakeStorage({(bucket, "_index/availability_index.parquet"): (b"x", 200.0)})  # 200m > 180m budget
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    target = meta_watchers.FreshnessTarget(
        bucket=bucket,
        blob_path="_index/availability_index.parquet",
        max_age_min=180.0,
        label="manifest-consolidator-tradfi",
        cloud_run_job=cli._consolidator_cloud_run_job("tradfi"),
    )
    results = meta_watchers.check_cron_fired(
        storage_client=storage,
        targets=[target],
        execution_history_reader=lambda _job: 0.5,  # consolidator SUCCEEDED 30s ago → demonstrably healthy
    )
    assert results[0].stale is True  # the _index read is stale...
    assert "DP_CRON_DID_NOT_FIRE" not in emitted  # ...but the recent execution suppresses the false alert


def test_cron_stale_sentinel_alerts_when_execution_unknown(monkeypatch):
    """None last-success (no success / job absent / API error) does NOT suppress (fail-safe-on)."""
    storage = FakeStorage({("deployment-scripts-prd", "vm-census/exit-code-last-run.json"): (b"{}", 40.0)})
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    meta_watchers.check_cron_fired(
        storage_client=storage,
        targets=[_monitor_target()],
        execution_history_reader=lambda _job: None,
    )
    assert "DP_CRON_DID_NOT_FIRE" in emitted


def test_reconcile_resolved_emits_bookend_when_alert_clears(monkeypatch):
    # A prior sweep fired DP_CATALOG_NOT_RUNNING::tradfi; this sweep it did NOT re-fire
    # (catalogue fresh) → a ✅ RESOLVED INFO bookend is emitted + the active set cleared,
    # so #data-pipeline-alerts reflects closure instead of a permanent RED.
    storage = FakeStorage(
        {
            (LOG_BUCKET, meta_watchers.ACTIVE_DP_ALERTS_BLOB): (
                json.dumps({"DP_CATALOG_NOT_RUNNING::tradfi": "DP_CATALOG_NOT_RUNNING"}).encode(),
                0.0,
            )
        }
    )
    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.meta_watchers.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    meta_watchers.reset_emitted_tracker()  # nothing re-fired this sweep
    resolved = meta_watchers.reconcile_resolved(storage_client=storage, log_bucket=LOG_BUCKET)
    assert resolved == ["DP_CATALOG_NOT_RUNNING::tradfi"]
    assert ("DP_CATALOG_NOT_RUNNING", "INFO") in emitted  # bookend is INFO — never pages
    assert (LOG_BUCKET, meta_watchers.ACTIVE_DP_ALERTS_BLOB) in storage.uploaded  # new (empty) set persisted


def test_reconcile_resolved_no_bookend_while_still_firing(monkeypatch):
    # Same condition still stale this sweep → it re-fires → NO RESOLVED bookend.
    storage = FakeStorage(
        {
            (LOG_BUCKET, meta_watchers.ACTIVE_DP_ALERTS_BLOB): (
                json.dumps({"DP_CATALOG_NOT_RUNNING::tradfi": "DP_CATALOG_NOT_RUNNING"}).encode(),
                0.0,
            )
        }
    )
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.meta_watchers.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    meta_watchers.reset_emitted_tracker()
    meta_watchers._EMITTED_THIS_SWEEP["DP_CATALOG_NOT_RUNNING::tradfi"] = "DP_CATALOG_NOT_RUNNING"  # re-fired
    resolved = meta_watchers.reconcile_resolved(storage_client=storage, log_bucket=LOG_BUCKET)
    assert resolved == []
    assert emitted == []  # no RESOLVED while the condition still fires
    meta_watchers.reset_emitted_tracker()  # clear module-global state for other tests


def test_monitor_cron_targets_carry_cloud_run_job_stems():
    targets = {t.label: t for t in meta_watchers.monitor_cron_targets("deployment-scripts-prd")}
    assert targets["dp-exit-code-monitor"].cloud_run_job == "dp-exit-code-monitor"
    assert targets["dp-heartbeat-monitor"].cloud_run_job == "dp-heartbeat-watcher"
    assert targets["dp-meta-monitor"].cloud_run_job == "dp-meta-watchers"


# ── KEY #3: DP_CATALOG_NOT_RUNNING env-short read + probed-path alert ─────────
def test_catalogue_targets_use_env_short_bucket_and_prod_prefix(monkeypatch):
    from deployment_service.data_pipeline_monitors import cli, meta_targets

    monkeypatch.setattr(meta_targets, "get_environment", lambda: "prod")
    monkeypatch.setattr(
        meta_targets,
        "resolve_bucket_name",
        lambda *, cloud, kind, asset_group=None: (
            f"{kind}-pred-prd-pid" if "prediction" in kind else f"{kind}-{asset_group}-prd-pid"
        ),
    )
    targets = cli._catalogue_targets()
    by_label = {t.label: t for t in targets}
    # blob prefix is the LONG env (prod/), filename catalog.parquet — matches the writer.
    assert by_label["sports"].blob_path == "prod/catalog.parquet"
    assert by_label["sports"].bucket == "instruments-store-sports-prd-pid"
    # prediction resolves via its FLAT key (no asset_group dict entry).
    assert by_label["prediction"].bucket == "instruments-store-prediction-pred-prd-pid"


# ── DP-FETCH-009 / consolidator-cron: prediction is NOT silently dropped ──────
def test_high_attempted_failed_targets_include_prediction_via_flat_key(monkeypatch):
    """``prediction`` has NO per-AG ``market-data`` bucket — it is a dedicated
    flat ``market-data-tick-prediction`` kind. The per-AG call RAISES for it, and
    the ``except Exception: continue`` in the target builder would then SILENTLY
    drop prediction from the high-attempted_failed sweep (DP-FETCH-009). Resolve
    it via the flat key instead so it stays monitored (mirrors the catalogue
    ``_INSTRUMENTS_STORE_KIND_OVERRIDE`` path)."""
    from deployment_service.data_pipeline_monitors import meta_targets

    def _fake_resolve(*, cloud: str, kind: str, asset_group: str | None = None) -> str:
        if kind == "market-data":
            if asset_group == "prediction":
                # Mirrors reality: no per-asset_group market-data bucket for prediction.
                raise ValueError("no market-data bucket for asset_group=prediction")
            return f"market-data-{asset_group}-prd-pid"
        if kind == "market-data-tick-prediction":
            assert asset_group is None  # flat key takes NO asset_group
            return "market-data-tick-prediction-prd-pid"
        raise AssertionError(f"unexpected kind {kind}")

    monkeypatch.setattr(meta_targets, "resolve_bucket_name", _fake_resolve)
    by_label = {t.label: t for t in meta_targets.high_attempted_failed_targets()}
    # All five asset_groups present — prediction is NOT dropped.
    assert set(by_label) == set(meta_targets.ASSET_GROUPS)
    assert by_label["prediction"].bucket == "market-data-tick-prediction-prd-pid"
    assert by_label["cefi"].bucket == "market-data-cefi-prd-pid"


def test_catalogue_absent_alert_shows_probed_path(monkeypatch):
    target = meta_watchers.FreshnessTarget(
        bucket="instruments-store-sports-prd-pid",
        blob_path="prod/catalog.parquet",
        max_age_min=24 * 60.0,
        label="sports",
    )
    storage = FakeStorage({})  # artifact ABSENT
    emitted: list[tuple[str, str, dict]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity, details or {})),
    )
    meta_watchers.check_catalogue_freshness(storage_client=storage, targets=[target])
    cat = next(e for e in emitted if e[0] == "DP_CATALOG_NOT_RUNNING")
    msg = str(cat[2].get("message", ""))
    assert "gs://instruments-store-sports-prd-pid/prod/catalog.parquet" in msg
    assert "ABSENT" in msg and "budget=24h" in msg
    assert cat[2]["artifact_present"] is False
    assert cat[2]["probed_path"] == "gs://instruments-store-sports-prd-pid/prod/catalog.parquet"


# ── KEY #4: OOM relaunch escalates to a bigger machine (CONSUMES the canonical
# launch_budget_registry MEMORY_TIER_LADDER — one ladder for launch-sizing AND
# OOM-escalation, so the two can never drift; tops out at n2-highmem-32 = 256 GB).
def test_escalated_machine_type_ladders_up():
    # On-ladder rungs step UP via the registry's next_memory_tier (ascending by RAM):
    #   e2-standard-4(16GB) → e2-standard-8(32GB) → n2-standard-16(64GB)
    #   → n2-highmem-16(128GB) → n2-highmem-32(256GB).
    assert escalation._escalated_machine_type("e2-standard-4") == "e2-standard-8"
    assert escalation._escalated_machine_type("e2-standard-8") == "n2-standard-16"
    assert escalation._escalated_machine_type("n2-standard-16") == "n2-highmem-16"
    # 128 GB → 256 GB: the Coinbase-class OOM rung (the explicit task ask).
    assert escalation._escalated_machine_type("n2-highmem-16") == "n2-highmem-32"
    # e2-highmem-16 is OFF-ladder (128 GB) → smallest ladder rung with MORE RAM
    # = n2-highmem-32 (256 GB). So a 128 GB OOM always reaches 256 GB.
    assert escalation._escalated_machine_type("e2-highmem-16") == "n2-highmem-32"
    # Already at the top rung (256 GB) → stay at the top (budget pages, no loop).
    assert escalation._escalated_machine_type("n2-highmem-32") == "n2-highmem-32"
    # Off-ladder but RAM resolves (n2-standard-8 not in the ladder; RAM unknown
    # in the registry table → falls to the high-mem fallback, never the same type).
    assert escalation._escalated_machine_type("n2-standard-8") == escalation._OOM_FALLBACK_MACHINE
    # Unknown family (RAM unresolvable) → high-mem fallback (never the same type).
    assert escalation._escalated_machine_type("c3-highcpu-176") == escalation._OOM_FALLBACK_MACHINE
    assert escalation._escalated_machine_type("") == escalation._OOM_FALLBACK_MACHINE


def test_escalated_machine_type_top_rung_is_256gb():
    """The canonical ladder ceiling an OOM relaunch escalates to is 256 GB
    (n2-highmem-32) — the next rung above the prior 128 GB cap (the task ask)."""
    from deployment_service.data_pipeline_monitors.launch_budget_registry import (
        MEMORY_TIER_LADDER,
        gce_machine_ram_gb,
    )

    top = MEMORY_TIER_LADDER[-1]
    assert top.machine_type == "n2-highmem-32"
    assert top.ram_gb == 256
    assert gce_machine_ram_gb("n2-highmem-32") == 256
    assert escalation._OOM_TOP_MACHINE == "n2-highmem-32"


def test_oom_relaunch_passes_bigger_machine_env(monkeypatch):
    captured: dict[str, object] = {}

    class _FakeActuator:
        def relaunch(self, vm_name, *, exit_code, launcher, asset_group="", launcher_env=None, dry_run=False):
            captured["launcher_env"] = launcher_env
            return {"status": "SUCCEEDED"}

    fake_mod = type("M", (), {"RelaunchBackfillVm": _FakeActuator})
    monkeypatch.setattr(escalation, "_ACTUATORS_AVAILABLE", True)
    monkeypatch.setattr(escalation.importlib, "import_module", lambda _name: fake_mod)
    finding = PipelineFinding(
        event="DP_VM_EXIT_NONZERO",
        severity="CRITICAL",
        tier=EscalationTier.AUTO_RECOVER,
        summary="OOM",
        details={
            "vm_name": "mtds-backfill-sports-x",
            "exit_code": 137,
            "relaunch_launcher": "launch-mtds-backfill-vm.sh",
            "machine_type": "e2-standard-8",
            "bigger_machine": True,
        },
    )
    out = escalation._recover_backfill_vm(finding, dry_run=False)
    assert out["recovered"] is True
    # e2-standard-8 (32 GB) → next canonical ladder rung n2-standard-16 (64 GB).
    assert captured["launcher_env"] == {"MACHINE_TYPE": "n2-standard-16"}
    assert out["escalated_machine_type"] == "n2-standard-16"


# ── JSON-sentinel freshness on a bare-last_modified bucket (regression 2026-06-23) ──
def _json_sentinel(ts_age_min: float) -> bytes:
    ts = (datetime.now(UTC) - timedelta(minutes=ts_age_min)).isoformat()
    return json.dumps({"mode": "exit-code", "ts": ts, "ok": True, "counts": {}}, sort_keys=True).encode()


def test_blob_age_minutes_reads_json_ts_when_last_modified_bare():
    """A fresh JSON sentinel with BARE metadata (age_min=None) reads its content `ts`,
    not None — the deployment-scripts-* `last_modified=None` quirk previously made every
    JSON sentinel read `age=None` → false 'sentinel stale: missing (never ran)'."""
    blob = "vm-census/exit-code-last-run.json"
    storage = FakeStorage({(LOG_BUCKET, blob): (_json_sentinel(3.0), None)})  # None ⇒ bare metadata
    age = _gcs.blob_age_minutes(storage, LOG_BUCKET, blob)
    assert age is not None
    assert 2.0 < age < 5.0


def test_monitor_sentinel_fresh_json_not_stale():
    """probe_freshness on a monitor_cron_target reads a fresh-but-bare-metadata JSON sentinel
    as FRESH (the exact deadman false-page path)."""
    targets = meta_watchers.monitor_cron_targets(LOG_BUCKET)
    target = next(t for t in targets if t.label == "dp-exit-code-monitor")
    storage = FakeStorage({(LOG_BUCKET, target.blob_path): (_json_sentinel(1.0), None)})
    result = meta_watchers.probe_freshness(storage, target)
    assert result.age_min is not None
    assert not result.stale


def test_blob_age_minutes_still_reads_epoch_sidecar():
    """The epoch-sidecar shape (heartbeat `<epoch>\\n<rc>\\n<status>`) still works — the JSON
    fallback is additive, not a replacement."""
    blob = "vm-heartbeat/some-vm.txt"
    epoch = int((datetime.now(UTC) - timedelta(minutes=2.0)).timestamp())
    storage = FakeStorage({(LOG_BUCKET, blob): (f"{epoch}\n0\nrunning\n".encode(), None)})
    age = _gcs.blob_age_minutes(storage, LOG_BUCKET, blob)
    assert age is not None
    assert 1.0 < age < 4.0


# ── Sidecar-authoritative heartbeat (FIX 1/1b, 2026-06-24) ──────────────────────
def test_sweep_sidecar_fresh_overrides_laggy_runlog(monkeypatch):
    """Healthy-slow VM: run.log PIPELINE_HEARTBEAT marker laggy (60m) but the FRESH
    sidecar (2m) is authoritative → ALIVE, no false DP_VM_STALL (the flood fix)."""
    vm = "tradfi-bf-cme-6e-2025"
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (_pipeline_hb_runlog(vm, marker_age_min=60.0), None)})
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
        sidecar_age_reader=lambda _vm: 2.0,  # FRESH sidecar = host alive
        asset_group_for_vm=lambda _vm: "tradfi",
        stall_minutes=10,
    )
    assert results[0].verdict is heartbeat_stall_watcher.LivenessVerdict.ALIVE
    assert not any(e[0] == "DP_VM_STALL" for e in emitted)


def test_sweep_sidecar_stale_stalls_and_autokills(monkeypatch):
    """Sidecar STALE (host/network wedged) + not capturing → STALL, and the
    sidecar-gated auto-kill fires (vm_killer called)."""
    vm = "tradfi-bf-cme-6z-2025"
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (_pipeline_hb_runlog(vm, marker_age_min=60.0), None)})
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.heartbeat_stall_watcher.log_event",
        lambda *a, **k: None,
    )
    killed: list[str] = []
    results = heartbeat_stall_watcher.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[(vm, "asia-northeast1-c")],
        vm_age_reader=lambda _n, _z: 120.0,
        captured_reader=lambda _vm: 0,
        sidecar_age_reader=lambda _vm: 60.0,  # STALE sidecar → host wedged
        asset_group_for_vm=lambda _vm: "tradfi",
        umbrella_for_vm=lambda _vm: "batch",
        vm_killer=lambda name, _zone: bool(killed.append(name)) or True,
        stall_minutes=10,
        kill_minutes=45,
    )
    assert results[0].verdict is heartbeat_stall_watcher.LivenessVerdict.STALL
    assert killed == [vm]  # sidecar 60m >= kill_minutes 45 → reaped


def test_should_auto_kill_fresh_sidecar_never_kills():
    """Fresh sidecar (heartbeat_age_min < kill_minutes) ⇒ is_vm_progressing True ⇒
    NEVER reaped even with a STALL verdict (hung-worker-on-live-host = alert-only)."""
    res = heartbeat_stall_watcher.LivenessResult(
        vm_name="vm",
        verdict=heartbeat_stall_watcher.LivenessVerdict.STALL,
        heartbeat_age_min=2.0,  # fresh sidecar
        captured_flat=False,
        run_log_age_min=95.0,
    )
    assert not heartbeat_stall_watcher.should_auto_kill(res, is_backfill=True, umbrella="batch", kill_minutes=45.0)


def test_should_auto_kill_stale_sidecar_kills():
    """Stale sidecar (heartbeat_age_min ≥ kill_minutes) + STALL + backfill ⇒ reaped."""
    res = heartbeat_stall_watcher.LivenessResult(
        vm_name="vm",
        verdict=heartbeat_stall_watcher.LivenessVerdict.STALL,
        heartbeat_age_min=60.0,  # stale sidecar
        captured_flat=False,
        run_log_age_min=None,
    )
    assert heartbeat_stall_watcher.should_auto_kill(res, is_backfill=True, umbrella="batch", kill_minutes=45.0)


def test_wave_launcher_host_cron_sentinel_fresh_not_stale():
    """FIX 2: the wave-launcher host-cron sentinel (fresh JSON ts, BARE metadata)
    probes FRESH via the JSON-ts path; the target carries NO cloud_run_job so it is
    never cross-checked against Cloud Run history (a host cron is invisible there)."""
    target = meta_watchers.FreshnessTarget(
        bucket=LOG_BUCKET,
        blob_path="vm-census/wave-launcher-last-run.json",
        max_age_min=360.0,
        label="tradfi-wave-launcher",
    )
    storage = FakeStorage({(LOG_BUCKET, target.blob_path): (_json_sentinel(30.0), None)})
    result = meta_watchers.probe_freshness(storage, target)
    assert result.age_min is not None
    assert not result.stale
    assert target.cloud_run_job == ""


# ── RESOLVED bookend generalized to heartbeat/exit-code (alert-lifecycle, 2026-06-24) ──
def test_reconcile_resolved_per_mode_blob_and_emitted(monkeypatch):
    """A prior-active alert NOT in this sweep's emitted set posts a ✅ RESOLVED, using a
    per-mode active blob + an injected emitted set (the heartbeat/exit-code path)."""
    blob = "vm-census/active-dp-alerts-heartbeat.json"
    prior = json.dumps({"DP_VM_STALL::vm-a": "DP_VM_STALL", "DP_VM_STALL::vm-b": "DP_VM_STALL"}).encode()
    storage = FakeStorage({(LOG_BUCKET, blob): (prior, 1.0)})
    emitted_events: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.meta_watchers.log_event",
        lambda event, severity="INFO", details=None: emitted_events.append((event, severity)),
    )
    # This sweep only re-fired vm-b → vm-a cleared → RESOLVED.
    resolved = meta_watchers.reconcile_resolved(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        active_blob=blob,
        emitted={"DP_VM_STALL::vm-b": "DP_VM_STALL"},
    )
    assert resolved == ["DP_VM_STALL::vm-a"]
    assert ("DP_VM_STALL", "INFO") in emitted_events  # the ✅ RESOLVED INFO bookend
    # The persisted active set is now exactly this sweep's emissions (vm-b only).
    assert (
        storage.uploaded[(LOG_BUCKET, blob)]
        == json.dumps({"DP_VM_STALL::vm-b": "DP_VM_STALL"}, sort_keys=True).encode()
    )


def test_heartbeat_sweep_populates_finding_sink(monkeypatch):
    """The heartbeat sweep appends each fired finding to finding_sink so the cli can
    reconcile the RESOLVED bookend (a stalled VM's finding is captured)."""
    vm = "tradfi-bf-cme-6z-2025"
    storage = FakeStorage({(LOG_BUCKET, _run_log_blob(vm)): (_pipeline_hb_runlog(vm, marker_age_min=60.0), None)})
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda *a, **k: None,
    )
    sink: list = []
    heartbeat_stall_watcher.sweep(
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=[(vm, "asia-northeast1-c")],
        vm_age_reader=lambda _n, _z: 120.0,
        captured_reader=lambda _vm: 0,
        sidecar_age_reader=lambda _vm: 60.0,  # stale sidecar → STALL finding
        asset_group_for_vm=lambda _vm: "tradfi",
        finding_sink=sink,
        stall_minutes=10,
    )
    assert len(sink) == 1
    assert meta_watchers.alert_key(sink[0]).startswith("DP_VM_STALL::")


# ── DP-VM-007: check_cloud_run_image_freshness ────────────────────────────────

_STALE_JOB = "deployment-service-job"
_STALE_SERVICE = "deployment-service"
# Two different digests — stale condition.
_RUNNING_DIGEST = "asia-northeast1-docker.pkg.dev/test-project/trading-system/deployment-service@sha256:a41ad9f7aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_LATEST_DIGEST = "sha256:f739a41bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
# Same digest — fresh condition.
_FRESH_DIGEST = "asia-northeast1-docker.pkg.dev/test-project/trading-system/deployment-service@sha256:f739a41bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def _make_stale_image_readers(
    *,
    running: str | None,
    latest: str | None,
) -> tuple[stale_image_watcher.ImageDigestReader, stale_image_watcher.LatestDigestReader]:
    return (lambda _p, _l, _j: running), (lambda _p, _l, _r, _i: latest)


def test_stale_image_emits_warn(monkeypatch):
    """Running digest != latest digest → DP_CLOUD_RUN_STALE_IMAGE WARN emitted."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    image_reader, latest_reader = _make_stale_image_readers(running=_RUNNING_DIGEST, latest=_LATEST_DIGEST)
    results = stale_image_watcher.check_cloud_run_image_freshness(
        job_names=[_STALE_JOB],
        job_to_service=lambda _: _STALE_SERVICE,
        image_digest_reader=image_reader,
        latest_digest_reader=latest_reader,
        project_id="test-project",
        location="asia-northeast1",
        artifact_repository="trading-system",
        dry_run=False,
    )
    assert len(results) == 1
    assert results[0].stale is True
    assert results[0].skipped is False
    assert any(e[0] == "DP_CLOUD_RUN_STALE_IMAGE" and e[1] == "WARN" for e in emitted), emitted


def test_fresh_image_no_alert(monkeypatch):
    """Running digest == latest digest → no alert emitted."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    image_reader, latest_reader = _make_stale_image_readers(running=_FRESH_DIGEST, latest=_LATEST_DIGEST)
    results = stale_image_watcher.check_cloud_run_image_freshness(
        job_names=[_STALE_JOB],
        job_to_service=lambda _: _STALE_SERVICE,
        image_digest_reader=image_reader,
        latest_digest_reader=latest_reader,
        project_id="test-project",
        location="asia-northeast1",
        artifact_repository="trading-system",
        dry_run=False,
    )
    assert len(results) == 1
    assert results[0].stale is False
    assert "DP_CLOUD_RUN_STALE_IMAGE" not in emitted


def test_missing_latest_digest_no_false_alert(monkeypatch):
    """Latest digest unavailable (API miss) → no false alert (skip the job)."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    image_reader, latest_reader = _make_stale_image_readers(running=_RUNNING_DIGEST, latest=None)
    results = stale_image_watcher.check_cloud_run_image_freshness(
        job_names=[_STALE_JOB],
        job_to_service=lambda _: _STALE_SERVICE,
        image_digest_reader=image_reader,
        latest_digest_reader=latest_reader,
        project_id="test-project",
        location="asia-northeast1",
        artifact_repository="trading-system",
        dry_run=False,
    )
    assert len(results) == 1
    assert results[0].skipped is True
    assert results[0].stale is False
    assert "DP_CLOUD_RUN_STALE_IMAGE" not in emitted


def test_missing_running_digest_no_false_alert(monkeypatch):
    """Running digest unavailable (job not found) → no false alert (skip the job)."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    image_reader, latest_reader = _make_stale_image_readers(running=None, latest=_LATEST_DIGEST)
    results = stale_image_watcher.check_cloud_run_image_freshness(
        job_names=[_STALE_JOB],
        job_to_service=lambda _: _STALE_SERVICE,
        image_digest_reader=image_reader,
        latest_digest_reader=latest_reader,
        project_id="test-project",
        location="asia-northeast1",
        artifact_repository="trading-system",
        dry_run=False,
    )
    assert len(results) == 1
    assert results[0].skipped is True
    assert "DP_CLOUD_RUN_STALE_IMAGE" not in emitted


def test_stale_image_consecutive_miss_suppresses_first_sweep(monkeypatch):
    """First stale probe suppressed by consecutive-miss gate (min_consecutive=2)."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    image_reader, latest_reader = _make_stale_image_readers(running=_RUNNING_DIGEST, latest=_LATEST_DIGEST)
    storage = FakeStorage({})
    t1 = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    stale_image_watcher.check_cloud_run_image_freshness(
        job_names=[_STALE_JOB],
        job_to_service=lambda _: _STALE_SERVICE,
        image_digest_reader=image_reader,
        latest_digest_reader=latest_reader,
        project_id="test-project",
        location="asia-northeast1",
        artifact_repository="trading-system",
        dry_run=False,
        miss_tracker=t1,
        min_consecutive=2,
    )
    assert "DP_CLOUD_RUN_STALE_IMAGE" not in emitted, "first stale sweep must be suppressed"
    t1.persist()
    # Second sweep → should fire.
    t2 = meta_watchers.MissTracker.load(storage_client=storage, log_bucket=LOG_BUCKET)
    stale_image_watcher.check_cloud_run_image_freshness(
        job_names=[_STALE_JOB],
        job_to_service=lambda _: _STALE_SERVICE,
        image_digest_reader=image_reader,
        latest_digest_reader=latest_reader,
        project_id="test-project",
        location="asia-northeast1",
        artifact_repository="trading-system",
        dry_run=False,
        miss_tracker=t2,
        min_consecutive=2,
    )
    assert "DP_CLOUD_RUN_STALE_IMAGE" in emitted, "second consecutive stale sweep must page"


def test_extract_digest_from_full_uri():
    """_extract_digest correctly pulls sha256:... from a full image URI."""
    uri = "asia-northeast1-docker.pkg.dev/proj/repo/service@sha256:abcdef1234"
    assert stale_image_watcher._extract_digest(uri) == "sha256:abcdef1234"


def test_extract_digest_bare():
    """_extract_digest passes through a bare sha256:... string."""
    assert stale_image_watcher._extract_digest("sha256:abcdef1234") == "sha256:abcdef1234"


def test_extract_digest_tag_only_returns_empty():
    """_extract_digest returns empty string for a tag-only image ref (no digest)."""
    assert stale_image_watcher._extract_digest("asia.pkg.dev/proj/repo/service:latest") == ""


def test_stale_image_dry_run_does_not_emit(monkeypatch):
    """dry_run=True → finding is detected but log_event is NOT called."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    image_reader, latest_reader = _make_stale_image_readers(running=_RUNNING_DIGEST, latest=_LATEST_DIGEST)
    results = stale_image_watcher.check_cloud_run_image_freshness(
        job_names=[_STALE_JOB],
        job_to_service=lambda _: _STALE_SERVICE,
        image_digest_reader=image_reader,
        latest_digest_reader=latest_reader,
        project_id="test-project",
        location="asia-northeast1",
        artifact_repository="trading-system",
        dry_run=True,
    )
    assert results[0].stale is True
    assert "DP_CLOUD_RUN_STALE_IMAGE" not in emitted


def test_stale_image_no_service_skips_job(monkeypatch):
    """job_to_service returns None → job skipped, no result emitted."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    image_reader, latest_reader = _make_stale_image_readers(running=_RUNNING_DIGEST, latest=_LATEST_DIGEST)
    results = stale_image_watcher.check_cloud_run_image_freshness(
        job_names=["unknown-job"],
        job_to_service=lambda _: None,  # no mapping
        image_digest_reader=image_reader,
        latest_digest_reader=latest_reader,
        project_id="test-project",
        location="asia-northeast1",
        artifact_repository="trading-system",
        dry_run=False,
    )
    assert results == []
    assert "DP_CLOUD_RUN_STALE_IMAGE" not in emitted


def test_stale_image_flags_prod_2026_06_23_scenario():
    """Sanity-check: the concrete 2026-06-26 prod scenario (instruments-service on
    the 2026-06-23 image missing f739a41/d279615) would flag as stale.

    Running image: ...@sha256:a41ad9f7... (2026-06-23 build, the stale one).
    Latest image:  sha256:f739a41b...   (the HEAD build with the fixes).
    Expected: stale=True, running_norm != latest_norm.
    """
    running = "asia-northeast1-docker.pkg.dev/test-project/trading-system/instruments-service@sha256:a41ad9f7aaa"
    latest = "sha256:f739a41bbbb"

    def _image_reader(_p: str, _l: str, _j: str) -> str | None:
        return running

    def _latest_reader(_p: str, _l: str, _r: str, _i: str) -> str | None:
        return latest

    results = stale_image_watcher.check_cloud_run_image_freshness(
        job_names=["instruments-service-t1-recon"],
        job_to_service=lambda _: "instruments-service",
        image_digest_reader=_image_reader,
        latest_digest_reader=_latest_reader,
        project_id="test-project",
        location="asia-northeast1",
        artifact_repository="trading-system",
        dry_run=True,  # dry_run: only verify the verdict, don't emit
    )
    assert len(results) == 1
    result = results[0]
    assert result.stale is True, (
        "prod scenario: instruments-service on 2026-06-23 image (a41ad9f7) != latest (f739a41b) "
        "must flag stale — this is the concrete prod issue the alert was built to catch"
    )


# ── DP-LIVE-002 tests ─────────────────────────────────────────────────────────


def _make_manifest_parquet(
    *,
    capture_status: str = "captured",
    venue: str = "POLYMARKET",
    data_type: str = "book_snapshot_5",
    date_iso: str = "2026-06-26",
    row_count: int = 937,
    written_at: str = "2026-06-26T20:22:08+00:00",
) -> bytes:
    import io

    import pandas as pd

    df = pd.DataFrame(
        [
            {
                "capture_status": capture_status,
                "venue": venue,
                "data_type": data_type,
                "date": date_iso,
                "row_count": row_count,
                "written_at": pd.Timestamp(written_at, tz="UTC"),
            }
        ]
    )
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


class _FakeStorageLswMismatch:
    """Fake StorageClient for DP-LIVE-002 tests."""

    def __init__(self, *, manifest_bytes: bytes, gcs_file_count: int, min_age_secs: int = 0) -> None:
        self._manifest_bytes = manifest_bytes
        self._gcs_file_count = gcs_file_count
        self._min_age_secs = min_age_secs

    def blob_exists(self, _bucket: str, _blob: str) -> bool:
        return True

    def download_bytes(self, _bucket: str, _blob: str) -> bytes:
        return self._manifest_bytes

    def list_blobs(self, _bucket: str, prefix: str = "") -> list[object]:
        if not prefix.startswith("raw_tick_data/"):
            return []

        class _Blob:
            def __init__(self, name: str) -> None:
                self.name = name

        venue = "venue=POLYMARKET"
        data_type = "data_type=book_snapshot_5"
        return [
            _Blob(f"raw_tick_data/by_date/day=2026-06-26/{venue}/{data_type}/instr{i}.parquet")
            for i in range(self._gcs_file_count)
        ]


def test_dp_live_002_fires_when_captured_but_gcs_empty() -> None:
    """DP-LIVE-002 fires when manifest shows captured rows but GCS has 0 files (the Plan04 bug)."""
    import io
    from datetime import UTC, datetime, timedelta

    import pandas as pd

    manifest = _make_manifest_parquet(
        capture_status="captured",
        written_at=(datetime.now(UTC) - timedelta(hours=2)).isoformat(),
    )
    storage = _FakeStorageLswMismatch(manifest_bytes=manifest, gcs_file_count=0)
    shard = live_stream_watcher.LiveVmShard(
        vm_name="prediction-live-polymarket-book-snapshot-5-20260626-201038",
        bucket="market-data-tick-pred-prd",
        blob_path="_index/per_vm/prediction-live-polymarket-book-snapshot-5-20260626-201038.parquet",
    )
    findings: list[live_stream_watcher.LiveVmShard] = []

    def _emit(finding: PipelineFinding, *, pm_repo_path: str | None, dry_run: bool) -> None:  # noqa: ARG001
        findings.append(finding)  # type: ignore[arg-type]

    import unittest.mock as mock

    with mock.patch(
        "deployment_service.data_pipeline_monitors.live_stream_watcher.emit_finding",
        side_effect=_emit,
    ):
        result = live_stream_watcher.check_live_stream_gcs_write_mismatch(
            storage_client=storage,  # type: ignore[arg-type]
            shards=[shard],
            min_age_hours=0.5,
        )
    assert len(result) == 1
    assert result[0].vm_name == shard.vm_name
    assert len(findings) == 1
    finding = findings[0]
    assert finding.registry_id == "DP-LIVE-002"  # type: ignore[attr-defined]
    assert "gcs-mismatch" in finding.details["label"]  # type: ignore[attr-defined]


def test_dp_live_002_no_fire_when_gcs_has_files() -> None:
    """DP-LIVE-002 does NOT fire when captured rows AND GCS files both exist."""
    from datetime import UTC, datetime, timedelta

    manifest = _make_manifest_parquet(
        capture_status="captured",
        written_at=(datetime.now(UTC) - timedelta(hours=2)).isoformat(),
    )
    storage = _FakeStorageLswMismatch(manifest_bytes=manifest, gcs_file_count=26)
    shard = live_stream_watcher.LiveVmShard(
        vm_name="prediction-live-polymarket-book-snapshot-5-ok",
        bucket="market-data-tick-pred-prd",
        blob_path="_index/per_vm/prediction-live-polymarket-book-snapshot-5-ok.parquet",
    )
    result = live_stream_watcher.check_live_stream_gcs_write_mismatch(
        storage_client=storage,  # type: ignore[arg-type]
        shards=[shard],
        min_age_hours=0.5,
    )
    assert len(result) == 0


def test_dp_live_002_no_fire_when_all_empty_confirmed() -> None:
    """DP-LIVE-002 does NOT fire when all rows are empty_confirmed (expected for illiquid markets)."""
    from datetime import UTC, datetime, timedelta

    manifest = _make_manifest_parquet(
        capture_status="empty_confirmed",
        written_at=(datetime.now(UTC) - timedelta(hours=2)).isoformat(),
    )
    storage = _FakeStorageLswMismatch(manifest_bytes=manifest, gcs_file_count=0)
    shard = live_stream_watcher.LiveVmShard(
        vm_name="prediction-live-polymarket-book-snapshot-5-empty",
        bucket="market-data-tick-pred-prd",
        blob_path="_index/per_vm/prediction-live-polymarket-book-snapshot-5-empty.parquet",
    )
    result = live_stream_watcher.check_live_stream_gcs_write_mismatch(
        storage_client=storage,  # type: ignore[arg-type]
        shards=[shard],
        min_age_hours=0.5,
    )
    assert len(result) == 0


def test_dp_live_002_suppressed_for_new_vms() -> None:
    """DP-LIVE-002 does NOT fire when VM is younger than min_age_hours (startup grace period)."""
    from datetime import UTC, datetime, timedelta

    manifest = _make_manifest_parquet(
        capture_status="captured",
        written_at=(datetime.now(UTC) - timedelta(minutes=5)).isoformat(),
    )
    storage = _FakeStorageLswMismatch(manifest_bytes=manifest, gcs_file_count=0)
    shard = live_stream_watcher.LiveVmShard(
        vm_name="prediction-live-polymarket-book-snapshot-5-new",
        bucket="market-data-tick-pred-prd",
        blob_path="_index/per_vm/prediction-live-polymarket-book-snapshot-5-new.parquet",
    )
    result = live_stream_watcher.check_live_stream_gcs_write_mismatch(
        storage_client=storage,  # type: ignore[arg-type]
        shards=[shard],
        min_age_hours=1.0,
    )
    assert len(result) == 0
