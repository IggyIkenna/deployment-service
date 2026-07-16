"""Unit tests for the ad-hoc single-VM liveness check (DP-VM-006).

Credential-free + block-network safe: GCS I/O is injected via the shared
``FakeStorage`` test double. Covers the manual-check-in gap surfaced by
``drift_v2_sig_index_program_wide_helius_oom_2026_07_15`` — a RUNNING VM whose
worker died (OOM) leaves a frozen run.log with no sidecar/shard-mtime signal,
which must classify as STALL, not silently read as healthy.
"""

from __future__ import annotations

from deployment_service.data_pipeline_monitors import check_vm, heartbeat_stall_watcher
from tests.unit.test_data_pipeline_monitors import LOG_BUCKET, FakeStorage

_VM = "mtds-solana-drift-backfill"
_ZONE = "asia-northeast1-c"


def _check(storage: FakeStorage, *, running_vms: list[tuple[str, str]] | None = None):
    return check_vm.check_vm(
        _VM,
        storage_client=storage,
        log_bucket=LOG_BUCKET,
        running_vms=running_vms if running_vms is not None else [(_VM, _ZONE)],
        captured_reader=lambda _vm: 0,
        shard_mtime_reader=lambda _vm: None,
        sidecar_age_reader=lambda _vm: None,
        asset_group_for_vm=lambda _vm: "defi",
        launcher_for_vm=lambda _vm: "",
        umbrella_for_vm=lambda _vm: "batch",
        stall_minutes=heartbeat_stall_watcher.DEFAULT_STALL_MINUTES,
        pm_repo_path=None,
    )


def test_check_vm_not_in_running_census_returns_none():
    result = _check(FakeStorage({}), running_vms=[])
    assert result is None


def test_check_vm_alive_no_signals_yet():
    # No run.log, no sidecar, no shard mtime, no prior captured baseline — the
    # heartbeat-absent + no-run.log + not-flat branch resolves ALIVE (fail-safe:
    # only a positively-measured stale signal counts as a hang).
    result = _check(FakeStorage({}))
    assert result is not None
    assert result.verdict is heartbeat_stall_watcher.LivenessVerdict.ALIVE


def test_check_vm_stall_on_frozen_run_log_zombie():
    """The OOM-zombie shape: worker died mid-run, run.log's last progress line
    is old, no sidecar/shard-mtime blob at all (host-level signals never
    started or were never wired) — must classify STALL, never ALIVE."""
    storage = FakeStorage(
        {
            (LOG_BUCKET, f"vm-logs/{_VM}/run.log"): (
                b"2020-01-01 00:00:00,000 INFO last line before the OOM\n",
                None,
            )
        }
    )
    result = _check(storage)
    assert result is not None
    assert result.verdict is heartbeat_stall_watcher.LivenessVerdict.STALL
    assert result.run_log_age_min is not None
    assert result.run_log_age_min > heartbeat_stall_watcher.DEFAULT_RUN_LOG_STALL_MINUTES
