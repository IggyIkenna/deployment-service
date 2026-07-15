"""Ad-hoc single-VM liveness check (DP-VM-006).

Gives the manual "VM roster + run.log tail" check-in convention (used across
long-running backfill-plan dispatches) a measured ALIVE/STALL verdict for ONE
VM instead of an agent eyeballing raw timestamps by hand — closing the
zombie-VM blind spot where a RUNNING instance with a dead worker process went
undetected for ~4h44m (drift_v2_sig_index_program_wide_helius_oom_2026_07_15).

Reuses the SAME tested ``heartbeat_stall_watcher.classify_vm_liveness`` the
periodic Cloud Run fleet sweep uses (via ``sweep()`` given a single-VM
census) — never re-derives the staleness thresholds. ``dry_run`` is always
True: this path only reports a verdict, it never alerts or auto-kills.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from unified_trading_library import StorageClient

from deployment_service.data_pipeline_monitors import heartbeat_stall_watcher
from deployment_service.data_pipeline_monitors.heartbeat_stall_watcher import LivenessResult

logger = logging.getLogger(__name__)


def check_vm(
    vm_name: str,
    *,
    storage_client: StorageClient,
    log_bucket: str,
    running_vms: list[tuple[str, str]],
    captured_reader: Callable[[str], int],
    asset_group_for_vm: Callable[[str], str],
    stall_minutes: float,
    pm_repo_path: str | None,
    shard_mtime_reader: Callable[[str], float | None] | None = None,
    sidecar_age_reader: Callable[[str], float | None] | None = None,
    launcher_for_vm: Callable[[str], str] | None = None,
    umbrella_for_vm: Callable[[str], str] | None = None,
) -> LivenessResult | None:
    """Liveness verdict for ONE RUNNING VM, or ``None`` if it isn't RUNNING.

    A ``None`` return means the VM is already TERMINATED (or the name is
    wrong) — that is NOT the zombie case this check exists for (a zombie is
    RUNNING with a dead worker), so callers should treat it as benign.
    """
    match = next((vm for vm in running_vms if vm[0] == vm_name), None)
    if match is None:
        return None
    results = heartbeat_stall_watcher.sweep(
        storage_client=storage_client,
        log_bucket=log_bucket,
        running_vms=[match],
        # Caller already knows it's RUNNING — never gate on grace/TOO_YOUNG.
        vm_age_reader=lambda _name, _zone: heartbeat_stall_watcher.DEFAULT_GRACE_MINUTES * 100,
        captured_reader=captured_reader,
        shard_mtime_reader=shard_mtime_reader,
        sidecar_age_reader=sidecar_age_reader,
        asset_group_for_vm=asset_group_for_vm,
        launcher_for_vm=launcher_for_vm,
        umbrella_for_vm=umbrella_for_vm,
        finding_sink=None,
        vm_killer=None,
        prior_captured={},
        stall_minutes=stall_minutes,
        pm_repo_path=pm_repo_path,
        dry_run=True,
    )
    return results[0]


def run_check_vm_mode(
    vm_name: str,
    *,
    storage_client: StorageClient,
    log_bucket: str,
    running_vms: list[tuple[str, str]],
    captured_reader: Callable[[str], int],
    asset_group_for_vm: Callable[[str], str],
    stall_minutes: float,
    pm_repo_path: str | None,
    shard_mtime_reader: Callable[[str], float | None] | None = None,
    sidecar_age_reader: Callable[[str], float | None] | None = None,
    launcher_for_vm: Callable[[str], str] | None = None,
    umbrella_for_vm: Callable[[str], str] | None = None,
) -> int:
    """CLI-mode wrapper: runs :func:`check_vm`, logs the verdict, returns an
    exit code (0 alive/not_running, 1 stall/starved) so a shell-scripted
    check-in can branch on it without parsing log text."""
    result = check_vm(
        vm_name,
        storage_client=storage_client,
        log_bucket=log_bucket,
        running_vms=running_vms,
        captured_reader=captured_reader,
        shard_mtime_reader=shard_mtime_reader,
        sidecar_age_reader=sidecar_age_reader,
        asset_group_for_vm=asset_group_for_vm,
        launcher_for_vm=launcher_for_vm,
        umbrella_for_vm=umbrella_for_vm,
        stall_minutes=stall_minutes,
        pm_repo_path=pm_repo_path,
    )
    if result is None:
        logger.info("check-vm %s: not in RUNNING census (terminated / wrong name)", vm_name)
        return 0
    logger.info(
        "check-vm %s: verdict=%s heartbeat_age_min=%s run_log_age_min=%s progress_age_min=%s",
        vm_name,
        result.verdict.value,
        result.heartbeat_age_min,
        result.run_log_age_min,
        result.progress_age_min,
    )
    if result.verdict.value in ("stall", "event_loop_starved"):
        logger.warning(
            "check-vm %s: %s — RUNNING instance, no recent progress (possible zombie); investigate",
            vm_name,
            result.verdict.value,
        )
        return 1
    return 0
