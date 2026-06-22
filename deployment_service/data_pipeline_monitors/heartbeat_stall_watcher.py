"""Heartbeat-stall watcher (DP-VM-003 / DP-VM-004).

Consumes the liveness signal emitted by running VMs via UTL
``emit_pipeline_heartbeat`` (``PIPELINE_HEARTBEAT``) and the durable 60s
heartbeat sidecar blob (``vm-heartbeat/{vm}.txt``). A registered RUNNING VM whose
last heartbeat / progress is older than the stall threshold is a silent stall —
the "idle/hung VM emits nothing" gap.

It sits BESIDE the zombie watchdog (does not fork it): the zombie watchdog KILLS
stale VMs (network-partition / wedged class), while this watcher emits the
graduated ``DP_VM_STALL`` (WARN, auto-recover tier) before the kill, and
``DP_EVENT_LOOP_STARVED`` (WARN) when the silence is total (no heartbeat blob at
all for a VM that should be heartbeating — the blocking-GCS-read-on-async-loop
class). It reuses the zombie watchdog's VM census (RUNNING set + per-VM shard
bucket) rather than re-implementing it.

Two stall flavours:
  - ``DP_VM_STALL``           : a heartbeat exists but is older than the stall
    threshold, OR the heartbeat is fresh but the per-VM captured count has gone
    FLAT across the progress window (alive-but-not-working).
  - ``DP_EVENT_LOOP_STARVED`` : a RUNNING VM past its grace age has NO heartbeat
    blob at all (total silence — the event-loop-starvation / blocking-read class).

Pure-function core (``classify_vm_liveness``) is unit-tested with synthetic
ages; the GCS reads are injected so the sweep is credential-free + block-network.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import StrEnum

from unified_trading_library import StorageClient
from unified_trading_library.events import DP_EVENT_LOOP_STARVED, DP_VM_STALL  # noqa: qg-deep-import

from deployment_service.data_pipeline_monitors import _gcs
from deployment_service.data_pipeline_monitors.escalation import (
    EscalationTier,
    PipelineFinding,
    route_finding,
)

logger = logging.getLogger(__name__)

DEFAULT_STALL_MINUTES = 15.0
DEFAULT_GRACE_MINUTES = 10.0  # don't flag a VM in its first N minutes (boot/warmup)


class LivenessVerdict(StrEnum):
    ALIVE = "alive"
    STALL = "stall"  # DP-VM-003
    EVENT_LOOP_STARVED = "event_loop_starved"  # DP-VM-004
    TOO_YOUNG = "too_young"


@dataclass(frozen=True)
class LivenessResult:
    vm_name: str
    verdict: LivenessVerdict
    heartbeat_age_min: float | None
    captured_flat: bool


def classify_vm_liveness(
    vm_name: str,
    *,
    vm_age_min: float,
    heartbeat_age_min: float | None,
    captured_flat: bool,
    stall_minutes: float = DEFAULT_STALL_MINUTES,
    grace_minutes: float = DEFAULT_GRACE_MINUTES,
) -> LivenessResult:
    """Pure liveness classification. No I/O.

    Precedence:
      - VM younger than ``grace_minutes``                        → TOO_YOUNG (skip)
      - no heartbeat blob at all (``heartbeat_age_min is None``) → EVENT_LOOP_STARVED
      - heartbeat older than ``stall_minutes``                  → STALL
      - heartbeat fresh but captured FLAT across the window      → STALL
      - otherwise                                                → ALIVE
    """
    if vm_age_min < grace_minutes:
        verdict = LivenessVerdict.TOO_YOUNG
    elif heartbeat_age_min is None:
        verdict = LivenessVerdict.EVENT_LOOP_STARVED
    elif heartbeat_age_min > stall_minutes or captured_flat:
        # heartbeat stale OR captured flat-across-window while alive → STALL.
        verdict = LivenessVerdict.STALL
    else:
        verdict = LivenessVerdict.ALIVE
    return LivenessResult(
        vm_name=vm_name,
        verdict=verdict,
        heartbeat_age_min=heartbeat_age_min,
        captured_flat=captured_flat,
    )


def _finding_for(result: LivenessResult, *, asset_group: str, stall_minutes: float) -> PipelineFinding | None:
    base: dict[str, object] = {
        "vm_name": result.vm_name,
        "asset_group": asset_group,
        "heartbeat_age_min": result.heartbeat_age_min,
        "captured_flat": result.captured_flat,
        "stall_threshold_min": stall_minutes,
    }
    if result.verdict is LivenessVerdict.STALL:
        return PipelineFinding(
            event=DP_VM_STALL,
            severity="WARN",
            tier=EscalationTier.AUTO_RECOVER,  # auto-kill+respawn then file issue
            summary=(
                f"VM {result.vm_name} stalled — "
                + (
                    f"heartbeat {result.heartbeat_age_min:.0f}m stale"
                    if result.heartbeat_age_min is not None
                    else "captured flat"
                )
            ),
            details=base,
            registry_id="DP-VM-003",
        )
    if result.verdict is LivenessVerdict.EVENT_LOOP_STARVED:
        return PipelineFinding(
            event=DP_EVENT_LOOP_STARVED,
            severity="WARN",
            tier=EscalationTier.FILE_ISSUE,
            summary=(f"VM {result.vm_name} emitting no heartbeat (event-loop starved / blocking read on async loop)"),
            details=base,
            registry_id="DP-VM-004",
        )
    return None


def sweep(
    *,
    storage_client: StorageClient,
    log_bucket: str,
    running_vms: Iterable[tuple[str, str]],
    vm_age_reader: Callable[[str, str], float],
    captured_reader: Callable[[str], int],
    asset_group_for_vm: Callable[[str], str],
    prior_captured: dict[str, int] | None = None,
    stall_minutes: float = DEFAULT_STALL_MINUTES,
    grace_minutes: float = DEFAULT_GRACE_MINUTES,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
) -> list[LivenessResult]:
    """Run one heartbeat-stall sweep over the RUNNING VM set.

    ``prior_captured`` (vm_name → last observed captured_cum) lets the watcher
    detect a FLAT captured count across ticks (alive-but-not-working). Callers
    persist + re-pass it; when omitted, captured-flat is not used (heartbeat age
    is the sole signal).
    """
    prior_captured = prior_captured or {}
    results: list[LivenessResult] = []
    for vm_name, zone in running_vms:
        try:
            vm_age = vm_age_reader(vm_name, zone)
        except Exception:
            vm_age = grace_minutes  # unknown age → treat as just-past-grace, defer
        hb_age = _gcs.blob_age_minutes(storage_client, log_bucket, _gcs.HEARTBEAT_BLOB.format(vm=vm_name))
        try:
            captured_now = captured_reader(vm_name)
        except Exception:
            captured_now = prior_captured.get(vm_name, 0)
        captured_flat = vm_name in prior_captured and captured_now <= prior_captured[vm_name]

        result = classify_vm_liveness(
            vm_name,
            vm_age_min=vm_age,
            heartbeat_age_min=hb_age,
            captured_flat=captured_flat,
            stall_minutes=stall_minutes,
            grace_minutes=grace_minutes,
        )
        results.append(result)

        finding = _finding_for(result, asset_group=asset_group_for_vm(vm_name), stall_minutes=stall_minutes)
        if finding is not None and not dry_run:
            route_finding(finding, pm_repo_path=pm_repo_path)
        if finding is not None:
            logger.warning("heartbeat_stall_watcher: %s verdict=%s hb_age=%s", vm_name, result.verdict, hb_age)

    return results
