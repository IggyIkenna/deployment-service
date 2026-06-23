"""Heartbeat-stall watcher (DP-VM-003 / DP-VM-004).

Consumes the WORKER-life liveness signal — the ``PIPELINE_HEARTBEAT`` marker the
data VM echoes into its tee'd ``run.log`` every 60s (the VM-life emitter wired in
``setup-data-pipeline-vm.sh _launch_with_tee``) — for EVERY running data VM
discovered from the compute API. A registered RUNNING data VM whose worker
heartbeat / progress is older than the stall threshold (or absent past grace) is
a silent stall — the "idle/hung VM emits nothing" gap.

**BUG2 (2026-06-22):** this watcher previously keyed liveness on the generic
infra ``vm-heartbeat/{vm}.txt`` sidecar, which the platform writes every 60s
regardless of whether the data worker is alive — so EVERY running VM read ALIVE
and a never-heartbeating / hung worker NEVER alerted (the operator's "zero alerts
in 1.5h" symptom). It now reads the worker's own ``PIPELINE_HEARTBEAT`` run.log
marker (decoupled from the always-fresh infra sidecar), so a VM whose data worker
died / never launched / has a broken heartbeat timer is caught.

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

# With the 60s BACKGROUND-TIMER heartbeat (UTL ``PipelineHeartbeatTimer``, every VM emits
# a PIPELINE_HEARTBEAT every ~60s INDEPENDENT of chunk/window boundaries —
# data_pipeline_hardening_self_monitoring_2026_06_22 Phase 2), a genuine hang is detected
# within ~one threshold + the ``*/5`` poll. 10 min = ~10 missed 60s heartbeats — tight
# enough to alert a mid-chunk hang fast, loose enough to tolerate the GCS-tee'd heartbeat
# blob lagging the on-VM emit by a minute or two + the */5 poll jitter (no false alarm on a
# healthy 60s heartbeat). Was 15.0 (coarse, sized for the old per-chunk-only cadence).
DEFAULT_STALL_MINUTES = 10.0
DEFAULT_GRACE_MINUTES = 10.0  # don't flag a VM in its first N minutes (boot/warmup)
# The run.log PROGRESS signal uses a SEPARATE, generous threshold (CLAUDE.md
# 2026-06-22): the GCS-tee'd run.log LAGS the on-VM log by minutes, and a healthy
# worker can legitimately go several minutes between log lines on a slow upstream
# fetch. A frozen run.log only signals a genuine hung-process stall once it is
# this far past its last advance — well beyond the heartbeat-blob staleness bound.
DEFAULT_RUN_LOG_STALL_MINUTES = 45.0


def _is_backfill_vm(vm_name: str) -> bool:
    """True for a BATCH backfill VM (logs continuously) vs a live-capture VM.

    Backfill VMs (``*-backfill-*``, ``tradfi-bf-*``, ``tm-backfill``,
    ``fs-backfill``) print a log line per date/league/chunk, so a FROZEN run.log
    is a meaningful hung-process signal. Live-capture VMs (``*-live-*``) stream
    over a WS and log sparsely — their run.log goes legitimately quiet, so the
    run.log-freshness signal is NOT applied to them (heartbeat-blob freshness is
    their liveness signal).
    """
    lowered = vm_name.lower()
    if "-live-" in lowered or lowered.endswith("-live"):
        return False
    return "backfill" in lowered or "-bf-" in lowered or lowered.startswith(("tradfi-bf", "tm-backfill", "fs-backfill"))


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
    run_log_age_min: float | None = None


def classify_vm_liveness(
    vm_name: str,
    *,
    vm_age_min: float,
    heartbeat_age_min: float | None,
    captured_flat: bool,
    run_log_age_min: float | None = None,
    is_backfill: bool = True,
    stall_minutes: float = DEFAULT_STALL_MINUTES,
    run_log_stall_minutes: float = DEFAULT_RUN_LOG_STALL_MINUTES,
    grace_minutes: float = DEFAULT_GRACE_MINUTES,
) -> LivenessResult:
    """Pure liveness classification. No I/O.

    Precedence:
      - VM younger than ``grace_minutes``                        → TOO_YOUNG (skip)
      - no heartbeat blob at all (``heartbeat_age_min is None``) → EVENT_LOOP_STARVED
      - heartbeat older than ``stall_minutes``                  → STALL
      - heartbeat fresh but the ``run.log`` is FROZEN past the
        generous ``run_log_stall_minutes`` (hung-process: bash heartbeat
        ticks but the worker made zero progress)               → STALL
      - heartbeat fresh, log advancing, but captured FLAT AND the
        log is itself stale past ``stall_minutes`` (corroborated
        no-progress, not just a between-window lull)            → STALL
      - otherwise                                                → ALIVE

    ``run_log_age_min`` is the PROGRESS signal (CLAUDE.md 2026-06-22 hung-process
    rule) on a SEPARATE generous threshold (the GCS-tee'd run.log lags the on-VM
    log by minutes — a tight bound false-flags a healthy slow fetch). ``None``
    (no parseable log) is NOT a hang (fail-safe: only flag a positively-measured
    stale log). ``captured_flat`` alone does NOT stall — a live-capture VM's
    per-VM shard count legitimately holds flat between ticks; it only contributes
    when CORROBORATED by a run.log that has also stopped advancing past the
    heartbeat threshold, so a genuine "alive but doing nothing" is still caught
    without false-flagging normal between-tick flatness.
    """
    if vm_age_min < grace_minutes:
        verdict = LivenessVerdict.TOO_YOUNG
    elif heartbeat_age_min is None:
        # No worker PIPELINE_HEARTBEAT marker. Two distinct causes that the marker
        # alone cannot tell apart: (a) the worker died / never launched (genuine
        # silence), or (b) the VM runs a PRE-heartbeat-tarball image whose worker
        # is alive but never emits the marker (the transition window — ~30 of 41
        # live VMs on 2026-06-22). Disambiguate via the run.log PROGRESS signal:
        #   - no log at all                         → total silence → EVENT_LOOP_STARVED
        #   - log frozen past run_log_stall_minutes → genuinely stalled → STALL
        #   - log still advancing                   → alive (old-tarball) → ALIVE
        # Without this fallback the watcher false-flags every healthy
        # pre-heartbeat-tarball VM as starved (29 false alerts) while the run.log
        # freshness correctly catches the genuinely-frozen ones (the 6.8h-dead
        # deribit/hyperliquid live VMs). Applied to ALL VMs (incl. live) in this
        # heartbeat-ABSENT branch — the live-sparse-logging exemption below only
        # guards the heartbeat-FRESH hung-process check.
        if run_log_age_min is None:
            verdict = LivenessVerdict.EVENT_LOOP_STARVED
        elif run_log_age_min > run_log_stall_minutes:
            verdict = LivenessVerdict.STALL
        else:
            verdict = LivenessVerdict.ALIVE
    elif heartbeat_age_min > stall_minutes:
        verdict = LivenessVerdict.STALL
    elif is_backfill and run_log_age_min is not None and run_log_age_min > run_log_stall_minutes:
        # Heartbeat fresh but the worker's own log froze → hung-process stall.
        # Backfill-only: a live-capture WS VM logs sparsely, so a quiet log with a
        # fresh heartbeat is normal (heartbeat freshness IS its liveness signal).
        verdict = LivenessVerdict.STALL
    elif captured_flat and run_log_age_min is not None and run_log_age_min > stall_minutes:
        # Captured flat AND the log has also stopped advancing past the heartbeat
        # bound → corroborated no-progress (alive-but-not-working).
        verdict = LivenessVerdict.STALL
    else:
        verdict = LivenessVerdict.ALIVE
    return LivenessResult(
        vm_name=vm_name,
        verdict=verdict,
        heartbeat_age_min=heartbeat_age_min,
        captured_flat=captured_flat,
        run_log_age_min=run_log_age_min,
    )


def _finding_for(
    result: LivenessResult,
    *,
    asset_group: str,
    stall_minutes: float,
    relaunch_launcher: str = "",
    umbrella: str = "",
) -> PipelineFinding | None:
    base: dict[str, object] = {
        "vm_name": result.vm_name,
        "asset_group": asset_group,
        "heartbeat_age_min": result.heartbeat_age_min,
        "run_log_age_min": result.run_log_age_min,
        "captured_flat": result.captured_flat,
        "stall_threshold_min": stall_minutes,
        # umbrella drives the alerting-service router channel split (LIVE →
        # #uts-live-alerts, BATCH → #data-pipeline-alerts). "" → batch default.
        "umbrella": umbrella,
        "cloud": "GCP",
    }
    # The launcher binding lets the DP_VM_STALL auto_recover actuator
    # (relaunch_stalled_vm) re-launch the watchdog-killed VM. Absent → the finding
    # falls through to file_issue (can't relaunch deterministically).
    if relaunch_launcher:
        base["relaunch_launcher"] = relaunch_launcher
    if result.verdict is LivenessVerdict.STALL:
        if result.heartbeat_age_min is not None:
            reason = f"heartbeat {result.heartbeat_age_min:.0f}m stale"
        elif result.run_log_age_min is not None:
            # heartbeat-absent + frozen run.log → genuinely-hung worker on a
            # pre-heartbeat-tarball VM (the 6.8h-dead live-capture class).
            reason = f"no heartbeat + run.log frozen {result.run_log_age_min:.0f}m"
        else:
            reason = "captured flat"
        return PipelineFinding(
            event=DP_VM_STALL,
            severity="WARN",
            tier=EscalationTier.AUTO_RECOVER,  # auto-kill+respawn then file issue
            summary=f"VM {result.vm_name} stalled — {reason}",
            details=base,
            registry_id="DP-VM-003",
        )
    if result.verdict is LivenessVerdict.EVENT_LOOP_STARVED:
        return PipelineFinding(
            event=DP_EVENT_LOOP_STARVED,
            severity="WARN",
            tier=EscalationTier.FILE_ISSUE,
            summary=(
                f"VM {result.vm_name} emitting NO PIPELINE_HEARTBEAT "
                "(data worker dead / never launched / heartbeat timer broken — silent VM)"
            ),
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
    launcher_for_vm: Callable[[str], str] | None = None,
    umbrella_for_vm: Callable[[str], str] | None = None,
    prior_captured: dict[str, int] | None = None,
    stall_minutes: float = DEFAULT_STALL_MINUTES,
    run_log_stall_minutes: float = DEFAULT_RUN_LOG_STALL_MINUTES,
    grace_minutes: float = DEFAULT_GRACE_MINUTES,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
) -> list[LivenessResult]:
    """Run one heartbeat-stall sweep over the RUNNING VM set.

    ``prior_captured`` (vm_name → last observed captured_cum) lets the watcher
    detect a FLAT captured count across ticks (alive-but-not-working). Callers
    persist + re-pass it; when omitted, captured-flat is not used (heartbeat age
    is the sole signal).

    ``launcher_for_vm`` (optional ``vm_name -> launcher-script-name`` resolver):
    when supplied, a ``DP_VM_STALL`` finding carries a ``relaunch_launcher``
    binding so the ``relaunch_stalled_vm`` auto_recover actuator can re-launch the
    watchdog-killed VM; absent it, the stall finding falls through to file_issue.
    """
    prior_captured = prior_captured or {}
    results: list[LivenessResult] = []
    for vm_name, zone in running_vms:
        try:
            vm_age = vm_age_reader(vm_name, zone)
        except Exception:
            vm_age = grace_minutes  # unknown age → treat as just-past-grace, defer
        # WORKER-life heartbeat (BUG2 fix, 2026-06-22): read the VM-life
        # PIPELINE_HEARTBEAT marker the data worker echoes into the tee'd run.log
        # every 60s — NOT the generic infra ``vm-heartbeat`` sidecar blob. The infra
        # sidecar ticks every 60s regardless of whether the data worker is alive /
        # progressing, so keying liveness off it made EVERY running VM read ALIVE →
        # a never-heartbeating worker NEVER alerted (the operator's "zero alerts in
        # 1.5h" symptom). The PIPELINE_HEARTBEAT marker only advances while the
        # worker's tee'd process tree is alive. ``None`` ⇒ NO worker heartbeat marker
        # at all (the worker died / never launched / the timer is broken) — the
        # genuine total-silence signal ``EVENT_LOOP_STARVED`` is meant for. Falls
        # back to the infra sidecar ONLY if the marker is absent AND the sidecar is
        # also absent (a VM that wrote neither) — but the marker is the authority.
        hb_age = _gcs.pipeline_heartbeat_age_minutes(storage_client, log_bucket, vm_name)
        # run.log progress signal — frozen log past threshold = hung-process stall
        # even when the bash heartbeat blob is fresh (CLAUDE.md 2026-06-22). ONLY
        # applies to BACKFILL VMs (they log continuously per date/league/chunk). A
        # LIVE-capture WS VM logs sparsely — its run.log goes legitimately quiet for
        # hours while the stream is healthy (captured climbs, heartbeat fresh) — so
        # the run.log-freshness signal would false-flag it. For live VMs the
        # heartbeat-blob freshness (the 60s timer) IS the liveness signal.
        # Compute run.log freshness for EVERY VM (not just backfill): it is the
        # fallback liveness signal in the heartbeat-ABSENT branch, so a frozen
        # live-capture VM running a pre-heartbeat tarball is still caught. The
        # live-sparse-logging exemption is enforced inside classify (the
        # ``is_backfill`` gate on the heartbeat-FRESH hung-process check only).
        is_backfill = _is_backfill_vm(vm_name)
        run_log_age = _gcs.run_log_age_minutes(storage_client, log_bucket, vm_name)
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
            run_log_age_min=run_log_age,
            is_backfill=is_backfill,
            stall_minutes=stall_minutes,
            run_log_stall_minutes=run_log_stall_minutes,
            grace_minutes=grace_minutes,
        )
        results.append(result)

        launcher = launcher_for_vm(vm_name) if launcher_for_vm is not None else ""
        umbrella = umbrella_for_vm(vm_name) if umbrella_for_vm is not None else ""
        finding = _finding_for(
            result,
            asset_group=asset_group_for_vm(vm_name),
            stall_minutes=stall_minutes,
            relaunch_launcher=launcher,
            umbrella=umbrella,
        )
        if finding is not None and not dry_run:
            route_finding(finding, pm_repo_path=pm_repo_path)
        if finding is not None:
            logger.warning("heartbeat_stall_watcher: %s verdict=%s hb_age=%s", vm_name, result.verdict, hb_age)

    return results
