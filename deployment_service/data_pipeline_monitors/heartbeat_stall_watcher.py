"""Heartbeat-stall watcher (DP-VM-003 / DP-VM-004).

Consumes the WORKER-life liveness signal — the ``PIPELINE_HEARTBEAT`` marker the
data VM echoes into its tee'd ``run.log`` every 60s (the VM-life emitter wired in
``setup-data-pipeline-vm.sh _launch_with_tee``) — for EVERY running data VM
discovered from the compute API. A registered RUNNING data VM whose worker
heartbeat / progress is older than the stall threshold (or absent past grace) is
a silent stall — the "idle/hung VM emits nothing" gap.

**BUG2 (2026-06-22) → REVISED (2026-06-24, tradfi-fleet incident).** History:
this watcher first keyed liveness on the infra ``vm-heartbeat/{vm}.txt`` sidecar;
BUG2 switched it to the worker's ``PIPELINE_HEARTBEAT`` run.log marker because the
sidecar (a separate ``nohup`` on the VM) keeps ticking if only the *worker* dies
while the *host* lives → a dead worker read ALIVE ("zero alerts in 1.5h"). But the
PIPELINE_HEARTBEAT marker rides the **GCS-tee'd run.log, which lags the on-VM log
by 42-78m** (verified 2026-06-24: 704 fresh sidecar blobs <2m old while their run.log
tees trailed 42-78m), so keying the STALL/auto-kill on it false-flagged every
healthy-but-slow VM → the ``DP_VM_STALL`` flood + (worse) a live auto-kill foot-gun.

REVISED MODEL — three signals, each with a distinct (freshness, fidelity) tradeoff:
  * **sidecar blob** (``vm-heartbeat/{vm}.txt``, FRESH GCS channel, 60s) → proves the
    VM *host/network* is alive. Goes stale ONLY when the host/network wedges (the
    18:00 wave: both sidecar AND run.log stale) — the genuinely-reapable hang. This
    is now the **authoritative ``heartbeat_age_min``** (a fresh sidecar ⇒ never STALL
    on tee-lag, never auto-killed).
  * **run.log advancing** (LAGGY tee, generous ``run_log_stall_minutes`` >> max tee
    lag) → the hung-*worker*-on-a-live-host corroborator. A fresh sidecar + a run.log
    frozen past the generous bound = a worker hung while the host lives → STALL
    **alert-only** (NOT auto-killed — the host is up; a human/relaunch handles it).
    This preserves BUG2's worker-death catch without the flood.
  * **per-VM shard mtime** (FRESH GCS, worker-life) → the best signal *when capturing*.
Auto-kill is therefore **gated on the SIDECAR being stale** (host/network wedged),
never on the laggy run.log — the safety the tradfi agent required before enabling it.

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

from unified_trading_library import StorageClient, log_event
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
# Auto-KILL threshold (DP-VM-005): a stalled backfill VM is DELETED once its
# stall is this old so the wave-launcher reclaims its cap-20 slot (a hung VM that
# only ALERTS keeps clogging a slot → throughput collapses; the P2 defense for the
# databento-chunk-hang class). Strictly ≥ DEFAULT_STALL_MINUTES — a fresh stall
# alerts (auto_recover relaunch) for a window before the heavier kill fires, so a
# transiently-slow-but-recovering VM is never reaped. Env-tunable via
# MTDS_DP_VM_KILL_MINUTES. The zombie watchdog kills on its OWN (partition/wedged)
# criteria, NOT heartbeat-stall, so without this a heartbeat-stalled-but-RUNNING VM
# is never reclaimed (the gap behind the relaunch actuator's "already KILLED"
# assumption). SSOT: tradfi_databento_backfill_hang_remediation_2026_06_23.md.
DEFAULT_KILL_MINUTES = 45.0
# Fleet-wide cap on auto-kills per sweep (a runaway kill loop must never delete the
# whole fleet — if >N VMs read stalled in one tick that is a watcher bug / fleet
# incident a human must see, not auto-reap). Env-tunable via MTDS_DP_VM_KILL_CAP.
DEFAULT_KILL_CAP_PER_SWEEP = 5
# Umbrella value for a LONG_LIVED_LIVE producer — NEVER auto-killed (a live-capture
# WS VM logs sparsely + must not be reaped for a quiet run.log). Mirrors
# deployment_classification.DeploymentUmbrella.LIVE.value.
_LIVE_UMBRELLA = "live"
# The run.log PROGRESS signal uses a SEPARATE, generous threshold. It is the
# hung-WORKER-on-a-live-host corroborator (sidecar fresh but the worker froze), so
# it MUST sit ABOVE the worst observed GCS-tee lag — empirically 42-78m (2026-06-24
# tradfi-fleet) — or a healthy-but-slow VM whose tee merely lags would false-STALL.
# Raised 45→90 (margin over the 78m max lag): a frozen run.log past 90m on a
# fresh-sidecar host is a genuinely hung worker (ALERT-only — the host is alive, so
# the sidecar-gated auto-kill never fires here; a human / the relaunch actuator
# handles it). The common reapable hang (host/network wedged) is caught FASTER by
# the sidecar going stale (DEFAULT_STALL_MINUTES), independent of this bound.
DEFAULT_RUN_LOG_STALL_MINUTES = 90.0


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
    # Age (min) of the per-VM manifest shard (_index/per_vm/{vm}.parquet) — the
    # AUTHORITATIVE low-lag progress signal (the worker writes it DIRECTLY to GCS
    # as it captures, vs the heartbeat marker which is read from the lagging
    # GCS-tee'd run.log). None ⇒ no shard / read miss.
    progress_age_min: float | None = None


def _pipeline_heartbeat_stale(age_min: float | None, threshold_min: float) -> bool:
    """True when the worker's ``PIPELINE_HEARTBEAT`` is stale enough to corroborate a hang.

    A FRESH marker (``age_min <= threshold_min``) PROVES the worker loop is alive
    (it emits one every 60s independent of chunk boundaries) → NOT hung → returns
    False so the hung-worker STALL is suppressed even when the progress-line
    ``run_log_age`` legitimately exceeds the bound on a slow single fetch. ``None``
    (no marker parsed — read miss / never emitted) → True, falling back to the
    progress-line signal alone (fail toward the original hung-detection behaviour).
    Uses the GENEROUS ``run_log_stall_minutes`` bound so a tee-lagged-but-fresh
    heartbeat (the tee trails the on-VM log ≤78m) is still treated as alive.
    """
    if age_min is None:
        return True
    return age_min > threshold_min


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
    progress_age_min: float | None = None,
    pipeline_heartbeat_age_min: float | None = None,
) -> LivenessResult:
    """Pure liveness classification. No I/O.

    ``heartbeat_age_min`` is the **sidecar blob** age (``vm-heartbeat/{vm}.txt``,
    fresh 60s GCS channel) — the authoritative HOST-liveness signal (REVISED model,
    see the module docstring). A fresh sidecar ⇒ the VM host/network is alive, so
    tee-lag on the run.log can NEVER by itself produce a STALL.

    Precedence:
      - VM younger than ``grace_minutes``                        → TOO_YOUNG (skip)
      - per-VM shard mtime fresh (``progress_age_min``)          → ALIVE (capturing)
      - no sidecar blob at all (``heartbeat_age_min is None``):
          · no run.log              → EVENT_LOOP_STARVED (total silence)
          · run.log frozen > bound  → STALL
          · run.log advancing       → ALIVE (sidecar never started, worker logging)
      - sidecar STALE (> ``stall_minutes``)                      → STALL (host/network
        wedged — the reapable hang; both sidecar AND run.log stale on the 18:00 wave)
      - sidecar FRESH but the ``run.log`` is FROZEN past the generous
        ``run_log_stall_minutes`` (>> tee lag)                  → STALL (hung worker
        on a live host — ALERT-only, the sidecar-gated kill never fires here)
      - sidecar FRESH, captured FLAT AND run.log also frozen past
        ``run_log_stall_minutes`` (corroborated no-progress)    → STALL
      - otherwise                                                → ALIVE

    ``run_log_age_min`` is the PROGRESS signal on a SEPARATE generous threshold (the
    GCS-tee'd run.log lags the on-VM log 42-78m — a tight bound false-flags a healthy
    slow fetch). ``None`` (no parseable log) is NOT a hang (fail-safe: only flag a
    positively-measured stale log). ``captured_flat`` alone does NOT stall — it only
    contributes when CORROBORATED by a run.log frozen past the same generous bound,
    so normal between-tick flatness on a fresh-sidecar host never false-flags.
    """
    if vm_age_min < grace_minutes:
        verdict = LivenessVerdict.TOO_YOUNG
    elif progress_age_min is not None and progress_age_min < stall_minutes:
        # AUTHORITATIVE low-lag progress signal (operator 2026-06-23): the per-VM
        # manifest shard (``_index/per_vm/{vm}.parquet``) was written within the
        # stall window. The worker writes that shard DIRECTLY to GCS as it captures,
        # so a fresh mtime PROVES the worker is alive AND making capture progress
        # right now — it OVERRIDES a stale ``PIPELINE_HEARTBEAT`` marker, which is
        # read from the GCS-TEE'd run.log that can lag the on-VM log by tens of
        # minutes (incident 2026-06-23: a tradfi-bf VM capturing 114k rows +
        # heartbeating on-box every 60s false-STALLed because its tee'd run.log was
        # 42m behind). Fail-safe: ``None`` ⇒ no shard / read miss ⇒ NO override, so
        # the heartbeat + run.log signals still catch a VM that never writes a shard.
        verdict = LivenessVerdict.ALIVE
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
            # No sidecar, no run.log, no PIPELINE_HEARTBEAT — but if the per-VM
            # captured count is still CLIMBING, the worker is provably alive + working
            # despite emitting none of the log/heartbeat instrumentation (a
            # pre-heartbeat-tarball VM whose old image lacks the sidecar + the run.log
            # tee, e.g. cefi-extended-2025-resume launched 2026-06-24 00:54 before the
            # 05:03 sidecar rollout). Only TOTAL silence — no heartbeat, no log, AND
            # captured FLAT — is a genuine starve. (operator 2026-06-24: false STARVED.)
            verdict = LivenessVerdict.EVENT_LOOP_STARVED if captured_flat else LivenessVerdict.ALIVE
        elif run_log_age_min > run_log_stall_minutes:
            verdict = LivenessVerdict.STALL
        else:
            verdict = LivenessVerdict.ALIVE
    elif heartbeat_age_min > stall_minutes:
        # Sidecar blob stale → the VM host/network stopped writing its 60s blob →
        # genuinely wedged (the reapable hang). This is what gates the auto-kill.
        verdict = LivenessVerdict.STALL
    elif (
        is_backfill
        and run_log_age_min is not None
        and run_log_age_min > run_log_stall_minutes
        and _pipeline_heartbeat_stale(pipeline_heartbeat_age_min, run_log_stall_minutes)
    ):
        # Sidecar FRESH (host alive) but the worker's own log froze past the generous
        # bound (>> tee lag) → hung WORKER on a live host. ALERT-only: the sidecar is
        # fresh so should_auto_kill / is_vm_progressing never reap it. Backfill-only:
        # a live-capture WS VM logs sparsely, so a quiet log with a fresh sidecar is
        # normal (sidecar freshness IS its liveness signal).
        # GATE (operator 2026-06-24): ``run_log_age_min`` is the age since the last
        # *progress* line (completed date/chunk), which legitimately exceeds 90m on a
        # VM grinding through ONE huge slow fetch (cefi-deribit-2025-light: an 8-min
        # deribit OPTIONS/options_chain fetch with fresh "streaming in progress …
        # elapsed" lines). The ``PIPELINE_HEARTBEAT`` marker is emitted every 60s
        # INDEPENDENT of chunk boundaries — a FRESH one PROVES the worker loop is
        # alive (slow-but-working), so it must ALSO be stale before we call it hung.
        verdict = LivenessVerdict.STALL
    elif (
        captured_flat
        and run_log_age_min is not None
        and run_log_age_min > run_log_stall_minutes
        and _pipeline_heartbeat_stale(pipeline_heartbeat_age_min, run_log_stall_minutes)
    ):
        # Captured flat AND the log has also stopped advancing past the GENEROUS
        # bound → corroborated no-progress (alive-but-not-working). Uses the generous
        # run_log_stall_minutes (not stall_minutes) so normal between-tick flatness on
        # a fresh-sidecar host whose tee merely lags never false-flags. Same
        # PIPELINE_HEARTBEAT gate as the hung-worker branch above.
        verdict = LivenessVerdict.STALL
    else:
        verdict = LivenessVerdict.ALIVE
    return LivenessResult(
        vm_name=vm_name,
        verdict=verdict,
        heartbeat_age_min=heartbeat_age_min,
        captured_flat=captured_flat,
        run_log_age_min=run_log_age_min,
        progress_age_min=progress_age_min,
    )


def is_vm_progressing(result: LivenessResult, *, kill_minutes: float = DEFAULT_KILL_MINUTES) -> bool:
    """EXPLICIT progress-guard (operator 2026-06-23) — True iff the VM shows a
    POSITIVE, RECENT progress signal, so it must NEVER be reaped.

    "Progress vs SLA, not live-status" (the operator rule): a VM doing real work
    has at least one of —
      - a FRESH worker heartbeat (``PIPELINE_HEARTBEAT`` echoed < ``kill_minutes``
        ago — the bash 60s timer ticks only while the worker process-tree is alive);
      - a RECENTLY-ADVANCING run.log (a per-date/league/chunk line written
        < ``kill_minutes`` ago — measured forward progress, the SLA throughput
        signal).
    Either signal within the kill window = "doing real work within its SLA" → the
    reaper must back off. This is DEFENCE-IN-DEPTH over the verdict precedence in
    ``classify_vm_liveness`` (a fresh heartbeat or advancing log already yields
    ALIVE, never STALL): it makes the "progressing VM is NEVER reaped" invariant a
    standalone, independently-testable guard rather than an emergent property of
    the classify ordering — so a future classify change can't silently regress it.
    The ``zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23`` incident is
    exactly the failure this prevents.

    A ``None`` age = NO measured signal (not "fresh") — fail toward NOT-progressing
    so a genuinely silent/hung VM is still reapable (the kill is gated by the STALL
    verdict + ``should_auto_kill``'s own age check; this guard only ever BLOCKS a
    kill, never forces one).
    """
    # AUTHORITATIVE low-lag progress signal first: a fresh per-VM manifest shard
    # mtime PROVES the worker is capturing right now → never reap (defence-in-depth
    # over the verdict, which is already ALIVE when progress is fresh).
    pa = result.progress_age_min
    if pa is not None and pa < kill_minutes:
        return True
    hb = result.heartbeat_age_min
    if hb is not None and hb < kill_minutes:
        return True
    rl = result.run_log_age_min
    return rl is not None and rl < kill_minutes


def should_auto_kill(
    result: LivenessResult,
    *,
    is_backfill: bool,
    umbrella: str,
    kill_minutes: float = DEFAULT_KILL_MINUTES,
) -> bool:
    """Pure predicate: should this STALLED VM be auto-DELETED to reclaim its slot?

    Guards (ALL must hold — fail-safe toward NOT killing):
      - verdict is ``STALL`` (a positively-measured stall, never EVENT_LOOP_STARVED
        — total silence is a code bug for a human to read, not a reap case);
      - **the VM is NOT progressing** (``is_vm_progressing`` False — the EXPLICIT
        progress-guard, operator 2026-06-23): a FRESH heartbeat OR a recently-
        advancing run.log within the kill window means real work within SLA → never
        reap. Defence-in-depth over the verdict (a progressing VM is already ALIVE,
        not STALL) so the "progressing VM is NEVER reaped" invariant holds even if
        the classify precedence later changes;
      - the VM is a self-deleting BACKFILL VM (``is_backfill``) — a live-capture
        producer is never reaped (it logs sparsely; killing it drops live data);
      - the umbrella is NOT ``live`` (defence-in-depth over ``is_backfill`` — a
        LONG_LIVED_LIVE producer must never be auto-killed even if its name slips
        the backfill heuristic);
      - the stall age (heartbeat age, else frozen-run.log age) is past the heavier
        ``kill_minutes`` threshold — so a fresh stall gets the lighter alert/relaunch
        window first and a transiently-slow VM is never reaped.

    A killed VM frees its wave-launcher cap-20 slot; the relaunch actuator then
    re-runs the backfill (now that "the watchdog already KILLED it" is actually
    true). SSOT: tradfi_databento_backfill_hang_remediation_2026_06_23.md +
    zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md (the progress-guard).
    """
    if result.verdict is not LivenessVerdict.STALL:
        return False
    # EXPLICIT progress-guard FIRST (operator 2026-06-23): a progressing VM is
    # never reaped, full stop — keyed on measured progress vs the SLA window, not
    # on live-status. Redundant with the STALL verdict by construction, kept as an
    # independent fail-safe against a future classify regression.
    if is_vm_progressing(result, kill_minutes=kill_minutes):
        return False
    if not is_backfill or umbrella == _LIVE_UMBRELLA:
        return False
    # The stall AGE is the heartbeat age when present, else the frozen-run.log age
    # (the heartbeat-absent + frozen-log hung-process case). One must be set for a
    # STALL verdict; if neither is (captured-flat-only stall), do NOT kill — that is
    # the soft signal, not a hung process.
    stall_age = result.heartbeat_age_min if result.heartbeat_age_min is not None else result.run_log_age_min
    if stall_age is None:
        return False
    return stall_age >= kill_minutes


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
        "progress_age_min": result.progress_age_min,
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
    shard_mtime_reader: Callable[[str], float | None] | None = None,
    sidecar_age_reader: Callable[[str], float | None] | None = None,
    asset_group_for_vm: Callable[[str], str],
    launcher_for_vm: Callable[[str], str] | None = None,
    umbrella_for_vm: Callable[[str], str] | None = None,
    finding_sink: list[PipelineFinding] | None = None,
    vm_killer: Callable[[str, str], bool] | None = None,
    prior_captured: dict[str, int] | None = None,
    stall_minutes: float = DEFAULT_STALL_MINUTES,
    run_log_stall_minutes: float = DEFAULT_RUN_LOG_STALL_MINUTES,
    grace_minutes: float = DEFAULT_GRACE_MINUTES,
    kill_minutes: float = DEFAULT_KILL_MINUTES,
    kill_cap_per_sweep: int = DEFAULT_KILL_CAP_PER_SWEEP,
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

    ``vm_killer`` (optional ``(vm_name, zone) -> bool`` deleter): when supplied, a
    STALL that passes :func:`should_auto_kill` (backfill, NOT live, stale past
    ``kill_minutes``) is DELETED so the wave-launcher reclaims its slot — at most
    ``kill_cap_per_sweep`` deletions per sweep (a runaway must page a human, not
    reap the fleet). Injected so the sweep stays credential-free + block-network in
    tests. ``dry_run`` short-circuits the actual delete (counts what it WOULD kill).
    """
    prior_captured = prior_captured or {}
    results: list[LivenessResult] = []
    kills_this_sweep = 0
    for vm_name, zone in running_vms:
        try:
            vm_age = vm_age_reader(vm_name, zone)
        except Exception:
            vm_age = grace_minutes  # unknown age → treat as just-past-grace, defer
        # HOST-liveness heartbeat (REVISED 2026-06-24, see module docstring): read the
        # FRESH infra sidecar blob (``vm-heartbeat/{vm}.txt``, written 60s via a direct
        # GCS channel) as the authoritative ``heartbeat_age_min``. BUG2 (2026-06-22)
        # had moved off it onto the run.log PIPELINE_HEARTBEAT marker — but that marker
        # rides the GCS-tee'd run.log which LAGS 42-78m, so keying STALL/auto-kill on it
        # false-flagged every healthy-but-slow VM (the DP_VM_STALL flood) AND made the
        # auto-kill a foot-gun. The sidecar is fresh-channel + goes stale only when the
        # host/network wedges (the genuinely-reapable hang) — so STALL/kill key on it,
        # and the laggy run.log becomes the hung-WORKER corroborator (below). Falls back
        # to the run.log marker only when no sidecar reader is wired (unit tests).
        _signals = _gcs.run_log_signals(storage_client, log_bucket, vm_name)
        try:
            sidecar_age = sidecar_age_reader(vm_name) if sidecar_age_reader is not None else None
        except Exception:
            sidecar_age = None
        hb_age = sidecar_age if sidecar_age_reader is not None else _signals.pipeline_heartbeat_age_min
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
        run_log_age = _signals.run_log_age_min
        try:
            captured_now = captured_reader(vm_name)
        except Exception:
            captured_now = prior_captured.get(vm_name, 0)
        captured_flat = vm_name in prior_captured and captured_now <= prior_captured[vm_name]
        # AUTHORITATIVE low-lag progress signal: age of the per-VM manifest shard
        # (written DIRECTLY to GCS as the worker captures). A fresh mtime overrides a
        # stale PIPELINE_HEARTBEAT read from the lagging GCS-tee'd run.log.
        try:
            progress_age = shard_mtime_reader(vm_name) if shard_mtime_reader is not None else None
        except Exception:
            progress_age = None

        result = classify_vm_liveness(
            vm_name,
            vm_age_min=vm_age,
            heartbeat_age_min=hb_age,
            captured_flat=captured_flat,
            run_log_age_min=run_log_age,
            progress_age_min=progress_age,
            pipeline_heartbeat_age_min=_signals.pipeline_heartbeat_age_min,
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
        # Record the fired finding for the RESOLVED-bookend lifecycle (the caller
        # reconciles it against the prior active set so a recovered/reaped VM posts a
        # ✅ RESOLVED). Tracked even on dry_run. Suppressed → no finding → not tracked.
        if finding is not None and finding_sink is not None:
            finding_sink.append(finding)
        if finding is not None and not dry_run:
            route_finding(finding, pm_repo_path=pm_repo_path)
        if finding is not None:
            logger.warning("heartbeat_stall_watcher: %s verdict=%s hb_age=%s", vm_name, result.verdict, hb_age)

        # P2 DEFENSE (DP-VM-005): auto-DELETE a stalled backfill VM so the
        # wave-launcher reclaims its cap-20 slot. Guarded by should_auto_kill
        # (backfill-only, NOT live, stale past kill_minutes) + a per-sweep cap (a
        # runaway must page a human, never reap the whole fleet). dry_run logs the
        # WOULD-kill without deleting.
        if vm_killer is not None and should_auto_kill(
            result, is_backfill=is_backfill, umbrella=umbrella, kill_minutes=kill_minutes
        ):
            if kills_this_sweep >= kill_cap_per_sweep:
                logger.warning(
                    "heartbeat_stall_watcher: kill cap %d reached this sweep — NOT killing %s "
                    "(fleet-wide stall? a human must inspect)",
                    kill_cap_per_sweep,
                    vm_name,
                )
            else:
                stall_age = result.heartbeat_age_min if result.heartbeat_age_min is not None else result.run_log_age_min
                kills_this_sweep += 1
                if dry_run:
                    logger.warning(
                        "heartbeat_stall_watcher: DRY_RUN would auto-kill stalled backfill VM %s "
                        "(stall_age=%.0fm >= kill_minutes=%.0f) to reclaim its slot",
                        vm_name,
                        stall_age or 0.0,
                        kill_minutes,
                    )
                    killed = False
                else:
                    killed = bool(vm_killer(vm_name, zone))
                    logger.warning(
                        "heartbeat_stall_watcher: auto-killed stalled backfill VM %s (stall_age=%.0fm) result=%s",
                        vm_name,
                        stall_age or 0.0,
                        killed,
                    )
                if not dry_run:
                    log_event(
                        DP_VM_STALL,
                        severity="WARN",
                        details={
                            "vm_name": vm_name,
                            "zone": zone,
                            "asset_group": asset_group_for_vm(vm_name),
                            "umbrella": umbrella,
                            "recovery_action": "auto_kill_stalled_vm",
                            "stall_age_min": stall_age,
                            "kill_minutes": kill_minutes,
                            "killed": killed,
                            "registry_id": "DP-VM-005",
                            "cloud": "GCP",
                        },
                    )

    return results
