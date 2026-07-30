"""Exit_code-aware fleet monitor (DP-VM-001 / DP-VM-002 / DP-VM-008 / DP-VM-010).

Closes the self-delete-masks-OOM blind spot (CLAUDE.md 2026-06-22): a batch/live
VM launched with ``VM_SHUTDOWN_ON_COMPLETION=true`` self-deletes on exit whether
it SUCCEEDED (exit 0) or CRASHED (exit 137=OOM / non-zero=error). A monitor that
only watches the RUNNING set + treats "VM gone ⇒ drained" is BLIND to mass
failures (incident 2026-06-22: 3 sports backfills OOM-died exit 137, self-deleted,
the drain-only monitor read 14→1 as healthy completion → no wake; coverage was 0%
with 75k+ attempted_failed rows).

This monitor is census-diffing + durable-signal-reading:

  1. Snapshot the RUNNING VM census each tick to a GCS census blob, recording per
     VM its observed cumulative captured count.
  2. A VM present in the PRIOR census but GONE this tick has *terminated*. For
     each such VM, read the persisted GCS ``run.log`` terminal ``exit_code``
     (survives self-delete via ``read_terminal_exit_code``) AND cross-check
     whether its manifest ``captured`` count climbed during the run.
  3. Emit, per terminated VM:
       - ``DP_VM_EXIT_NONZERO`` (CRITICAL) when ``exit_code != 0`` (incl. 137 OOM).
       - ``DP_VM_GONE_NO_CAPTURE`` (CRITICAL) when the VM drained but captured did
         NOT climb (``exit_code == 0`` but flat captured — the silent-zero class).
       - ``DP_VM_PREEMPTED`` (INFO, ``auto_recover``) when the durable ``PREEMPTED``
         signal blob is present (SPOT reclaim), OR — when the blob is absent AND
         a ``project_id`` was supplied — the Compute Operations API confirms a
         ``compute.instances.preempted`` event for this VM (``_gcp_ops.py``; a
         fallback for the guest-shutdown-script race on an early-boot
         preemption). Dispatches a preemption-aware relaunch
         (``relaunch_backfill_vm.RelaunchPreemptedVm``, replaying the
         launch-time env captured by ``lc_write_launch_params``) THROUGH the
         launcher's own ``tardis_concurrency_guard``. Closes the P0 "SPOT
         preemption DELETES waves and nothing relaunches them" gap
         (``cefi_completion_program_2026_07_15.md``) — the actuator self-emits a
         CRITICAL ``DP_VM_PREEMPTED_NO_RELAUNCH`` whenever the relaunch itself
         fails (no launcher bound / budget exhausted / guard refusal / launcher
         error), so a preempted backfill can never silently vanish again.
       - ``DP_VM_PARTIAL_UNCONFIRMED`` (WARN, ``auto_recover``) when captured
         climbed but NO durable terminal marker was ever written (``exit_code is
         None`` — neither an ``EXIT_STATUS`` blob nor a ``command exited rc=<n>``
         ``run.log`` line). Closes the "a VM force-killed mid-run with real
         partial progress is indistinguishable from CLEAN" gap
         (``exit_code_fleet_monitor_clean_misclassifies_premature_kill_2026_07_21.md``
         — 2 ``af-backfill-*`` shards sat dead ~22h with zero self-heal because
         this exact shape resolved to CLEAN and fired nothing). Reuses the SAME
         checkpoint-resume relaunch actuator as PREEMPTED.

The wake/alert condition is ``exit_code != 0 OR captured did not climb OR (captured
climbed with NO durable terminal marker)`` — NEVER "VM gone ⇒ success".

**LIVE/LONG_LIVED_LIVE exemption (2026-06-27, incident: 17 false-fire CRITICAL storm
on ``mtds-live-cefi-*`` consolidation)**: for LIVE VMs (``umbrella == "live"``) the
manifest ``captured`` count is the INSTRUMENT COUNT (~15, stable) NOT a batch
instrument-days counter that must climb.  Flat captured on a LIVE VM is by design —
live data volume grows in the EVENT-LOG/GCS sink, NOT in the manifest captured count.
The ``DP_VM_GONE_NO_CAPTURE`` check is therefore SKIPPED for live VMs:
  - LIVE VM + exit 0 + flat captured  → ``EXPECTED_NO_CAPTURE`` (no alert — benign)
  - LIVE VM + exit != 0               → ``EXIT_NONZERO`` (still alerts, crash is a crash)
Live capture health (VM alive but stream dead) is covered by ``live_stream_watcher.py``
DP-LIVE-001/002, not by this exit-code sweep.

Pure-function core (``classify_terminated_vm``) is unit-tested with fixtures; the
GCS/compute I/O is injected so the sweep is credential-free + block-network safe.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import cast

from unified_trading_library import StorageClient
from unified_trading_library.events import (  # noqa: qg-deep-import
    DP_SOURCE_RATE_LIMITED,
    DP_VM_EXIT_NONZERO,
    DP_VM_GONE_NO_CAPTURE,
)

from deployment_service.data_pipeline_monitors import _gcp_ops, _gcs
from deployment_service.data_pipeline_monitors._gcs import NoCaptureReason
from deployment_service.data_pipeline_monitors.escalation import (
    EscalationTier,
    PipelineFinding,
    route_finding,
)

logger = logging.getLogger(__name__)

# The census blob lives in the heartbeat/log bucket so it is durable + cheap.
CENSUS_BLOB = "vm-census/exit-code-fleet-census.json"


class TerminationVerdict(StrEnum):
    """Per terminated-VM classification outcome."""

    EXIT_NONZERO = "exit_nonzero"  # DP-VM-001
    GONE_NO_CAPTURE = "gone_no_capture"  # DP-VM-002 — genuine silent zero → ALERT
    RATE_LIMITED = "rate_limited"  # flat captured but run.log shows a 429/throttle → backoff-retry
    EXPECTED_NO_CAPTURE = "expected_no_capture"  # flat captured but benign (honest-absence / shard already complete / rows written but consolidated lags) → NO alert
    CLEAN = (
        "clean"  # exit 0 + captured climbed → healthy completion (CONFIRMED — a durable terminal marker was observed)
    )
    PARTIAL_UNCONFIRMED = "partial_unconfirmed"  # exit_code is None (no durable terminal marker at all) + captured climbed — ambiguous: might be a genuine completion whose terminal write raced the VM's teardown, might be a premature kill with real partial progress. Auto-recover-routed (self-heals via the SAME resume-from-checkpoint relaunch as PREEMPTED) so it is never silently indistinguishable from CLEAN.
    UNKNOWN = "unknown"  # no durable exit code AND no captured signal
    PREEMPTED = "preempted"  # GCE spot preemption → benign relaunch, NO alert (INFO only)


@dataclass(frozen=True)
class TerminationResult:
    """The result of classifying one terminated VM."""

    vm_name: str
    verdict: TerminationVerdict
    exit_code: int | None
    captured_before: int
    captured_after: int
    # WHY a flat-captured run was flat (run.log-derived) — drives the
    # GONE_NO_CAPTURE-vs-EXPECTED_NO_CAPTURE-vs-RATE_LIMITED split. SILENT for a
    # genuine silent-zero (the only flat-captured case that still alerts).
    no_capture_reason: NoCaptureReason = NoCaptureReason.SILENT
    preempted: bool = False

    @property
    def captured_climbed(self) -> bool:
        return self.captured_after > self.captured_before


def classify_terminated_vm(
    vm_name: str,
    *,
    exit_code: int | None,
    captured_before: int,
    captured_after: int,
    no_capture_reason: NoCaptureReason = NoCaptureReason.SILENT,
    preempted: bool = False,
    is_live_vm: bool = False,
) -> TerminationResult:
    """Pure classification of a terminated VM. No I/O.

    Verdict precedence (most severe first):
      - ``preempted == True``                         → PREEMPTED (benign SPOT relaunch)
          GCE preemption SIGTERM causes a non-zero exit; the PREEMPTED blob written
          by the VM's shutdown-script is the authoritative "this was benign" signal —
          checked BEFORE exit_code so a preempted VM never false-fires DP-VM-001.
      - ``exit_code != 0`` (incl. 137 OOM)            → EXIT_NONZERO  (DP-VM-001)
      - captured CLIMBED + ``exit_code == 0``         → CLEAN (a durable terminal marker CONFIRMS the completion)
      - captured CLIMBED + ``exit_code is None``      → PARTIAL_UNCONFIRMED (DP-VM-010)
          A VM with NO durable terminal marker at all (no ``EXIT_STATUS`` blob, no
          ``command exited rc=<n>`` line in ``run.log``) is indistinguishable, from
          this signal alone, between "finished but the terminal write raced the
          VM's teardown" and "force-killed mid-run with real partial progress" (a
          SPOT preemption whose ``PREEMPTED`` marker never got written is one
          instance of the latter, but NOT the only one — see
          ``exit_code_fleet_monitor_clean_misclassifies_premature_kill_2026_07_21.md``).
          Blindly resolving this to CLEAN was the exact bug that incident hit: no
          alert, no relaunch, no operator signal that anything was ambiguous. Routes
          to ``auto_recover`` (the SAME resume-from-``PROGRESS.json``-checkpoint
          relaunch actuator as PREEMPTED) so a genuine partial run self-heals AND a
          truly-finished run is at worst harmlessly re-verified — never silent.
      - ``is_live_vm == True`` + captured FLAT        → EXPECTED_NO_CAPTURE (no alert)
          A LIVE/LONG_LIVED_LIVE VM's manifest ``captured`` count is the INSTRUMENT
          COUNT (~15, stable by design) — it does NOT climb like a batch
          instrument-days counter.  Flat captured on a live VM is therefore benign;
          DP_VM_GONE_NO_CAPTURE must NOT fire.  A live crash (exit != 0) is still
          caught above via EXIT_NONZERO.  Live stream health (VM alive, data dead)
          is deferred to ``live_stream_watcher.py`` DP-LIVE-001/002.
      - captured FLAT — split on the run.log reason (the 2026-06-23
        false-positive killer; a flat consolidated/shard count is NOT inherently
        a failure):
          * ``no_capture_reason == RATE_LIMITED``     → RATE_LIMITED
            (a 429/throttle wrote partial → backoff-retry tier, NOT a silent zero)
          * ``no_capture_reason == PROGRESS``         → EXPECTED_NO_CAPTURE
            (the writer's own shard wrote rows; the CONSOLIDATED count merely lags)
          * ``no_capture_reason == HONEST_ABSENCE``   → EXPECTED_NO_CAPTURE
            (source legitimately empty / shard already complete / off-season)
          * ``no_capture_reason == SILENT`` (default) → GONE_NO_CAPTURE
            (no benign signal → genuine silent zero: auth fail / 0-universe /
            unexpected empty — STILL ALERTS, the operator guardrail)

    On the FLAT-captured path, both ``exit_code == 0`` and ``exit_code is None``
    follow the captured/reason split: a None exit code is no longer a blanket
    fail-safe to GONE_NO_CAPTURE — a benign run.log reason (PROGRESS /
    HONEST_ABSENCE / RATE_LIMITED) overrides it, so a self-deleting VM that
    honestly recorded absence no longer false-fires. A None exit code with a
    SILENT reason still alerts (never infer success from "the VM is gone" + no
    evidence of work). On the CLIMBED path, ``exit_code is None`` is treated
    DIFFERENTLY from ``exit_code == 0`` (see PARTIAL_UNCONFIRMED above) — a
    climbing captured count is real work, but without a durable terminal marker
    it is evidence of PROGRESS, not evidence of COMPLETION.
    """
    climbed = captured_after > captured_before
    if preempted:
        verdict = TerminationVerdict.PREEMPTED
    elif exit_code is not None and exit_code != 0:
        verdict = TerminationVerdict.EXIT_NONZERO
    elif climbed:
        # exit_code is guaranteed 0 or None here (nonzero already routed above).
        verdict = TerminationVerdict.CLEAN if exit_code == 0 else TerminationVerdict.PARTIAL_UNCONFIRMED
    elif is_live_vm:
        # LIVE VMs: captured is the instrument count (~15, stable). Flat is benign.
        # Live capture health is owned by live_stream_watcher.py DP-LIVE-001/002.
        verdict = TerminationVerdict.EXPECTED_NO_CAPTURE
    elif no_capture_reason is NoCaptureReason.RATE_LIMITED:
        verdict = TerminationVerdict.RATE_LIMITED
    elif no_capture_reason in (NoCaptureReason.PROGRESS, NoCaptureReason.HONEST_ABSENCE):
        verdict = TerminationVerdict.EXPECTED_NO_CAPTURE
    else:  # NoCaptureReason.SILENT — genuine silent zero (incl. exit_code is None)
        verdict = TerminationVerdict.GONE_NO_CAPTURE
    return TerminationResult(
        vm_name=vm_name,
        verdict=verdict,
        exit_code=exit_code,
        captured_before=captured_before,
        captured_after=captured_after,
        no_capture_reason=no_capture_reason,
        preempted=preempted,
    )


def _finding_for(
    result: TerminationResult,
    *,
    asset_group: str,
    relaunch_launcher: str = "",
    umbrella: str = "",
    launch_env: dict[str, str] | None = None,
    progress_checkpoint: dict[str, str] | None = None,
) -> PipelineFinding | None:
    """Build the escalation finding for a non-clean termination (None when clean).

    An **OOM** exit (exit_code 137) routes to the ``auto_recover`` tier — the
    ``relaunch_backfill_vm`` actuator re-launches it (≤2/day per vm-prefix then
    page). The binding is the ``relaunch_launcher`` script name (resolved by the
    sweep's ``launcher_for_vm``); when no launcher binding is available the
    actuator skips and the finding falls through to ``file_issue`` (the
    escalation hop guarantees no auto_recover finding is lost). A non-OOM
    non-zero exit stays ``page_operator``.

    A **PREEMPTED** verdict (SPOT reclaim) ALSO routes to ``auto_recover`` — see
    ``cefi_completion_program_2026_07_15.md`` P0 "Close the SPOT-preemption
    relaunch gap": today's ``exit_code_fleet_monitor`` only logged an INFO line
    CLAIMING a relaunch that never actually happened, so a preempted multi-day
    backfill silently vanished. The ``relaunch_backfill_vm.RelaunchPreemptedVm``
    actuator re-invokes ``relaunch_launcher`` (unconditional on exit code — the
    PREEMPTED verdict itself is the trigger, same shape as the STALL actuator),
    replaying ``launch_env`` (captured at VM-creation time by
    ``lc_write_launch_params``) so the relaunch reproduces the SAME
    venues/START_DATE/concurrency/lease — THROUGH the launcher's own
    ``tardis_concurrency_guard`` (never a blind relaunch). Base severity here is
    deliberately ``INFO`` (routine/benign — unlike OOM, a SPOT reclaim is
    expected): the actuator itself self-emits a CRITICAL
    ``DP_VM_PREEMPTED_NO_RELAUNCH`` on every failure path (no launcher bound /
    budget exhausted / the guard refusing / the launcher subprocess failing) so
    the "no relaunch will happen" case is never silent — see
    ``escalation._recover_preempted_vm``.
    """
    base_details: dict[str, object] = {
        "vm_name": result.vm_name,
        "asset_group": asset_group,
        "exit_code": result.exit_code,
        "captured_before": result.captured_before,
        "captured_after": result.captured_after,
        # umbrella drives the alerting-service router channel split (LIVE →
        # #uts-live-alerts, BATCH → #data-pipeline-alerts). "" → batch default.
        "umbrella": umbrella,
        "cloud": "GCP",
    }
    if result.verdict is TerminationVerdict.PREEMPTED:
        preempted_details: dict[str, object] = {**base_details}
        if relaunch_launcher:
            preempted_details["relaunch_launcher"] = relaunch_launcher
        if launch_env:
            preempted_details["launch_env"] = launch_env
        # The VM's own resume checkpoint (PROGRESS.json) — lets the actuator RESUME
        # from the last completed date instead of replaying START_DATE from genesis
        # (the force-run day-one-replay fix). Absent for a run that never emitted it.
        if progress_checkpoint:
            preempted_details["progress_checkpoint"] = progress_checkpoint
        return PipelineFinding(
            event="DP_VM_PREEMPTED",
            severity="INFO",
            tier=EscalationTier.AUTO_RECOVER,
            summary=(
                f"VM {result.vm_name} preempted (SPOT reclaim) — relaunching through "
                "the Tardis/launcher concurrency guard"
            ),
            details=preempted_details,
            registry_id="DP-VM-008",
        )
    if result.verdict is TerminationVerdict.PARTIAL_UNCONFIRMED:
        # Same relaunch shape as PREEMPTED (reuses the checkpoint-resume actuator —
        # see escalation._recover_preempted_vm) since either it IS a premature kill
        # with real partial progress (self-heals the same way) or it was actually
        # a clean finish whose terminal-marker write raced teardown (a harmless
        # extra pass; the launcher's own presence-skip logic makes a re-run of an
        # already-complete range a fast no-op, not duplicate work).
        partial_details: dict[str, object] = {**base_details}
        if relaunch_launcher:
            partial_details["relaunch_launcher"] = relaunch_launcher
        if launch_env:
            partial_details["launch_env"] = launch_env
        if progress_checkpoint:
            partial_details["progress_checkpoint"] = progress_checkpoint
        return PipelineFinding(
            event="DP_VM_PARTIAL_UNCONFIRMED",
            severity="WARN",
            tier=EscalationTier.AUTO_RECOVER,
            summary=(
                f"VM {result.vm_name} terminated with NO durable exit marker (exit_code=None) "
                f"but captured climbed ({result.captured_before} → {result.captured_after}) — "
                "cannot confirm this was a clean finish vs. a premature kill with partial "
                "progress; resuming from checkpoint to be safe"
            ),
            details=partial_details,
            registry_id="DP-VM-010",
        )
    if result.verdict is TerminationVerdict.EXIT_NONZERO:
        oom = result.exit_code == 137
        summary = (
            f"VM {result.vm_name} terminated with exit_code={result.exit_code}"
            f"{' (OOM)' if oom else ''} — captured did not complete cleanly"
        )
        oom_details: dict[str, object] = {**base_details, "oom": oom}
        if oom and relaunch_launcher:
            oom_details["relaunch_launcher"] = relaunch_launcher
            # OOM (exit-137) means the machine was too small — a same-machine
            # relaunch re-OOMs. Carry a bigger_machine hint the auto_recover
            # actuator honors (escalation._recover_backfill_vm maps it to a
            # MACHINE_TYPE env the launcher reads), so the relaunch lands on a
            # higher-memory tier. KEY #4 (operator 2026-06-23): match the
            # actuator to the failure MODE — OOM → relaunch BIGGER, not same.
            oom_details["bigger_machine"] = True
        return PipelineFinding(
            event=DP_VM_EXIT_NONZERO,
            severity="CRITICAL",
            tier=EscalationTier.AUTO_RECOVER if oom else EscalationTier.PAGE_OPERATOR,
            summary=summary,
            details=oom_details,
            registry_id="DP-VM-001",
        )
    if result.verdict is TerminationVerdict.GONE_NO_CAPTURE:
        return PipelineFinding(
            event=DP_VM_GONE_NO_CAPTURE,
            severity="CRITICAL",
            tier=EscalationTier.PAGE_OPERATOR,
            summary=(
                f"VM {result.vm_name} drained but manifest captured did not climb "
                f"({result.captured_before} → {result.captured_after}) AND its run.log "
                "shows no rows-written / honest-absence / rate-limit signal — a genuine "
                "silent zero (auth fail / 0-universe / unexpected empty)"
            ),
            details={**base_details, "no_capture_reason": str(result.no_capture_reason)},
            registry_id="DP-VM-002",
        )
    if result.verdict is TerminationVerdict.RATE_LIMITED:
        # A 429/throttle wrote a partial result (exit 0, flat captured). Route to
        # the backoff-retry tier — NOT a relaunch (relaunch re-hits the limit). The
        # DP_SOURCE_RATE_LIMITED event has no wired relaunch actuator, so the
        # escalation hop falls it through to file_issue (the backoff the runtime
        # owns) rather than re-launching the VM. WARN, not CRITICAL — transient.
        return PipelineFinding(
            event=DP_SOURCE_RATE_LIMITED,
            severity="WARN",
            tier=EscalationTier.AUTO_RECOVER,
            summary=(
                f"VM {result.vm_name} drained with a flat captured count "
                f"({result.captured_before} → {result.captured_after}) because its "
                "run.log shows an HTTP-429 / rate-limit throttle — back off + retry "
                "(do NOT relaunch immediately, it re-hits the limit)"
            ),
            details={**base_details, "no_capture_reason": str(result.no_capture_reason)},
            registry_id="DP-VM-002",
        )
    # PREEMPTED (spot preemption → benign relaunch, INFO logged by sweep) +
    # EXPECTED_NO_CAPTURE (rows written but consolidated lags / honest-absence /
    # shard already complete) + CLEAN + UNKNOWN → no finding (benign / not a failure).
    return None


def load_census(storage_client: StorageClient, log_bucket: str) -> dict[str, int]:
    """Load the prior RUNNING-VM census (vm_name → observed captured_cum)."""
    raw = _gcs.read_text(storage_client, log_bucket, CENSUS_BLOB)
    if not raw:
        return {}
    try:
        loaded = cast("object", json.loads(raw))
    except (json.JSONDecodeError, ValueError):
        return {}
    if not isinstance(loaded, dict):
        return {}
    data = cast("dict[str, object]", loaded)
    vms_obj = data.get("vms", {})
    if not isinstance(vms_obj, dict):
        return {}
    vms = cast("dict[str, object]", vms_obj)
    out: dict[str, int] = {}
    for key, value in vms.items():
        if isinstance(value, (int, float, str)):
            try:
                out[str(key)] = int(value)
            except (TypeError, ValueError):
                continue
    return out


def write_census(storage_client: StorageClient, log_bucket: str, census: dict[str, int]) -> None:
    """Persist the current RUNNING-VM census. Best-effort (never raises the sweep)."""
    payload = json.dumps(
        {"updated_at": datetime.now(UTC).isoformat(), "vms": census},
        sort_keys=True,
    ).encode("utf-8")
    try:
        storage_client.upload_bytes(log_bucket, CENSUS_BLOB, payload, content_type="application/json")
    except Exception as exc:
        logger.warning("exit_code_fleet_monitor: failed to persist census: %s", exc)


def sweep(
    *,
    storage_client: StorageClient,
    log_bucket: str,
    running_vms: Iterable[tuple[str, str]],
    captured_reader: Callable[[str], int],
    asset_group_for_vm: Callable[[str], str],
    launcher_for_vm: Callable[[str], str] | None = None,
    umbrella_for_vm: Callable[[str], str] | None = None,
    finding_sink: list[PipelineFinding] | None = None,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    project_id: str = "",
    preemption_operations_checker: Callable[[str, str], bool] | None = None,
) -> list[TerminationResult]:
    """Run one exit-code-aware fleet sweep.

    Args:
        storage_client: cloud-agnostic storage client.
        log_bucket: the durable log/heartbeat bucket (``deployment-scripts-{pid}``).
        running_vms: iterable of ``(vm_name, zone)`` for currently-RUNNING VMs.
        captured_reader: ``vm_name -> captured_cum`` (per-VM manifest shard count).
        asset_group_for_vm: ``vm_name -> asset_group`` for the alert detail.
        launcher_for_vm: optional ``vm_name -> launcher-script-name`` resolver. When
            supplied, an OOM (exit-137) finding carries a ``relaunch_launcher``
            binding so the ``relaunch_backfill_vm`` auto_recover actuator can
            re-launch it; absent it, the OOM finding falls through to file_issue.
        umbrella_for_vm: optional ``vm_name -> umbrella`` resolver (LIVE/BATCH/...). When
            supplied, the DP_VM_* finding carries the umbrella so the alerting-service
            router splits LIVE → #uts-live-alerts vs BATCH → #data-pipeline-alerts;
            absent it (or "" returned) the alert routes to the batch default.
            Critically, ``umbrella == "live"`` also gates the LIVE-VM exemption:
            ``DP_VM_GONE_NO_CAPTURE`` is NOT fired for live VMs because their manifest
            ``captured`` count is the instrument count (~15, stable by design), not a
            batch instrument-days counter.  Live capture health is owned by
            ``live_stream_watcher.py`` DP-LIVE-001/002.
        pm_repo_path: optional PM clone path for the file_issue tier.
        dry_run: when True, classify + return but emit nothing / persist nothing.
        project_id: GCP project for the Compute Operations API preemption
            fallback (see ``_gcp_ops.py``). Empty string (the default) skips
            the fallback entirely — behavior is then identical to before this
            parameter existed (blob-only preemption detection).
        preemption_operations_checker: optional ``(project_id, vm_name) -> bool``
            override for ``_gcp_ops.was_vm_preempted_via_operations_api`` — a
            test injection point; production callers leave this ``None``.

    Returns the list of TerminationResult for VMs that terminated since last tick.
    """
    prior = load_census(storage_client, log_bucket)
    running = dict(running_vms)

    # Update census for currently-running VMs (record their latest captured_cum).
    current_census: dict[str, int] = {}
    for name in running:
        try:
            current_census[name] = max(captured_reader(name), prior.get(name, 0))
        except Exception:
            current_census[name] = prior.get(name, 0)

    # Terminated = in prior census, gone now.
    terminated = [name for name in prior if name not in running]

    results: list[TerminationResult] = []
    for name in terminated:
        exit_code = _gcs.read_terminal_exit_code(storage_client, log_bucket, name)
        captured_before = prior.get(name, 0)
        try:
            captured_after = max(captured_reader(name), captured_before)
        except Exception:
            captured_after = captured_before
        # Resolve umbrella first — it gates the is_live_vm flag used in
        # classify_terminated_vm and the alert channel routing.
        umbrella = umbrella_for_vm(name) if umbrella_for_vm is not None else ""
        is_live_vm = umbrella == "live"
        # Only resolve the run.log no-capture reason when the verdict would
        # otherwise be flat-captured-and-clean-exit (the GONE_NO_CAPTURE candidate
        # path) — a non-zero exit, a climbing capture, or a live VM never needs it,
        # so we don't download the run.log for those (keeps the per-tick download
        # count low, mirrors the OOM-fix double-download discipline).
        is_preempted = _gcs.is_vm_preempted(storage_client, log_bucket, name)
        if (
            not is_preempted
            and not is_live_vm
            and (exit_code is None or exit_code == 0)
            and captured_after <= captured_before
            and project_id
        ):
            # Fallback: the guest-written PREEMPTED blob can race an
            # early-boot preemption's shutdown grace window (confirmed
            # 2026-07-30 — see _gcp_ops.py docstring for the incident). Only
            # worth the extra Compute Operations API call for the candidate
            # class that would otherwise resolve GONE_NO_CAPTURE — a
            # non-zero exit or a climbing capture is already unambiguous
            # without it.
            checker = preemption_operations_checker or _gcp_ops.was_vm_preempted_via_operations_api
            is_preempted = checker(project_id, name)
            if is_preempted:
                logger.info(
                    "exit_code_fleet_monitor: %s — PREEMPTED blob absent but the Compute "
                    "Operations API confirms a compute.instances.preempted event; classifying "
                    "as benign SPOT reclaim instead of DP_VM_GONE_NO_CAPTURE",
                    name,
                )
        # A terminated VM with NO durable exit marker AND climbing captured is the
        # PARTIAL_UNCONFIRMED candidate (see classify_terminated_vm) — it needs the
        # SAME relaunch-replay env as PREEMPTED. Resolve the gate here (mirrors the
        # verdict's own precedence: nonzero-exit already routes elsewhere, so this
        # is exactly "would resolve PARTIAL_UNCONFIRMED").
        needs_relaunch_env = is_preempted or (exit_code is None and captured_after > captured_before)
        # Only resolve the launch-params replay env when the verdict actually reads
        # it (PREEMPTED or PARTIAL_UNCONFIRMED) — keeps the per-tick GCS read count
        # down, same discipline as the run.log no-capture-reason gate just below.
        launch_env = _gcs.read_launch_params(storage_client, log_bucket, name) if needs_relaunch_env else None
        # The VM's own PROGRESS.json resume checkpoint — same gate as launch_env
        # (one extra GCS read only on a verdict that consumes it). Lets the
        # relaunch RESUME from last_completed_date instead of replaying START_DATE.
        progress_checkpoint = (
            _gcs.read_progress_checkpoint(storage_client, log_bucket, name) if needs_relaunch_env else None
        )
        needs_reason = (
            not is_preempted
            and not is_live_vm
            and (exit_code is None or exit_code == 0)
            and captured_after <= captured_before
        )
        no_capture_reason = (
            _gcs.no_capture_reason_from_run_log(storage_client, log_bucket, name)
            if needs_reason
            else NoCaptureReason.SILENT
        )
        result = classify_terminated_vm(
            name,
            exit_code=exit_code,
            captured_before=captured_before,
            captured_after=captured_after,
            no_capture_reason=no_capture_reason,
            preempted=is_preempted,
            is_live_vm=is_live_vm,
        )
        results.append(result)

        if result.verdict is TerminationVerdict.PREEMPTED:
            logger.info(
                "exit_code_fleet_monitor: %s preempted (SPOT reclaim) — dispatching a "
                "preemption-aware relaunch via the auto_recover tier (DP-VM-008); a "
                "CRITICAL DP_VM_PREEMPTED_NO_RELAUNCH fires only if that relaunch fails",
                name,
            )
        if result.verdict is TerminationVerdict.PARTIAL_UNCONFIRMED:
            logger.warning(
                "exit_code_fleet_monitor: %s terminated with NO durable exit marker but "
                "captured climbed (%d->%d) — cannot confirm CLEAN vs premature kill; "
                "dispatching a checkpoint-resume relaunch via the auto_recover tier (DP-VM-010)",
                name,
                result.captured_before,
                result.captured_after,
            )
        if is_live_vm and result.verdict is TerminationVerdict.EXPECTED_NO_CAPTURE:
            logger.info(
                "exit_code_fleet_monitor: %s is a LIVE VM (umbrella=live) — "
                "flat captured is instrument-count (stable by design), not a silent zero; "
                "DP_VM_GONE_NO_CAPTURE suppressed. Live capture health → live_stream_watcher DP-LIVE-001.",
                name,
            )

        launcher = launcher_for_vm(name) if launcher_for_vm is not None else ""
        finding = _finding_for(
            result,
            asset_group=asset_group_for_vm(name),
            relaunch_launcher=launcher,
            umbrella=umbrella,
            launch_env=launch_env,
            progress_checkpoint=progress_checkpoint,
        )

        if finding is not None:
            # Make the alert ACTIONABLE: attach the durable run.log error/warn
            # snippet + a click-through console link. The GCS-tee'd run.log
            # survives the VM's self-delete, so this works even though the VM is
            # already gone. The alerting-service formatter renders ``run_log_tail``
            # as an inline fenced-code trace block + ``log_url`` as a deep-link
            # action. Best-effort — a missing/unreadable log just omits the snippet.
            snippet = _gcs.error_snippet_from_run_log(storage_client, log_bucket, name)
            if snippet:
                finding.details["run_log_tail"] = snippet
            finding.details["log_url"] = _gcs.run_log_console_url(log_bucket, name)
        # Record the fired finding for the RESOLVED-bookend lifecycle (the caller
        # reconciles it against the prior active set). Tracked even on dry_run so the
        # reconcile is dry too. Suppressed verdicts produce no finding → not tracked.
        if finding is not None and finding_sink is not None:
            finding_sink.append(finding)
        if finding is not None and not dry_run:
            route_finding(finding, pm_repo_path=pm_repo_path)
        if finding is not None:
            logger.warning(
                "exit_code_fleet_monitor: %s verdict=%s exit_code=%s captured=%d->%d",
                name,
                result.verdict,
                exit_code,
                captured_before,
                result.captured_after,
            )

    if not dry_run:
        write_census(storage_client, log_bucket, current_census)

    return results
