"""Escalation hop for data-pipeline findings (Phase 0 P1).

Mirrors ``ci_failure_watcher.py``'s auto-recover-vs-escalate model
(``scripts/repo-management/ci_failure_watcher.py`` in unified-trading-pm): a
finding from any Wave-4 watcher is routed through ONE of three tiers per its
registry ``escalation`` value:

  1. ``auto_recover`` — deterministic, in-band fix (e.g. backoff/debounce,
     key-pool rotate, stale-shard re-merge). The watcher supplies the callable;
     this hop just records that the recover tier ran.
  2. ``file_issue``  — writes ``plans/active/issues/<slug>_<date>.md`` into the
     PM clone + pings the orchestrator inbox (the standard audit→issue→plan
     flow). The LLM-judgment verdicts escalate to a planning-VM slot from here.
  3. ``page_operator`` — protective/safety only (CRITICAL with no auto-recover
     scope). Emits the finding's CRITICAL event (which the alerting-service
     router already pages via PagerDuty/Telegram + incident gateway).

The DP_* event itself is ALWAYS emitted (verbose-to-start) via
``unified_trading_library.events.log_event`` regardless of tier — the tier only
governs the EXTRA action (issue-file / recover / page). This module never
raises: an escalation-side failure must not crash the watcher sweep.

Auto-recover actuators (Phase 6 C)
----------------------------------
An ``auto_recover``-tagged finding is routed through ``_DP_RECOVERY_ACTIONS`` —
a small per-event dispatch onto the Layer-0 ``scripts/recovery/`` actuators (the
shipped ``refetch_feed`` pattern: idempotent, dry-runnable, cloud-agnostic,
per-action cooldown / budget). Today wired:

  - ``CONSOLIDATOR_DOWN`` → ``relaunch_consolidator`` (re-execute the
    ``manifest-consolidator-{ag}`` Cloud Run Job — in-scope-autonomous per the
    autonomous-recovery-matrix; bounded 1/cooldown).
  - ``DP_VM_EXIT_NONZERO`` with ``oom``/``exit_code==137`` →
    ``relaunch_backfill_vm`` (re-launch the OOM'd backfill via its launcher;
    ≤2 relaunches per (vm-prefix, day) then page_operator).
  - ``DP_VM_STALL`` → ``relaunch_stalled_vm`` (the heartbeat-stall watcher KILLS
    the hung VM; this RE-LAUNCHES it via its launcher — ≤2 per (vm-prefix, day)
    then page_operator; the relaunch is unconditional on exit code, the stall
    verdict is the trigger). A finding carrying no ``relaunch_launcher`` binding
    falls through to ``file_issue``.
  - ``DP_VM_PREEMPTED`` → ``relaunch_preempted_vm`` (SPOT reclaim; unconditional
    on exit code, same shape as the STALL actuator) — replays the exact
    venues/START_DATE/concurrency/lease captured at launch time
    (``details["launch_env"]``) THROUGH the launcher's own
    ``tardis_concurrency_guard``, so the cap can never be blindly re-violated.
    Unlike the other actuators, EVERY failure path here (no launcher / budget
    spent / guard refusal / launcher error) self-emits a CRITICAL
    ``DP_VM_PREEMPTED_NO_RELAUNCH`` — a preempted backfill silently vanishing
    was the exact bug this closes, so it never falls through quietly.

An ``auto_recover`` event with **no** wired actuator (e.g.
``DP_SOURCE_RATE_LIMITED`` — a backoff the runtime owns) does NOT silently
no-op: it **falls through to ``file_issue``** so a planning-VM slot still picks
it up. The actuator's own cooldown/budget guarantees it cannot loop; a FAILED
actuator (SDK error) also falls through to ``file_issue``.

Cross-repo issue write: the deployment-service watcher runs on a Cloud Run Job /
VM that does NOT hold the PM working tree. ``file_issue`` therefore targets the
PM clone at ``PM_REPO_PATH`` (default ``$UNIFIED_TRADING_PM_PATH``, else the
sibling ``../unified-trading-pm``). When that path is absent (the Cloud Run Job
case), the issue body + ping are emitted into the ``DP_*`` event ``details`` and
the operator-inbox ping is best-effort skipped — the alert itself carries the
candidate list so nothing is lost; a planning-VM slot files the doc from the
alert. The path-present case (a VM/slot with the PM clone) writes the file.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import re
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from http.client import HTTPResponse
from pathlib import Path
from typing import TYPE_CHECKING, cast

from unified_trading_library import (  # noqa: qg-deep-import
    get_secret_client,
    log_event,
)

if TYPE_CHECKING:
    # Static-analysis-only import (never executed at runtime — TYPE_CHECKING is
    # always False there) so basedpyright can type the dynamically-imported
    # actuator class below without violating the load-safe-packaging HARD RULE
    # documented above (scripts.recovery is absent from the packaged wheel).
    from scripts.recovery.relaunch_backfill_vm import RelaunchPreemptedVm as _RelaunchPreemptedVmType


# The Layer-0 recovery actuators live in the top-level ``scripts/recovery/`` dir,
# NOT inside the installed ``deployment_service`` wheel — so in a packaged runtime
# (the deployment-api Cloud Run image installs the package then DROPS the source,
# and ``scripts/vm/`` launchers are absent too) ``scripts.recovery`` is not
# importable. A module-level import there would crash EVERY monitor at load (the
# 2026-06-23 incident). So we capability-PROBE and import lazily inside each
# dispatch fn. When the actuators are absent the ``auto_recover`` tier degrades to
# ``file_issue`` — never a crash, never a silent no-op. SSOT: data-pipeline-alerts.md
# § "Self-heal actuator layer" + the P1 "package the actuators into the image" todo.
def _probe_actuators_available() -> bool:
    """True iff the Layer-0 ``scripts.recovery`` actuators are importable here.

    ``find_spec`` imports the PARENT packages to resolve a dotted name, so when a
    top-level ``scripts`` exists (the image ships deployment-api's own ``scripts/``)
    but has no ``recovery`` submodule it raises ``ModuleNotFoundError`` rather than
    returning ``None``. Catching that narrow error is a capability PROBE — there is
    no import statement here and no fallback module, so it is not a fallback-import.
    """
    try:
        return importlib.util.find_spec("scripts.recovery.relaunch_consolidator") is not None
    except ModuleNotFoundError:
        return False


_ACTUATORS_AVAILABLE = _probe_actuators_available()

logger = logging.getLogger(__name__)

# Default PM clone discovery. The cross-repo issue-file writer targets this path.
_DEFAULT_PM_SIBLINGS = ("../unified-trading-pm", "../../unified-trading-pm")
_ISSUES_SUBDIR = "plans/active/issues"
_ORCHESTRATOR_INBOX = "harsh_orchestrator/_agent_pings.md"


class EscalationTier(StrEnum):
    """The closed set of escalation tiers (mirrors the registry ``escalation`` column)."""

    AUTO_RECOVER = "auto_recover"
    FILE_ISSUE = "file_issue"
    PAGE_OPERATOR = "page_operator"


@dataclass(frozen=True)
class PipelineFinding:
    """A single data-pipeline monitor finding to route through the escalation hop.

    Attributes:
        event: the UTL ``DP_*`` event constant (e.g. ``DP_VM_EXIT_NONZERO``).
        severity: ``INFO`` / ``WARN`` / ``CRITICAL`` (matches the registry).
        tier: the escalation tier governing the extra action.
        summary: one-line human summary (becomes the issue title + alert text).
        details: structured event payload (vm_name, asset_group, candidate list…).
        registry_id: the ``DP-<CAT>-<NNN>`` id, for traceability in the issue doc.
    """

    event: str
    severity: str
    tier: EscalationTier
    summary: str
    details: dict[str, object] = field(default_factory=dict)
    registry_id: str = ""


# Events that have a wired auto_recover actuator. An auto_recover finding whose
# event is NOT here falls through to file_issue (never a silent no-op).
_EVENT_CONSOLIDATOR_DOWN = "CONSOLIDATOR_DOWN"
_EVENT_VM_EXIT_NONZERO = "DP_VM_EXIT_NONZERO"
_EVENT_VM_STALL = "DP_VM_STALL"
# DP_VM_PREEMPTED is deliberately NOT a UTL-exported constant (unlike
# DP_VM_EXIT_NONZERO/DP_VM_GONE_NO_CAPTURE/DP_VM_STALL) — this fix is scoped to
# deployment-service only; ``log_event`` takes a plain string, no registry
# validates it. Mirrors the existing local-string-constant pattern above. Its
# failure-path sibling ``DP_VM_PREEMPTED_NO_RELAUNCH`` is emitted directly by
# the actuator (``relaunch_backfill_vm.RelaunchPreemptedVm``), not routed
# through here, so it has no constant in this module.
_EVENT_VM_PREEMPTED = "DP_VM_PREEMPTED"

# The PM repo + repository_dispatch event-type that fast-spawns an autonomous
# worker for a data-pipeline wall (the same fast path CI failures use). The
# dispatch is BEST-EFFORT — a missing GH token / network failure must NEVER
# break route_finding; the file_issue + PlanRegenLoop path still picks the
# finding up.
_DISPATCH_PM_REPO = "IggyIkenna/unified-trading-pm"
_DISPATCH_EVENT_TYPE = "escalate-to-orchestrator"
_DISPATCH_WALL_TYPE = "data_pipeline_failure"
# Secret-Manager name of the workflow-capable GitHub PAT (carries repo + workflow
# scope). Absent on a token-less Cloud Run Job → dispatch SKIPPED gracefully.
_GH_PAT_SECRET = "GH_PAT"

# Where a filed issue routes a worker. The data-pipeline observability epic owns
# this VM; PlanRegenLoop only ingests an issues/ doc that declares an explicit
# assigned_vm, so this MUST be present for AutoSpawn to pick the finding up.
_ISSUE_PARENT_EPIC = "observability_master"
_ISSUE_ASSIGNED_VM = "vm-cross-cutting"

# A misclassified-empty / not-v9 / divergence finding → the per-AG MTDS/IS repo;
# a VM-lifecycle finding (stall / exit / starvation) → deployment-service. Used
# to NAME the target repo in the actionable issue todo (so a worker knows where
# to look). Keyed on the DP_* event family.
_VM_LIFECYCLE_EVENTS = frozenset(
    {"DP_VM_STALL", "DP_VM_EXIT_NONZERO", "DP_EVENT_LOOP_STARVED", "CONSOLIDATOR_DOWN", _EVENT_VM_PREEMPTED}
)


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    return slug[:64] or "data_pipeline_finding"


def _recover_consolidator(finding: PipelineFinding, *, dry_run: bool) -> dict[str, object]:
    """Auto-recover ``CONSOLIDATOR_DOWN`` → re-execute the consolidator Cloud Run Job.

    Returns the actuator result dict (carries ``status``). ``recovered`` is True
    only when the relaunch SUCCEEDED (or was skipped by its own cooldown — the
    job is already in-flight). A FAILED/PAGE verdict → ``recovered=False`` so the
    caller falls through to file_issue.
    """
    if not _ACTUATORS_AVAILABLE:
        return {
            "recovered": False,
            "actuator": "relaunch_consolidator",
            "result": {"status": "UNAVAILABLE", "reason": "actuators_not_in_runtime"},
        }
    # Dynamic import (NOT a function-level `import` statement) — load-safe
    # where scripts.recovery is absent; guarded by _ACTUATORS_AVAILABLE above.
    _mod = importlib.import_module("scripts.recovery.relaunch_consolidator")

    asset_group = str(finding.details.get("asset_group", "")).strip()
    actuator = _mod.RelaunchConsolidator()
    result = actuator.relaunch(asset_group, dry_run=dry_run)
    recovered = result.get("status") in ("SUCCEEDED", "DRY_RUN")
    return {"recovered": recovered, "actuator": "relaunch_consolidator", "result": result}


# Machine-type escalation for an OOM relaunch (KEY #4, operator 2026-06-23):
# an exit-137 OOM means the machine was too small — a SAME-machine relaunch
# re-OOMs, so the relaunch MUST land on a HIGHER-memory tier. The "next size up"
# is NO LONGER a hardcoded dict here — it CONSUMES the canonical memory-tier
# ladder (``MEMORY_TIER_LADDER`` / ``next_memory_tier`` / ``gce_machine_ram_gb``)
# owned by ``launch_budget_registry`` (import-only; a SEPARATE autonomy agent
# owns that registry). One ladder = the launcher sizes a backfill correctly on
# the FIRST try AND the OOM actuator climbs the SAME rungs on a 137, so the two
# can never drift. The ladder tops out at ``n2-highmem-32`` (256 GB) — so a
# Coinbase-class OOM escalates 128 GB → 256 GB before page_operator (the prior
# hardcoded dict topped at ``n2-highmem-32`` only when the current machine was
# exactly ``n2-highmem-16``; the registry ladder makes 256 GB the canonical top
# rung for every family). The launcher reads ``MACHINE_TYPE`` from ``launcher_env``.
from deployment_service.data_pipeline_monitors.launch_budget_registry import (  # noqa: qg-deep-import
    MEMORY_TIER_LADDER,
    gce_machine_ram_gb,
    memory_tier_for_machine_type,
    next_memory_tier,
)

# When the current machine type isn't known AND has no resolvable RAM, escalate
# to a generous high-mem tier — better to over-provision a relaunch than re-OOM.
# Sourced from the canonical ladder (the 128 GB rung) so it tracks the registry.
_OOM_FALLBACK_MACHINE = "n2-highmem-16"
# The canonical TOP rung (256 GB) — the ceiling an OOM relaunch escalates to
# before page_operator. Derived from the ladder so it can never drift from the
# registry SSOT (extend the ladder there → this follows automatically).
_OOM_TOP_MACHINE = MEMORY_TIER_LADDER[-1].machine_type


def _escalated_machine_type(current: str) -> str:
    """Next-bigger machine type for an OOM relaunch (KEY #4) — consumes the
    canonical ``launch_budget_registry`` memory-tier ladder.

    Resolution (never returns the SAME type — a same-machine relaunch re-OOMs):
      1. ``current`` is a ladder rung → ``next_memory_tier`` (the registry's
         next size up); already at the top (``n2-highmem-32`` / 256 GB) → stay
         at the top rung (the actuator's budget then pages rather than loop).
      2. ``current`` is OFF-ladder but its RAM resolves (``gce_machine_ram_gb``)
         → the SMALLEST ladder rung with strictly MORE RAM (so an off-ladder
         ``n2-standard-8`` = 32 GB escalates to the 64 GB rung, etc.). If it is
         already ≥ the top rung's RAM, stay at the top.
      3. RAM unknown (a never-seen family) → the high-mem fallback (128 GB).
    """
    stripped = current.strip()
    on_ladder = memory_tier_for_machine_type(stripped)
    if on_ladder is not None:
        nxt = next_memory_tier(on_ladder.name)
        # Already at the top rung → escalate to the top (can't go higher; the
        # actuator's relaunch budget pages once spent rather than loop forever).
        return nxt.machine_type if nxt is not None else _OOM_TOP_MACHINE
    current_ram = gce_machine_ram_gb(stripped)
    if current_ram > 0:
        for tier in MEMORY_TIER_LADDER:
            if tier.ram_gb > current_ram:
                return tier.machine_type
        # Off-ladder but already ≥ the top rung's RAM → the top rung is the
        # ceiling (e.g. a 256 GB off-ladder type → stay at n2-highmem-32).
        return _OOM_TOP_MACHINE
    return _OOM_FALLBACK_MACHINE


def _recover_backfill_vm(finding: PipelineFinding, *, dry_run: bool) -> dict[str, object]:
    """Auto-recover ``DP_VM_EXIT_NONZERO`` (OOM only) → re-launch the backfill VM.

    Only the OOM subcase (``oom`` true / ``exit_code==137``) is recoverable here;
    a non-OOM crash returns ``recovered=False`` → file_issue. A budget-exceeded
    relaunch returns ``recovered=False`` too (the actuator already paged).

    **KEY #4 (operator 2026-06-23) — match the actuator to the failure MODE.** An
    OOM relaunch escalates to a HIGHER-memory machine: when the finding carries a
    ``bigger_machine`` hint, this passes a ``MACHINE_TYPE`` env (the next-bigger
    tier, or a high-mem fallback) through ``launcher_env`` so the relaunched VM has
    more memory — a same-machine relaunch would just re-OOM. The launcher reads
    ``MACHINE_TYPE`` (env-overridable in every ``launch-*-vm.sh``).
    """
    details = finding.details
    exit_code_raw = details.get("exit_code")
    exit_code = (
        int(exit_code_raw)
        if isinstance(exit_code_raw, (int, float, str)) and str(exit_code_raw).lstrip("-").isdigit()
        else None
    )
    launcher = str(details.get("relaunch_launcher", "")).strip()
    if not launcher:
        # No launcher binding in the finding → cannot relaunch deterministically.
        return {
            "recovered": False,
            "actuator": "relaunch_backfill_vm",
            "result": {"status": "SKIPPED", "reason": "no_launcher_binding"},
        }
    if not _ACTUATORS_AVAILABLE:
        return {
            "recovered": False,
            "actuator": "relaunch_backfill_vm",
            "result": {"status": "UNAVAILABLE", "reason": "actuators_not_in_runtime"},
        }
    # Dynamic import (NOT a function-level `import` statement) — load-safe
    # where scripts.recovery is absent; guarded by _ACTUATORS_AVAILABLE above.
    _mod = importlib.import_module("scripts.recovery.relaunch_backfill_vm")

    # OOM → escalate the machine type. The current type (when the finding carries
    # it) ladders up; otherwise the high-mem fallback. Only on the OOM path —
    # a non-OOM crash never reaches the actuator's relaunch branch anyway.
    launcher_env: dict[str, str] = {}
    if bool(details.get("bigger_machine")) and exit_code == 137:
        current_machine = str(details.get("machine_type", "")).strip()
        launcher_env["MACHINE_TYPE"] = _escalated_machine_type(current_machine)

    actuator = _mod.RelaunchBackfillVm()
    result = actuator.relaunch(
        str(details.get("vm_name", "")),
        exit_code=exit_code,
        launcher=launcher,
        asset_group=str(details.get("asset_group", "")),
        launcher_env=launcher_env or None,
        dry_run=dry_run,
    )
    recovered = result.get("status") in ("SUCCEEDED", "DRY_RUN")
    return {
        "recovered": recovered,
        "actuator": "relaunch_backfill_vm",
        "result": result,
        "escalated_machine_type": launcher_env.get("MACHINE_TYPE", ""),
    }


def _recover_stalled_vm(finding: PipelineFinding, *, dry_run: bool) -> dict[str, object]:
    """Auto-recover ``DP_VM_STALL`` → re-launch the watchdog-killed stalled VM.

    The heartbeat-stall watcher already KILLED the hung VM; this re-launches it
    via its launcher (bounded ≤2/(vm-prefix, day) then page). Unconditional on
    exit code — the stall verdict is the trigger. A finding with no
    ``relaunch_launcher`` binding returns ``recovered=False`` → file_issue.
    """
    details = finding.details
    launcher = str(details.get("relaunch_launcher", "")).strip()
    if not _ACTUATORS_AVAILABLE:
        return {
            "recovered": False,
            "actuator": "relaunch_stalled_vm",
            "result": {"status": "UNAVAILABLE", "reason": "actuators_not_in_runtime"},
        }
    # Dynamic import (NOT a function-level `import` statement) — load-safe
    # where scripts.recovery is absent; guarded by _ACTUATORS_AVAILABLE above.
    _mod = importlib.import_module("scripts.recovery.relaunch_stalled_vm")

    actuator = _mod.RelaunchStalledVm()
    result = actuator.relaunch(
        str(details.get("vm_name", "")),
        launcher=launcher,
        asset_group=str(details.get("asset_group", "")),
        dry_run=dry_run,
    )
    recovered = result.get("status") in ("SUCCEEDED", "DRY_RUN")
    return {"recovered": recovered, "actuator": "relaunch_stalled_vm", "result": result}


def _recover_preempted_vm(finding: PipelineFinding, *, dry_run: bool) -> dict[str, object]:
    """Auto-recover ``DP_VM_PREEMPTED`` → re-launch the SPOT-reclaimed backfill VM.

    Closes the P0 "SPOT-preemption relaunch gap"
    (``cefi_completion_program_2026_07_15.md``): today a preempted wave was
    DELETED and nothing relaunched it (exit_code_fleet_monitor only logged an
    INFO line that falsely claimed a relaunch). Unconditional on exit code — the
    PREEMPTED verdict itself is the trigger (same shape as ``DP_VM_STALL``'s
    actuator, not the OOM one). Replays ``details["launch_env"]`` (the exact
    venues/START_DATE/concurrency/lease captured at VM-creation time by
    ``lc_write_launch_params``) as the relaunch subprocess's env, so the
    relaunch reproduces the SAME job the preempted VM was running — THROUGH the
    launcher's own ``tardis_concurrency_guard`` (sourced inside every
    Tardis-consuming launcher), never a blind relaunch that could re-create the
    cap violation.

    Unlike OOM/STALL, EVERY failure path here (no launcher bound / budget
    exhausted / the guard refusing because e.g. a live/forward VM holds the
    single Tardis slot / the launcher subprocess erroring) self-emits a
    CRITICAL ``DP_VM_PREEMPTED_NO_RELAUNCH`` from inside the actuator — a
    preempted backfill silently vanishing with zero operator signal was the
    exact bug this closes, so silence on failure is never acceptable here (see
    ``scripts.recovery.relaunch_backfill_vm.RelaunchPreemptedVm``).
    """
    if not _ACTUATORS_AVAILABLE:
        return {
            "recovered": False,
            "actuator": "relaunch_preempted_vm",
            "result": {"status": "UNAVAILABLE", "reason": "actuators_not_in_runtime"},
        }
    # Dynamic import (NOT a function-level `import` statement) — load-safe
    # where scripts.recovery is absent; guarded by _ACTUATORS_AVAILABLE above.
    # cast to the TYPE_CHECKING-only import above — basedpyright can then type
    # the actuator properly instead of cascading `Any` from the untyped
    # importlib.import_module() result (ModuleType has no static member info).
    _mod = importlib.import_module("scripts.recovery.relaunch_backfill_vm")
    actuator_cls = cast("type[_RelaunchPreemptedVmType]", _mod.RelaunchPreemptedVm)

    details = finding.details
    launcher = str(details.get("relaunch_launcher", "")).strip()
    launch_env_raw = details.get("launch_env")
    launch_env: dict[str, str] | None = None
    if isinstance(launch_env_raw, dict):
        launch_env = {str(k): str(v) for k, v in cast("dict[object, object]", launch_env_raw).items()}

    actuator = actuator_cls()
    result = actuator.relaunch(
        str(details.get("vm_name", "")),
        launcher=launcher,
        asset_group=str(details.get("asset_group", "")),
        launch_env=launch_env,
        dry_run=dry_run,
    )
    recovered = result.get("status") in ("SUCCEEDED", "DRY_RUN")
    return {"recovered": recovered, "actuator": "relaunch_preempted_vm", "result": result}


# event-name → actuator dispatch (the auto_recover tier). An auto_recover finding
# whose event is absent here falls through to file_issue (never a silent no-op).
# DP_EVENT_LOOP_STARVED is DELIBERATELY absent — a VM that never emits ANY
# heartbeat is a code bug (a blocking GCS read on the async loop), not a relaunch
# case, so it stays file_issue (its finding is tagged FILE_ISSUE at source).
_DP_RECOVERY_ACTIONS: dict[str, Callable[..., dict[str, object]]] = {
    _EVENT_CONSOLIDATOR_DOWN: _recover_consolidator,
    _EVENT_VM_EXIT_NONZERO: _recover_backfill_vm,
    _EVENT_VM_STALL: _recover_stalled_vm,
    _EVENT_VM_PREEMPTED: _recover_preempted_vm,
}


def _target_repo_for(finding: PipelineFinding) -> str:
    """Name the repo a worker should fix the finding in (for the actionable todo).

    A VM-lifecycle finding (stall / exit / starvation / consolidator) → the
    launcher/monitor home ``deployment-service``. A data-correctness finding
    (misclassified-empty / not-v9 / divergence) → the per-asset_group pipeline
    repo (MTDS owns the capture; the asset_group is in the finding details). When
    the asset_group isn't known, fall back to the generic MTDS repo.
    """
    if finding.event in _VM_LIFECYCLE_EVENTS:
        return "deployment-service"
    return "market-tick-data-service"


def _resolve_pm_path(pm_repo_path: str | None) -> Path | None:
    if pm_repo_path:
        candidate = Path(pm_repo_path)
        return candidate if candidate.is_dir() else None
    here = Path(__file__).resolve()
    for sibling in _DEFAULT_PM_SIBLINGS:
        candidate = (here.parent / sibling).resolve()
        if candidate.is_dir():
            return candidate
    return None


def _write_issue_doc(pm_root: Path, finding: PipelineFinding) -> Path | None:
    """Write a ``plans/active/issues/<slug>_<date>.md`` issue doc into the PM clone.

    Returns the written path, or ``None`` on any failure (never raises).
    """
    try:
        today = datetime.now(UTC).strftime("%Y_%m_%d")
        slug = _slugify(finding.summary)
        issues_dir = pm_root / _ISSUES_SUBDIR
        issues_dir.mkdir(parents=True, exist_ok=True)
        path = issues_dir / f"{slug}_{today}.md"
        if path.exists():
            return path  # idempotent — the daily run already filed this candidate
        target_repo = _target_repo_for(finding)
        root_cause = (
            "the stalled VM's hung-process root cause (almost always an outbound HTTP/scrape call "
            "lacking a `timeout=` per the CLAUDE.md hung-process rule)"
            if finding.event in _VM_LIFECYCLE_EVENTS
            else "the manifest/capture root cause (misclassified-empty vs real gap, not-v9 schema row, "
            "or oracle-expects-but-empty divergence)"
        )
        lines = [
            "---",
            f'title: "{finding.summary}"',
            f"created: {datetime.now(UTC).strftime('%Y-%m-%d')}",
            "author: data-pipeline-fleet-monitor",
            f"parent_epic: {_ISSUE_PARENT_EPIC}",
            f"assigned_vm: {_ISSUE_ASSIGNED_VM}",
            "source:",
            f"  - {finding.registry_id or finding.event}",
            "locked_by: live-defi-rollout",
            "---",
            "",
            f"# {finding.summary}",
            "",
            "## What I found",
            "",
            f"Auto-filed by the data-pipeline fleet monitor (event `{finding.event}`, "
            f"severity {finding.severity}, registry id `{finding.registry_id or 'n/a'}`).",
            "",
            "```json",
            _details_block(finding.details),
            "```",
            "",
            "## Why it matters",
            "",
            "A data-pipeline failure-mode fired its `DP_*` guard. Per "
            "`codex/05-infrastructure/data-pipeline-alerts.md` the alert IS the work "
            "item — drive it to zero (fix the root cause, don't mute).",
            "",
            "## Recommended decision",
            "",
            "A worker diagnoses + fixes the root cause (or re-baselines the guard if "
            "it is a confirmed false-positive), then closes this issue. Cold-start "
            "context: read `unified-trading-pm/cursor-configs/SUB_AGENT_MANDATORY_RULES.md` "
            "in full + `codex/05-infrastructure/data-pipeline-alerts.md` + the finding "
            "`details` JSON above before acting.",
            "",
            "## Todos",
            "",
            f"- [ ] [CODE] P1. {finding.summary} — diagnose + fix {root_cause} in "
            f"`{target_repo}`. Read `SUB_AGENT_MANDATORY_RULES.md` + the data-pipeline "
            f"codex SSOT + the finding `details` above first (event `{finding.event}`, "
            f"registry `{finding.registry_id or 'n/a'}`).",
            "",
        ]
        path.write_text("\n".join(lines), encoding="utf-8")
        return path
    except Exception as exc:  # never crash the sweep on an issue-write failure
        logger.warning("file_issue: failed to write issue doc: %s", exc)
        return None


def _details_block(details: dict[str, object]) -> str:
    parts: list[str] = []
    for key in sorted(details):
        parts.append(f'  "{key}": {details[key]!r}')
    return "{\n" + ",\n".join(parts) + "\n}" if parts else "{}"


def _ping_orchestrator_inbox(pm_root: Path, finding: PipelineFinding, issue_path: Path | None) -> bool:
    """Append an orchestrator-inbox ping referencing the filed issue. Best-effort."""
    try:
        inbox = pm_root / _ORCHESTRATOR_INBOX
        if not inbox.exists():
            return False
        ref = issue_path.name if issue_path else (finding.registry_id or finding.event)
        stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
        entry = (
            f"\n## [data-pipeline-monitor] {stamp}\n"
            f"- {finding.severity} `{finding.event}` — {finding.summary} "
            f"(plan: `{_ISSUES_SUBDIR}/{ref}`)\n"
        )
        with inbox.open("a", encoding="utf-8") as handle:
            handle.write(entry)
        return True
    except Exception as exc:
        logger.warning("file_issue: failed to ping orchestrator inbox: %s", exc)
        return False


def _dispatch_to_orchestrator(finding: PipelineFinding, issue_path: Path | None) -> dict[str, object]:
    """Best-effort `repository_dispatch` → `escalate-to-orchestrator` (the CI fast path).

    Fires the SAME fast-spawn path CI failures use: a `repository_dispatch` to the
    PM repo with `client_payload[wall_type]=data_pipeline_failure`, which the
    `escalate-to-orchestrator.yml` workflow routes to AutoSpawn → an autonomous
    worker. Auths with the workflow-capable `GH_PAT` from Secret-Manager.

    BEST-EFFORT — every failure mode (no GH token on the Cloud Run Job, SM access
    denied, network error, non-2xx) returns a structured `{dispatched: False,
    reason: ...}` and NEVER raises: a dispatch failure must not break the finding
    (mirrors the alerting-service soft-gates). When the token is genuinely absent
    the finding still reaches a worker via the file_issue + PlanRegenLoop path.
    """
    try:
        token = get_secret_client().get_secret(_GH_PAT_SECRET)
    except Exception as exc:
        # No GH token (token-less Cloud Run Job) or SM access denied → SKIP
        # gracefully; the PlanRegenLoop path (Fix 2) still picks the finding up.
        logger.info("dispatch: GH token unavailable, skipping fast-spawn (PlanRegenLoop path covers it): %s", exc)
        return {"dispatched": False, "reason": "no_gh_token"}
    if not token:
        return {"dispatched": False, "reason": "no_gh_token"}

    # Structured relaunch binding — so the worker relaunches DETERMINISTICALLY
    # from the registries (DeploymentsRegistry row + launcher_registry), never by
    # parsing the context text. A relaunch hand-off = a VM-lifecycle finding whose
    # in-image auto_recover could not actuate (actuators/launchers absent from the
    # Cloud Run monitor image — the 2026-06-23 decision: relaunch via a planning-VM
    # worker, not by packaging scripts into the image).
    details = finding.details
    vm_name = str(details.get("vm_name", "")).strip()
    relaunch_launcher = str(details.get("relaunch_launcher", "")).strip()
    deployment_id = str(details.get("deployment_id", "")).strip()
    asset_group = str(details.get("asset_group", "")).strip()
    is_relaunch = finding.event in _VM_LIFECYCLE_EVENTS
    relaunch_ctx = (
        f" RELAUNCH vm={vm_name or '?'} launcher={relaunch_launcher or '(resolve via launcher_registry)'} "
        f"deployment_id={deployment_id or '?'} asset_group={asset_group or '?'}. "
        "Runbook: codex/15-runbooks/incidents/rb_infra_relaunch.md."
        if is_relaunch
        else ""
    )
    context = (
        f"{finding.severity} {finding.event} ({finding.registry_id or 'n/a'}) — {finding.summary}. "
        f"Filed issue: {issue_path.name if issue_path else '(none — alert carries the details)'}."
        f"{relaunch_ctx} "
        "Read SUB_AGENT_MANDATORY_RULES.md + codex/05-infrastructure/data-pipeline-alerts.md + the filed issue."
    )
    payload = {
        "event_type": _DISPATCH_EVENT_TYPE,
        "client_payload": {
            "repo": _target_repo_for(finding),
            "pr_number": "0",
            "wall_type": _DISPATCH_WALL_TYPE,
            "context": context,
            "authoring_slot": "dp-fleet-monitor",
            "model": "sonnet",
            # Structured relaunch binding (empty strings when N/A) — the worker
            # reads these + the DeploymentsRegistry row, not the context text.
            "action": "relaunch_vm" if is_relaunch else "investigate",
            "vm_name": vm_name,
            "relaunch_launcher": relaunch_launcher,
            "deployment_id": deployment_id,
            "asset_group": asset_group,
        },
    }
    request = urllib.request.Request(
        f"https://api.github.com/repos/{_DISPATCH_PM_REPO}/dispatches",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "User-Agent": "dp-fleet-monitor",
        },
    )
    try:
        # nosec B310: the URL host is the fixed GitHub API (https://api.github.com),
        # never a user-controlled / file:/ scheme — the repo path is a module constant.
        resp = cast("HTTPResponse", urllib.request.urlopen(request, timeout=15))  # nosec B310
        try:
            status: int = resp.status
        finally:
            resp.close()
        return {"dispatched": 200 <= status < 300, "reason": f"http_{status}", "http_status": status}
    except urllib.error.HTTPError as exc:
        logger.warning("dispatch: repository_dispatch HTTP %s (best-effort): %s", exc.code, exc)
        return {"dispatched": False, "reason": f"http_{exc.code}", "http_status": exc.code}
    except Exception as exc:  # network / timeout — never break the finding
        logger.warning("dispatch: repository_dispatch failed (best-effort): %s", exc)
        return {"dispatched": False, "reason": f"error: {exc!r}"[:200]}


def route_finding(
    finding: PipelineFinding,
    *,
    pm_repo_path: str | None = None,
    auto_recover_ran: bool = False,
    dry_run: bool = False,
) -> dict[str, object]:
    """Route a finding: emit the ``DP_*`` event + perform its tier's extra action.

    The ``DP_*`` event is ALWAYS emitted (the alerting-service router does the
    Slack mirror + CRITICAL paging). The tier governs the extra action:
      - ``auto_recover``  : invoke the wired Layer-0 actuator (``relaunch_*``)
                            via ``_DP_RECOVERY_ACTIONS``. If the actuator
                            RECOVERED (or its cooldown skipped a redundant fire),
                            the tier stays ``auto_recover``. If there is **no**
                            wired actuator for the event, or the actuator FAILED
                            / paged (budget spent), the finding **falls through
                            to ``file_issue``** so a planning-VM slot still picks
                            it up (never a silent no-op).
      - ``file_issue``    : write the PM-clone issue doc + ping the orchestrator
                            inbox (no-op when the PM clone isn't on disk — the
                            event carries the candidate list so a planning-VM
                            slot files it from the alert).
      - ``page_operator`` : the CRITICAL event is the page (router-side).

    ``auto_recover_ran`` is a back-compat hint a caller may set when it ran the
    fix out-of-band; it is recorded in the event details but the wired actuator
    is the authoritative path.

    Returns a structured result dict (the tier taken, the issue path if any,
    the actuator outcome). Never raises.
    """
    result: dict[str, object] = {
        "event": finding.event,
        "tier": str(finding.tier),
        "severity": finding.severity,
        "issue_path": None,
        "inbox_pinged": False,
        "emitted": False,
    }

    event_details = dict(finding.details)
    event_details["escalation_tier"] = str(finding.tier)
    if finding.registry_id:
        event_details["registry_id"] = finding.registry_id
    # The human-readable one-liner. The alerting-service router builds its Slack
    # summary as ``[<event>] <details.message>`` — without this, every DP_* alert
    # summarised to the bare event name (the generic-alert symptom). Carrying the
    # finding's ``summary`` here makes the delivered alert read e.g. "VM
    # instr-backfill-sports-… terminated with exit_code=137 (OOM) …". Only set
    # when the finding actually has a summary AND the emitter didn't already put
    # a ``message`` in details (the explicit payload wins).
    if finding.summary and "message" not in event_details:
        event_details["message"] = finding.summary

    # The effective tier — an auto_recover finding whose actuator does NOT
    # recover (no wired actuator, FAILED, or budget-paged) falls through to
    # file_issue so it is never lost.
    effective_tier = finding.tier
    if finding.tier is EscalationTier.AUTO_RECOVER:
        event_details["auto_recover_ran"] = auto_recover_ran
        actuator = _DP_RECOVERY_ACTIONS.get(finding.event)
        if actuator is None:
            event_details["auto_recover_actuator"] = "none"
            event_details["auto_recover_fell_through"] = "no_actuator"
            effective_tier = EscalationTier.FILE_ISSUE
        else:
            try:
                outcome = actuator(finding, dry_run=dry_run)
            except Exception as exc:  # an actuator must never crash the sweep
                logger.warning("auto_recover: actuator for %s raised: %s", finding.event, exc)
                outcome = {
                    "recovered": False,
                    "actuator": "error",
                    "result": {"status": "FAILED", "detail": repr(exc)[:300]},
                }
            result["recovery"] = outcome
            event_details["auto_recover_actuator"] = outcome.get("actuator")
            event_details["auto_recover_recovered"] = bool(outcome.get("recovered"))
            if not outcome.get("recovered"):
                event_details["auto_recover_fell_through"] = "actuator_not_recovered"
                effective_tier = EscalationTier.FILE_ISSUE
    result["effective_tier"] = str(effective_tier)

    filed_issue_path: Path | None = None
    if effective_tier is EscalationTier.FILE_ISSUE and not dry_run:
        pm_root = _resolve_pm_path(pm_repo_path)
        if pm_root is not None:
            filed_issue_path = _write_issue_doc(pm_root, finding)
            if filed_issue_path is not None:
                result["issue_path"] = str(filed_issue_path)
                event_details["filed_issue"] = filed_issue_path.name
                result["inbox_pinged"] = _ping_orchestrator_inbox(pm_root, finding, filed_issue_path)
        else:
            # Cloud Run Job case — no PM clone. The event carries the candidate
            # list; a planning-VM slot files the doc from the alert.
            event_details["file_issue_deferred"] = "no_pm_clone_on_disk"

    # Fast CI-parity auto-spawn (Fix 3, best-effort): for a page_operator-tier
    # (CRITICAL) finding OR a confirmed file_issue, ALSO fire the SAME
    # repository_dispatch fast path CI failures use → AutoSpawn a worker. A
    # dispatch failure NEVER breaks the finding (no GH token on a Cloud Run Job →
    # SKIP gracefully; the file_issue + PlanRegenLoop path covers it).
    # A WIRED auto_recover actuator that could NOT recover (UNAVAILABLE in the
    # Cloud Run monitor image / FAILED / budget-paged) MUST hand off to a worker
    # — the relaunch happens on a planning-VM slot (which has scripts/vm + gcloud
    # + the registries), NOT in the monitor image. This dispatch fires even with
    # NO PM clone on disk (the Cloud Run case where filed_issue_path is None), so
    # the relaunch is never stranded. The "no wired actuator" fall-through (e.g. a
    # rate-limit backoff the runtime owns) does NOT dispatch a worker.
    actuator_needs_worker = (
        finding.tier is EscalationTier.AUTO_RECOVER
        and effective_tier is EscalationTier.FILE_ISSUE
        and event_details.get("auto_recover_fell_through") == "actuator_not_recovered"
    )
    should_dispatch = (
        effective_tier is EscalationTier.PAGE_OPERATOR
        or (effective_tier is EscalationTier.FILE_ISSUE and filed_issue_path is not None)
        or actuator_needs_worker
    )
    if should_dispatch and not dry_run:
        dispatch = _dispatch_to_orchestrator(finding, filed_issue_path)
        result["dispatch"] = dispatch
        event_details["fast_spawn_dispatched"] = bool(dispatch.get("dispatched"))
        if not dispatch.get("dispatched"):
            event_details["fast_spawn_skipped"] = str(dispatch.get("reason", "unknown"))

    if not dry_run:
        try:
            log_event(finding.event, severity=finding.severity, details=event_details)
            result["emitted"] = True
        except Exception as exc:  # never crash on an emit failure
            logger.warning("route_finding: log_event(%s) failed: %s", finding.event, exc)

    return result
