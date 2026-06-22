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

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from unified_trading_library.events import log_event  # noqa: qg-deep-import

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


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    return slug[:64] or "data_pipeline_finding"


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
        lines = [
            "---",
            f'title: "{finding.summary}"',
            f"created: {datetime.now(UTC).strftime('%Y-%m-%d')}",
            "author: data-pipeline-fleet-monitor",
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
            "A planning-VM slot triages: confirm vs false-positive, fix the root "
            "cause (or re-baseline the guard), then close this issue.",
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
      - ``auto_recover``  : the caller already ran (or will run) the in-band fix;
                            we record ``auto_recover_ran`` in the event details.
      - ``file_issue``    : write the PM-clone issue doc + ping the orchestrator
                            inbox (no-op when the PM clone isn't on disk — the
                            event carries the candidate list so a planning-VM
                            slot files it from the alert).
      - ``page_operator`` : the CRITICAL event is the page (router-side).

    Returns a structured result dict (the tier taken, the issue path if any).
    Never raises.
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
    if finding.tier is EscalationTier.AUTO_RECOVER:
        event_details["auto_recover_ran"] = auto_recover_ran

    if finding.tier is EscalationTier.FILE_ISSUE and not dry_run:
        pm_root = _resolve_pm_path(pm_repo_path)
        if pm_root is not None:
            issue_path = _write_issue_doc(pm_root, finding)
            if issue_path is not None:
                result["issue_path"] = str(issue_path)
                event_details["filed_issue"] = issue_path.name
                result["inbox_pinged"] = _ping_orchestrator_inbox(pm_root, finding, issue_path)
        else:
            # Cloud Run Job case — no PM clone. The event carries the candidate
            # list; a planning-VM slot files the doc from the alert.
            event_details["file_issue_deferred"] = "no_pm_clone_on_disk"

    if not dry_run:
        try:
            log_event(finding.event, severity=finding.severity, details=event_details)
            result["emitted"] = True
        except Exception as exc:  # never crash on an emit failure
            logger.warning("route_finding: log_event(%s) failed: %s", finding.event, exc)

    return result
