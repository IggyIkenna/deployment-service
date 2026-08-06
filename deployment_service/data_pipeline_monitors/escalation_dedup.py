"""Escalation-dispatch dedup (Option A, operator-confirmed 2026-08-06).

``plans/active/issues/dp_escalation_worker_dispatch_no_open_issue_check_2026_07_29.md``
documents 30+ redundant ``data_pipeline_failure`` orchestrator-agent spawns for
the SAME already-diagnosed DP-FETCH-009 conditions: a CRITICAL page re-fires
for a cell whose OPEN issue doc already covers it, and whose ``attempted_failed``
backlog has not genuinely grown since the last verified reading — yet
``escalation.py``'s fast-spawn ``repository_dispatch`` fires again anyway.
Before that dispatch fires, ``check_dispatch_dedup`` checks whether an OPEN
issue doc already covers the exact ``(asset_group, data_type)`` tuple +
registry id AND whether anything genuinely NEW has happened since that doc's
last verified reading; if so, the caller SKIPS the fresh full dispatch and
this module appends a lightweight verification note instead (the convention
the doc's own Progress Log entries already converged on by hand).

Split into its own module (not added to ``escalation.py``, already at its
930L file-size cap) — mirrors why ``launch_budget_registry.py`` and
``consolidator_scheduler_watcher.py`` exist as their own files.

Signal choice: raw byte-compare is wrong
-----------------------------------------
The issue doc's later entries (2026-07-30 ``agt-40f31f``) found that a RAW
``attempted_failed`` byte-compare is the WRONG dedup signal — the numerator
can move purely from writes already accounted for, and a naive
numerator-changed check would then wrongly force a full re-diagnosis. The
right signal is "no new *write* activity since the last verified reading",
which the manifest already exposes as ``max_attempted_at`` (the newest
``attempted_failed`` row's ``attempted_at`` — see
``meta_watchers.AttemptedFailedCell.max_attempted_at`` /
``_read_attempted_failed_cells``): an ISO-8601 UTC string that sorts
lexicographically == chronologically, so comparing it needs no extra GCS read
(the alert already carries it).

Checkpoint persistence
-----------------------
The "last verified reading" checkpoint is persisted directly on the matched
OPEN issue doc's own frontmatter (``dp_escalation_checkpoint``), written
surgically (never a full YAML re-serialize, which would reflow human-authored
formatting elsewhere in the doc) so a LATER re-fire at the SAME
``max_attempted_at`` can dedup-skip without re-deriving anything.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

import yaml

logger = logging.getLogger(__name__)


class _FindingLike(Protocol):
    """Structural type for the ``escalation.PipelineFinding`` shape this module
    needs — avoids importing ``escalation.py`` (which imports THIS module for
    the dedup check; a real import would be circular)."""

    event: str
    registry_id: str
    details: dict[str, object]


ISSUES_SUBDIR = "plans/active/issues"
DEDUP_CHECKPOINT_FIELD = "dp_escalation_checkpoint"
_ISSUE_STATUS_OPEN = "open"
_ISSUE_ASSET_GROUP_KEY = "asset_group"
_ISSUE_TAGS_KEY = "tags"
_ISSUE_STATUS_KEY = "status"

# Matches the doc corpus's established convention of embedding the escalated
# tuple as free text (title / summary / source), e.g. "asset_group=cefi
# data_type=derivative_ticker" — there is no structured `data_type:`
# frontmatter field on an issue doc today, so this is the load-bearing match.
_CHECKPOINT_BLOCK_RE = re.compile(
    rf"^{re.escape(DEDUP_CHECKPOINT_FIELD)}:.*?(?=\n[A-Za-z_][A-Za-z0-9_]*:|\Z)",
    re.DOTALL | re.MULTILINE,
)


def parse_frontmatter(text: str) -> dict[str, object] | None:
    """Split a doc's leading ``---``-delimited YAML block and parse it.

    Mirrors ``unified-trading-pm/scripts/docs/docspec.py::parse_frontmatter``'s
    exact minimal contract (``text.split("---", 2)`` + ``yaml.safe_load`` on the
    middle chunk; ``None`` when there's no frontmatter block, a non-dict parse
    coerces to ``{}``) — deliberately re-implemented here (not imported from the
    PM clone) since PM's ``scripts/`` is unversioned tooling, not a package, and
    importing it would be a new cross-repo code-import surface; ``pyyaml`` is
    already a first-class ``deployment-service`` dependency, so this stays a
    ~10-line local helper rather than a hand-rolled ad hoc parser.
    """
    parts = text.split("---", 2)
    if len(parts) < 3:
        return None
    try:
        loaded = cast("object", yaml.safe_load(parts[1]))
    except yaml.YAMLError:
        return None
    if isinstance(loaded, dict):
        return cast("dict[str, object]", loaded)
    return {}


def find_open_issue_for_tuple(
    pm_root: Path, *, asset_group: str, data_type: str, registry_id: str
) -> tuple[Path, dict[str, object]] | None:
    """Find an OPEN ``plans/active/issues/*.md`` doc already covering this exact
    ``(asset_group, data_type, registry_id)`` tuple, or ``None``.

    Matches on: frontmatter ``status: open``, ``asset_group`` present in the
    frontmatter ``asset_group:`` list, ``registry_id`` (lowercased, e.g.
    ``dp-fetch-009``) present in ``tags:`` (the corpus's established
    convention), and the free-text ``asset_group=<ag> data_type=<dt>``
    signature appearing somewhere in the doc body (title/summary/source all
    carry it in every observed doc).
    """
    if not asset_group or not data_type:
        return None
    issues_dir = pm_root / ISSUES_SUBDIR
    if not issues_dir.is_dir():
        return None
    registry_slug = registry_id.strip().lower()
    tuple_re = re.compile(
        rf"asset_group={re.escape(asset_group)}\s+data_type={re.escape(data_type)}",
        re.IGNORECASE,
    )
    for path in sorted(issues_dir.glob("*.md")):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        fm = parse_frontmatter(text)
        if not fm:
            continue
        if str(fm.get(_ISSUE_STATUS_KEY, "")).strip().lower() != _ISSUE_STATUS_OPEN:
            continue
        ag_field = fm.get(_ISSUE_ASSET_GROUP_KEY)
        ag_values: set[str] = set()
        if isinstance(ag_field, list):
            ag_values = {str(a).strip().lower() for a in cast("list[object]", ag_field)}
        if asset_group.lower() not in ag_values:
            continue
        if registry_slug:
            tags_field = fm.get(_ISSUE_TAGS_KEY)
            tag_values: set[str] = set()
            if isinstance(tags_field, list):
                tag_values = {str(t).strip().lower() for t in cast("list[object]", tags_field)}
            if registry_slug not in tag_values:
                continue
        if not tuple_re.search(text):
            continue
        return path, fm
    return None


def checkpoint_has_new_activity(fm: dict[str, object], *, max_attempted_at: str) -> bool:
    """``True`` (never dedup-skip) whenever: ``max_attempted_at`` is empty (can't
    compare, so never guess), the matched doc carries no checkpoint yet
    (nothing to compare against — this reading BOOTSTRAPS one), or
    ``max_attempted_at`` is strictly newer than the checkpoint's (a genuinely
    new ``attempted_failed`` row landed since the last verified reading).
    ISO-8601 UTC timestamp strings compare lexicographically == chronologically,
    so no datetime parsing is needed.
    """
    if not max_attempted_at:
        return True
    checkpoint = fm.get(DEDUP_CHECKPOINT_FIELD)
    if not isinstance(checkpoint, dict):
        return True
    checkpoint_dict = cast("dict[str, object]", checkpoint)
    checkpoint_value = str(checkpoint_dict.get("max_attempted_at", "")).strip()
    if not checkpoint_value:
        return True
    return max_attempted_at > checkpoint_value


def upsert_checkpoint(path: Path, *, max_attempted_at: str) -> bool:
    """Surgically insert/replace ONLY the ``dp_escalation_checkpoint:`` frontmatter
    block on ``path``, leaving every other line byte-identical.

    Deliberately never round-trips the whole frontmatter through
    ``yaml.safe_dump`` — that would reflow human-authored formatting elsewhere in
    the doc (multi-line titles, flow-style lists). Best-effort; returns ``True``
    on a successful write, ``False`` on any failure (never raises — a dedup
    bookkeeping failure must not break the escalation hop).
    """
    try:
        text = path.read_text(encoding="utf-8")
        parts = text.split("---", 2)
        if len(parts) < 3:
            return False
        fm_text = parts[1]
        checked_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        block = (
            f"{DEDUP_CHECKPOINT_FIELD}:\n"
            f"  max_attempted_at: {json.dumps(max_attempted_at)}\n"
            f"  checked_at: {json.dumps(checked_at)}"
        )
        new_fm_text = (
            _CHECKPOINT_BLOCK_RE.sub(block, fm_text, count=1)
            if _CHECKPOINT_BLOCK_RE.search(fm_text)
            else fm_text.rstrip("\n") + "\n" + block + "\n"
        )
        # The replaced block may now be the LAST frontmatter field (the checkpoint
        # regex's end-of-string anchor then consumes the newline that used to sit
        # right before the closing `---`) — always re-normalize a trailing newline
        # so the closing `---` never gets glued onto the checkpoint's last line.
        if not new_fm_text.endswith("\n"):
            new_fm_text += "\n"
        path.write_text(f"---{new_fm_text}---{parts[2]}", encoding="utf-8")
        return True
    except Exception as exc:
        logger.warning("escalation dedup: failed to upsert checkpoint on %s: %s", path, exc)
        return False


def append_progress_log_entry(path: Path, entry: str) -> bool:
    """Append one dated bullet to the doc's trailing ``## Progress Log`` section
    (every observed issue doc's Progress Log is its final section, growing by
    appended bullets — mirrors that convention). Best-effort; never raises."""
    try:
        text = path.read_text(encoding="utf-8")
        stamp = datetime.now(UTC).strftime("%Y-%m-%d")
        line = f"- **{stamp} (escalation-dispatch dedup, automated):** {entry}\n"
        suffix = "\n" + line if "## Progress Log" in text else "\n\n## Progress Log\n\n" + line
        path.write_text(text.rstrip("\n") + suffix, encoding="utf-8")
        return True
    except Exception as exc:
        logger.warning("escalation dedup: failed to append progress log entry to %s: %s", path, exc)
        return False


def check_dispatch_dedup(
    *,
    pm_root: Path,
    asset_group: str,
    data_type: str,
    registry_id: str,
    max_attempted_at: str,
    event: str,
) -> dict[str, object] | None:
    """Option A dedup — see the module docstring above.

    Returns ``None`` when dedup isn't applicable (no matching OPEN issue doc,
    or ``asset_group``/``data_type`` weren't resolvable) — the caller's
    original always-dispatch behaviour is unaffected. Returns a result dict
    (``skipped``, ``issue_path``, ``reason``) when a matching OPEN doc was
    found and evaluated. Never raises: dedup is a pure OPTIMIZATION, so any
    internal failure here falls through to the original behaviour (the caller
    treats ``None`` the same as "no dedup applicable").
    """
    try:
        match = find_open_issue_for_tuple(
            pm_root, asset_group=asset_group, data_type=data_type, registry_id=registry_id
        )
        if match is None:
            return None
        issue_path, issue_fm = match
        has_new_activity = checkpoint_has_new_activity(issue_fm, max_attempted_at=max_attempted_at)
        upsert_checkpoint(issue_path, max_attempted_at=max_attempted_at)
        if has_new_activity:
            return {"skipped": False, "issue_path": str(issue_path), "reason": "new_activity_or_no_checkpoint"}
        append_progress_log_entry(
            issue_path,
            f"Escalation-dispatch DEDUP SKIP — `{event}` ({registry_id or 'n/a'}) re-fired for this exact tuple "
            f"with no new activity (max_attempted_at={max_attempted_at or 'n/a'}) since the last verified "
            "checkpoint on this OPEN issue doc. Per "
            "`/plans/active/issues/dp_escalation_worker_dispatch_no_open_issue_check_2026_07_29.md` Option A, the "
            "fresh `data_pipeline_failure` orchestrator-agent dispatch was SKIPPED (no new work to find) — only "
            "this automated checkpoint verification ran, no worker session spawned.",
        )
        return {"skipped": True, "issue_path": str(issue_path), "reason": "no_new_activity_since_checkpoint"}
    except Exception as exc:  # dedup must never break the escalation hop
        logger.warning("escalation dedup: check failed (falling through to full dispatch): %s", exc)
        return None


def check_dispatch_dedup_for_finding(finding: _FindingLike, *, pm_root: Path) -> dict[str, object] | None:
    """Thin adapter: pull the ``(asset_group, data_type, max_attempted_at)``
    shape out of an ``escalation.PipelineFinding``-like object and hand it to
    :func:`check_dispatch_dedup`. ``None`` when the finding doesn't carry
    that detail shape (today, only DP-FETCH-009 / ``DP_RUN_MOSTLY_EMPTY``
    does — see ``meta_watchers.check_high_attempted_failed``'s ``details=``)."""
    asset_group = str(finding.details.get("asset_group_name", "")).strip()
    data_type = str(finding.details.get("data_type", "")).strip()
    if not asset_group or not data_type:
        return None
    return check_dispatch_dedup(
        pm_root=pm_root,
        asset_group=asset_group,
        data_type=data_type,
        registry_id=finding.registry_id or "",
        max_attempted_at=str(finding.details.get("max_attempted_at", "")).strip(),
        event=finding.event,
    )
