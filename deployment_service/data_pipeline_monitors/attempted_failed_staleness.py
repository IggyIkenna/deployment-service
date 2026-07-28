"""Staleness LABELING for DP-FETCH-009 (``DP_RUN_MOSTLY_EMPTY``) attempted_failed cells.

Split into its own module rather than added to ``meta_watchers.py`` (already at its
920-line file-size cap — mirrors why ``renag_tracker.py`` / ``known_dead_cells_registry.py``
exist as siblings instead of growing that file further).

Per
``plans/active/issues/cefi_high_attempted_failed_batch_cluster_2026_07_23.md``'s
"Alerting-hygiene question" — CRITICAL-paging a ``(asset_group, data_type)`` cell that has
sat unchanged for days looks identical, in the alert body, to a cell that JUST failed. This
module computes the plain FACT the operator needs to tell them apart (how many days since the
cell's newest ``attempted_failed`` row) and a labeling threshold for annotating the alert
body/details — it does **not** decide whether to suppress or change paging cadence for a
stale cell. That is a separate, still-open policy question for the operator/alerting-service
owner (visible pressure on a known backlog vs. alert fatigue) — this module only makes the
distinction VISIBLE, deliberately leaving delivery behavior untouched.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

# A cell whose newest attempted_failed row is at least this many days old is annotated
# "STATIC BACKLOG" in the alert body/details. Purely a labeling threshold — does NOT gate
# whether/how often DP_RUN_MOSTLY_EMPTY still pages (see module docstring).
STATIC_BACKLOG_STALE_DAYS_THRESHOLD = 1


def stale_days_since(max_attempted_at: str, *, now: datetime | None = None) -> int | None:
    """Whole days between ``max_attempted_at`` (ISO-8601) and ``now`` (default: current
    UTC time). ``None`` for an empty/unparseable timestamp — mirrors
    ``known_dead_cells_registry.is_known_dead``'s own fail-safe convention of never
    asserting staleness on a data quirk."""
    if not max_attempted_at:
        return None
    ts = pd.to_datetime(max_attempted_at, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    moment = now or datetime.now(UTC)
    return max(0, (moment - ts.to_pydatetime()).days)


def stale_backlog_annotation(stale_days: int | None) -> tuple[bool, str]:
    """Return ``(is_static_backlog, summary_suffix)`` for a cell's staleness — a one-line
    fact an alert body can append so a reader can tell "static, already-tracked backlog"
    from "fresh failure" at a glance. Never suppresses/changes paging (see module
    docstring); ``stale_days=None`` (legacy/unknown) annotates nothing."""
    if stale_days is None:
        return False, ""
    if stale_days >= STATIC_BACKLOG_STALE_DAYS_THRESHOLD:
        return True, (
            f" STATIC BACKLOG — no new attempted_failed activity in {stale_days}d; "
            f"already-tracked, not a fresh regression."
        )
    return False, f" Fresh — newest attempted_failed activity {stale_days}d ago."
