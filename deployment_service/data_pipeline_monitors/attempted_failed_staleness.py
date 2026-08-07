"""Staleness LABELING and TRAILING-WINDOW computation for DP-FETCH-009 cells.

Split into its own module rather than added to ``meta_watchers.py`` (already at its
920-line file-size cap — mirrors why ``renag_tracker.py`` / ``known_dead_cells_registry.py``
exist as siblings instead of growing that file further).

Per
``plans/active/issues/cefi_high_attempted_failed_batch_cluster_2026_07_23.md``'s
"Alerting-hygiene question" — CRITICAL-paging a ``(asset_group, data_type)`` cell that has
sat unchanged for days looks identical, in the alert body, to a cell that JUST failed. This
module computes (a) a trailing-window count used by ``_read_attempted_failed_cells`` to gate
the ``high`` verdict on recent activity only (Option A, RULED 2026-08-06: use a recency
window instead of lifetime manifest counts so a cell whose root cause is fixed stops paging
once the fixed-era failures age out — see
``plans/active/issues/cefi_liquidations_attempted_failed_lifetime_count_stale_2026_07_30.md``),
and (b) the staleness LABEL the operator needs to distinguish "static backlog" from "fresh
regression" in the alert body/details.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd

# A cell whose newest attempted_failed row is at least this many days old is annotated
# "STATIC BACKLOG" in the alert body/details. Purely a labeling threshold.
STATIC_BACKLOG_STALE_DAYS_THRESHOLD = 1

# Trailing window for the high-attempted_failed threshold decision (Option A, 2026-08-06):
# only rows with attempted_at within this many days of now count toward the abs/ratio thresholds.
# A cell whose root cause is fixed stops generating new failures, so its trailing count
# decays to zero within ATTEMPTED_FAILED_TRAILING_DAYS days of the last fix landing.
ATTEMPTED_FAILED_TRAILING_DAYS = 14


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


def trailing_activity_mask(attempted_at: pd.Series, *, now: datetime | None = None) -> pd.Series:
    """Boolean mask, True where ``attempted_at`` (ISO-8601 strings) falls within
    ``ATTEMPTED_FAILED_TRAILING_DAYS`` days of ``now``.

    Used by ``_read_attempted_failed_cells`` to compute the trailing-window count that
    gates the ``high`` verdict (Option A, RULED 2026-08-06). NaT/unparseable rows are
    False — same fail-safe convention as :func:`recent_activity_mask`.
    """
    ts = pd.to_datetime(attempted_at, utc=True, errors="coerce")
    moment = now or datetime.now(UTC)
    cutoff = pd.Timestamp(moment - timedelta(days=ATTEMPTED_FAILED_TRAILING_DAYS))
    return ts >= cutoff


def recent_activity_mask(attempted_at: pd.Series, *, now: datetime | None = None) -> pd.Series:
    """Boolean mask, True where ``attempted_at`` (ISO-8601 strings) falls within
    ``STATIC_BACKLOG_STALE_DAYS_THRESHOLD`` days of ``now``. NaT/unparseable rows are
    False — never counted as recent, mirroring ``stale_days_since``'s fail-safe
    convention. Lets a caller count how much of a cell's ``attempted_failed`` volume is
    genuinely NEW, not just whether the single newest row is recent (see
    ``stale_backlog_annotation``'s ``materiality_floor``)."""
    ts = pd.to_datetime(attempted_at, utc=True, errors="coerce")
    moment = now or datetime.now(UTC)
    cutoff = pd.Timestamp(moment - timedelta(days=STATIC_BACKLOG_STALE_DAYS_THRESHOLD))
    return ts >= cutoff


def stale_backlog_annotation(
    stale_days: int | None,
    *,
    recent_attempted_failed: int = 0,
    materiality_floor: int = 0,
) -> tuple[bool, str]:
    """Return ``(is_static_backlog, summary_suffix)`` for a cell's staleness — a one-line
    fact an alert body can append so a reader can tell "static, already-tracked backlog"
    from "fresh failure" at a glance. Never suppresses/changes paging (see module
    docstring); ``stale_days=None`` (legacy/unknown) annotates nothing.

    ``recent_attempted_failed`` / ``materiality_floor`` (both optional, default 0 —
    exact prior behaviour when unset): a cell can have its single newest row land
    today (``stale_days == 0``) while the OVERWHELMING majority of its
    ``attempted_failed`` count is old, already-tracked debt and only a small, decaying
    trickle of genuinely-new failures lands each day (confirmed live,
    ``cefi_high_attempted_failed_batch_cluster_2026_07_23.md`` re-fire 2026-07-31: a
    300k-row cefi/book_snapshot_5 cell whose fresh 24h volume was 16 rows, the residual
    tail of an already-largely-fixed writer bug). A bare ``stale_days == 0`` check reads
    that as permanently "Fresh" and never lets the already-shipped STATIC BACKLOG
    severity-downgrade apply. When the caller supplies a ``materiality_floor`` (meta_watchers
    passes its own ``ATTEMPTED_FAILED_ABS_THRESHOLD`` — the SAME bar the alert itself uses to
    decide "high" in the first place, just applied to the recent window instead of the
    all-time total), a trickle BELOW that floor is treated as backlog too, even at
    ``stale_days == 0``."""
    if stale_days is None:
        return False, ""
    if stale_days >= STATIC_BACKLOG_STALE_DAYS_THRESHOLD:
        return True, (
            f" STATIC BACKLOG — no new attempted_failed activity in {stale_days}d; "
            f"already-tracked, not a fresh regression."
        )
    if recent_attempted_failed < materiality_floor:
        return True, (
            f" STATIC BACKLOG — only {recent_attempted_failed} attempted_failed row(s) in the last "
            f"{STATIC_BACKLOG_STALE_DAYS_THRESHOLD}d (below the {materiality_floor}-row materiality floor); "
            f"a decaying trickle on already-tracked backlog, not a fresh regression."
        )
    return (
        False,
        f" Fresh — {recent_attempted_failed} attempted_failed row(s) in the last {STATIC_BACKLOG_STALE_DAYS_THRESHOLD}d.",
    )
