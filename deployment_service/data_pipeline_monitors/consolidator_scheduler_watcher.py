"""DP-WATCHER-003 — a non-``-legacy-`` manifest-consolidator scheduler is PAUSED.

Split into its own module rather than added to ``meta_watchers.py`` (already at
its 900-line file-size cap). Deliberately the INVERSE of
``meta_watchers.check_cron_fired``'s pause-awareness (KEY #2): that check treats
``PAUSED`` as "intentional, suppress" for schedulers paused by design during a
manual-backfill campaign. This check exists for the OTHER case — an ACCIDENTAL
pause on a job nobody meant to stop, which silently starves an entire
asset_group/bucket pair of manifest freshness for an unbounded period
(root-caused 2026-07-13: both sports consolidator schedulers found PAUSED with
no maintenance marker, discovered only because an unrelated investigation
happened to need a fresh index that day —
``features_sports_unbounded_memory_early_history_dates_2026_07_13.md``).
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from unified_trading_library.events import DP_CONSOLIDATOR_SCHEDULER_PAUSED  # noqa: qg-deep-import

from deployment_service.data_pipeline_monitors import meta_watchers
from deployment_service.data_pipeline_monitors.escalation import EscalationTier, PipelineFinding

logger = logging.getLogger(__name__)

# () -> every manifest-consolidator scheduler job's short name currently
# registered in the fleet's canonical location. Discovers jobs LIVE rather than
# reconstructing names from a per-asset_group/kind list that can drift from
# terraform (manifest_consolidator_scheduler.tf's ``manifest_consolidator_buckets``
# map has 10+ market-data/instruments keys plus legacy variants — more than the
# simple 5-asset_group enumeration elsewhere in this codebase covers). Empty list
# ⇒ no jobs found / API error → the caller alerts nothing this sweep rather than
# guessing (fail toward silence on a listing failure, NOT toward inventing names).
SchedulerJobLister = Callable[[], list[str]]


def _consolidator_paused_miss_key(job_name: str) -> str:
    return f"{DP_CONSOLIDATOR_SCHEDULER_PAUSED}::{job_name}"


def check_consolidator_scheduler_paused(
    *,
    scheduler_job_lister: SchedulerJobLister,
    scheduler_state_reader: meta_watchers.SchedulerStateReader,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: meta_watchers.MissTracker | None = None,
    min_consecutive: int = meta_watchers.DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> list[str]:
    """DP-WATCHER-003 — a non-``-legacy-`` manifest-consolidator scheduler is PAUSED.

    ``-legacy-``-tagged jobs are excluded (those ARE deliberately
    paused/deprecated, e.g. the tradfi legacy-bucket crons).

    Returns the list of job names currently found PAUSED-and-unexpected (for
    logging/testing); pages CRITICAL (after the consecutive-miss gate) for each.
    """
    paused_unexpected: list[str] = []
    for job_name in scheduler_job_lister():
        if "-legacy-" in job_name:
            continue
        state = scheduler_state_reader(job_name)
        miss_key = _consolidator_paused_miss_key(job_name)
        if state != "PAUSED":
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            continue
        paused_unexpected.append(job_name)
        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=True)
            if misses < min_consecutive:
                logger.info(
                    "consolidator_scheduler_watcher: DP_CONSOLIDATOR_SCHEDULER_PAUSED for '%s' "
                    "below consecutive-miss threshold (%d/%d) — not paging yet",
                    job_name,
                    misses,
                    min_consecutive,
                )
                continue
        meta_watchers._emit(  # intra-package reuse of the shared RESOLVED-bookend emitter
            PipelineFinding(
                event=DP_CONSOLIDATOR_SCHEDULER_PAUSED,
                severity="CRITICAL",
                tier=EscalationTier.PAGE_OPERATOR,
                summary=f"manifest-consolidator scheduler '{job_name}' is PAUSED (not -legacy-)",
                details={"scheduler_job": job_name},
                registry_id="DP-WATCHER-003",
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
    return paused_unexpected


__all__ = ["SchedulerJobLister", "check_consolidator_scheduler_paused"]
