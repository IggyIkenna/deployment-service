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

MAINTENANCE-WINDOW-AWARE (2026-07-29, closes the DP-WATCHER-003 maintenance-window
gap — ``dp_watcher_003_consolidator_scheduler_paused_maintenance_window_gap_2026_07_29.md``).
Still pages on any PAUSED job by default; but if the pause was registered via
``scheduler_maintenance.pause_for_maintenance`` (a live, unexpired window whose
``scheduler_jobs`` names this job), the page downgrades to an INFO log instead —
see ``check_consolidator_scheduler_paused``'s ``maintenance_window_reader`` param.
An unregistered pause (the accidental case this watcher exists to catch) still
pages exactly as before.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable

from unified_trading_library import MaintenanceWindow
from unified_trading_library.events import DP_CONSOLIDATOR_SCHEDULER_PAUSED  # noqa: qg-deep-import

from deployment_service.data_pipeline_monitors import meta_targets, meta_watchers, scheduler_maintenance
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

# job_name -> the live MaintenanceWindow covering it, or None (no bucket resolvable
# for this job / no live window / a live window exists but doesn't name this job).
# Mirrors the SchedulerJobLister/SchedulerStateReader injection pattern so this
# module stays credential-free + testable — ``make_consolidator_maintenance_window_reader``
# below combines a job-name -> bucket map with ``scheduler_maintenance.maintenance_status``.
MaintenanceWindowReader = Callable[[str], MaintenanceWindow | None]


def _consolidator_paused_miss_key(job_name: str) -> str:
    return f"{DP_CONSOLIDATOR_SCHEDULER_PAUSED}::{job_name}"


def check_consolidator_scheduler_paused(
    *,
    scheduler_job_lister: SchedulerJobLister,
    scheduler_state_reader: meta_watchers.SchedulerStateReader,
    maintenance_window_reader: MaintenanceWindowReader | None = None,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: meta_watchers.MissTracker | None = None,
    min_consecutive: int = meta_watchers.DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> list[str]:
    """DP-WATCHER-003 — a non-``-legacy-`` manifest-consolidator scheduler is PAUSED.

    ``-legacy-``-tagged jobs are excluded (those ARE deliberately
    paused/deprecated, e.g. the tradfi legacy-bucket crons).

    **MAINTENANCE-WINDOW-AWARE (2026-07-29).** When ``maintenance_window_reader`` is
    injected AND it returns a live (unexpired) window whose ``scheduler_jobs`` names
    this ``job_name``, the pause is a SANCTIONED, plan-tracked pause (registered via
    ``scheduler_maintenance.pause_for_maintenance``) — downgrade to an INFO log and
    skip the page, mirroring ``check_cron_fired``'s KEY #2 pause-suppression. An
    expired/absent window, a window that doesn't name this job, or no reader
    injected at all, still pages CRITICAL exactly as before — this watcher's whole
    reason to exist is catching an ACCIDENTAL pause, so an unregistered pause must
    never silently suppress (fail toward alerting).

    Returns the list of job names currently found PAUSED-and-unexpected (for
    logging/testing) — a job suppressed by a live maintenance window is NOT
    "unexpected" and is excluded from this list; pages CRITICAL (after the
    consecutive-miss gate) for each returned job.
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
        if maintenance_window_reader is not None:
            window = maintenance_window_reader(job_name)
            if window is not None and job_name in window.scheduler_jobs:
                logger.info(
                    "consolidator_scheduler_watcher: DP_CONSOLIDATOR_SCHEDULER_PAUSED suppressed "
                    "for '%s' — live maintenance window (surface=%r, reason=%r, locked_by=%r, "
                    "until %s) covers this job; sanctioned pause, not paging",
                    job_name,
                    window.surface,
                    window.reason,
                    window.locked_by,
                    window.expires_at,
                )
                if miss_tracker is not None:
                    miss_tracker.register(miss_key, stale=False)  # sanctioned pause → not a real miss
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
                registry_id="DP-WATCHER-004",
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
    return paused_unexpected


def make_consolidator_maintenance_window_reader() -> MaintenanceWindowReader:
    """Return ``job_name -> live MaintenanceWindow | None`` for ``check_consolidator_scheduler_paused``.

    Lives here (not in ``cli.py``, unlike the credential-bound ``_make_*`` factories
    it sits alongside) specifically so it doesn't grow that file — ``cli.py`` is at
    its own QG file-size ceiling already; this factory needs no deferred cloud-SDK
    import of its own (``scheduler_maintenance.maintenance_status`` already owns
    that boundary), so nothing about it requires living there.

    Maps each of the 10 core per-asset_group consolidator scheduler jobs (5
    market-data-tick + 5 instruments-store — the ``manifest_consolidator_buckets``
    terraform map's core set) to its owning bucket, then reads any live window via
    ``scheduler_maintenance.maintenance_status``. A job outside this map (e.g. a
    Group-B "extended" features/strategy/execution/ml consolidator, or any future
    job) resolves to ``None`` — fail toward paging, the same direction every
    sibling ``_make_*`` factory in ``cli.py`` already takes on a resolution failure.
    """
    job_to_bucket: dict[str, str] = {}
    for ag in meta_targets.ASSET_GROUPS:
        with contextlib.suppress(Exception):
            job_to_bucket[meta_targets.consolidator_scheduler_job(ag)] = meta_targets.market_data_bucket(ag)
        with contextlib.suppress(Exception):
            job_to_bucket[meta_targets.consolidator_instruments_scheduler_job(ag)] = (
                meta_targets.instruments_store_bucket(ag)
            )

    def _read(job_name: str) -> MaintenanceWindow | None:
        bucket = job_to_bucket.get(job_name)
        if bucket is None:
            return None
        return scheduler_maintenance.maintenance_status(bucket)

    return _read


__all__ = [
    "MaintenanceWindowReader",
    "SchedulerJobLister",
    "check_consolidator_scheduler_paused",
    "make_consolidator_maintenance_window_reader",
]
