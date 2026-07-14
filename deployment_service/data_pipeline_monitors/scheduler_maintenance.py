"""Coordinated pause/resume for Cloud Scheduler crons — safe against the recurring
cron-collision failure mode (plans/active/issues/sports_cf8_available_at_backfill_regression_2026_07_13.md
Finding 1): a manual "please don't touch this" pause is invisible to a second agent, or an
operator running a raw ``gcloud scheduler jobs resume`` from a terminal unaware the job was
paused on purpose. Recurred 3+ times in one day against the sports manifest-consolidator
crons alone.

Wraps ``unified_trading_library.maintenance_window`` (the generic, race-free CAS marker)
around the actual Cloud Scheduler pause/resume calls, mirroring the deferred-import
Cloud Scheduler client pattern already used by ``cli.py``'s ``_make_scheduler_state_reader``
(``google.cloud`` is QG-banned outside this credential-bound boundary).

Usage::

    pause_for_maintenance(
        surface="instruments-sports",
        bucket="instruments-store-sports-prd-<project>",
        scheduler_jobs=["prd-manifest-consolidator-instruments-sports-cron"],
        reason="CF-8 available_at full-corpus backfill",
        locked_by="slot-6",
    )
    ...  # do the backfill
    resume_after_maintenance(
        bucket="instruments-store-sports-prd-<project>",
        scheduler_jobs=["prd-manifest-consolidator-instruments-sports-cron"],
        locked_by="slot-6",
    )

``--status`` (below, and the CLI entrypoint) lets ANY caller — including an operator about
to run a raw ``gcloud scheduler jobs resume`` — check whether a window is live BEFORE
touching the job, directly targeting the visibility gap Finding 1 named.

**This module does NOT retrofit the existing ad-hoc CF-8 backfill scripts to use it** —
that adoption is a separate, deliberate follow-up (routing which scripts should switch and
when is an operator/infra-owner decision, not this module's own scope). This ships the
primitive + CLI so that decision has something concrete to adopt.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import sys
from collections.abc import Callable, Sequence
from typing import cast

from unified_trading_library import (
    MaintenanceWindow,
    MaintenanceWindowActiveError,
    UnifiedCloudConfig,
    acquire_maintenance_window,
    get_storage_client,
    read_maintenance_window,
    release_maintenance_window,
)

logger = logging.getLogger(__name__)

#: The fleet's canonical Cloud Scheduler region (mirrors cli.py's ``_make_scheduler_state_reader``).
_SCHEDULER_LOCATION = "asia-northeast1"

#: A job-name -> None action (pause or resume that one job). Matches the existing
#: ``SchedulerStateReader = Callable[[str], str | None]`` injection pattern in meta_watchers.py
#: so this module's tests never need real GCP credentials.
SchedulerAction = Callable[[str], None]


def _project_id() -> str:
    """Resolve the GCP project id from UnifiedCloudConfig (never hardcoded)."""
    try:
        cfg = UnifiedCloudConfig()
    except (ValueError, RuntimeError, OSError):
        return ""
    return getattr(cfg, "gcp_project_id", "") or ""


def _make_scheduler_action(method_name: str) -> SchedulerAction:
    """Return a ``job_name -> None`` action bound to ``client.<method_name>(name=...)``.

    Deferred-imports ``google.cloud.scheduler_v1`` (credential-bound SDK boundary) —
    mirrors ``cli.py``'s ``_make_scheduler_state_reader``, never imported at module top so
    this module stays import-safe + credential-free until an action actually runs.
    """
    project = _project_id()
    scheduler_mod = importlib.import_module("google.cloud.scheduler_v1")  # noqa: imports-inside-functions — credential-bound SDK, deferred
    client = scheduler_mod.CloudSchedulerClient()  # pyright: ignore[reportAny] — dynamic cloud-SDK boundary

    def _act(job_name: str) -> None:
        qualified = f"projects/{project}/locations/{_SCHEDULER_LOCATION}/jobs/{job_name}"
        getattr(client, method_name)(name=qualified)  # pyright: ignore[reportAny] — dynamic SDK method dispatch

    return _act


def make_scheduler_pauser() -> SchedulerAction:
    """Real ``pause_job`` action, credentials resolved at call time."""
    return _make_scheduler_action("pause_job")


def make_scheduler_resumer() -> SchedulerAction:
    """Real ``resume_job`` action, credentials resolved at call time."""
    return _make_scheduler_action("resume_job")


def pause_for_maintenance(
    *,
    bucket: str,
    surface: str,
    scheduler_jobs: Sequence[str],
    reason: str,
    locked_by: str,
    ttl_minutes: int = 120,
    pauser: SchedulerAction | None = None,
) -> MaintenanceWindow:
    """Acquire the maintenance window THEN pause every named scheduler job.

    Order matters: the window is acquired first (raises :class:`MaintenanceWindowActiveError`
    if another caller already holds a live one for this surface), so a caller never pauses a
    cron it then can't safely account for. ``pauser`` defaults to the real Cloud Scheduler
    action; tests inject a fake to avoid credentials.
    """
    window = acquire_maintenance_window(
        get_storage_client(),
        bucket,
        surface=surface,
        scheduler_jobs=tuple(scheduler_jobs),
        reason=reason,
        locked_by=locked_by,
        ttl_minutes=ttl_minutes,
    )
    act = pauser or make_scheduler_pauser()
    for job in scheduler_jobs:
        act(job)
        logger.info("Paused scheduler job %r for maintenance (%r)", job, reason)
    return window


def resume_after_maintenance(
    *,
    bucket: str,
    scheduler_jobs: Sequence[str],
    locked_by: str,
    force: bool = False,
    resumer: SchedulerAction | None = None,
) -> bool:
    """Release the maintenance window THEN resume every named scheduler job.

    Refuses (raises :class:`MaintenanceWindowActiveError`) when a LIVE window is held by a
    DIFFERENT caller and ``force`` is not set — this is the enforcement point that stops a
    second agent (or an operator who checked ``--status`` first) from resuming a cron mid
    someone else's declared maintenance. Returns whatever
    :func:`~unified_trading_library.maintenance_window.release_maintenance_window` returns
    (``True`` iff a window was actually cleared).
    """
    cleared = release_maintenance_window(get_storage_client(), bucket, locked_by=locked_by, force=force)
    act = resumer or make_scheduler_resumer()
    for job in scheduler_jobs:
        act(job)
        logger.info("Resumed scheduler job %r after maintenance", job)
    return cleared


def maintenance_status(bucket: str) -> MaintenanceWindow | None:
    """Read-only check — what's currently held (if anything), for ``--status`` / pre-flight."""
    return read_maintenance_window(get_storage_client(), bucket)


# ── CLI entrypoint ────────────────────────────────────────────────────────────
#
# python -m deployment_service.data_pipeline_monitors.scheduler_maintenance \
#     --bucket <bucket> --status
#
# A single sanctioned command any agent OR operator can run to check whether a surface is
# under a declared maintenance window before touching its scheduler jobs directly — this is
# the visibility mechanism Finding 1 asked for.


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bucket", required=True, help="The surface's GCS bucket (e.g. instruments-store-sports-prd-<project>)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_status = sub.add_parser("status", help="Read-only: show the current maintenance window, if any.")
    p_status.set_defaults(command="status")

    p_pause = sub.add_parser("pause", help="Acquire a window + pause the named scheduler job(s).")
    p_pause.add_argument("--surface", required=True)
    p_pause.add_argument("--job", action="append", required=True, dest="jobs", help="Repeatable.")
    p_pause.add_argument("--reason", required=True)
    p_pause.add_argument("--locked-by", required=True)
    p_pause.add_argument("--ttl-minutes", type=int, default=120)

    p_resume = sub.add_parser("resume", help="Release the window + resume the named scheduler job(s).")
    p_resume.add_argument("--job", action="append", required=True, dest="jobs", help="Repeatable.")
    p_resume.add_argument("--locked-by", required=True)
    p_resume.add_argument("--force", action="store_true", help="Override a foreign live window.")

    args = parser.parse_args(argv)
    # argparse.Namespace attribute access is untyped (reportAny) — cast each field to its
    # declared shape once here, mirroring cli.py's own args.* handling convention.
    command: str = str(cast("object", args.command))
    bucket: str = str(cast("object", args.bucket))

    if command == "status":
        window = maintenance_status(bucket)
        if window is None:
            print(f"[maintenance-window] {bucket}: no live window — safe to pause/resume freely.")
            return 0
        print(
            f"[maintenance-window] {bucket}: HELD by {window.locked_by!r} "
            f"(surface={window.surface!r}, reason={window.reason!r}, "
            f"scheduler_jobs={list(window.scheduler_jobs)}, until {window.expires_at}). "
            "Do NOT resume without coordinating with the holder (or pass --force if you are "
            "certain it is safe to override)."
        )
        return 1

    jobs: list[str] = [str(j) for j in cast("list[object]", args.jobs)]
    locked_by: str = str(cast("object", args.locked_by))

    try:
        if command == "pause":
            surface: str = str(cast("object", args.surface))
            reason: str = str(cast("object", args.reason))
            ttl_minutes: int = int(cast("object", args.ttl_minutes))
            window = pause_for_maintenance(
                bucket=bucket,
                surface=surface,
                scheduler_jobs=jobs,
                reason=reason,
                locked_by=locked_by,
                ttl_minutes=ttl_minutes,
            )
            print(f"[maintenance-window] acquired + paused {jobs} until {window.expires_at}")
            return 0
        if command == "resume":
            force: bool = bool(cast("object", args.force))
            resume_after_maintenance(
                bucket=bucket,
                scheduler_jobs=jobs,
                locked_by=locked_by,
                force=force,
            )
            print(f"[maintenance-window] released + resumed {jobs}")
            return 0
    except MaintenanceWindowActiveError as exc:
        print(f"[maintenance-window] REFUSED: {exc}", file=sys.stderr)
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(_cli())
