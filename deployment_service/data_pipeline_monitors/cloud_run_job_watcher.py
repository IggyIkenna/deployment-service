"""DP-WATCHER-006 — generic per-execution failure detector for every classified GCP Cloud Run Job.

Extends the narrower ``consolidator_oom_watcher.py`` (DP-WATCHER-005) — scoped to
the manifest-consolidator family with OOM classification — to ALL jobs registered in
``cloud_run_job_registry.CLOUD_RUN_JOBS``.

For each job it reads the Cloud Run API's execution history via the injected
``execution_reader`` callable and checks whether the most recent FAILED execution is
more recent than any successful one. When yes, it emits
``PipelineFinding(event=_DP_CLOUD_RUN_JOB_FAILED, tier=EscalationTier.FILE_ISSUE)``
gated on ``MissTracker`` consecutive-miss threshold so a transient single-execution
failure that self-heals on the next scheduler tick does not page.

Plan: ``plans/active/issues/infra_health_audit_alert_coverage_gaps_2026_08_07.md``
todo 2 (gap A — Cloud Run Job compute-failure blind spot).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable

from unified_api_contracts import DeploymentTarget

from deployment_service.data_pipeline_monitors.escalation import EscalationTier, PipelineFinding
from deployment_service.data_pipeline_monitors.meta_watchers import (
    DEFAULT_MIN_CONSECUTIVE_MISSES,
    MissTracker,
    emit_finding,
)

logger = logging.getLogger(__name__)

_DP_CLOUD_RUN_JOB_FAILED = "DP_CLOUD_RUN_JOB_FAILED"

# (job_stem) -> {"failed_count": int, "completion_age_min": float | None} | None.
# Returns None when the most recent execution succeeded (or no failed execution was
# found / API error). Injected for testability; the CLI wires the real Cloud Run v2
# SDK via make_cloud_run_job_execution_reader().
CloudRunJobExecutionReader = Callable[[str], "dict[str, object] | None"]


def _crj_miss_key(job_name: str) -> str:
    return f"{_DP_CLOUD_RUN_JOB_FAILED}::{job_name}"


def check_cloud_run_jobs(
    *,
    jobs: Iterable[DeploymentTarget],
    execution_reader: CloudRunJobExecutionReader,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> dict[str, dict[str, object]]:
    """DP-WATCHER-006 — detect Cloud Run Job execution failures across the full registry.

    For each job in ``jobs``: calls the injected ``execution_reader(job.name)`` to
    retrieve the most recent failed execution diagnostics. Emits
    ``DP_CLOUD_RUN_JOB_FAILED`` (severity=CRITICAL, tier=FILE_ISSUE) when
    ``failed_count > 0``, gated on ``MissTracker`` so a single transient failure
    that self-resolves before the next sweep does not page.

    Returns a dict ``{job_name: finding_details | {}}``. A non-empty sub-dict means
    the finding fired for that job.
    """
    findings: dict[str, dict[str, object]] = {}
    for job in jobs:
        job_name = job.name
        miss_key = _crj_miss_key(job_name)
        diag = execution_reader(job_name)

        if diag is None:
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            findings[job_name] = {}
            continue

        failed_count = int(diag.get("failed_count", 0))
        completion_age_min = diag.get("completion_age_min")

        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=True)
            if misses < min_consecutive:
                logger.info(
                    "cloud_run_job_watcher: '%s' has %d failed task(s) — below consecutive-miss "
                    "threshold (%d/%d), not paging yet",
                    job_name,
                    failed_count,
                    misses,
                    min_consecutive,
                )
                findings[job_name] = {}
                continue

        summary = (
            f"Cloud Run Job '{job_name}' (service={job.service}): latest execution has "
            f"{failed_count} failed task(s)"
            + (f", completed {completion_age_min:.0f}m ago" if completion_age_min is not None else "")
        )
        details: dict[str, object] = {
            "job_name": job_name,
            "service": job.service,
            "asset_group": job.asset_group,
            "failed_count": failed_count,
            "completion_age_min": completion_age_min,
        }
        emit_finding(
            PipelineFinding(
                event=_DP_CLOUD_RUN_JOB_FAILED,
                severity="CRITICAL",
                tier=EscalationTier.FILE_ISSUE,
                summary=summary,
                details=details,
                registry_id="DP-WATCHER-006",
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
        findings[job_name] = details

    return findings


def make_cloud_run_job_execution_reader(
    project_id: str,
    env_prefix: str = "",
    location: str = "asia-northeast1",
) -> CloudRunJobExecutionReader:
    """Return ``job_stem -> {"failed_count": int, "completion_age_min": float | None} | None``.

    For the given job stem, constructs the full Cloud Run Job name as
    ``{env_prefix}-{stem}`` (when env_prefix is non-empty, e.g. "uts-prod") or
    just ``{stem}``, then reads its execution history via ``run_v2.ExecutionsClient``.

    Returns diagnostics when the most recent FAILED execution is newer than any
    successful one. Returns ``None`` when the latest execution succeeded, no failed
    execution was found, or the API errors (fail toward silence — the daily
    DEPLOYMENT_DIGEST still surfaces job-level failures as a backstop).

    Deferred-import of ``google.cloud.run_v2`` (credential-bound SDK boundary) so
    this module stays importable in test environments without the GCP SDK installed.
    An SDK import failure degrades gracefully to a no-op reader for all calls.
    """
    import importlib  # noqa: imports-inside-functions
    from datetime import UTC, datetime  # noqa: imports-inside-functions

    try:
        run_mod = importlib.import_module("google.cloud.run_v2")  # noqa: imports-inside-functions
        executions_client = run_mod.ExecutionsClient()  # pyright: ignore[reportAny]
    except Exception as exc:
        logger.info("cloud-run-job reader unavailable (DP-WATCHER-006 off this sweep): %s", exc)
        return lambda _stem: None

    def _read(job_stem: str) -> dict[str, object] | None:
        if not project_id or not job_stem:
            return None
        job_name = f"{env_prefix}-{job_stem}" if env_prefix else job_stem
        parent = f"projects/{project_id}/locations/{location}/jobs/{job_name}"
        try:
            latest_failed_time: datetime | None = None
            latest_success_time: datetime | None = None
            latest_failed_exec: object | None = None
            for execution in executions_client.list_executions(parent=parent):  # pyright: ignore[reportAny]
                failed_count = int(getattr(execution, "failed_count", 0) or 0)  # pyright: ignore[reportAny]
                completion = getattr(execution, "completion_time", None)  # pyright: ignore[reportAny]
                if completion is None:
                    continue
                completed_dt = (
                    completion
                    if isinstance(completion, datetime)
                    else completion.ToDatetime(tzinfo=UTC)  # pyright: ignore[reportAny]
                )
                if failed_count > 0:
                    if latest_failed_time is None or completed_dt > latest_failed_time:
                        latest_failed_time = completed_dt
                        latest_failed_exec = execution  # pyright: ignore[reportAny]
                else:
                    if latest_success_time is None or completed_dt > latest_success_time:
                        latest_success_time = completed_dt
            if latest_failed_exec is None:
                return None
            # Suppress when a successful execution is NEWER than the latest failure
            # (the job already self-healed on the next trigger).
            if latest_success_time is not None and latest_success_time >= latest_failed_time:  # pyright: ignore[reportOperatorIssue]
                return None
            failed_count = int(getattr(latest_failed_exec, "failed_count", 0) or 0)  # pyright: ignore[reportAny]
            completion_age = (
                (datetime.now(UTC) - latest_failed_time).total_seconds() / 60.0
                if latest_failed_time is not None
                else None
            )
            return {"failed_count": failed_count, "completion_age_min": completion_age}
        except Exception as exc:
            logger.info("cloud-run-job lookup for '%s' → unknown: %s", job_name, exc)
            return None

    return _read


__all__ = [
    "CloudRunJobExecutionReader",
    "check_cloud_run_jobs",
    "make_cloud_run_job_execution_reader",
]
