"""DP-WATCHER-006 — generic Cloud Run Job per-execution failure detector.

Covers every job in cloud_run_job_registry.CLOUD_RUN_JOBS that is NOT already
handled by a family-specific watcher. The manifest-consolidator-* family is
explicitly excluded: it is handled by consolidator_oom_watcher.py (DP-WATCHER-005)
which carries OOM-specific diagnostics and AUTO_RECOVER routing.

For each registered job, reads its most recent completed execution via
run_v2.ExecutionsClient and emits CLOUD_RUN_JOB_FAILED (PAGE_OPERATOR, CRITICAL)
when the latest execution has failed_count > 0.

Gated on MissTracker so a single transient failure sweep does not page — only
N consecutive sweeps with failed latest-execution escalate.

Issue: plans/active/issues/infra_health_audit_alert_coverage_gaps_2026_08_07.md § (A)
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable

from deployment_service.data_pipeline_monitors.escalation import EscalationTier, PipelineFinding
from deployment_service.data_pipeline_monitors.meta_watchers import (
    DEFAULT_MIN_CONSECUTIVE_MISSES,
    MissTracker,
    emit_finding,
)

logger = logging.getLogger(__name__)

# job_stem -> latest execution diagnostics | None.
# None means: latest execution SUCCEEDED, no completed executions found, or API error.
# Non-None dict carries {"failed_count": int, "succeeded_count": int,
# "job_name": str, "completion_age_min": float | None}.
JobExecutionFailureReader = Callable[[Iterable[str]], dict[str, dict[str, object] | None]]

# The manifest-consolidator family is excluded: DP-WATCHER-005 covers it with
# OOM-signature detection + AUTO_RECOVER routing. Adding DP-WATCHER-006 on top
# would cause double-alerting for the same failure.
_CONSOLIDATOR_JOB_PREFIX = "manifest-consolidator-"


def _failure_miss_key(job_stem: str) -> str:
    return f"CLOUD_RUN_JOB_FAILED::{job_stem}"


def check_cloud_run_job_failures(
    *,
    job_stems: Iterable[str],
    execution_reader: JobExecutionFailureReader,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> dict[str, dict[str, object]]:
    """DP-WATCHER-006 — detect Cloud Run Job executions with failed_count > 0.

    For each job_stem: reads the most recent completed execution via the injected
    execution_reader. If the latest execution has failed_count > 0, emits
    CLOUD_RUN_JOB_FAILED (PAGE_OPERATOR, CRITICAL) gated on MissTracker so a
    transient failure only pages after min_consecutive consecutive sweeps.

    Returns a dict {job_stem: finding_details | {}} for logging/testing. A
    non-empty sub-dict means the finding fired for that stem.
    """
    diagnostics = execution_reader(job_stems)

    findings: dict[str, dict[str, object]] = {}
    for stem in job_stems:
        diag = diagnostics.get(stem)
        miss_key = _failure_miss_key(stem)

        if diag is None:
            # Latest execution SUCCEEDED or no data — reset the miss counter.
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            findings[stem] = {}
            continue

        failed_count = int(diag.get("failed_count", 0))
        succeeded_count = int(diag.get("succeeded_count", 0))
        job_name = str(diag.get("job_name", stem))
        completion_age_min = diag.get("completion_age_min")

        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=True)
            if misses < min_consecutive:
                logger.info(
                    "cloud_run_job_failure_watcher: %s failed execution below consecutive-miss "
                    "threshold (%d/%d) — not paging yet",
                    job_name,
                    misses,
                    min_consecutive,
                )
                findings[stem] = {}
                continue

        age_str = (
            f", completion_age={completion_age_min:.0f}m"
            if completion_age_min is not None
            else ""
        )
        summary = (
            f"{job_name}: latest execution failed "
            f"(failed_count={failed_count}, succeeded_count={succeeded_count}{age_str})"
        )
        details: dict[str, object] = {
            "job_stem": stem,
            "job_name": job_name,
            "failed_count": failed_count,
            "succeeded_count": succeeded_count,
            "completion_age_min": completion_age_min,
        }
        emit_finding(
            PipelineFinding(
                event="CLOUD_RUN_JOB_FAILED",
                severity="CRITICAL",
                tier=EscalationTier.PAGE_OPERATOR,
                summary=summary,
                details=details,
                registry_id="DP-WATCHER-006",
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
        findings[stem] = details

    return findings


def make_cloud_run_job_execution_reader(
    project_id: str,
    env_prefix: str = "",
    location: str = "asia-northeast1",
) -> JobExecutionFailureReader:
    """Return ``job_stems -> {stem: failure_diagnostics | None}`` for DP-WATCHER-006.

    Reads the most recent completed execution of each job via run_v2.ExecutionsClient.
    Returns failure diagnostics when the LATEST completed execution has failed_count > 0.
    Returns None per-stem when:
    - The latest execution SUCCEEDED (failed_count == 0)
    - No completed executions were found
    - The API returned an error (fail toward silence — MissTracker handles the gate)

    Deferred-import of google.cloud.run_v2 (credential-bound SDK boundary).
    An SDK import failure returns an empty dict for every call.
    """
    import importlib  # noqa: imports-inside-functions
    from datetime import UTC, datetime  # noqa: imports-inside-functions

    try:
        run_mod = importlib.import_module("google.cloud.run_v2")  # noqa: imports-inside-functions — credential-bound SDK, deferred
        executions_client = run_mod.ExecutionsClient()  # pyright: ignore[reportAny] — dynamic cloud-SDK boundary
    except Exception as exc:
        logger.info(
            "cloud-run-job-failure reader unavailable (DP-WATCHER-006 off this sweep): %s", exc
        )
        return lambda _stems: {}

    def _read(job_stems: Iterable[str]) -> dict[str, dict[str, object] | None]:
        if not project_id:
            return dict.fromkeys(job_stems, None)
        result: dict[str, dict[str, object] | None] = {}
        for stem in job_stems:
            job_name = f"{env_prefix}-{stem}" if env_prefix else stem
            parent = f"projects/{project_id}/locations/{location}/jobs/{job_name}"
            result[stem] = None
            try:
                # Find the most recently completed execution (succeeded OR failed).
                latest_execution: object | None = None
                latest_completion: datetime | None = None
                for execution in executions_client.list_executions(parent=parent):  # pyright: ignore[reportAny] — dynamic SDK page iterator
                    completion = getattr(execution, "completion_time", None)  # pyright: ignore[reportAny] — dynamic SDK attr
                    if completion is None:
                        continue
                    completed_dt = (
                        completion
                        if isinstance(completion, datetime)
                        else completion.ToDatetime(tzinfo=UTC)  # pyright: ignore[reportAny] — protobuf Timestamp
                    )
                    if latest_completion is None or completed_dt > latest_completion:
                        latest_completion = completed_dt
                        latest_execution = execution  # pyright: ignore[reportAny]
                if latest_execution is None:
                    # No completed executions for this job — leave result[stem]=None.
                    continue
                failed_count = int(getattr(latest_execution, "failed_count", 0) or 0)  # pyright: ignore[reportAny] — dynamic SDK attr
                if failed_count <= 0:
                    # Latest execution succeeded — no failure to report.
                    continue
                succeeded_count = int(getattr(latest_execution, "succeeded_count", 0) or 0)  # pyright: ignore[reportAny] — dynamic SDK attr
                completion_age = (
                    (datetime.now(UTC) - latest_completion).total_seconds() / 60.0
                    if latest_completion is not None
                    else None
                )
                result[stem] = {
                    "failed_count": failed_count,
                    "succeeded_count": succeeded_count,
                    "job_name": job_name,
                    "completion_age_min": completion_age,
                }
            except Exception as exc:
                logger.info(
                    "cloud-run-job-failure lookup for %s -> unknown: %s", job_name, exc
                )
        return result

    return _read


__all__ = [
    "JobExecutionFailureReader",
    "check_cloud_run_job_failures",
    "make_cloud_run_job_execution_reader",
]
