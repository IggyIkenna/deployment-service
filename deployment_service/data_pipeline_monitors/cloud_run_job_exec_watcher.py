"""DP-VM-013 — generic Cloud Run Job per-execution failure detector.

Generalises ``consolidator_oom_watcher.py`` (DP-WATCHER-005) from the specific
``manifest-consolidator-{ag}`` family to the FULL Cloud Run job inventory in
``cloud_run_job_registry.CLOUD_RUN_JOBS``.  The OOM watcher needs a cross-check
with index staleness to avoid alerting when the consolidator self-recovered; this
simpler watcher inspects the MOST RECENTLY COMPLETED execution for each registered
job and alerts when ``failed_count > 0``, gated on the standard ``MissTracker``
consecutive-miss threshold.

The gap this closes: findings 2 and 7 from the 2026-08-07 infra-health audit —
``client-reporting-batch`` OOM (512Mi, hourly since 2026-08-06) and
``live-event-log-compactor`` OOM (2026-08-01→08-07) both showed zero Slack alerts.
No Cloud Run Job failure detector existed outside the consolidator family.

Jobs whose Cloud Run names don't match the ``{env_prefix}-{stem}`` convention
(e.g. the consolidator family, which uses ``{env_prefix}-manifest-consolidator-
market-data-{ag}``) will receive NotFound responses from the execution API — these
are silently dropped as ``None`` (no alert), leaving them fully covered by the
dedicated ``consolidator_oom_watcher``.

Plan: ``plans/active/issues/infra_health_audit_alert_coverage_gaps_2026_08_07.md``
todo 2.
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

# job_stems -> {stem: {"failed_count": int, "completion_age_min": float | None} | None}
# None per stem when: most-recent completed execution succeeded, no executions found, or API error.
# Injected so the check stays credential-free + testable.
JobExecFailedReader = Callable[[Iterable[str]], dict[str, dict[str, object] | None]]


def _job_miss_key(stem: str) -> str:
    return f"DP_CLOUD_RUN_JOB_EXEC_FAILED::{stem}"


def all_cloud_run_job_stems() -> list[str]:
    """Return all job stems from the Cloud Run job registry (deferred import)."""
    import deployment_service.cloud_run_job_registry as _reg  # noqa: imports-inside-functions — deferred avoids circular module-level dep

    return [job.name for job in _reg.CLOUD_RUN_JOBS]


def check_cloud_run_job_exec_failures(
    *,
    job_stems: Iterable[str],
    execution_failed_reader: JobExecFailedReader,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> dict[str, dict[str, object]]:
    """DP-VM-013 — detect failed Cloud Run Job executions across the full job registry.

    For each stem in ``job_stems``: reads the most-recently-completed execution via
    the injected ``execution_failed_reader``, emits
    ``PipelineFinding(event="DP_CLOUD_RUN_JOB_EXEC_FAILED", tier=FILE_ISSUE)`` when
    ``failed_count > 0``.  Gated on ``MissTracker`` so a transient blip only files an
    issue after ``min_consecutive`` sweeps.

    Returns a dict ``{stem: finding_details | {}}`` for logging/testing.  A non-empty
    sub-dict means the finding fired for that job stem.
    """
    stems_list = list(job_stems)
    diagnostics = execution_failed_reader(stems_list)

    findings: dict[str, dict[str, object]] = {}
    for stem in stems_list:
        diag = diagnostics.get(stem)
        miss_key = _job_miss_key(stem)

        if diag is None:
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            findings[stem] = {}
            continue

        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=True)
            if misses < min_consecutive:
                logger.info(
                    "cloud_run_job_exec_watcher: '%s' below consecutive-miss threshold (%d/%d) — not filing yet",
                    stem,
                    misses,
                    min_consecutive,
                )
                findings[stem] = {}
                continue

        failed_count = int(diag.get("failed_count", 0))
        completion_age_min = diag.get("completion_age_min")
        age_str = f"{completion_age_min:.0f}m ago" if completion_age_min is not None else "unknown age"
        summary = (
            f"Cloud Run Job '{stem}' most-recent execution has {failed_count} failed task(s) "
            f"(completed {age_str})"
        )
        details: dict[str, object] = {
            "job_name": stem,
            "failed_count": failed_count,
            "completion_age_min": completion_age_min,
        }
        emit_finding(
            PipelineFinding(
                event="DP_CLOUD_RUN_JOB_EXEC_FAILED",
                severity="WARN",
                tier=EscalationTier.FILE_ISSUE,
                summary=summary,
                details=details,
                registry_id="DP-VM-013",
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
) -> JobExecFailedReader:
    """Return ``job_stems -> {stem: {failed_count, completion_age_min} | None}`` for DP-VM-013.

    For each stem, resolves the full Cloud Run Job name as ``{env_prefix}-{stem}``
    (same convention as ``stale_image_watcher.make_image_digest_reader``), then reads
    all executions via ``run_v2.ExecutionsClient`` and finds the most recently COMPLETED
    one (by ``completion_time``).  Returns diagnostics when that latest execution has
    ``failed_count > 0``; returns ``None`` when it succeeded, no executions exist, or
    the API errors.

    Jobs whose GCP names differ from the ``{env_prefix}-{stem}`` convention (e.g. the
    manifest-consolidator family) will receive NotFound from the API — silently dropped
    as ``None`` (fail toward no false alert), covered by ``consolidator_oom_watcher``.

    Deferred-import of ``google.cloud.run_v2`` (credential-bound SDK boundary).
    An import failure returns an empty dict for every call (off this sweep, fail-safe).
    """
    import importlib  # noqa: imports-inside-functions
    from datetime import UTC, datetime  # noqa: imports-inside-functions

    try:
        run_mod = importlib.import_module("google.cloud.run_v2")  # noqa: imports-inside-functions
        executions_client = run_mod.ExecutionsClient()  # pyright: ignore[reportAny]
    except Exception as exc:
        logger.info("cloud-run-job-exec reader unavailable (DP-VM-013 off this sweep): %s", exc)
        return lambda _stems: {}

    def _read(job_stems: Iterable[str]) -> dict[str, dict[str, object] | None]:
        if not project_id:
            return dict.fromkeys(job_stems, None)
        result: dict[str, dict[str, object] | None] = {}
        for stem in job_stems:
            full_name = f"{env_prefix}-{stem}" if env_prefix else stem
            parent = f"projects/{project_id}/locations/{location}/jobs/{full_name}"
            result[stem] = None
            try:
                latest_exec: object | None = None
                latest_completion: datetime | None = None
                for execution in executions_client.list_executions(parent=parent):  # pyright: ignore[reportAny]
                    completion = getattr(execution, "completion_time", None)  # pyright: ignore[reportAny]
                    if completion is None:
                        continue  # execution still running — skip
                    completed_dt = (
                        completion
                        if isinstance(completion, datetime)
                        else completion.ToDatetime(tzinfo=UTC)  # pyright: ignore[reportAny]
                    )
                    if latest_completion is None or completed_dt > latest_completion:
                        latest_completion = completed_dt
                        latest_exec = execution  # pyright: ignore[reportAny]

                if latest_exec is None:
                    continue  # no completed executions found

                failed_count = int(getattr(latest_exec, "failed_count", 0) or 0)  # pyright: ignore[reportAny]
                if failed_count <= 0:
                    continue  # most-recent execution succeeded — no alert

                completion_age = (
                    (datetime.now(UTC) - latest_completion).total_seconds() / 60.0
                    if latest_completion is not None
                    else None
                )
                result[stem] = {
                    "failed_count": failed_count,
                    "completion_age_min": completion_age,
                }
            except Exception as exc:
                logger.info("cloud-run-job-exec lookup for %s → unknown: %s", full_name, exc)
        return result

    return _read


__all__ = [
    "JobExecFailedReader",
    "all_cloud_run_job_stems",
    "check_cloud_run_job_exec_failures",
    "make_cloud_run_job_execution_reader",
]
