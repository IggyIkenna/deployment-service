"""DP-VM-013 — generic Cloud Run Job per-execution failure detector.

Reads the latest Cloud Run Job execution for every job in
``cloud_run_job_registry.CLOUD_RUN_JOBS`` EXCEPT the ``manifest-consolidator-*``
family (already covered by DP-WATCHER-005 / ``consolidator_oom_watcher.py``).

When the latest execution has ``failed_count > 0``, emits
``DP_CLOUD_RUN_JOB_EXECUTION_FAILED`` with ``EscalationTier.FILE_ISSUE`` —
CRITICAL severity, no auto-recover actuator, root cause always needs human
diagnosis.

Gap closed: ``infra_health_audit_alert_coverage_gaps_2026_08_07.md`` finding (A)
— ``client-reporting-batch`` OOMed repeatedly since 2026-08-06 with zero alert.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from deployment_service.data_pipeline_monitors.escalation import EscalationTier, PipelineFinding
from deployment_service.data_pipeline_monitors.meta_watchers import (
    DEFAULT_MIN_CONSECUTIVE_MISSES,
    MissTracker,
    emit_finding,
)

logger = logging.getLogger(__name__)

# ``job_name -> latest-failed-execution diagnostics | None``.
# Non-None when the latest execution has ``failed_count > 0``; None when the
# latest execution SUCCEEDED, no executions exist, or the API errors.
# Each non-None entry carries ``{"failed_count": int, "failure_reason": str,
# "completion_age_min": float | None}``.
CloudRunJobExecutionReader = Callable[[str], dict[str, object] | None]

# Stem prefix used by the manifest-consolidator family — these are already
# monitored by DP-WATCHER-005; exclude them from this generic detector.
_CONSOLIDATOR_STEM_PREFIX = "manifest-consolidator-"


def _job_miss_key(job_name: str) -> str:
    return f"DP_CLOUD_RUN_JOB_EXECUTION_FAILED::{job_name}"


def check_cloud_run_job_executions(
    *,
    execution_reader: CloudRunJobExecutionReader,
    env_prefix: str = "",
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> dict[str, dict[str, object]]:
    """DP-VM-013 — detect failed executions across the Cloud Run Job estate.

    Iterates every job in ``cloud_run_job_registry.CLOUD_RUN_JOBS``, skipping
    the ``manifest-consolidator-*`` family (DP-WATCHER-005 covers those).
    For each job, calls the injected ``execution_reader`` with the fully-qualified
    job name (``{env_prefix}{stem}``).  Emits ``DP_CLOUD_RUN_JOB_EXECUTION_FAILED``
    when the reader returns a non-None diagnostics dict.  Gated on ``MissTracker``
    so a single blip does not page.

    Returns a dict ``{job_name: finding_details | {}}`` for logging/testing.
    A non-empty sub-dict means the finding fired for that job.
    """
    from deployment_service.cloud_run_job_registry import CLOUD_RUN_JOBS  # noqa: imports-inside-functions

    findings: dict[str, dict[str, object]] = {}

    for target in CLOUD_RUN_JOBS:
        stem: str = target.name
        if stem.startswith(_CONSOLIDATOR_STEM_PREFIX):
            continue

        job_name = f"{env_prefix}{stem}" if env_prefix else stem
        miss_key = _job_miss_key(job_name)

        diag = execution_reader(job_name)

        if diag is None:
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            findings[job_name] = {}
            continue

        failed_count = int(diag.get("failed_count", 0))
        failure_reason = str(diag.get("failure_reason", ""))
        completion_age_min = diag.get("completion_age_min")

        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=True)
            if misses < min_consecutive:
                logger.info(
                    "cloud_run_job_failure_watcher: %s failed (failed_count=%d) below "
                    "consecutive-miss threshold (%d/%d) — not paging yet",
                    job_name,
                    failed_count,
                    misses,
                    min_consecutive,
                )
                findings[job_name] = {}
                continue

        summary = (
            f"{job_name} execution failed: failed_count={failed_count}"
            + (f", reason={failure_reason!r}" if failure_reason else "")
            + (
                f", completed {completion_age_min:.0f}m ago"
                if completion_age_min is not None
                else ""
            )
        )
        details: dict[str, object] = {
            "job_name": job_name,
            "stem": stem,
            "failed_count": failed_count,
            "failure_reason": failure_reason,
            "completion_age_min": completion_age_min,
        }
        emit_finding(
            PipelineFinding(
                event="DP_CLOUD_RUN_JOB_EXECUTION_FAILED",
                severity="CRITICAL",
                tier=EscalationTier.FILE_ISSUE,
                summary=summary,
                details=details,
                registry_id="DP-VM-013",
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
    """Return ``job_name -> failed-execution diagnostics | None`` for DP-VM-013.

    Reads the latest execution of the given Cloud Run Job via
    ``run_v2.ExecutionsClient``.  Returns diagnostics when the latest execution
    has ``failed_count > 0``; returns ``None`` when the latest execution
    SUCCEEDED, no executions exist, or the API errors.

    The ``job_name`` argument is already the fully-qualified GCP name
    (``{env_prefix}{stem}``) — this reader does NOT re-apply the prefix.

    Deferred-import of ``google.cloud.run_v2`` (credential-bound SDK boundary).
    An SDK import failure returns a reader that always returns ``None`` (fail
    toward silence — a transient API unavailability should not flood issues).
    """
    import importlib  # noqa: imports-inside-functions
    from datetime import UTC, datetime  # noqa: imports-inside-functions

    try:
        run_mod = importlib.import_module("google.cloud.run_v2")  # noqa: imports-inside-functions
        executions_client = run_mod.ExecutionsClient()  # pyright: ignore[reportAny]
    except Exception as exc:
        logger.info("cloud-run-job reader unavailable (DP-VM-013 off this sweep): %s", exc)
        return lambda _job_name: None

    def _read(job_name: str) -> dict[str, object] | None:
        if not project_id:
            return None
        parent = f"projects/{project_id}/locations/{location}/jobs/{job_name}"
        try:
            latest_failed: object | None = None
            latest_completion: datetime | None = None
            for execution in executions_client.list_executions(parent=parent):  # pyright: ignore[reportAny]
                failed_count = int(getattr(execution, "failed_count", 0) or 0)  # pyright: ignore[reportAny]
                if failed_count <= 0:
                    continue
                completion = getattr(execution, "completion_time", None)  # pyright: ignore[reportAny]
                if completion is None:
                    continue
                completed_dt = (
                    completion
                    if isinstance(completion, datetime)
                    else completion.ToDatetime(tzinfo=UTC)  # pyright: ignore[reportAny]
                )
                if latest_completion is None or completed_dt > latest_completion:
                    latest_completion = completed_dt
                    latest_failed = execution
            if latest_failed is None:
                return None
            failed_count = int(getattr(latest_failed, "failed_count", 0) or 0)  # pyright: ignore[reportAny]
            conditions = list(getattr(latest_failed, "conditions", []) or [])  # pyright: ignore[reportAny]
            failure_reason = _classify_failure_reason(conditions)
            completion_age = (
                (datetime.now(UTC) - latest_completion).total_seconds() / 60.0
                if latest_completion is not None
                else None
            )
            return {
                "failed_count": failed_count,
                "failure_reason": failure_reason,
                "completion_age_min": completion_age,
            }
        except Exception as exc:
            logger.info("cloud-run-job lookup for %s -> unknown: %s", job_name, exc)
            return None

    return _read


_OOM_SIGNATURES: tuple[str, ...] = (
    "signal: 9",
    "signal 9",
    "sigkill",
    "exit code 137",
    "exit_code=137",
    "out of memory",
    "oomkill",
    "oom killed",
    "memory limit exceeded",
    "memorylimit exceeded",
    "container killed",
)


def _classify_failure_reason(conditions: list[object]) -> str:
    """Extract a human-readable failure reason from Cloud Run execution conditions.

    Concatenates ``reason + " " + message`` (lowercased) for each condition and
    matches against known signatures, returning the first match.  Returns ""
    when no condition carries a recognised signature.
    """
    for cond in conditions:
        reason = str(getattr(cond, "reason", "") or "")
        message = str(getattr(cond, "message", "") or "")
        combined = f"{reason} {message}".lower()
        for sig in _OOM_SIGNATURES:
            if sig in combined:
                return f"OOM({sig})"
        if combined.strip():
            return combined[:120]
    return ""


__all__ = [
    "CloudRunJobExecutionReader",
    "check_cloud_run_job_executions",
    "make_cloud_run_job_execution_reader",
]
