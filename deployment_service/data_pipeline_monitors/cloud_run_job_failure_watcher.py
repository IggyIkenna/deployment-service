"""DP-WATCHER-006 — generic Cloud Run Job per-execution failure detector.

Reads the latest execution of EVERY job in
``deployment_service.cloud_run_job_registry.CLOUD_RUN_JOBS`` via the GCP
``run_v2.ExecutionsClient`` and emits
``DP_CLOUD_RUN_JOB_EXEC_FAILED`` (CRITICAL / PAGE_OPERATOR) when the most
recent execution has ``failed_count > 0``.

This closes the alert-coverage blind spot identified in
``infra_health_audit_alert_coverage_gaps_2026_08_07.md`` finding A:
``consolidator_oom_watcher.py`` (DP-WATCHER-005) covered ONLY the
``manifest-consolidator-{ag}`` family; every other Cloud Run Job in the
registry had zero per-execution failure alerting.

Gap vs. DP-WATCHER-005: this watcher is SIMPLER on purpose — no OOM
cross-check, no index-age cross-check. Any job whose latest execution
has ``failed_count > 0`` fires. The consolidator OOM watcher retains its
own, richer actuator path (AUTO_RECOVER + machine-tier escalation).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence

from unified_api_contracts import DeploymentTarget

from deployment_service.data_pipeline_monitors.escalation import EscalationTier, PipelineFinding
from deployment_service.data_pipeline_monitors.meta_watchers import (
    DEFAULT_MIN_CONSECUTIVE_MISSES,
    MissTracker,
    emit_finding,
)

logger = logging.getLogger(__name__)

# Callable injected by the CLI: takes a sequence of DeploymentTarget and returns
# a dict mapping ``target.name -> {"failed_count": int, "completion_age_min": float|None}``
# when the latest execution has failed_count > 0, or None when the latest execution
# SUCCEEDED / no executions exist / the API errors.
CloudRunJobExecutionReader = Callable[
    [Sequence[DeploymentTarget]], dict[str, dict[str, object] | None]
]


def _failure_miss_key(job_stem: str) -> str:
    return f"DP_CLOUD_RUN_JOB_EXEC_FAILED::{job_stem}"


def check_cloud_run_job_failures(
    *,
    targets: Sequence[DeploymentTarget],
    execution_reader: CloudRunJobExecutionReader,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> dict[str, dict[str, object]]:
    """DP-WATCHER-006 — detect any Cloud Run Job execution with failed_count > 0.

    For each target in ``targets``: reads the latest execution via the injected
    ``execution_reader``, emits ``DP_CLOUD_RUN_JOB_EXEC_FAILED`` (CRITICAL /
    PAGE_OPERATOR) when ``failed_count > 0`` on the most-recent execution.
    Gated on ``MissTracker`` so a transient API blip only pages when the
    failure is sustained across ``min_consecutive`` sweeps.

    Returns ``{job_stem: finding_details | {}}`` for logging/testing.
    """
    diagnostics = execution_reader(targets)

    findings: dict[str, dict[str, object]] = {}
    for target in targets:
        stem = target.name
        miss_key = _failure_miss_key(stem)
        diag = diagnostics.get(stem)

        if diag is None:
            # Latest execution succeeded (or no executions / API error) — reset.
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            findings[stem] = {}
            continue

        failed_count = int(diag.get("failed_count", 0))
        completion_age_min = diag.get("completion_age_min")

        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=True)
            if misses < min_consecutive:
                logger.info(
                    "cloud_run_job_failure_watcher: %s failed_count=%d below consecutive-miss "
                    "threshold (%d/%d) — not paging yet",
                    stem,
                    failed_count,
                    misses,
                    min_consecutive,
                )
                findings[stem] = {}
                continue

        age_str = (
            f"{completion_age_min:.0f}m ago"
            if completion_age_min is not None
            else "unknown age"
        )
        summary = (
            f"Cloud Run Job '{stem}' (service={target.service}) latest execution "
            f"failed: {failed_count} failed task(s), completed {age_str}"
        )
        details: dict[str, object] = {
            "job_stem": stem,
            "service": target.service,
            "asset_group": target.asset_group,
            "umbrella": str(target.umbrella),
            "failed_count": failed_count,
            "completion_age_min": completion_age_min,
        }
        emit_finding(
            PipelineFinding(
                event="DP_CLOUD_RUN_JOB_EXEC_FAILED",
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
) -> CloudRunJobExecutionReader:
    """Return a reader for DP-WATCHER-006.

    For each target in the sequence, reads the latest execution of
    ``{env_prefix}{target.name}`` (or ``{target.name}`` when env_prefix is
    empty) via ``run_v2.ExecutionsClient``. Returns ``{"failed_count": int,
    "completion_age_min": float|None}`` when the latest execution has
    ``failed_count > 0``, ``None`` otherwise.

    Deferred-import of ``google.cloud.run_v2`` keeps this credential-free in
    unit tests. An import failure returns an all-None dict for every call
    (fail toward silence on a transient API unavailability — existing staleness
    probes still catch genuinely-dead jobs via cron-freshness checks).
    """
    import importlib  # noqa: imports-inside-functions
    from datetime import UTC, datetime  # noqa: imports-inside-functions

    try:
        run_mod = importlib.import_module("google.cloud.run_v2")  # noqa: imports-inside-functions
        executions_client = run_mod.ExecutionsClient()  # pyright: ignore[reportAny]
    except Exception as exc:
        logger.info(
            "cloud-run-job-failure reader unavailable (DP-WATCHER-006 off this sweep): %s", exc
        )
        return lambda _targets: {}

    def _read(targets: Sequence[DeploymentTarget]) -> dict[str, dict[str, object] | None]:
        if not project_id:
            return {t.name: None for t in targets}
        result: dict[str, dict[str, object] | None] = {}
        for target in targets:
            job_name = f"{env_prefix}{target.name}" if env_prefix else target.name
            parent = f"projects/{project_id}/locations/{location}/jobs/{job_name}"
            result[target.name] = None
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
                        latest_failed = execution  # pyright: ignore[reportAny]
                if latest_failed is None:
                    continue
                failed_count = int(getattr(latest_failed, "failed_count", 0) or 0)  # pyright: ignore[reportAny]
                completion_age = (
                    (datetime.now(UTC) - latest_completion).total_seconds() / 60.0
                    if latest_completion is not None
                    else None
                )
                result[target.name] = {
                    "failed_count": failed_count,
                    "completion_age_min": completion_age,
                }
            except Exception as exc:
                logger.info(
                    "cloud-run-job-failure lookup for %s -> unknown: %s", job_name, exc
                )
        return result

    return _read


__all__ = [
    "CloudRunJobExecutionReader",
    "check_cloud_run_job_failures",
    "make_cloud_run_job_execution_reader",
]
