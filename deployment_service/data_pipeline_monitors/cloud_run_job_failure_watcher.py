"""DP-WATCHER-006 — generic Cloud Run Job per-execution failure detector.

Reads the Cloud Run ``run_v2`` execution history for every GCP Cloud Run Job
registered in ``cloud_run_job_registry.CLOUD_RUN_JOBS`` and pages when the
most recently completed execution has ``failed_count > 0`` for N consecutive
sweeps.

Gap closed: the 2026-08-07 infra-health audit found zero of 11 production
failures produced a Slack alert. ``consolidator_oom_watcher`` (DP-WATCHER-005)
covers only the ``manifest-consolidator-{ag}`` family. Every other GCP Cloud
Run Job in the registry was invisible to the alerting spine — this watcher
closes that blind spot.

Design decisions:
  - No OOM classification (OOM detection is consolidator-specific). ``failed_count > 0``
    on the most recently completed execution is the only signal.
  - No index-age cross-check (consolidator-specific). The execution failure itself is the
    signal regardless of any downstream artifact.
  - ``EscalationTier.PAGE_OPERATOR`` — no deterministic auto-recover for a generic job
    failure (the root cause is unknown and job-specific).
  - Gated on ``MissTracker`` (the same consecutive-miss counter the other meta-watchers
    use) to avoid false pages on a single transient API blip.
  - Manifest-consolidator jobs are EXCLUDED (``manifest-consolidator-*`` stem prefix) —
    they are already covered by DP-WATCHER-005 with OOM-aware auto-recovery.

Issue: plans/active/issues/infra_health_audit_alert_coverage_gaps_2026_08_07.md § (A)
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Final

from unified_api_contracts import DeploymentCloud, DeploymentKind

from deployment_service.cloud_run_job_registry import CLOUD_RUN_JOBS
from deployment_service.data_pipeline_monitors.escalation import EscalationTier, PipelineFinding
from deployment_service.data_pipeline_monitors.meta_watchers import (
    DEFAULT_MIN_CONSECUTIVE_MISSES,
    MissTracker,
    emit_finding,
)

logger = logging.getLogger(__name__)

_EVENT_CLOUD_RUN_JOB_FAILED: Final[str] = "DP_CLOUD_RUN_JOB_FAILED"
_REGISTRY_ID: Final[str] = "DP-WATCHER-006"

# Manifest-consolidator jobs are already covered by DP-WATCHER-005 with OOM-aware
# auto-recovery. Skip them here to avoid double-alerting on the same root cause.
_CONSOLIDATOR_STEM_PREFIX: Final[str] = "manifest-consolidator-"

# job_stem -> None (latest execution OK / no completed executions / API error)
#          or dict with failure diagnostics
JobExecutionReader = Callable[[str], "dict[str, object] | None"]


def _failure_miss_key(job_stem: str) -> str:
    return f"{_EVENT_CLOUD_RUN_JOB_FAILED}::{job_stem}"


def _gcp_job_stems() -> list[tuple[str, str]]:
    """Return ``(job_stem, service)`` for every GCP Cloud Run Job in the registry.

    Excludes ``manifest-consolidator-*`` (already covered by DP-WATCHER-005).
    Filters to ``kind=CLOUD_RUN_JOB, cloud=GCP`` only — consistent with the
    rest of the registry consumers.
    """
    results: list[tuple[str, str]] = []
    for target in CLOUD_RUN_JOBS:
        if target.cloud is not DeploymentCloud.GCP:
            continue
        if target.kind is not DeploymentKind.CLOUD_RUN_JOB:
            continue
        stem = (target.name or "").strip()
        if not stem or stem.startswith(_CONSOLIDATOR_STEM_PREFIX):
            continue
        results.append((stem, target.service or ""))
    return results


def check_cloud_run_job_failures(
    *,
    execution_reader: JobExecutionReader,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> dict[str, dict[str, object]]:
    """DP-WATCHER-006 — detect failed Cloud Run Job executions across the full registry.

    For each non-consolidator GCP Cloud Run Job in ``cloud_run_job_registry.CLOUD_RUN_JOBS``,
    calls the injected ``execution_reader`` to find the most recently completed execution.
    Emits ``DP_CLOUD_RUN_JOB_FAILED`` with ``PAGE_OPERATOR`` when the latest completed
    execution has ``failed_count > 0``, gated on ``min_consecutive`` consecutive-miss
    sweeps via ``MissTracker`` to absorb transient API blips.

    Returns ``{job_stem: finding_details | {}}`` — a non-empty sub-dict means the
    finding fired for that job stem.
    """
    jobs = _gcp_job_stems()
    findings: dict[str, dict[str, object]] = {}

    for stem, service in jobs:
        miss_key = _failure_miss_key(stem)
        diag = execution_reader(stem)

        if diag is None:
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            findings[stem] = {}
            continue

        failed_count = int(diag.get("failed_count", 0))
        completion_age_min = diag.get("completion_age_min")
        job_name = str(diag.get("job_name", stem))

        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=True)
            if misses < min_consecutive:
                logger.info(
                    "cloud_run_job_failure_watcher: %s failed (failed_count=%d, age=%s) "
                    "below consecutive-miss threshold (%d/%d) — not paging yet",
                    job_name,
                    failed_count,
                    f"{completion_age_min:.0f}m" if completion_age_min is not None else "unknown",
                    misses,
                    min_consecutive,
                )
                findings[stem] = {}
                continue

        age_str = f"{completion_age_min:.0f}m ago" if completion_age_min is not None else "unknown age"
        summary = f"{job_name} Cloud Run Job failed: {failed_count} failed task(s), {age_str}"
        details: dict[str, object] = {
            "job_stem": stem,
            "job_name": job_name,
            "service": service,
            "failed_count": failed_count,
            "completion_age_min": completion_age_min,
            "label": stem,  # used by _alert_key for per-job RESOLVED bookend tracking
        }
        emit_finding(
            PipelineFinding(
                event=_EVENT_CLOUD_RUN_JOB_FAILED,
                severity="CRITICAL",
                tier=EscalationTier.PAGE_OPERATOR,
                summary=summary,
                details=details,
                registry_id=_REGISTRY_ID,
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
        findings[stem] = details

    return findings


def make_job_execution_reader(
    project_id: str,
    env_prefix: str = "",
    location: str = "asia-northeast1",
) -> JobExecutionReader:
    """Return ``job_stem -> failure diagnostics | None`` for DP-WATCHER-006.

    For each job stem, finds the most recently COMPLETED execution (by ``completion_time``)
    in the job's execution history. Returns failure diagnostics when that execution has
    ``failed_count > 0``. Returns ``None`` when the latest execution succeeded, no
    completed executions exist, the job is not found, or the API errors — fail toward
    silence so a transient GCP API blip does not flood the channel.

    Deferred-import of ``google.cloud.run_v2`` (credential-bound SDK boundary). An import
    failure returns the null reader (fail toward silence) — the Cloud Monitoring alert
    policies in ``cloud_run_service_liveness.tf`` still provide an independent signal.
    """
    try:
        run_mod = importlib.import_module("google.cloud.run_v2")  # noqa: imports-inside-functions — credential-bound SDK, deferred
        executions_client = run_mod.ExecutionsClient()  # pyright: ignore[reportAny] — dynamic cloud-SDK boundary
    except Exception as exc:
        logger.info("cloud-run-job-failure reader unavailable (DP-WATCHER-006 off this sweep): %s", exc)
        return lambda _stem: None

    def _read(job_stem: str) -> dict[str, object] | None:
        if not project_id:
            return None
        job_name = f"{env_prefix}-{job_stem}" if env_prefix else job_stem
        parent = f"projects/{project_id}/locations/{location}/jobs/{job_name}"
        try:
            latest_completion: datetime | None = None
            latest_failed_count = 0
            latest_succeeded_count = 0

            for execution in executions_client.list_executions(parent=parent):  # pyright: ignore[reportAny] — dynamic SDK page iterator
                completion = getattr(execution, "completion_time", None)  # pyright: ignore[reportAny] — dynamic SDK attr
                if completion is None:
                    continue
                completed_dt: datetime = (
                    completion if isinstance(completion, datetime) else completion.ToDatetime(tzinfo=UTC)  # pyright: ignore[reportAny] — protobuf Timestamp
                )
                if latest_completion is None or completed_dt > latest_completion:
                    latest_completion = completed_dt
                    latest_failed_count = int(getattr(execution, "failed_count", 0) or 0)  # pyright: ignore[reportAny]
                    latest_succeeded_count = int(getattr(execution, "succeeded_count", 0) or 0)  # pyright: ignore[reportAny]

            if latest_completion is None:
                return None  # no completed executions yet (e.g. first run still in progress)
            if latest_failed_count <= 0:
                return None  # most recent execution succeeded (or zero tasks failed)

            completion_age = (datetime.now(UTC) - latest_completion).total_seconds() / 60.0
            return {
                "failed_count": latest_failed_count,
                "succeeded_count": latest_succeeded_count,
                "completion_age_min": completion_age,
                "job_name": job_name,
            }
        except Exception as exc:
            logger.info("cloud-run-job-failure lookup for %s -> unknown (fail-silent): %s", job_name, exc)
            return None

    return _read


__all__ = [
    "JobExecutionReader",
    "check_cloud_run_job_failures",
    "make_job_execution_reader",
]
