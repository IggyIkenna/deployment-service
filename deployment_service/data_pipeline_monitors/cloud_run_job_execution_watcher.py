"""DP-WATCHER-006 — generic Cloud Run Job per-execution failure detector.

Reads the execution history for every job in
``deployment_service.cloud_run_job_registry.CLOUD_RUN_JOBS`` via the
``run_v2.ExecutionsClient`` API and pages on any job whose latest execution
within the lookback window has ``failed_count > 0`` with no subsequent
SUCCEEDED execution (i.e. the failure has not self-recovered).

This is intentionally broader than ``consolidator_oom_watcher`` (DP-WATCHER-005),
which doubles-checks OOM + index staleness for the consolidator family only.  The
two detectors are complementary: a consolidator OOM that is also index-stale fires
DP-WATCHER-005 (AUTO_RECOVER); every other Cloud Run Job failure that reaches this
watcher fires DP-WATCHER-006 (PAGE_OPERATOR — no auto-recover actuator exists for
the general job estate).

Gap closed: ``infra_health_audit_alert_coverage_gaps_2026_08_07.md`` finding A
(7 of 11 audit findings had no alert precisely because this class of detector was
missing for the general Cloud Run Job estate).

Plan: ``infra_health_audit_alert_coverage_gaps_2026_08_07.md`` todo
``[INFRA] P1. Build a generic Cloud Run Job per-execution failure detector``.
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
from deployment_service.deployment_classification import DeploymentTarget

logger = logging.getLogger(__name__)

# Callable: full_job_name -> failure diagnostics dict | None.
# Returns None when the latest execution SUCCEEDED, no executions were found within
# the lookback window, or the API call errored.  A non-None dict carries:
#   "failed_count": int, "completion_age_min": float | None,
#   "succeeded_after": bool (True when a more recent SUCCEEDED execution exists).
CloudRunJobExecutionReader = Callable[[Iterable[str]], dict[str, dict[str, object] | None]]

# Default lookback window in hours.  48 h covers daily-cron jobs (which may fire at
# most once per day) without flagging stale historical failures from previous weeks.
_DEFAULT_LOOKBACK_HOURS = 48.0

_EVENT = "DP_CLOUD_RUN_JOB_EXECUTION_FAILED"


def _miss_key(full_job_name: str) -> str:
    return f"{_EVENT}::{full_job_name}"


def check_cloud_run_job_executions(
    *,
    job_targets: Iterable[DeploymentTarget],
    execution_reader: CloudRunJobExecutionReader,
    env_prefix: str = "",
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> dict[str, dict[str, object]]:
    """DP-WATCHER-006 — detect recent execution failures across all Cloud Run Jobs.

    For each target in ``job_targets``:
    - Constructs the full deployed job name as ``{env_prefix}-{target.name}`` (or
      ``{target.name}`` when ``env_prefix`` is empty).
    - Passes all full job names to the injected ``execution_reader``.
    - Emits ``DP_CLOUD_RUN_JOB_EXECUTION_FAILED`` (CRITICAL / PAGE_OPERATOR) when
      the latest execution within the lookback window has ``failed_count > 0`` and
      no subsequent SUCCEEDED execution exists (``succeeded_after=False``).
    - Gated on ``MissTracker`` so a single transient failure only pages after
      ``min_consecutive`` consecutive sweeps showing the failure.

    Returns a dict ``{full_job_name: finding_details | {}}`` for testing/logging.
    A non-empty sub-dict means a finding fired for that job.
    """
    targets_list = list(job_targets)
    full_names = [
        f"{env_prefix}-{t.name}" if env_prefix else t.name for t in targets_list
    ]
    name_to_target: dict[str, DeploymentTarget] = dict(zip(full_names, targets_list))

    diagnostics = execution_reader(full_names)

    findings: dict[str, dict[str, object]] = {}
    for full_name, target in name_to_target.items():
        diag = diagnostics.get(full_name)
        miss_key = _miss_key(full_name)

        if diag is None:
            # No recent failure (or API error / no executions in window).
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            findings[full_name] = {}
            continue

        succeeded_after: bool = bool(diag.get("succeeded_after", False))
        if succeeded_after:
            # A more recent SUCCEEDED execution exists — job self-recovered.
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            logger.info(
                "cloud_run_job_execution_watcher: job '%s' had a failed execution but "
                "a more recent SUCCEEDED execution was found — suppressing (self-recovered)",
                full_name,
            )
            findings[full_name] = {}
            continue

        failed_count = int(diag.get("failed_count", 0))
        completion_age_min = diag.get("completion_age_min")

        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=True)
            if misses < min_consecutive:
                logger.info(
                    "cloud_run_job_execution_watcher: job '%s' below consecutive-miss "
                    "threshold (%d/%d) — not paging yet",
                    full_name,
                    misses,
                    min_consecutive,
                )
                findings[full_name] = {}
                continue

        age_str = f"{completion_age_min:.0f}m" if completion_age_min is not None else "unknown"
        summary = (
            f"Cloud Run Job '{full_name}' (service={target.service}, "
            f"asset_group={target.asset_group or 'n/a'}) has a recent failed execution "
            f"({failed_count} failed task(s), completed {age_str} ago) with no "
            f"subsequent successful run"
        )
        details: dict[str, object] = {
            "job_name": full_name,
            "job_stem": target.name,
            "service": target.service,
            "asset_group": target.asset_group or "",
            "umbrella": str(target.umbrella.value) if hasattr(target.umbrella, "value") else str(target.umbrella),
            "failed_count": failed_count,
            "completion_age_min": completion_age_min,
        }
        emit_finding(
            PipelineFinding(
                event=_EVENT,
                severity="CRITICAL",
                tier=EscalationTier.PAGE_OPERATOR,
                summary=summary,
                details=details,
                registry_id="DP-WATCHER-006",
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
        findings[full_name] = details

    return findings


def make_cloud_run_job_execution_reader(
    project_id: str,
    env_prefix: str = "",
    location: str = "asia-northeast1",
    lookback_hours: float = _DEFAULT_LOOKBACK_HOURS,
) -> CloudRunJobExecutionReader:
    """Return ``full_job_names -> {name: failure_diagnostics | None}`` for DP-WATCHER-006.

    For each job name, reads the execution list via ``run_v2.ExecutionsClient``,
    filters to executions completed within ``lookback_hours``, and returns:
    - ``None`` when no execution in the window has ``failed_count > 0``, or when the
      API errors.
    - A diagnostics dict when the LATEST failed execution in the window has no
      subsequent SUCCEEDED execution.  ``"succeeded_after": True`` suppresses the
      finding in ``check_cloud_run_job_executions``.

    Deferred-import of ``google.cloud.run_v2`` (credential-bound SDK boundary).
    An SDK import failure returns an empty dict for every call (fail toward silence
    on a transient API unavailability — the cron-fired / stale-image watchers still
    catch genuinely-dead jobs by other signals).

    Note: ``env_prefix`` is passed separately (not baked in) so the reader can be
    stubbed in tests without knowing the prefix.  The caller (``cli.py``) constructs
    full names once and passes them here — the reader does NOT re-apply the prefix.
    """
    import importlib  # noqa: imports-inside-functions
    from datetime import UTC, datetime, timedelta  # noqa: imports-inside-functions

    try:
        run_mod = importlib.import_module("google.cloud.run_v2")  # noqa: imports-inside-functions
        executions_client = run_mod.ExecutionsClient()  # pyright: ignore[reportAny]
    except Exception as exc:
        logger.info("cloud-run-job-execution reader unavailable (DP-WATCHER-006 off this sweep): %s", exc)
        return lambda _names: {}

    def _read(full_job_names: Iterable[str]) -> dict[str, dict[str, object] | None]:
        if not project_id:
            return dict.fromkeys(full_job_names, None)
        cutoff = datetime.now(UTC) - timedelta(hours=lookback_hours)
        result: dict[str, dict[str, object] | None] = {}
        for full_name in full_job_names:
            if not full_name:
                continue
            parent = f"projects/{project_id}/locations/{location}/jobs/{full_name}"
            result[full_name] = None
            try:
                latest_failed_completion: datetime | None = None
                latest_failed_count: int = 0
                latest_succeeded_completion: datetime | None = None
                for execution in executions_client.list_executions(parent=parent):  # pyright: ignore[reportAny]
                    completion = getattr(execution, "completion_time", None)  # pyright: ignore[reportAny]
                    if completion is None:
                        continue
                    completed_dt = (
                        completion
                        if isinstance(completion, datetime)
                        else completion.ToDatetime(tzinfo=UTC)  # pyright: ignore[reportAny]
                    )
                    if completed_dt < cutoff:
                        continue
                    failed_count = int(getattr(execution, "failed_count", 0) or 0)  # pyright: ignore[reportAny]
                    succeeded_count = int(getattr(execution, "succeeded_count", 0) or 0)  # pyright: ignore[reportAny]
                    if failed_count > 0 and (
                        latest_failed_completion is None or completed_dt > latest_failed_completion
                    ):
                        latest_failed_completion = completed_dt
                        latest_failed_count = failed_count
                    if succeeded_count > 0 and (
                        latest_succeeded_completion is None or completed_dt > latest_succeeded_completion
                    ):
                        latest_succeeded_completion = completed_dt
                if latest_failed_completion is None:
                    continue
                succeeded_after = (
                    latest_succeeded_completion is not None
                    and latest_succeeded_completion > latest_failed_completion
                )
                completion_age_min = (
                    (datetime.now(UTC) - latest_failed_completion).total_seconds() / 60.0
                )
                result[full_name] = {
                    "failed_count": latest_failed_count,
                    "completion_age_min": completion_age_min,
                    "succeeded_after": succeeded_after,
                }
            except Exception as exc:
                logger.info("cloud-run-job-execution lookup for %s -> skipping: %s", full_name, exc)
        return result

    return _read


def all_cloud_run_job_targets() -> tuple[DeploymentTarget, ...]:
    """Return all classified Cloud Run Job targets from the canonical registry."""
    import deployment_service.cloud_run_job_registry as _reg  # noqa: imports-inside-functions — registry dep; deferred avoids circular module-level import
    return _reg.CLOUD_RUN_JOBS


__all__ = [
    "CloudRunJobExecutionReader",
    "all_cloud_run_job_targets",
    "check_cloud_run_job_executions",
    "make_cloud_run_job_execution_reader",
]
