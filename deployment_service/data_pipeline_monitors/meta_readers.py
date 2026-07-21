"""Credential-bound reader/lister factories for the ``meta`` mode sweep.

Split out of ``cli.py`` (2026-07-21, codex-compliance file-size ceiling) — these
are the Cloud Scheduler / Cloud Run reader factories that ``main()`` wires into
``meta_watchers.check_catalogue_freshness`` / ``check_cron_fired`` and
``consolidator_scheduler_watcher.check_consolidator_scheduler_paused``. Unlike
``meta_targets.py`` (pure, no cloud SDK), each factory here deferred-imports its
``google.cloud.*`` client at call time so module import stays credential-free;
only the returned closures touch the network. ``cli.py`` imports the public
names below aliased to their original underscore-prefixed names so existing
call sites (incl. tests, which monkeypatch ``cli._make_*``) keep working.
"""

from __future__ import annotations

import importlib
import logging
from datetime import UTC, datetime

from unified_trading_library import UnifiedCloudConfig

from deployment_service.data_pipeline_monitors import consolidator_scheduler_watcher, meta_watchers
from deployment_service.data_pipeline_monitors.meta_targets import scheduler_env_prefix

logger = logging.getLogger(__name__)

# Bounds the Cloud Scheduler get_job/list_jobs RPCs (KEY #2/DP-WATCHER-003). The
# GAPIC client's default retry policy treats a transient-looking auth/network
# failure as retryable and can block far longer than this module's documented
# fail-safe intent (return None/[] promptly) — a real outage should not stall
# the whole meta sweep. 10s is generous for a single-job describe/list call.
SCHEDULER_RPC_TIMEOUT_SECS = 10.0
# The fleet's canonical Cloud Scheduler + Cloud Run region.
_REGION = "asia-northeast1"


def _project_id() -> str:
    """Resolve the GCP project id from UnifiedCloudConfig (mirrors ``cli._project_id``)."""
    try:
        cfg = UnifiedCloudConfig()
    except (ValueError, RuntimeError, OSError):
        return ""
    return getattr(cfg, "gcp_project_id", "") or ""


def make_scheduler_state_reader() -> meta_watchers.SchedulerStateReader:
    """Return ``job_name -> "ENABLED"|"PAUSED"|... | None`` via Cloud Scheduler.

    PAUSE-AWARE meta-watcher input (KEY #2): a scheduler PAUSED-by-design during
    the manual-backfill campaign should NOT fire DP_CRON_DID_NOT_FIRE. The
    google.cloud.scheduler_v1 client is deferred-imported here (the credential-bound
    surface) — NOT at module top — mirroring the ``vm_zombie_watchdog`` deferral, so
    the watcher modules stay import-safe + credential-free. A lookup error / missing
    client returns ``None`` (UNKNOWN → the watcher does NOT suppress, the fail-safe
    direction). The job's short name is resolved to the fully-qualified
    ``projects/{p}/locations/{loc}/jobs/{name}`` path.
    """
    project = _project_id()
    try:
        scheduler_mod = importlib.import_module("google.cloud.scheduler_v1")  # noqa: imports-inside-functions — credential-bound SDK, deferred
        client = scheduler_mod.CloudSchedulerClient()
    except Exception as exc:
        logger.info("scheduler-state reader unavailable (pause-awareness off, alerts fail-safe-on): %s", exc)
        return lambda _job: None

    def _read(job_name: str) -> str | None:
        if not project or not job_name:
            return None
        qualified = f"projects/{project}/locations/{_REGION}/jobs/{job_name}"
        try:
            job = client.get_job(name=qualified, timeout=SCHEDULER_RPC_TIMEOUT_SECS)
            # Job.state is an enum; .name gives "ENABLED"/"PAUSED"/"DISABLED"/...
            return str(getattr(job.state, "name", "") or "") or None
        except Exception as exc:
            # NotFound / permission / transient → UNKNOWN; the watcher does not
            # suppress on None (a genuinely-missing scheduler still alerts).
            logger.info("scheduler-state lookup for %s → unknown: %s", job_name, exc)
            return None

    return _read


def make_consolidator_scheduler_lister() -> consolidator_scheduler_watcher.SchedulerJobLister:
    """Return ``() -> every manifest-consolidator scheduler job's short name``.

    Lists the fleet's Cloud Scheduler jobs LIVE and filters to
    ``manifest-consolidator`` in the name, rather than reconstructing names from a
    per-asset_group/kind list (which drifts — ``manifest_consolidator_buckets`` in
    ``manifest_consolidator_scheduler.tf`` has 10+ market-data/instruments keys plus
    legacy variants). Deferred-import, mirrors ``make_scheduler_state_reader``. An
    error / missing client returns an empty list (fail toward checking nothing
    this sweep, never toward inventing job names).
    """
    project = _project_id()
    try:
        scheduler_mod = importlib.import_module("google.cloud.scheduler_v1")  # noqa: imports-inside-functions — credential-bound SDK, deferred
        client = scheduler_mod.CloudSchedulerClient()  # pyright: ignore[reportAny] — dynamic cloud-SDK boundary
    except Exception as exc:
        logger.info("consolidator-scheduler lister unavailable (DP-WATCHER-003 off this sweep): %s", exc)
        return lambda: []

    def _list() -> list[str]:
        if not project:
            return []
        parent = f"projects/{project}/locations/{_REGION}"
        try:
            names: list[str] = []
            for job in client.list_jobs(  # pyright: ignore[reportAny] — dynamic SDK page iterator
                parent=parent, timeout=SCHEDULER_RPC_TIMEOUT_SECS
            ):
                full_name = str(getattr(job, "name", "") or "")  # pyright: ignore[reportAny] — dynamic SDK attr
                short_name = full_name.rsplit("/", 1)[-1]
                if "manifest-consolidator" in short_name:
                    names.append(short_name)
            return names
        except Exception as exc:
            logger.info("consolidator-scheduler list_jobs failed → skipping DP-WATCHER-003 this sweep: %s", exc)
            return []

    return _list


def make_execution_history_reader() -> meta_watchers.ExecutionHistoryReader:
    """Return ``job_stem -> minutes since last SUCCEEDED Cloud Run execution | None``.

    KEY #4: suppresses DP_CRON_DID_NOT_FIRE when execution history shows a recent
    success (vs lagging GCS sentinel). Deferred-import; errors return ``None``.
    """
    project = _project_id()
    env_prefix = scheduler_env_prefix()
    try:
        run_mod = importlib.import_module("google.cloud.run_v2")  # noqa: imports-inside-functions — credential-bound SDK, deferred
        client = run_mod.ExecutionsClient()  # pyright: ignore[reportAny] — dynamic cloud-SDK boundary
    except Exception as exc:
        logger.info("execution-history reader unavailable (KEY#4 cross-check off, alerts fail-safe-on): %s", exc)
        return lambda _job: None

    def _read(job_stem: str) -> float | None:
        if not project or not job_stem:
            return None
        job_name = f"{env_prefix}-{job_stem}" if env_prefix else job_stem
        parent = f"projects/{project}/locations/{_REGION}/jobs/{job_name}"
        try:
            youngest_age: float | None = None
            for execution in client.list_executions(parent=parent):  # pyright: ignore[reportAny] — dynamic SDK page iterator
                # A SUCCEEDED execution has a non-zero succeeded_count + a
                # completion_time. Take the most RECENT completion across successes.
                if int(getattr(execution, "succeeded_count", 0) or 0) <= 0:  # pyright: ignore[reportAny] — dynamic SDK attr
                    continue
                completion = getattr(execution, "completion_time", None)  # pyright: ignore[reportAny] — dynamic SDK attr
                if completion is None:
                    continue
                completed_dt = completion if isinstance(completion, datetime) else completion.ToDatetime(tzinfo=UTC)  # pyright: ignore[reportAny] — protobuf Timestamp
                age_min = (datetime.now(UTC) - completed_dt).total_seconds() / 60.0
                if youngest_age is None or age_min < youngest_age:
                    youngest_age = age_min
            return youngest_age
        except Exception as exc:
            # NotFound / permission / transient → UNKNOWN; the watcher does not
            # suppress on None (a genuinely-dead job still alerts).
            logger.info("execution-history lookup for %s → unknown: %s", job_name, exc)
            return None

    return _read
