"""Meta-watchers — "is the checker itself alive" (DP-CATALOG-001 / DP-WATCHER-001/002).

The blind spot the other Wave-4 watchers don't cover: a watcher / cron / catalogue
generator that ITSELF stops running emits nothing, so its absence is invisible.
These are lean freshness probes — one per failure mode — that read the durable
GCS artifact each producer leaves behind and alert when it goes stale:

  - DP-CATALOG-001 (``DP_CATALOG_NOT_RUNNING``, CRITICAL): per AG, the instrument
    catalogue artifact has not been refreshed within ``max_age`` (default 24h) →
    the enumerator/catalogue regen stopped.
  - DP-WATCHER-001 (``DP_ZOMBIE_WATCHDOG_DOWN``, CRITICAL): the zombie-VM
    watchdog's own census blob (``vm-census/watchdog-census.json``) is stale →
    the watchdog daemon is down (the meta-watcher of the watcher).
  - DP-WATCHER-002 (``DP_CRON_DID_NOT_FIRE``, CRITICAL): a scheduled audit /
    consolidator / digest cron's heartbeat/output artifact missed its window.

Each probe is the same shape: read the artifact's age, compare to a threshold,
emit on stale/missing. The GCS read is injected for credential-free testing.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from unified_trading_library import StorageClient
from unified_trading_library.events import (  # noqa: qg-deep-import
    DP_CATALOG_NOT_RUNNING,
    DP_CRON_DID_NOT_FIRE,
    DP_ZOMBIE_WATCHDOG_DOWN,
)

from deployment_service.data_pipeline_monitors import _gcs
from deployment_service.data_pipeline_monitors.escalation import (
    EscalationTier,
    PipelineFinding,
    route_finding,
)

logger = logging.getLogger(__name__)

# The zombie watchdog persists its census here (SSOT: vm_zombie_watchdog.py).
WATCHDOG_CENSUS_BLOB = "vm-census/watchdog-census.json"
DEFAULT_CATALOGUE_MAX_AGE_MIN = 24 * 60.0
DEFAULT_WATCHDOG_MAX_AGE_MIN = 30.0  # the watchdog ticks every 5 min

# The fleet-monitor / meta-watcher sweeps + their cadence (minutes). Each writes a
# ``vm-census/<mode>-last-run.json`` sentinel at end-of-sweep; the budget is 2x the
# cadence (a single missed tick is benign jitter; two missed = the cron stopped).
# The meta sweep CANNOT detect its OWN death in-band (it would have to be running to
# probe its own sentinel) — that is Layer-2's (the out-of-band deadman) job; it is
# still listed so the deadman reads it from the same builder.
MONITOR_CRON_CADENCE_MIN: dict[str, float] = {
    "exit-code": 5.0,  # */5 (data_pipeline_fleet_monitor_scheduler.tf)
    "heartbeat": 5.0,  # */5
    "meta": 15.0,  # */15
}

# monitor mode → Cloud Run **Job** name STEM (env-prefix-agnostic; the real job
# carries a ``${env_prefix}-`` prefix). The execution-history cross-check (KEY #4)
# queries this job's last SUCCEEDED execution to suppress a false stale-sentinel
# DP_CRON_DID_NOT_FIRE. SSOT for the stems: cloud_run_job_registry.CLOUD_RUN_JOBS.
MONITOR_CRON_CLOUD_RUN_JOB: dict[str, str] = {
    "exit-code": "dp-exit-code-monitor",
    "heartbeat": "dp-heartbeat-watcher",
    "meta": "dp-meta-watchers",
}


@dataclass(frozen=True)
class FreshnessTarget:
    """A single freshness probe target: a durable artifact + its staleness budget."""

    bucket: str
    blob_path: str
    max_age_min: float
    label: str  # human label (asset_group / cron name) for the alert
    # Cloud Scheduler job name backing this cron (e.g.
    # ``manifest-consolidator-market-data-sports``). When set + the injected
    # ``scheduler_state_reader`` reports it ``PAUSED``, ``check_cron_fired`` SKIPS
    # the stale-artifact alert (paused-by-design during the manual-backfill
    # campaign — KEY #2). "" → no pause check (always alert on stale, as before).
    scheduler_job: str = ""
    # Cloud Run **Job** name backing this cron (e.g. ``dp-exit-code-monitor``).
    # When set + the injected ``execution_history_reader`` reports the job's last
    # SUCCESSFUL execution within the budget, ``check_cron_fired`` SUPPRESSES the
    # stale-SENTINEL alert — the job DID fire (its run.log sentinel write is a
    # lagging/secondary signal that can silently miss while the execution itself
    # Completes), so a stale sentinel + a recent successful execution is a
    # FALSE-POSITIVE, not a dead cron (KEY #4, operator 2026-06-23:
    # dp-exit-code-monitor false-fired DP_CRON_DID_NOT_FIRE while its executions
    # Completed every 5 min). "" → no execution-history cross-check (sentinel is the
    # sole signal, the prior behaviour).
    cloud_run_job: str = ""


# A scheduler-state reader: ``job_name -> "ENABLED" | "PAUSED" | "DISABLED" | None``.
# ``None`` ⇒ the job state is UNKNOWN (job not found / API error) → the caller does
# NOT suppress (fail toward alerting on an unknown state — a missing job is itself
# worth an alert, never a silent skip). Injected so the watcher stays pure +
# credential-free; the cli wires the real Cloud Scheduler query.
SchedulerStateReader = Callable[[str], str | None]

# A Cloud Run **Job** last-success-age reader: ``job_name -> minutes since the
# job's most recent SUCCEEDED execution`` (``None`` ⇒ no successful execution found
# / job not found / API error → the caller does NOT suppress, fail toward
# alerting). Lets ``check_cron_fired`` cross-check the REAL execution history
# (``gcloud run jobs executions list``) against a stale SENTINEL: a job firing on
# schedule but whose run.log sentinel write lagged/missed is a false positive, so a
# recent successful execution suppresses the stale-sentinel alert (KEY #4). Injected
# so the watcher stays pure + credential-free; the cli wires the real query.
ExecutionHistoryReader = Callable[[str], float | None]


@dataclass(frozen=True)
class FreshnessResult:
    target: FreshnessTarget
    age_min: float | None  # None = artifact missing entirely
    stale: bool


def probe_freshness(
    storage_client: StorageClient,
    target: FreshnessTarget,
) -> FreshnessResult:
    """Probe one artifact's freshness. A missing artifact counts as stale."""
    age = _gcs.blob_age_minutes(storage_client, target.bucket, target.blob_path)
    stale = age is None or age > target.max_age_min
    return FreshnessResult(target=target, age_min=age, stale=stale)


def _emit(
    finding: PipelineFinding,
    *,
    pm_repo_path: str | None,
    dry_run: bool,
) -> None:
    if not dry_run:
        route_finding(finding, pm_repo_path=pm_repo_path)
    logger.warning("meta_watchers: %s %s", finding.event, finding.summary)


def check_catalogue_freshness(
    *,
    storage_client: StorageClient,
    targets: Iterable[FreshnessTarget],
    pm_repo_path: str | None = None,
    dry_run: bool = False,
) -> list[FreshnessResult]:
    """DP-CATALOG-001 — per-AG instrument-catalogue freshness."""
    results: list[FreshnessResult] = []
    for target in targets:
        result = probe_freshness(storage_client, target)
        results.append(result)
        if result.stale:
            # KEY #3 (operator 2026-06-23): the alert MUST SHOW WHAT IT PROBED —
            # the exact gs://bucket/path, the age it read (or "artifact ABSENT"),
            # and the freshness budget — so "(missing)" becomes a diagnosable
            # "probed gs://<bucket>/<path>, artifact ABSENT, budget=<N>h".
            budget_h = target.max_age_min / 60.0
            probed = f"gs://{target.bucket}/{target.blob_path}"  # noqa: gs-uri — human alert label (the probed path the operator clicks), not a storage lookup
            if result.age_min is None:
                state = "artifact ABSENT"
            else:
                state = f"age {result.age_min:.0f}m (={result.age_min / 60.0:.1f}h) > budget"
            summary = (
                f"instrument catalogue for {target.label} is stale — probed {probed}, {state}, budget={budget_h:.0f}h"
            )
            _emit(
                PipelineFinding(
                    event=DP_CATALOG_NOT_RUNNING,
                    severity="CRITICAL",
                    tier=EscalationTier.PAGE_OPERATOR,
                    summary=summary,
                    details={
                        "asset_group": target.label,
                        "bucket": target.bucket,
                        "blob_path": target.blob_path,
                        "probed_path": probed,
                        "artifact_present": result.age_min is not None,
                        "age_min": result.age_min,
                        "max_age_min": target.max_age_min,
                        "budget_hours": budget_h,
                    },
                    registry_id="DP-CATALOG-001",
                ),
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
    return results


def check_zombie_watchdog_alive(
    *,
    storage_client: StorageClient,
    log_bucket: str,
    max_age_min: float = DEFAULT_WATCHDOG_MAX_AGE_MIN,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
) -> FreshnessResult:
    """DP-WATCHER-001 — the zombie-VM watchdog's own census freshness."""
    target = FreshnessTarget(
        bucket=log_bucket,
        blob_path=WATCHDOG_CENSUS_BLOB,
        max_age_min=max_age_min,
        label="vm-zombie-watchdog",
    )
    result = probe_freshness(storage_client, target)
    if result.stale:
        _emit(
            PipelineFinding(
                event=DP_ZOMBIE_WATCHDOG_DOWN,
                severity="CRITICAL",
                tier=EscalationTier.PAGE_OPERATOR,
                summary=(
                    "vm-zombie-watchdog census stale "
                    + (f"({result.age_min:.0f}m)" if result.age_min is not None else "(missing) — watchdog down")
                ),
                details={
                    "bucket": log_bucket,
                    "blob_path": WATCHDOG_CENSUS_BLOB,
                    "age_min": result.age_min,
                    "max_age_min": max_age_min,
                },
                registry_id="DP-WATCHER-001",
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
    return result


def monitor_cron_targets(log_bucket: str) -> list[FreshnessTarget]:
    """Build a FreshnessTarget per fleet-monitor / meta-watcher sweep sentinel.

    Each sweep writes ``vm-census/<mode>-last-run.json`` (see ``_gcs`` /
    ``write_monitor_last_run``); a stale/absent sentinel = that monitor cron
    stopped firing → ``check_cron_fired`` emits ``DP_CRON_DID_NOT_FIRE``
    (DP-WATCHER-002, CRITICAL/page). The budget is 2x the cron's cadence
    (``MONITOR_CRON_CADENCE_MIN``) — one missed tick is benign jitter, two is the
    cron being down. This is the cron-watches-cron (in-band) layer; the meta sweep
    cannot detect its OWN death this way (Layer-2 / the out-of-band deadman owns
    that), but its sentinel is still listed so the deadman reads it from here too.
    """
    targets: list[FreshnessTarget] = []
    for mode, cadence_min in sorted(MONITOR_CRON_CADENCE_MIN.items()):
        targets.append(
            FreshnessTarget(
                bucket=log_bucket,
                blob_path=_gcs.MONITOR_LAST_RUN_BLOB.format(mode=mode),
                max_age_min=2.0 * cadence_min,
                label=f"dp-{mode}-monitor",
                # KEY #4 cross-check: a stale sentinel is suppressed when this Cloud
                # Run job's real execution history shows a recent SUCCEEDED run.
                cloud_run_job=MONITOR_CRON_CLOUD_RUN_JOB.get(mode, ""),
            )
        )
    return targets


def check_monitor_crons_fired(
    *,
    storage_client: StorageClient,
    log_bucket: str,
    scheduler_state_reader: SchedulerStateReader | None = None,
    execution_history_reader: ExecutionHistoryReader | None = None,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
) -> list[FreshnessResult]:
    """DP-WATCHER-002 — the fleet-monitor / meta-watcher crons fired on schedule.

    Convenience wrapper over :func:`check_cron_fired` for the monitor-sweep
    sentinels (built by :func:`monitor_cron_targets`). A stopped monitor cron
    leaves its ``vm-census/<mode>-last-run.json`` sentinel stale → emits
    ``DP_CRON_DID_NOT_FIRE`` (cron-did-not-fire, CRITICAL/page). NOTE: the meta
    sweep cannot catch its OWN death in-band — Layer-2's out-of-band deadman does.

    Pause-aware via ``scheduler_state_reader`` (KEY #2) — though the monitor-sweep
    schedulers (``dp-exit-code-monitor`` / ``dp-heartbeat-monitor`` /
    ``dp-meta-monitor``) are NOT paused during the campaign, so this is wired for
    parity; a paused monitor cron would (correctly) suppress.
    """
    return check_cron_fired(
        storage_client=storage_client,
        targets=monitor_cron_targets(log_bucket),
        scheduler_state_reader=scheduler_state_reader,
        execution_history_reader=execution_history_reader,
        pm_repo_path=pm_repo_path,
        dry_run=dry_run,
    )


def check_cron_fired(
    *,
    storage_client: StorageClient,
    targets: Iterable[FreshnessTarget],
    scheduler_state_reader: SchedulerStateReader | None = None,
    execution_history_reader: ExecutionHistoryReader | None = None,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
) -> list[FreshnessResult]:
    """DP-WATCHER-002 — scheduled audit/consolidator/digest crons fired on schedule.

    Each ``FreshnessTarget`` names a cron's durable output/heartbeat artifact +
    its expected cadence budget (e.g. a daily digest's report blob with a 25h
    budget). A stale artifact = the cron did not fire on schedule.

    **PAUSE-AWARE (KEY #2, operator 2026-06-23).** When a target names a
    ``scheduler_job`` AND the injected ``scheduler_state_reader`` reports it
    ``PAUSED``, the stale-artifact alert is SUPPRESSED — the scheduler is paused
    BY DESIGN during the manual-backfill campaign, so its stale output is expected,
    not a failure. An ``ENABLED`` job that missed its window (or any UNKNOWN/None
    state — fail toward alerting) STILL fires the CRITICAL alert. No
    ``scheduler_job`` / no reader ⇒ no pause check (always alert on stale, the
    prior behaviour).

    **EXECUTION-HISTORY CROSS-CHECK (KEY #4, operator 2026-06-23).** The SENTINEL
    artifact is a SECONDARY signal — a Cloud Run **Job** can fire on schedule + its
    execution ``Completes`` while its run.log sentinel write lags or silently misses
    (the ``dp-exit-code-monitor`` false-positive: executions Completed every 5 min
    yet DP_CRON_DID_NOT_FIRE fired CRITICAL on the stale sentinel). So when a target
    names a ``cloud_run_job`` AND the injected ``execution_history_reader`` reports
    the job's last SUCCESSFUL execution within the freshness budget, the
    stale-sentinel alert is SUPPRESSED — the cron DID fire. The execution history is
    the AUTHORITATIVE signal; the sentinel only catches a job that genuinely stopped
    executing. A ``None`` last-success age (no successful execution / job absent /
    API error) does NOT suppress (fail toward alerting). No ``cloud_run_job`` / no
    reader ⇒ no cross-check (sentinel is the sole signal, the prior behaviour).
    """
    results: list[FreshnessResult] = []
    for target in targets:
        result = probe_freshness(storage_client, target)
        results.append(result)
        if not result.stale:
            continue
        # Pause-aware skip: a PAUSED-by-design scheduler's stale artifact is
        # EXPECTED — suppress (no actuator, no page). Only an explicit PAUSED
        # suppresses; ENABLED / DISABLED / None (unknown) all fall through to alert.
        if target.scheduler_job and scheduler_state_reader is not None:
            state = scheduler_state_reader(target.scheduler_job)
            if state == "PAUSED":
                logger.info(
                    "meta_watchers: DP_CRON_DID_NOT_FIRE suppressed for '%s' — scheduler job "
                    "'%s' is PAUSED (paused-by-design during the manual-backfill campaign)",
                    target.label,
                    target.scheduler_job,
                )
                continue
        # Execution-history cross-check (KEY #4): the AUTHORITATIVE signal is the
        # Cloud Run Job's real execution history, NOT the lagging sentinel. A recent
        # SUCCEEDED execution within budget ⇒ the cron fired ⇒ suppress the false
        # stale-sentinel alert. None (no success / job absent / API error) → alert.
        if target.cloud_run_job and execution_history_reader is not None:
            last_success_age = execution_history_reader(target.cloud_run_job)
            if last_success_age is not None and last_success_age <= target.max_age_min:
                logger.info(
                    "meta_watchers: DP_CRON_DID_NOT_FIRE suppressed for '%s' — Cloud Run job "
                    "'%s' last SUCCEEDED execution %.0fm ago (<= budget %.0fm); the sentinel is "
                    "stale but the job IS firing (false positive)",
                    target.label,
                    target.cloud_run_job,
                    last_success_age,
                    target.max_age_min,
                )
                continue
        if result.stale:
            _emit(
                PipelineFinding(
                    event=DP_CRON_DID_NOT_FIRE,
                    severity="CRITICAL",
                    tier=EscalationTier.PAGE_OPERATOR,
                    summary=(
                        f"cron '{target.label}' did not fire on schedule "
                        + (f"(last output {result.age_min:.0f}m ago)" if result.age_min is not None else "(no output)")
                    ),
                    details={
                        "cron": target.label,
                        "bucket": target.bucket,
                        "blob_path": target.blob_path,
                        "age_min": result.age_min,
                        "max_age_min": target.max_age_min,
                    },
                    registry_id="DP-WATCHER-002",
                ),
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
    return results
