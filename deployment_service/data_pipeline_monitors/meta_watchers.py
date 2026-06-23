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
from collections.abc import Iterable
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


@dataclass(frozen=True)
class FreshnessTarget:
    """A single freshness probe target: a durable artifact + its staleness budget."""

    bucket: str
    blob_path: str
    max_age_min: float
    label: str  # human label (asset_group / cron name) for the alert


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
            _emit(
                PipelineFinding(
                    event=DP_CATALOG_NOT_RUNNING,
                    severity="CRITICAL",
                    tier=EscalationTier.PAGE_OPERATOR,
                    summary=(
                        f"instrument catalogue for {target.label} not refreshed in "
                        + (f"{result.age_min:.0f}m" if result.age_min is not None else ">budget (missing)")
                    ),
                    details={
                        "asset_group": target.label,
                        "bucket": target.bucket,
                        "blob_path": target.blob_path,
                        "age_min": result.age_min,
                        "max_age_min": target.max_age_min,
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
            )
        )
    return targets


def check_monitor_crons_fired(
    *,
    storage_client: StorageClient,
    log_bucket: str,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
) -> list[FreshnessResult]:
    """DP-WATCHER-002 — the fleet-monitor / meta-watcher crons fired on schedule.

    Convenience wrapper over :func:`check_cron_fired` for the monitor-sweep
    sentinels (built by :func:`monitor_cron_targets`). A stopped monitor cron
    leaves its ``vm-census/<mode>-last-run.json`` sentinel stale → emits
    ``DP_CRON_DID_NOT_FIRE`` (cron-did-not-fire, CRITICAL/page). NOTE: the meta
    sweep cannot catch its OWN death in-band — Layer-2's out-of-band deadman does.
    """
    return check_cron_fired(
        storage_client=storage_client,
        targets=monitor_cron_targets(log_bucket),
        pm_repo_path=pm_repo_path,
        dry_run=dry_run,
    )


def check_cron_fired(
    *,
    storage_client: StorageClient,
    targets: Iterable[FreshnessTarget],
    pm_repo_path: str | None = None,
    dry_run: bool = False,
) -> list[FreshnessResult]:
    """DP-WATCHER-002 — scheduled audit/consolidator/digest crons fired on schedule.

    Each ``FreshnessTarget`` names a cron's durable output/heartbeat artifact +
    its expected cadence budget (e.g. a daily digest's report blob with a 25h
    budget). A stale artifact = the cron did not fire on schedule.
    """
    results: list[FreshnessResult] = []
    for target in targets:
        result = probe_freshness(storage_client, target)
        results.append(result)
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
