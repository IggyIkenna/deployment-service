"""Classified inventory of every GCP Cloud Run job (the scheduler-tf job set).

The deployment-observability surface (deployment-api ``/api/deployments``,
deployment-ui Deployments page, the Slack deployment notifier) needs to know
**every** compute unit — not just the VMs. VMs come from
``vm_zombie_watchdog.VM_PREFIX_TO_BUCKET``; the Cloud Run **jobs** come from
``terraform/gcp/*_scheduler.tf``. This module enumerates every scheduler-tf job
into a classified :class:`DeploymentTarget` tuple so the surface has the full
picture.

**Job-name STEM convention** — terraform job names carry a ``${local.env_prefix}-``
template prefix (resolving to e.g. ``prd-``) and, for per-asset_group jobs, a
``-${each.key}`` suffix. The registry stores the **stem** (the stable part with
those template fragments stripped) so a name is environment- and asset_group-
agnostic. The guard test
(``tests/unit/test_cloud_run_job_registry_guard.py``) parses every
``*_scheduler.tf`` and asserts each tf's job-name stem appears here — the
canonical "added a Cloud Run job, forgot to classify it" guard.

Classification: infra audits / consolidator / catalogue / expected-universe /
monitors / digests / hygiene / rollups / recon / snapshots / reporting →
``BATCH``; the paper-week determinism + paper-engine jobs → ``PAPER``. Every job
is ``kind=CLOUD_RUN_JOB``, ``cloud=GCP``. Cross-asset jobs carry
``asset_group=""``; per-asset_group jobs (``manifest-consolidator-{ag}``,
``expected-universe-v2-{ag}``, ``lifecycle-catalogue-regen-{ag}``,
``features-onchain-collect-{ag}``, ``mtds-collect-{ag}``, ``*-t1-recon``) are
registered once per asset_group / service.

SSOT: ``plans/active/deployment_observability_parity_live_batch_paper_2026_06_22.md``
Phase 0.
"""

from __future__ import annotations

from typing import Final

from unified_api_contracts import (
    DeploymentCloud,
    DeploymentKind,
    DeploymentTarget,
    DeploymentUmbrella,
)

# The 5 canonical asset_groups (for the per-AG ``for_each`` Cloud Run jobs).
_ASSET_GROUPS: Final[tuple[str, ...]] = ("cefi", "defi", "tradfi", "sports", "prediction")


def _batch(name: str, *, service: str, asset_group: str = "") -> DeploymentTarget:
    """Build a BATCH Cloud Run job target (kind=CLOUD_RUN_JOB, cloud=GCP)."""
    return DeploymentTarget(
        name=name,
        kind=DeploymentKind.CLOUD_RUN_JOB,
        umbrella=DeploymentUmbrella.BATCH,
        cloud=DeploymentCloud.GCP,
        service=service,
        asset_group=asset_group,
        lifecycle_class="",  # Cloud Run jobs have no VM lifecycle_class.
    )


def _paper(name: str, *, service: str) -> DeploymentTarget:
    """Build a PAPER Cloud Run job target (paper-week determinism / paper engine)."""
    return DeploymentTarget(
        name=name,
        kind=DeploymentKind.CLOUD_RUN_JOB,
        umbrella=DeploymentUmbrella.PAPER,
        cloud=DeploymentCloud.GCP,
        service=service,
        asset_group="",
        lifecycle_class="",
    )


def _live(name: str, *, service: str, asset_group: str = "") -> DeploymentTarget:
    """Build a LIVE Cloud Run job target (a scheduler that boots a near-real-time
    LIVE-capture VM — the cron itself is the classified scheduler-tf job stem)."""
    return DeploymentTarget(
        name=name,
        kind=DeploymentKind.CLOUD_RUN_JOB,
        umbrella=DeploymentUmbrella.LIVE,
        cloud=DeploymentCloud.GCP,
        service=service,
        asset_group=asset_group,
        lifecycle_class="",
    )


# ---------------------------------------------------------------------------
# Per-asset_group / per-service job families (for_each in terraform).
# ---------------------------------------------------------------------------
_MANIFEST_CONSOLIDATOR_JOBS: Final[tuple[DeploymentTarget, ...]] = tuple(
    _batch(f"manifest-consolidator-{ag}", service="manifest-consolidator", asset_group=ag) for ag in _ASSET_GROUPS
)
_EXPECTED_UNIVERSE_V2_JOBS: Final[tuple[DeploymentTarget, ...]] = tuple(
    _batch(f"expected-universe-v2-{ag}", service="instruments-service", asset_group=ag) for ag in _ASSET_GROUPS
)
_LIFECYCLE_CATALOGUE_JOBS: Final[tuple[DeploymentTarget, ...]] = tuple(
    _batch(f"lifecycle-catalogue-regen-{ag}", service="instruments-service", asset_group=ag)
    for ag in ("cefi", "defi", "tradfi", "sports")  # local.lifecycle_catalogue_asset_groups (no prediction)
)
_FEATURES_ONCHAIN_COLLECT_JOBS: Final[tuple[DeploymentTarget, ...]] = (
    _batch("features-onchain-collect-lst-seasonal-rewards", service="features-onchain-service", asset_group="defi"),
)
_DEFI_COLLECT_JOBS: Final[tuple[DeploymentTarget, ...]] = (
    _batch("mtds-collect-defi", service="market-tick-data-service", asset_group="defi"),
)
# T+1 recon jobs (t1_batch_scheduler.tf) — one per service, cross-asset (asset_group="").
_T1_RECON_JOBS: Final[tuple[DeploymentTarget, ...]] = (
    _batch("instruments-service-t1-recon", service="instruments-service"),
    _batch("instruments-service-cefi-t1-recon", service="instruments-service", asset_group="cefi"),
    _batch("instruments-service-sports-fixtures", service="instruments-service", asset_group="sports"),
    _batch("market-tick-data-service-fast-t1-recon", service="market-tick-data-service"),
    _batch("market-tick-data-service-cefi-t1-recon", service="market-tick-data-service", asset_group="cefi"),
    _batch("market-data-processing-service-t1-recon", service="market-data-processing-service"),
    _batch("features-calendar-service-t1-recon", service="features-calendar-service"),
    _batch("features-delta-one-service-t1-recon", service="features-delta-one-service"),
    _batch("features-volatility-service-t1-recon", service="features-volatility-service"),
    _batch("features-onchain-service-t1-recon", service="features-onchain-service"),
    _batch("features-sports-service-t1-recon", service="features-sports-service", asset_group="sports"),
    _batch("features-cross-instrument-service-t1-recon", service="features-cross-instrument-service"),
    _batch("features-multi-timeframe-service-t1-recon", service="features-multi-timeframe-service"),
    _batch("features-commodity-service-t1-recon", service="features-commodity-service"),
    _batch("ml-service-t1-recon", service="ml-service"),
    _batch("strategy-service-t1-recon", service="strategy-service"),
    _batch("batch-live-reconciliation-service", service="batch-live-reconciliation-service"),
    _batch("execution-service-config-snapshot", service="execution-service"),
)

# ---------------------------------------------------------------------------
# Singleton infra jobs (one per scheduler tf).
# ---------------------------------------------------------------------------
_SINGLETON_JOBS: Final[tuple[DeploymentTarget, ...]] = (
    # batch_live_smoke_matrix_scheduler.tf
    _batch("batch-live-smoke-matrix-daily", service="e2e-testing"),
    # catalogue_regen_scheduler.tf (cron hits the `catalogue-regen` container job)
    _batch("catalogue-regen-nightly", service="instruments-service"),
    # cf_manifest_audit_scheduler.tf
    _batch("cf-manifest-audit", service="manifest-consolidator"),
    # client_reporting_scheduler.tf (jobs created at runtime by backends/cloud_run.py)
    _batch("client-reporting-update", service="client-reporting-api"),
    _batch("client-reporting-daily-snapshot", service="client-reporting-api"),
    # code_tarball_refresh_scheduler.tf
    _batch("code-tarball-refresh", service="deployment-service"),
    # consolidator_liveness_scheduler.tf
    _batch("consolidator-liveness-watchdog", service="manifest-consolidator"),
    # data_pipeline_audit_scheduler.tf
    _batch("dp-daily-digest", service="deployment-service"),
    _batch("dp-manifest-hygiene-changed", service="deployment-service"),
    _batch("dp-manifest-hygiene-full", service="deployment-service"),
    _batch("dp-reprobe-empty", service="deployment-service"),
    # data_pipeline_fleet_monitor_scheduler.tf
    _batch("dp-exit-code-monitor", service="deployment-service"),
    _batch("dp-heartbeat-watcher", service="deployment-service"),
    _batch("dp-meta-watchers", service="deployment-service"),
    # data_status_rollup_scheduler.tf (cron hits the data-status-rollup-svc /rollup-run endpoint)
    _batch("data-status-rollup", service="deployment-api"),
    # honest_coverage_scheduler.tf
    _batch("honest-coverage-daily-launcher", service="instruments-service"),
    # instrument_catalogue_scheduler.tf
    _batch("instrument-catalogue-regen", service="instruments-service"),
    # orphan_ping_audit_scheduler.tf
    _batch("orphan-ping-audit", service="unified-trading-pm"),
    # hygiene_sweep_scheduler.tf
    _batch("plan-hygiene-sweep", service="unified-trading-pm"),
    # qg_snapshot_scheduler.tf
    _batch("qg-snapshot-daily", service="unified-trading-pm"),
    # subgraph_health_probe_scheduler.tf
    _batch("subgraph-health-probe", service="market-tick-data-service"),
    # defi_forward_poll_scheduler.tf — 3 */5 schedulers each boot an ephemeral
    # defi-fwd-<op>-poll VM running the MTDS DeFi handler in LIVE mode (continuous
    # near-real-time DeFi price capture). LIVE umbrella; the cron stem is defi-fwd-poll.
    _live("defi-fwd-poll", service="market-tick-data-service", asset_group="defi"),
    # tarball_cleanup_scheduler.tf
    _batch("tarball-cleanup", service="deployment-service"),
    # vm_log_archival_scheduler.tf
    _batch("vm-log-archival", service="deployment-service"),
    # vm_serial_capture_scheduler.tf
    _batch("vm-serial-capture", service="deployment-service"),
    # paper_week_determinism_scheduler.tf — PAPER umbrella (paper-week determinism + paper engine)
    _paper("paper-engine-run", service="strategy-service"),
    _paper("blrs-daily-determinism", service="batch-live-reconciliation-service"),
    _paper("daily-ledger-digest", service="batch-live-reconciliation-service"),
)


CLOUD_RUN_JOBS: Final[tuple[DeploymentTarget, ...]] = (
    *_SINGLETON_JOBS,
    *_MANIFEST_CONSOLIDATOR_JOBS,
    *_EXPECTED_UNIVERSE_V2_JOBS,
    *_LIFECYCLE_CATALOGUE_JOBS,
    *_FEATURES_ONCHAIN_COLLECT_JOBS,
    *_DEFI_COLLECT_JOBS,
    *_T1_RECON_JOBS,
)
"""Every classified GCP Cloud Run job (the scheduler-tf job set).

The guard test asserts each ``terraform/gcp/*_scheduler.tf`` file's job-name stem
appears in this tuple."""
