# Epic: observability_master
# Lifecycle: permanent
"""Classified inventory of GCP Cloud Run Services under active liveness monitoring.

Unlike Cloud Run Jobs (``cloud_run_job_registry.py``), Cloud Run Services are
long-running HTTP endpoints. They do not appear in ``terraform/gcp/*_scheduler.tf``
and have no heartbeat convention — their only observable signal is the
``terminal_condition`` returned by the Cloud Run v2 API. This registry names every
service whose liveness should be checked by
``data_pipeline_monitors/cloud_run_service_watcher.py`` (DP-VM-012,
``DP_CLOUD_RUN_SERVICE_DOWN``).

Classification: all entries are ``kind=CLOUD_RUN_SERVICE``, ``cloud=GCP``.

**Service-name convention**: the Cloud Run Service name in GCP (e.g.
``market-data-query-service``, without any ``${env_prefix}-`` terraform fragment
— Cloud Run Services use fixed names in prod, unlike Jobs which carry ``prd-``).

SSOT: ``plans/active/issues/infra_health_audit_alert_coverage_gaps_2026_08_07.md``
todo 1 (Cloud Run Service liveness/OOM registry + detector).
"""

from __future__ import annotations

from typing import Final

from unified_api_contracts import (
    DeploymentCloud,
    DeploymentKind,
    DeploymentTarget,
    DeploymentUmbrella,
)


def _live_svc(name: str, *, service: str, asset_group: str = "") -> DeploymentTarget:
    """Build a LIVE Cloud Run Service target (kind=CLOUD_RUN_SERVICE, cloud=GCP)."""
    return DeploymentTarget(
        name=name,
        kind=DeploymentKind.CLOUD_RUN_SERVICE,
        umbrella=DeploymentUmbrella.LIVE,
        cloud=DeploymentCloud.GCP,
        service=service,
        asset_group=asset_group,
        lifecycle_class="",
    )


CLOUD_RUN_SERVICES: Final[tuple[DeploymentTarget, ...]] = (
    # market-data-query-service: HTTP query proxy for market data.
    # Crash-looping due to a hardcoded wrong-bucket reference since 2025-10-20 (~9.5mo).
    # Gap: infra_health_audit_alert_coverage_gaps_2026_08_07.md finding 1.
    _live_svc("market-data-query-service", service="market-data-query-service"),
    # central-market-data-tardis-loader: Tardis-sourced market data loader (LIVE, cefi).
    # Never-started for ~19mo (minScale=2, underlying boot failure — service never reached Ready).
    # Gap: infra_health_audit_alert_coverage_gaps_2026_08_07.md finding 11.
    _live_svc("central-market-data-tardis-loader", service="market-tick-data-service", asset_group="cefi"),
    # uts-prod-data-status-rollup-svc: data-status ML rollup endpoint.
    # OOM-ceiling at 32Gi/8vCPU; root cause tracked in
    # data_status_rollup_ml_service_full_blob_missing_2026_07_26.md.
    # Gap: infra_health_audit_alert_coverage_gaps_2026_08_07.md finding 3.
    _live_svc("uts-prod-data-status-rollup-svc", service="deployment-api"),
)
"""Every GCP Cloud Run Service under active liveness monitoring (DP-VM-012).

Add new entries here when a Cloud Run Service joins the fleet that should be paged on
terminal failure — no guard test enforces completeness (unlike cloud_run_job_registry.py
which is tied to terraform job discovery), so the operator must keep this in sync with
the Cloud Run Service estate."""
