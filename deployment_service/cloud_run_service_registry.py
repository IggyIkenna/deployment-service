# Epic: observability_master
# Lifecycle: permanent
"""Registry of Cloud Run **Services** to monitor for liveness and OOM.

Mirrors ``cloud_run_job_registry.py`` (which covers Cloud Run *Jobs*), but for
the long-running Cloud Run *Services* that the existing DP-VM detector family and
``critical_service_uptime.tf`` do not cover fleet-wide.

**Why this exists** — the 2026-08-07 infra-health audit found three production
services broken for 9.5-19 months with zero alert coverage:

  - ``market-data-query-service`` crash-looping (wrong bucket hardcoded since 2025-10-20)
  - ``central-market-data-tardis-loader`` never-started (minScale=2, 0 healthy instances
    for 19 months)
  - ``uts-prod-data-status-rollup-svc`` OOM at 32 GiB / 8 vCPU ceiling

The guard test ``test_cloud_run_service_liveness_watcher.py`` asserts each entry
in this tuple is a non-empty, non-duplicate service name.

SSOT: ``plans/active/issues/infra_health_audit_alert_coverage_gaps_2026_08_07.md``
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final


@dataclass(frozen=True)
class CloudRunServiceTarget:
    """A single Cloud Run Service entry in the monitoring registry.

    Attributes:
        name: the Cloud Run service name WITHOUT the ``${local.env_prefix}-``
            terraform template prefix — i.e. the stable, environment-agnostic
            stem (e.g. ``"market-data-query-service"``). The watcher prepends
            the env-prefix at runtime.
        description: human-readable label shown in alert text and issue docs.
        location: GCP region where the service is deployed (default: primary
            production region ``asia-northeast1``).
        min_scale: the minimum number of healthy (READY) instances this service
            is expected to maintain. When the Cloud Run service reports fewer
            ready instances than this value the detector flags it as DOWN.
            Use ``0`` only for services that are legitimately allowed to scale
            to zero (i.e. request-driven services that can cold-start).
    """

    name: str
    description: str
    location: str = "asia-northeast1"
    min_scale: int = 1
    extra: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Services to monitor (the audit-identified blind-spot set).
# ---------------------------------------------------------------------------
# These are the three services from the 2026-08-07 audit that have NO alerting
# pathway and have been silently broken for months. Add new services here as
# they come online — the watcher ``check_cloud_run_service_liveness`` iterates
# this tuple on every sweep.
#
# Do NOT add services already covered by ``critical_service_uptime.tf``'s
# GCP-native external probers (``deployment-api``, ``agent-orchestrator``,
# ``alerting-service``, ``deployment-dashboard``, ``trading-ui``) — those have
# an out-of-band email channel that survives the alerting-service SPOF; this
# Python watcher's findings route THROUGH the alerting path so it should not
# duplicate coverage for the SPOF-sensitive critical-five.
CLOUD_RUN_SERVICES: Final[tuple[CloudRunServiceTarget, ...]] = (
    # Finding #1 from the 2026-08-07 audit.
    # market-data-query-service has hardcoded a wrong bucket path since 2025-10-20,
    # causing it to crash-loop on every container start. Terminal condition FAILED.
    CloudRunServiceTarget(
        name="market-data-query-service",
        description="market-data-query-service (crash-loop since 2025-10-20 — wrong bucket hardcoded)",
    ),
    # Finding #11 from the 2026-08-07 audit.
    # central-market-data-tardis-loader was deployed with minScale=2 but never
    # successfully started. 19 months old with 0 healthy instances.
    CloudRunServiceTarget(
        name="central-market-data-tardis-loader",
        description="central-market-data-tardis-loader (never-started 19mo, minScale=2)",
        min_scale=2,
    ),
    # Finding #3 from the 2026-08-07 audit.
    # uts-prod-data-status-rollup-svc hits GKE 32 GiB / 8 vCPU Cloud Run ceiling,
    # OOMKilled on every request when loading the full blob. Should page on OOM
    # conditions rather than relying on manual gcloud logging reads.
    CloudRunServiceTarget(
        name="uts-prod-data-status-rollup-svc",
        description="uts-prod-data-status-rollup-svc (OOM at 32 GiB / 8 vCPU ceiling)",
    ),
)

__all__ = ["CLOUD_RUN_SERVICES", "CloudRunServiceTarget"]
