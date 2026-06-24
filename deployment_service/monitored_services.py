"""Classified inventory of every long-lived deployable SERVICE (gap #6).

The deployment-observability surface classifies VMs (via
``vm_zombie_watchdog.VM_PREFIX_TO_BUCKET``) and Cloud Run **jobs** (via
``cloud_run_job_registry.CLOUD_RUN_JOBS``) — but a third deployment kind, the
**long-lived Cloud Run SERVICE** (the always-on FastAPI surface each
``service`` / ``api-service`` / ``api`` repo ships), had no machine-readable
"this deployable service registers for monitoring" surface. So a service could
ship — and even pass CI — without ever declaring itself to the cockpit
inventory (gap #6: a long-lived deployable service can be invisible to
monitoring). This module closes that gap: it is the SSOT the cockpit inventory
reads to make every long-lived service inventory-visible.

Each entry is a classified :class:`DeploymentTarget` (mirroring the
``cloud_run_job_registry`` idiom) carrying:

* the repo / logical service ``name``;
* the classified :class:`DeploymentUmbrella` (every long-lived service here is
  ``LONG_LIVED_LIVE`` by lifecycle → ``LIVE`` umbrella, resolved through
  :func:`classify_deployment_target` so it is consistent with every other
  surface — never a silent default);
* a ``health_path`` (``/health``, the UTL ``make_health_router`` endpoint);
* a derived :class:`ShardResponsibility` (Phase 4.5) — the central binding from
  this deployment to the availability-manifest shards it owns. Replaces the old
  ``data_freshness: bool`` self-report (operator 2026-06-23: liveness ≠ data
  freshness; the manifest is the per-shard freshness SSOT, and only genuine
  shard owners carry an obligation). ``owns_data_freshness`` is the bool view.

The guard test (``tests/unit/test_monitored_services_registry_guard.py``)
asserts every ``service`` / ``api-service`` / ``api`` repo in
``workspace-manifest.json`` has a ``MONITORED_SERVICES`` entry — the canonical
"added a service, forgot to register it for monitoring" guard. ``batch-service``
repos are NOT registered here; they register as Cloud Run JOBS in
``CLOUD_RUN_JOBS``.

SSOT: ``plans/active/unified_deployment_health_cockpit_2026_06_23.md``
Phase 4 (gap #6).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from unified_api_contracts import (
    DeploymentKind,
    DeploymentTarget,
    DeploymentUmbrella,
    ShardResponsibility,
    ShardResponsibilityKind,
)

from deployment_service.deployment_classification import classify_deployment_target
from deployment_service.deployment_cluster_registry import responsibility_for_deployment

# ---------------------------------------------------------------------------
# Repos that carry a deployable-service ``type`` in workspace-manifest.json but
# are NOT long-lived Cloud Run services this registry tracks. Each exclusion is
# named + reasoned so the guard stays honest (prefer registering over
# excluding). Currently empty — every long-lived service classifies + is
# registered below.
# ---------------------------------------------------------------------------
_NOT_LONG_LIVED_SERVICE_REPOS: Final[frozenset[str]] = frozenset()


@dataclass(frozen=True)
class MonitoredService:
    """A long-lived deployable service registered for cockpit monitoring.

    Wraps the classified :class:`DeploymentTarget` with its ``health_path`` and
    its derived :class:`ShardResponsibility` — the central binding from this
    deployment to the availability-manifest shards it owns (Phase 4.5). The old
    ``data_freshness: bool`` self-report was REPLACED by ``responsibility``
    (operator 2026-06-23: a liveness ping is not data-freshness; only genuine
    shard owners carry a freshness obligation, and the manifest — not a
    per-service callback — is the per-shard freshness SSOT). ``owns_data_freshness``
    is the bool view: ``True`` iff the resolved responsibility is not ``NONE``.
    """

    target: DeploymentTarget
    health_path: str
    responsibility: ShardResponsibility

    @property
    def name(self) -> str:
        """The repo / logical service name (the registry key)."""
        return self.target.name

    @property
    def umbrella(self) -> DeploymentUmbrella:
        """The classified deployment umbrella (LIVE for every long-lived svc)."""
        return self.target.umbrella

    @property
    def owns_data_freshness(self) -> bool:
        """Whether this service owns a (non-NONE) data-freshness obligation.

        ``True`` for the capture producers / manifest-consolidator / strategy
        service; ``False`` (liveness-only) for API gateways + consumers — the
        cockpit renders the latter as ``liveness_only``, never a false "fresh".
        """
        return self.responsibility.kind is not ShardResponsibilityKind.NONE


def _live_service(name: str, *, health_path: str = "/health") -> MonitoredService:
    """Build a LIVE (LONG_LIVED_LIVE) long-lived Cloud Run service entry.

    Classification routes through :func:`classify_deployment_target` with
    ``lifecycle_class="LONG_LIVED_LIVE"`` (→ LIVE umbrella) and an explicit
    ``service=name`` so the derivation matches the cockpit's other surfaces and
    raises ``UnclassifiedDeploymentError`` rather than silently defaulting. The
    kind is ``CLOUD_RUN_JOB`` — DeploymentKind models VM vs Cloud-Run-unit, and
    a long-lived service is the Cloud-Run unit (not a VM). The
    :class:`ShardResponsibility` is DERIVED from the classified target (Phase
    4.5) — never hand-supplied — so the capture producers carry their
    obligation and the gateways/consumers correctly resolve to liveness-only.
    """
    target = classify_deployment_target(
        name,
        lifecycle_class="LONG_LIVED_LIVE",
        kind=DeploymentKind.CLOUD_RUN_JOB,
        asset_group="",  # cross-asset control/data-plane services
        service=name,
    )
    return MonitoredService(
        target=target,
        health_path=health_path,
        responsibility=responsibility_for_deployment(target),
    )


# ---------------------------------------------------------------------------
# Every long-lived deployable service (workspace-manifest type ∈
# {service, api-service, api}). The ShardResponsibility is DERIVED per entry
# (Phase 4.5): the capture producers (MTDS / MDPS / instruments / features) ->
# ASSET_GROUP_CAPTURE, strategy-service -> STRATEGY_SHARD, and the gateways +
# consumers (alerting / client-reporting / execution / fund-admin / greeks / ml
# / trading-agent / deployment-api / unified-trading-api) -> NONE (liveness-only,
# no data-freshness obligation — the operator's correction to the old blanket
# data_freshness=True). Manifest-consolidator is a Cloud Run JOB (CLOUD_RUN_JOBS),
# not a long-lived service, so it is not listed here.
# ---------------------------------------------------------------------------
_MONITORED_SERVICE_LIST: Final[tuple[MonitoredService, ...]] = (
    _live_service("alerting-service"),
    _live_service("client-reporting-api"),
    _live_service("execution-service"),
    _live_service("features-service"),
    _live_service("fund-administration-service"),
    _live_service("greeks-service"),
    _live_service("instruments-service"),
    _live_service("market-data-processing-service"),
    _live_service("market-tick-data-service"),
    _live_service("ml-service"),
    _live_service("strategy-service"),
    _live_service("trading-agent-service"),
    _live_service("deployment-api"),
    _live_service("unified-trading-api"),
)


MONITORED_SERVICES: Final[dict[str, MonitoredService]] = {svc.name: svc for svc in _MONITORED_SERVICE_LIST}
"""Every long-lived deployable service, keyed by repo / service name.

The guard test asserts every ``service`` / ``api-service`` / ``api`` repo in
``workspace-manifest.json`` (minus ``_NOT_LONG_LIVED_SERVICE_REPOS``) has an
entry here — the "added a service, forgot to register it for monitoring" guard.
"""


def is_service_monitored(repo_name: str) -> bool:
    """Return whether ``repo_name`` has a MONITORED_SERVICES entry.

    The base-service.sh monitoring-registration check calls this (via a tiny
    Python one-liner) to prove a long-lived service is inventory-visible.
    """
    return repo_name in MONITORED_SERVICES


def monitored_service_names() -> frozenset[str]:
    """Return the frozenset of every registered long-lived service name."""
    return frozenset(MONITORED_SERVICES)
