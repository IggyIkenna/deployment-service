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
* a ``data_freshness`` flag — whether the service supplies a *meaningful*
  ``make_health_router(data_freshness=...)`` callback (``True`` for data-plane /
  data-reporting services; ``False`` for the pure API gateways that proxy
  rather than own data).

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
)

from deployment_service.deployment_classification import classify_deployment_target

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

    Wraps the classified :class:`DeploymentTarget` with the two monitoring
    self-report fields the cockpit needs (``health_path`` + ``data_freshness``).
    """

    target: DeploymentTarget
    health_path: str
    data_freshness: bool

    @property
    def name(self) -> str:
        """The repo / logical service name (the registry key)."""
        return self.target.name

    @property
    def umbrella(self) -> DeploymentUmbrella:
        """The classified deployment umbrella (LIVE for every long-lived svc)."""
        return self.target.umbrella


def _live_service(name: str, *, data_freshness: bool, health_path: str = "/health") -> MonitoredService:
    """Build a LIVE (LONG_LIVED_LIVE) long-lived Cloud Run service entry.

    Classification routes through :func:`classify_deployment_target` with
    ``lifecycle_class="LONG_LIVED_LIVE"`` (→ LIVE umbrella) and an explicit
    ``service=name`` so the derivation matches the cockpit's other surfaces and
    raises ``UnclassifiedDeploymentError`` rather than silently defaulting. The
    kind is ``CLOUD_RUN_JOB`` — DeploymentKind models VM vs Cloud-Run-unit, and
    a long-lived service is the Cloud-Run unit (not a VM).
    """
    target = classify_deployment_target(
        name,
        lifecycle_class="LONG_LIVED_LIVE",
        kind=DeploymentKind.CLOUD_RUN_JOB,
        asset_group="",  # cross-asset control/data-plane services
        service=name,
    )
    return MonitoredService(target=target, health_path=health_path, data_freshness=data_freshness)


# ---------------------------------------------------------------------------
# Every long-lived deployable service (workspace-manifest type ∈
# {service, api-service, api}). ``data_freshness=True`` = the service supplies a
# meaningful make_health_router(data_freshness=...) callback (data-plane /
# data-reporting); ``False`` = a pure API gateway / control plane that proxies
# rather than owns data (its freshness callback is a fixed "serving").
# ---------------------------------------------------------------------------
_MONITORED_SERVICE_LIST: Final[tuple[MonitoredService, ...]] = (
    # --- data-plane / data-reporting services (real data_freshness) ---
    _live_service("alerting-service", data_freshness=True),
    _live_service("client-reporting-api", data_freshness=True),
    _live_service("execution-service", data_freshness=True),
    _live_service("features-service", data_freshness=True),
    _live_service("fund-administration-service", data_freshness=True),
    _live_service("greeks-service", data_freshness=True),
    _live_service("instruments-service", data_freshness=True),
    _live_service("market-data-processing-service", data_freshness=True),
    _live_service("market-tick-data-service", data_freshness=True),
    _live_service("ml-service", data_freshness=True),
    _live_service("strategy-service", data_freshness=True),
    _live_service("trading-agent-service", data_freshness=True),
    # --- pure API gateways / control plane (fixed "serving" freshness) ---
    _live_service("deployment-api", data_freshness=False),
    _live_service("unified-trading-api", data_freshness=False),
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
