"""Client isolation resolution for deployment materialisation.

Translates a cluster + optional ClientSubscription into a materialisation plan:
for each service in the cluster, decide whether this client gets a dedicated
instance (ISOLATED) or routes to the shared pool (SHARED).

Reads isolation policies and SLA tier specs from the runtime topology SSOT
via UTL topology_reader. Enforces:

  1. An override requires the service's policy to include it in `allowed`.
  2. The client's SLA tier must permit the requested isolation.
  3. SLA tier `min_isolated_services` forces isolation regardless of default.

Used by cluster materialisers (cluster.py, cli/handlers/cluster_handler.py) and
by deployment-api subscription validation.
"""

from __future__ import annotations

from pydantic import BaseModel, Field
from unified_api_contracts.internal.domain.deployment_service import (
    ClientSubscription,
    IsolationPolicy,
    ServiceIsolationSpec,
    SLATierSpec,
)
from unified_trading_library import (
    get_isolation_policy,
    get_sla_tier_spec,
    resolve_deployment,
)


class ServiceMaterialisation(BaseModel):
    """How one service materialises for one client."""

    service_name: str
    isolation: IsolationPolicy
    client_id: str | None = Field(
        None,
        description="Set when isolation=ISOLATED. None means shared pool.",
    )
    source: str = Field(
        ...,
        description="Why this isolation was chosen: 'default' | 'sla_min' | 'override'",
    )


class ClusterMaterialisationPlan(BaseModel):
    """Full plan for materialising a cluster for one client (or no client)."""

    cluster_name: str
    client_id: str | None
    sla_tier: str | None
    services: list[ServiceMaterialisation]

    @property
    def isolated_services(self) -> list[str]:
        return [m.service_name for m in self.services if m.isolation == IsolationPolicy.ISOLATED]

    @property
    def shared_services(self) -> list[str]:
        return [m.service_name for m in self.services if m.isolation == IsolationPolicy.SHARED]


def resolve_service_for_client(
    service_name: str,
    subscription: ClientSubscription | None,
) -> ServiceMaterialisation:
    """Resolve isolation for one service given an optional client subscription.

    Resolution order (highest precedence first):
      1. Subscription.service_overrides[service].isolation — if explicit override and
         the override is valid per service policy AND SLA tier permits it.
      2. SLA tier's min_isolated_services — forces ISOLATED regardless of default.
      3. Service default from topology.

    Raises:
        ValueError: If an override is invalid for the service's policy or the
            SLA tier does not allow the requested isolation.
    """
    policy = get_isolation_policy(service_name)

    if subscription is None:
        return ServiceMaterialisation(
            service_name=service_name,
            isolation=policy.default,
            client_id=None,
            source="default",
        )

    tier_spec = get_sla_tier_spec(subscription.sla_tier)
    _validate_tier_allows_policy(subscription, tier_spec, policy)

    override = _find_override(subscription, service_name)
    if override is not None:
        _validate_override(service_name, override, policy, tier_spec)
        return ServiceMaterialisation(
            service_name=service_name,
            isolation=override,
            client_id=subscription.client_id,
            source="override",
        )

    if service_name in tier_spec.min_isolated_services:
        return ServiceMaterialisation(
            service_name=service_name,
            isolation=IsolationPolicy.ISOLATED,
            client_id=subscription.client_id,
            source="sla_min",
        )

    resolved = resolve_deployment(service_name, subscription.client_id)
    return ServiceMaterialisation(
        service_name=service_name,
        isolation=resolved,
        client_id=subscription.client_id if resolved == IsolationPolicy.ISOLATED else None,
        source="default",
    )


def build_cluster_plan(
    cluster_name: str,
    service_names: list[str],
    subscription: ClientSubscription | None,
) -> ClusterMaterialisationPlan:
    """Build a full materialisation plan for a cluster + optional subscription."""
    return ClusterMaterialisationPlan(
        cluster_name=cluster_name,
        client_id=subscription.client_id if subscription else None,
        sla_tier=str(subscription.sla_tier) if subscription else None,
        services=[resolve_service_for_client(svc, subscription) for svc in service_names],
    )


def _find_override(subscription: ClientSubscription, service_name: str) -> IsolationPolicy | None:
    for override in subscription.service_overrides:
        if override.service_name == service_name:
            return override.isolation
    return None


def _validate_override(
    service_name: str,
    override: IsolationPolicy,
    policy: ServiceIsolationSpec,
    tier_spec: SLATierSpec,
) -> None:
    if override not in policy.allowed:
        raise ValueError(
            f"Client override '{override}' not in service '{service_name}' "
            f"allowed list {[str(p) for p in policy.allowed]}. "
            f"Reason: {policy.reason}"
        )
    if override not in tier_spec.allowed_isolations:
        raise ValueError(
            f"SLA tier '{tier_spec.tier}' does not permit isolation '{override}'. "
            f"Allowed on this tier: {[str(p) for p in tier_spec.allowed_isolations]}"
        )


def _validate_tier_allows_policy(
    subscription: ClientSubscription,
    tier_spec: SLATierSpec,
    policy: ServiceIsolationSpec,
) -> None:
    # An SLA tier must permit the service's default isolation — otherwise the
    # topology is self-contradictory (e.g. a tier that forbids ISOLATED but the
    # service defaults to ISOLATED with no SHARED fallback in `allowed`).
    _ = subscription  # reserved — future tier/override-interaction checks
    if policy.default not in tier_spec.allowed_isolations:
        raise ValueError(
            f"Incoherent topology: service default '{policy.default}' "
            f"not allowed by SLA tier '{tier_spec.tier}' "
            f"(allowed: {[str(p) for p in tier_spec.allowed_isolations]})"
        )


__all__ = [
    "ClusterMaterialisationPlan",
    "ServiceMaterialisation",
    "build_cluster_plan",
    "resolve_service_for_client",
]
