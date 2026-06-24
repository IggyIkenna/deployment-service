"""Deployment cluster registry: shard-responsibility resolver.

``responsibility_for_deployment`` maps a classified :class:`DeploymentTarget` to
the :class:`ShardResponsibility` it owns. The derivation keys off the
already-classified ``service`` + ``asset_group`` fields — not a brittle
hand-dict, but a structured rule set matching the four
:class:`ShardResponsibilityKind` variants.

Derivation rules (in priority order):
1. ``service == "strategy-service"`` -> ``STRATEGY_SHARD``
2. ``service == "manifest-consolidator"`` -> ``MANIFEST_CONSOLIDATION(asset_group)``
3. The target's service name is in ``_DATA_PIPELINE_SERVICES``
   -> ``ASSET_GROUP_CAPTURE(asset_group)``
4. Else -> ``NONE`` (liveness-only; pure API gateways, control plane)

SSOT: ``plans/active/unified_deployment_health_cockpit_2026_06_23.md``
Phase 4.5 item 2.
"""

from __future__ import annotations

from unified_api_contracts import DeploymentTarget, ShardResponsibility, ShardResponsibilityKind

_DATA_PIPELINE_SERVICES: frozenset[str] = frozenset(
    {
        "alerting-service",
        "client-reporting-api",
        "execution-service",
        "features-service",
        "fund-administration-service",
        "greeks-service",
        "instruments-service",
        "market-data-processing-service",
        "market-tick-data-service",
        "ml-service",
        "trading-agent-service",
    }
)


def responsibility_for_deployment(target: DeploymentTarget) -> ShardResponsibility:
    """Resolve the :class:`ShardResponsibility` for a classified deployment target."""
    service = target.service
    asset_group = target.asset_group

    if service == "strategy-service":
        return ShardResponsibility(kind=ShardResponsibilityKind.STRATEGY_SHARD)

    if service == "manifest-consolidator":
        return ShardResponsibility(
            kind=ShardResponsibilityKind.MANIFEST_CONSOLIDATION,
            asset_group=asset_group,
        )

    if service in _DATA_PIPELINE_SERVICES:
        return ShardResponsibility(
            kind=ShardResponsibilityKind.ASSET_GROUP_CAPTURE,
            asset_group=asset_group,
        )

    return ShardResponsibility(kind=ShardResponsibilityKind.NONE)
