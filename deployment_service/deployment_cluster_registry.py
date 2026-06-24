"""Resolve a :class:`DeploymentTarget` to the data shards it is responsible for.

Phase 4.5 of the deployment-health cockpit (operator correction 2026-06-23): a
service's **health** (a liveness ping) is NOT the same as its **data
freshness**. The per-service ``make_health_router(data_freshness=...)`` callback
is ad-hoc + in-memory (e.g. MTDS returns a single ``_last_tick_batch`` timestamp;
deployment-api / unified-trading-api have none), so a blanket
``MONITORED_SERVICES.data_freshness: bool`` overstated which deployments actually
own freshness. The REAL per-shard freshness SSOT already exists — the availability
**manifest** (``capture_status`` 4-state + ``available_at`` per
venue x data_type x asset_group x pipeline_mode x day shard). What was MISSING is
the **central binding** *deployment -> the shard-set it owns*, so freshness can be
attributed PER deployment rather than via a per-service self-report.

This module is that binding. :func:`responsibility_for_deployment` is a pure
DERIVATION (not a brittle hand-maintained dict) keyed off the target's already-
classified ``service`` + ``asset_group`` + ``umbrella``:

* a data-plane **capture** producer (MTDS / MDPS / instruments / features) ->
  ``ASSET_GROUP_CAPTURE(asset_group)`` — it owns the availability-manifest shards
  for its asset_group;
* the **manifest-consolidator** -> ``MANIFEST_CONSOLIDATION(asset_group)`` — it
  owns the consolidated ``_index`` heartbeat;
* **strategy-service** -> ``STRATEGY_SHARD(archetype, shard, mode)`` — it owns a
  strategy shard's as-if-filled state (mode derived from the umbrella);
* everything else (API gateways / control plane / consumers like alerting /
  execution / ml / greeks) -> ``NONE`` = liveness-only, **no** data-freshness
  expectation (the operator's key correction — these previously read
  ``data_freshness=True`` and overstated their obligation).

The guard test asserts a data-plane producer never silently resolves to
``NONE`` (a data service that owns shards must declare them), so the
capture/consolidation/strategy classification can never rot into a false
liveness-only.

SSOT: ``plans/active/unified_deployment_health_cockpit_2026_06_23.md`` Phase 4.5 +
``codex/02-data/availability-manifest-and-data-status.md`` (the manifest is the
per-shard freshness SSOT this binding points at).
"""

from __future__ import annotations

from typing import Final

from unified_api_contracts import (
    DeploymentTarget,
    DeploymentUmbrella,
    ShardResponsibility,
    ShardResponsibilityKind,
)

# Data-plane producers that OWN per-asset_group availability-manifest capture
# shards (the ``capture_status`` 4-state the cockpit's freshness reads). These
# are the services whose primary job is producing per-asset_group time-series
# the availability manifest tracks — NOT every service that happens to touch
# data (a consumer like execution / alerting / ml is liveness-only).
_ASSET_GROUP_CAPTURE_SERVICES: Final[frozenset[str]] = frozenset(
    {
        "market-tick-data-service",
        "market-data-processing-service",
        "instruments-service",
        "features-service",
    }
)

# The logical service name the manifest consolidator classifies as.
_MANIFEST_CONSOLIDATOR_SERVICE: Final[str] = "manifest-consolidator"

# The strategy service that owns strategy-shard as-if-filled state.
_STRATEGY_SERVICE: Final[str] = "strategy-service"

# umbrella -> strategy "mode". Strategy shards run as one of these operational
# modes; the umbrella already encodes it, so derive rather than parse the name.
_UMBRELLA_TO_MODE: Final[dict[DeploymentUmbrella, str]] = {
    DeploymentUmbrella.LIVE: "live",
    DeploymentUmbrella.PAPER: "paper",
    DeploymentUmbrella.BATCH: "batch",
}


def _strategy_mode(target: DeploymentTarget) -> str:
    """Strategy operational mode derived from the umbrella ('' when unmapped)."""
    return _UMBRELLA_TO_MODE.get(target.umbrella, "")


def responsibility_for_deployment(target: DeploymentTarget) -> ShardResponsibility:
    """Bind a classified deployment to the data shards it is responsible for.

    Pure derivation off ``target.service`` + ``target.asset_group`` +
    ``target.umbrella`` — never a hand-maintained per-target dict. A deployment
    whose service owns no capture / consolidation / strategy shards resolves to
    ``ShardResponsibilityKind.NONE`` (liveness-only); the per-deployment
    freshness endpoint renders those as ``liveness_only`` rather than a false
    "fresh".
    """
    service = target.service
    asset_group = target.asset_group

    if service == _MANIFEST_CONSOLIDATOR_SERVICE:
        return ShardResponsibility(
            kind=ShardResponsibilityKind.MANIFEST_CONSOLIDATION,
            asset_group=asset_group,
        )
    if service == _STRATEGY_SERVICE:
        return ShardResponsibility(
            kind=ShardResponsibilityKind.STRATEGY_SHARD,
            asset_group=asset_group,
            mode=_strategy_mode(target),
        )
    if service in _ASSET_GROUP_CAPTURE_SERVICES:
        return ShardResponsibility(
            kind=ShardResponsibilityKind.ASSET_GROUP_CAPTURE,
            asset_group=asset_group,
        )
    return ShardResponsibility(kind=ShardResponsibilityKind.NONE)


def is_data_plane_service(service: str) -> bool:
    """Whether ``service`` owns a (non-NONE) data-freshness obligation.

    True for the capture producers, the manifest consolidator, and the strategy
    service — the services whose deployments a guard test must prove never
    resolve to ``NONE``.
    """
    return service in _ASSET_GROUP_CAPTURE_SERVICES or service in {
        _MANIFEST_CONSOLIDATOR_SERVICE,
        _STRATEGY_SERVICE,
    }


__all__ = [
    "is_data_plane_service",
    "responsibility_for_deployment",
]
