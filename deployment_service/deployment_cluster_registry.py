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

# ---------------------------------------------------------------------------
# VM launcher-family coverage (Phase 4.5 follow-up, finding 2026-06-24).
#
# The canonical-name checks above resolve a *service* deployment (the long-lived
# repos in ``MONITORED_SERVICES``: ``strategy-service``, ``market-tick-data-
# service`` …). But the cockpit inventory's rows are mostly **VMs**, whose
# ``DeploymentTarget.service`` is ``_derive_service(name)`` — a launcher-family
# stem (``strategy-live-…`` / ``mtds-backfill-…`` / ``cefi-binance-…``), NOT a
# canonical service name. Those rows fell through to ``NONE`` (liveness_only)
# even though they own real capture / strategy shards.
#
# These two curated prefix tuples (matched on the original VM ``name``, mirroring
# the existing explicit ``PAPER_PREFIXES`` pattern in deployment_classification)
# upgrade those VM rows to their real responsibility. They are a CONSERVATIVE
# allowlist: a name that matches nothing stays ``NONE`` / liveness_only — honest,
# never a false "fresh" (the same fail-safe direction as
# ``resolve_launcher_for_vm`` — under-claim, never wrongly attribute). Capture
# additionally requires a derivable ``asset_group`` (no asset_group ⇒ freshness
# cannot be attributed ⇒ stays liveness_only).

# Strategy / paper-trading launcher families. A VM whose name starts with one of
# these owns a STRATEGY_SHARD's as-if-filled state; the mode is derived from the
# umbrella (``strategy-paper-`` / ``defi-paper-`` / ``funding-ensemble-paper-`` →
# PAPER; ``strategy-live-`` → LIVE).
_STRATEGY_LAUNCHER_PREFIXES: Final[tuple[str, ...]] = (
    "strategy-live-",
    "strategy-paper-",
    "defi-paper-",
    "funding-ensemble-paper-",
)

# Data-pipeline CAPTURE launcher families (market-data / instruments / features
# producers: backfill + forward-poll + live capture). A VM whose name starts with
# one of these AND carries a derivable asset_group owns that asset_group's
# availability-manifest capture shards. The cefi per-venue families are listed
# explicitly (rather than a bare ``cefi-``) so a non-capture one-off such as
# ``cefi-rogue-`` is NOT swept in — conservative by construction.
_CAPTURE_LAUNCHER_PREFIXES: Final[tuple[str, ...]] = (
    # market-tick-data-service / market-data-processing-service
    "mtds-",
    "mdps-",
    # instruments-service per-asset_group backfill
    "instr-backfill-",
    "cefi-instr-",
    "tradfi-instr-",
    # features-service
    "features-",
    "fss-backfill-",
    "fs-backfill-",
    # cefi per-venue OHLCV / tick capture + forward-poll
    "cefi-binance-",
    "cefi-bybit-",
    "cefi-deribit-",
    "cefi-coinbase-",
    "cefi-okx-",
    "cefi-upbit-",
    "cefi-hyperliquid-",
    "cefi-bitfinex-",
    "cefi-bitget-",
    "cefi-kraken-",
    "cefi-aster-",
    "cefi-cme-",
    "cefi-extended-",
    "cefi-lighter-",
    "cefi-pacifica-",
    "cefi-mr-",
    "cefi-fwd-",
    "cefi-ext-bfill-",
    # tradfi capture
    "tradfi-bf-",
    "tradfi-fwd-",
    "tradfi-recent-",
    "tradfi-event-contract-",
    "cme-events-",
    # defi capture
    "defi-fwd-",
    "aster-fwd-",
    # prediction capture
    "prediction-fwd-",
    "prediction-live-",
    # sports source forward-poll / backfill
    "footystats-fwd-",
    "sfi-fwd-",
    "af-backfill-",
    "af-recover-",
)

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
    ``target.umbrella`` (canonical service deployments) and ``target.name``
    (VM launcher families) — never a hand-maintained per-target dict. A
    deployment whose service / launcher family owns no capture / consolidation /
    strategy shards resolves to ``ShardResponsibilityKind.NONE`` (liveness-only);
    the per-deployment freshness endpoint renders those as ``liveness_only``
    rather than a false "fresh".

    Resolution order (most-specific first): manifest consolidator → strategy
    (canonical service OR a ``_STRATEGY_LAUNCHER_PREFIXES`` VM) → asset-group
    capture (canonical service OR a ``_CAPTURE_LAUNCHER_PREFIXES`` VM with a
    derivable asset_group) → NONE.
    """
    service = target.service
    asset_group = target.asset_group
    name = target.name

    if service == _MANIFEST_CONSOLIDATOR_SERVICE:
        return ShardResponsibility(
            kind=ShardResponsibilityKind.MANIFEST_CONSOLIDATION,
            asset_group=asset_group,
        )
    if service == _STRATEGY_SERVICE or name.startswith(_STRATEGY_LAUNCHER_PREFIXES):
        return ShardResponsibility(
            kind=ShardResponsibilityKind.STRATEGY_SHARD,
            asset_group=asset_group,
            mode=_strategy_mode(target),
        )
    if service in _ASSET_GROUP_CAPTURE_SERVICES or (bool(asset_group) and name.startswith(_CAPTURE_LAUNCHER_PREFIXES)):
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
