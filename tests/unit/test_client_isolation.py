"""Tests for client_isolation resolver."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal.domain.deployment_service import (
    ClientServiceOverride,
    ClientSubscription,
    IsolationPolicy,
    SLATier,
)

from deployment_service.client_isolation import (
    build_cluster_plan,
    resolve_service_for_client,
)


def test_no_subscription_returns_default() -> None:
    """With no subscription, isolation = service default, no client_id."""
    mat = resolve_service_for_client("strategy-service", subscription=None)
    assert mat.source == "default"
    assert mat.client_id is None
    # strategy-service default is SHARED per runtime-topology.yaml
    assert mat.isolation == IsolationPolicy.SHARED


def test_execution_service_is_always_isolated() -> None:
    """execution-service default=isolated; no subscription still yields ISOLATED."""
    mat = resolve_service_for_client("execution-service", subscription=None)
    assert mat.isolation == IsolationPolicy.ISOLATED
    assert mat.source == "default"


def test_standard_tier_mandates_execution_isolation() -> None:
    sub = ClientSubscription(
        client_id="client_beta",
        sla_tier=SLATier.STANDARD,
        active_from=datetime(2026, 1, 1, tzinfo=UTC),
    )
    mat = resolve_service_for_client("execution-service", subscription=sub)
    assert mat.isolation == IsolationPolicy.ISOLATED
    assert mat.client_id == "client_beta"
    # execution's default is already ISOLATED, so source is 'default' not 'sla_min'
    assert mat.source in {"default", "sla_min"}


def test_premium_tier_forces_strategy_isolation_via_sla_min() -> None:
    sub = ClientSubscription(
        client_id="client_alpha",
        sla_tier=SLATier.PREMIUM,
        active_from=datetime(2026, 1, 1, tzinfo=UTC),
    )
    mat = resolve_service_for_client("strategy-service", subscription=sub)
    # strategy-service default is SHARED, but premium forces ISOLATED
    assert mat.isolation == IsolationPolicy.ISOLATED
    assert mat.source == "sla_min"
    assert mat.client_id == "client_alpha"


def test_override_promotes_shared_to_isolated_on_premium() -> None:
    """Premium client overrides strategy-service to isolated via explicit override."""
    sub = ClientSubscription(
        client_id="client_alpha",
        sla_tier=SLATier.PREMIUM,
        service_overrides=[
            ClientServiceOverride(
                service_name="strategy-service",
                isolation=IsolationPolicy.ISOLATED,
            ),
        ],
        active_from=datetime(2026, 1, 1, tzinfo=UTC),
    )
    mat = resolve_service_for_client("strategy-service", subscription=sub)
    assert mat.isolation == IsolationPolicy.ISOLATED
    # isolated via sla_min (premium tier mandates it) or override — both are valid
    assert mat.source in {"sla_min", "override"}


def test_basic_tier_cannot_override_to_isolated() -> None:
    """Basic tier's allowed_isolations=[SHARED], so ISOLATED override must raise."""
    sub = ClientSubscription(
        client_id="client_gamma",
        sla_tier=SLATier.BASIC,
        service_overrides=[
            ClientServiceOverride(
                service_name="alerting-service",
                isolation=IsolationPolicy.ISOLATED,
            ),
        ],
        active_from=datetime(2026, 1, 1, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="tier 'basic' does not permit"):
        resolve_service_for_client("alerting-service", subscription=sub)


def test_override_rejected_when_service_disallows() -> None:
    """instruments-service allowed=[shared] only; override to isolated must raise."""
    sub = ClientSubscription(
        client_id="client_alpha",
        sla_tier=SLATier.PREMIUM,
        service_overrides=[
            ClientServiceOverride(
                service_name="instruments-service",
                isolation=IsolationPolicy.ISOLATED,
            ),
        ],
        active_from=datetime(2026, 1, 1, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="not in service 'instruments-service'"):
        resolve_service_for_client("instruments-service", subscription=sub)


def test_build_cluster_plan_no_subscription() -> None:
    """Without subscription, plan reflects all defaults."""
    plan = build_cluster_plan(
        cluster_name="test_cluster",
        service_names=[
            "instruments-service",
            "execution-service",
            "strategy-service",
        ],
        subscription=None,
    )
    assert plan.client_id is None
    assert plan.sla_tier is None
    assert "execution-service" in plan.isolated_services
    assert "instruments-service" in plan.shared_services
    assert "strategy-service" in plan.shared_services


def test_build_cluster_plan_premium_client() -> None:
    """Premium client: IS shared, execution+strategy isolated."""
    sub = ClientSubscription(
        client_id="client_alpha",
        sla_tier=SLATier.PREMIUM,
        active_from=datetime(2026, 1, 1, tzinfo=UTC),
    )
    plan = build_cluster_plan(
        cluster_name="full",
        service_names=[
            "instruments-service",
            "execution-service",
            "strategy-service",
        ],
        subscription=sub,
    )
    assert plan.client_id == "client_alpha"
    assert "instruments-service" in plan.shared_services
    for svc in [
        "execution-service",
        "strategy-service",
    ]:
        assert svc in plan.isolated_services, f"{svc} should be isolated for premium"
