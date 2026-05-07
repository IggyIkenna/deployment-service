"""Tests for cluster.py isolation / client_id materialisation plumbing.

Focused on the new surface introduced by Phase 2a (cluster materialise):
  - ClusterOrchestrator._load_subscription reads YAML from configs/client_subscriptions/
  - ClusterOrchestrator._isolation_env injects CLIENT_ID + ISOLATION_POLICY env vars
    only when the materialisation plan marks the service ISOLATED for a client.
  - Subscriptions with unsafe client_id values are rejected.

We avoid spinning up real subprocesses — these tests only cover the
deterministic plumbing, not the subprocess side of _start_local_service.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from unified_api_contracts.internal.domain.deployment_service import (
    ClientServiceOverride,
    ClientSubscription,
    IsolationPolicy,
    SLATier,
)

from deployment_service.client_isolation import (
    ClusterMaterialisationPlan,
    ServiceMaterialisation,
    build_cluster_plan,
)
from deployment_service.cluster import ClusterOrchestrator


@pytest.fixture(autouse=True)
def _stub_setup_events(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip the real setup_events() in ClusterOrchestrator.__init__.

    UTL's setup_events() now requires a sink= in batch mode; ClusterOrchestrator
    passes sink=None unconditionally. That's a pre-existing issue in cluster.py
    orthogonal to these tests — mock it so the materialisation-plumbing tests
    (our real target) can run deterministically.
    """
    monkeypatch.setattr("deployment_service.cluster.setup_events", lambda **_kwargs: None)
    ClusterOrchestrator._events_initialized = False


def _make_orchestrator(tmp_path: Path) -> ClusterOrchestrator:
    """Instantiate an orchestrator rooted at tmp_path (no real services needed)."""
    (tmp_path / "clusters").mkdir(parents=True, exist_ok=True)
    (tmp_path / "client_subscriptions").mkdir(parents=True, exist_ok=True)
    return ClusterOrchestrator(
        config_dir=str(tmp_path),
        workspace_root=str(tmp_path.parent),
        cloud_provider="local",
        mock_mode=True,
    )


def _write_subscription(tmp_path: Path, sub: ClientSubscription) -> None:
    subs_dir = tmp_path / "client_subscriptions"
    subs_dir.mkdir(parents=True, exist_ok=True)
    path = subs_dir / f"{sub.client_id}.yaml"
    path.write_text(yaml.safe_dump(sub.model_dump(mode="json"), sort_keys=False))


class TestLoadSubscription:
    def test_round_trip(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        sub = ClientSubscription(
            client_id="client_alpha",
            sla_tier=SLATier.PREMIUM,
            active_from=datetime(2026, 1, 1, tzinfo=UTC),
        )
        _write_subscription(tmp_path, sub)
        loaded = orch._load_subscription("client_alpha")
        assert loaded.client_id == "client_alpha"
        assert loaded.sla_tier == SLATier.PREMIUM

    def test_missing_raises_file_not_found(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        with pytest.raises(FileNotFoundError, match="No client subscription"):
            orch._load_subscription("ghost")

    def test_unsafe_client_id_rejected(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        with pytest.raises(ValueError, match="Invalid client_id"):
            orch._load_subscription("../evil")


class TestIsolationEnv:
    def test_none_plan_returns_empty(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        assert orch._isolation_env("execution-service", None) == {}

    def test_isolated_service_injects_client_id(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        plan = ClusterMaterialisationPlan(
            cluster_name="full",
            client_id="client_alpha",
            sla_tier="premium",
            services=[
                ServiceMaterialisation(
                    service_name="execution-service",
                    isolation=IsolationPolicy.ISOLATED,
                    client_id="client_alpha",
                    source="default",
                ),
            ],
        )
        env = orch._isolation_env("execution-service", plan)
        assert env == {"CLIENT_ID": "client_alpha", "ISOLATION_POLICY": "isolated"}

    def test_shared_service_injects_policy_only(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        plan = ClusterMaterialisationPlan(
            cluster_name="full",
            client_id=None,
            sla_tier=None,
            services=[
                ServiceMaterialisation(
                    service_name="instruments-service",
                    isolation=IsolationPolicy.SHARED,
                    client_id=None,
                    source="default",
                ),
            ],
        )
        env = orch._isolation_env("instruments-service", plan)
        assert env == {"ISOLATION_POLICY": "shared"}

    def test_service_not_in_plan_returns_empty(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        plan = ClusterMaterialisationPlan(
            cluster_name="full",
            client_id=None,
            sla_tier=None,
            services=[],
        )
        assert orch._isolation_env("execution-service", plan) == {}


class TestBuildCluterPlanIntegration:
    """Verify build_cluster_plan is called correctly with the loaded subscription.

    The actual policy resolution is covered by test_client_isolation.py; here we
    just assert the orchestrator wires subscription → plan correctly.
    """

    def test_premium_subscription_produces_isolated_exec_strategy_pbm_risk(
        self, tmp_path: Path
    ) -> None:
        sub = ClientSubscription(
            client_id="client_alpha",
            sla_tier=SLATier.PREMIUM,
            active_from=datetime(2026, 1, 1, tzinfo=UTC),
        )
        _write_subscription(tmp_path, sub)
        _orch = _make_orchestrator(tmp_path)  # exercises _load_subscription path
        loaded = _orch._load_subscription("client_alpha")
        plan = build_cluster_plan(
            "full",
            [
                "instruments-service",
                "execution-service",
                "strategy-service",
                "position-balance-monitor-service",
                "risk-and-exposure-service",
                "pnl-attribution-service",
            ],
            loaded,
        )
        assert "instruments-service" in plan.shared_services
        for svc in [
            "execution-service",
            "strategy-service",
            "position-balance-monitor-service",
            "risk-and-exposure-service",
        ]:
            assert svc in plan.isolated_services

    def test_override_via_subscription(self, tmp_path: Path) -> None:
        sub = ClientSubscription(
            client_id="client_beta",
            sla_tier=SLATier.STANDARD,
            service_overrides=[
                ClientServiceOverride(
                    service_name="risk-and-exposure-service",
                    isolation=IsolationPolicy.ISOLATED,
                ),
            ],
            active_from=datetime(2026, 1, 1, tzinfo=UTC),
        )
        _write_subscription(tmp_path, sub)
        orch = _make_orchestrator(tmp_path)
        loaded = orch._load_subscription("client_beta")
        plan = build_cluster_plan(
            "full",
            ["execution-service", "risk-and-exposure-service", "pnl-attribution-service"],
            loaded,
        )
        assert "execution-service" in plan.isolated_services  # standard mandates
        assert "risk-and-exposure-service" in plan.isolated_services  # override
        assert "pnl-attribution-service" in plan.shared_services  # no override
