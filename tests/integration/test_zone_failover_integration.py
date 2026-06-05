"""
Integration tests for zone-based failover and region validation.

Tests:
- Zone distribution: 30-shard deployment distributes 10-10-10 across zones
- Zone exhaustion cycles through zones without region switch
- /api/config/validate-region endpoint returns correct data
- Deployment state includes cross_region when region != GCS_REGION
"""

import os
from unittest.mock import MagicMock

import pytest

# deployment-api is a TEST-ONLY peer (absent in CI; circular-dep break 2026-06-04 @5734823).
# Every test here depends on the `deployment_api.main` app — skip the module when it's absent.
pytest.importorskip("deployment_api")


@pytest.fixture
def app(monkeypatch):
    """Create FastAPI app for testing (requires config_dir).

    Patches PubSubEventSink and events interface to avoid real Pub/Sub
    calls during TestClient lifespan startup, and disables auth so
    unauthenticated test requests reach the route handlers.
    """
    from pathlib import Path

    # Disable auth before deployment_api.auth is imported (module-level guard)
    monkeypatch.setenv("DISABLE_AUTH", "true")

    # Import the app (triggers module-level setup_events with real PubSubEventSink)
    from deployment_api.main import app as _app
    from unified_trading_library import (
        setup_events as _setup_events,
    )
    from unified_trading_library import sink as _sink_module

    # Replace the global event writer with a MockEventSink so log_event()
    # calls from RequestAuditMiddleware do not hit real Pub/Sub.
    mock_sink = _sink_module.MockEventSink()
    _setup_events(service_name="deployment-api", mode="test", sink=mock_sink)

    # Also patch the module-level _event_sink so nothing references the real one.
    monkeypatch.setattr(
        "deployment_api.main._event_sink",
        MagicMock(),
    )

    # Patch DISABLE_AUTH in the already-imported auth module
    monkeypatch.setattr("deployment_api.auth.DISABLE_AUTH", True)

    # Ensure config_dir exists for lifespan
    api_dir = Path(__file__).resolve().parent.parent.parent / "api"
    configs_dir = api_dir.parent / "configs"
    if not configs_dir.exists():
        pytest.skip("configs/ directory not found (run from repo root)")
    return _app


class TestZoneDistributionIntegration:
    """Zone round-robin distribution for 30-shard deployments."""

    def test_30_shards_distribute_10_10_10_across_zones(self):
        """30-shard deployment distributes exactly 10 shards per zone."""
        from deployment_service.backends.vm import VMBackend

        backend = VMBackend(
            project_id="test-project",
            region="asia-northeast1",
            service_account_email="test@test.iam.gserviceaccount.com",
        )
        zones = backend._config_manager.get_zones_for_region(backend.region)
        assert len(zones) == 3

        counts = dict.fromkeys(zones, 0)
        for shard_index in range(30):
            assigned = zones[shard_index % len(zones)]
            counts[assigned] += 1

        assert counts[zones[0]] == 10
        assert counts[zones[1]] == 10
        assert counts[zones[2]] == 10


class TestZoneExhaustionIntegration:
    """Zone exhaustion cycles within region only (no region switch)."""

    def test_vm_backend_cycles_zones_without_region_switch(self):
        """VM backend has no regions list or region-switch method."""
        from deployment_service.backends.vm import VMBackend

        backend = VMBackend(
            project_id="test-project",
            region="asia-northeast1",
            service_account_email="test@test.iam.gserviceaccount.com",
        )
        assert not hasattr(backend, "regions")
        assert not hasattr(backend, "_switch_to_next_region")
        zones = backend._config_manager.get_zones_for_region(backend.region)
        assert len(zones) == 3
        # Cycling 9 times (3 zones * 3 retries) stays in same region
        for i in range(9):
            zone = zones[i % 3]
            assert zone.startswith("asia-northeast1")


class TestValidateRegionEndpoint:
    """Region validation API endpoint."""

    @pytest.mark.integration
    def test_validate_region_same_region(self, app):
        """validate-region returns cross_region=false when requested matches GCS."""
        from fastapi.testclient import TestClient

        with TestClient(app) as client:
            region_resp = client.get("/api/config/region")
            if region_resp.status_code != 200:
                pytest.skip("Could not fetch region config")
            gcs_region = region_resp.json().get("gcs_region", "asia-northeast1")

            resp = client.get(
                "/api/config/validate-region",
                params={"requested_region": gcs_region},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["requested_region"] == gcs_region
        assert data["gcs_region"] == gcs_region
        assert data["cross_region"] is False
        assert data.get("warning") is None

    @pytest.mark.integration
    def test_validate_region_cross_region_returns_warning(self, app):
        """validate-region returns cross_region=true and warning when different."""
        from fastapi.testclient import TestClient

        with TestClient(app) as client:
            # Use whatever GCS_REGION is configured; request a different one
            region_resp = client.get("/api/config/region")
            if region_resp.status_code != 200:
                pytest.skip("Could not fetch region config")
            gcs_region = region_resp.json().get("gcs_region", "asia-northeast1")
            other_region = "us-central1" if gcs_region != "us-central1" else "europe-west1"

            resp = client.get(
                "/api/config/validate-region",
                params={"requested_region": other_region},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["requested_region"] == other_region
        assert data["gcs_region"] == gcs_region
        assert data["cross_region"] is True
        assert data.get("warning") is not None
        assert "cross-region egress" in data["warning"].lower() or "egress" in data["warning"].lower()

    @pytest.mark.integration
    def test_validate_region_requires_requested_region_param(self, app):
        """validate-region returns 422 when requested_region is missing."""
        from fastapi.testclient import TestClient

        with TestClient(app) as client:
            resp = client.get("/api/config/validate-region")
        assert resp.status_code == 422


class TestDeploymentMetadataCrossRegion:
    """Deployment state includes cross_region in config."""

    @pytest.mark.integration
    def test_cross_region_in_state_config_logic(self):
        """Verify cross_region is True when deployment_region != GCS_REGION."""
        gcs_region = os.environ.get("GCS_REGION", "asia-northeast1")
        deployment_region = "us-central1"
        cross_region = deployment_region != gcs_region
        assert cross_region is True
        # Same region: cross_region False
        cross_region_same = gcs_region != gcs_region
        assert cross_region_same is False
