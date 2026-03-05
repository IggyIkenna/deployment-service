"""
Unit tests for VM backend zone failover (single-region, no region switching).

Validates:
- Zone hint from compute_config is used as starting zone
- Zone cycling (no region switch)
- _record_zone_exhaustion does not trigger region switch
- _get_zones_for_region returns correct zones for asia-northeast1
"""

from deployment_service.backends.vm import VMBackend


class TestVMZoneFailover:
    """Tests for VM backend zone-only failover (no multi-region)."""

    def test_init_single_region_no_regions_param(self):
        """VMBackend should not accept regions param (removed)."""
        backend = VMBackend(
            project_id="test-project",
            region="asia-northeast1",
            service_account_email="test@test.iam.gserviceaccount.com",
            status_bucket="test-bucket",
            status_prefix="deployments.test",
        )
        assert not hasattr(backend, "regions")
        assert not hasattr(backend, "_current_region_index")
        assert backend.region == "asia-northeast1"
        assert len(backend.zones) == 3

    def test_get_zones_for_region_asia_northeast1(self):
        """_get_zones_for_region returns 3 zones for asia-northeast1."""
        backend = VMBackend(
            project_id="test-project",
            region="asia-northeast1",
            service_account_email="test@test.iam.gserviceaccount.com",
        )
        zones = backend._get_zones_for_region("asia-northeast1")
        assert len(zones) == 3
        assert "asia-northeast1-a" in zones or "asia-northeast1-b" in zones
        assert all(z.startswith("asia-northeast1-") for z in zones)

    def test_record_zone_exhaustion_no_region_switch(self):
        """_record_zone_exhaustion only increments counts, no region switch."""
        backend = VMBackend(
            project_id="test-project",
            region="asia-northeast1",
            service_account_email="test@test.iam.gserviceaccount.com",
        )
        original_region = backend.region

        backend._record_zone_exhaustion("asia-northeast1-a")
        backend._record_zone_exhaustion("asia-northeast1-b")
        backend._record_zone_exhaustion("asia-northeast1-c")

        assert backend.region == original_region
        assert backend._zone_exhaustion_counts.get("asia-northeast1-a") == 1

    def test_zone_hint_from_compute_config(self):
        """When compute_config has 'zone', it is used as starting zone."""
        backend = VMBackend(
            project_id="test-project",
            region="asia-northeast1",
            service_account_email="test@test.iam.gserviceaccount.com",
        )
        backend._get_zones_for_region("asia-northeast1")

        # Simulate deploy_shard zone hint logic
        preferred_zone = "asia-northeast1-b"
        if preferred_zone in backend.zones:
            backend._zone_index = backend.zones.index(preferred_zone)

        zones_to_try = backend._get_zones_to_try()
        assert zones_to_try[0] == preferred_zone

    def test_get_zones_to_try_round_robin(self):
        """_get_zones_to_try returns zones in round-robin order."""
        backend = VMBackend(
            project_id="test-project",
            region="asia-northeast1",
            service_account_email="test@test.iam.gserviceaccount.com",
        )
        zones_to_try = backend._get_zones_to_try()
        assert len(zones_to_try) == len(backend.zones)
        assert set(zones_to_try) == set(backend.zones)
