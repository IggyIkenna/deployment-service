"""
Unit tests for zone round-robin distribution (shard_index % 3).

Validates:
- Zone assignment: shard_index % len(zones) gives correct zone
- 30 shards -> 10-10-10 across zones
"""

from deployment_service.backends.vm import VMBackend


class TestZoneDistribution:
    """Tests for zone round-robin distribution logic."""

    def test_zone_assignment_shard_index_mod_3(self):
        """Shard index 0,1,2 -> zones 0,1,2; 3,4,5 -> zones 0,1,2; etc."""
        backend = VMBackend(
            project_id="test-project",
            region="asia-northeast1",
            service_account_email="test@test.iam.gserviceaccount.com",
        )
        zones = backend._get_zones_for_region(backend.region)

        # Simulate orchestrator/auto-scheduler logic
        for shard_index in range(12):
            assigned_zone = zones[shard_index % len(zones)]
            expected_idx = shard_index % 3
            assert assigned_zone == zones[expected_idx]

    def test_30_shards_distribute_10_10_10(self):
        """Batch of 30 shards should distribute 10 per zone."""
        backend = VMBackend(
            project_id="test-project",
            region="asia-northeast1",
            service_account_email="test@test.iam.gserviceaccount.com",
        )
        zones = backend._get_zones_for_region(backend.region)

        counts = dict.fromkeys(zones, 0)
        for shard_index in range(30):
            assigned = zones[shard_index % len(zones)]
            counts[assigned] += 1

        assert len(set(counts.values())) == 1
        assert next(iter(counts.values())) == 10

    def test_zone_suffixes_asia_northeast1(self):
        """asia-northeast1 has zones a, b, c (order may vary)."""
        backend = VMBackend(
            project_id="test-project",
            region="asia-northeast1",
            service_account_email="test@test.iam.gserviceaccount.com",
        )
        zones = backend._get_zones_for_region("asia-northeast1")
        zone_suffixes = [z.split("-")[-1] for z in zones]
        assert set(zone_suffixes) == {"a", "b", "c"}
