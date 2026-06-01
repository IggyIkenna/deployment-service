"""Unit tests for deployment_service/backends/services/vm_config.py helpers.

Covers the pure-logic methods that don't require GCP API calls:
  - extract_registry_region()
  - generate_instance_name()
  - get_status_path()
  - is_zone_exhausted_error()
  - is_regional_quota_error()
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_vm_config() -> object:
    """Return a VMConfigManager with GCP client patched out."""
    from deployment_service.backends.services.vm_config import VMConfigManager

    with patch("deployment_service.backends.services.vm_config.get_images_client", return_value=MagicMock()):
        mgr = VMConfigManager(project_id="test-proj", region="asia-northeast1")
    return mgr


class TestExtractRegistryRegion:
    def test_valid_asia_northeast1(self) -> None:
        mgr = _make_vm_config()
        region = mgr.extract_registry_region(  # type: ignore[attr-defined]
            "asia-northeast1-docker.pkg.dev/proj/repo/image:latest"
        )
        assert region == "asia-northeast1"

    def test_valid_us_central1(self) -> None:
        mgr = _make_vm_config()
        region = mgr.extract_registry_region(  # type: ignore[attr-defined]
            "us-central1-docker.pkg.dev/proj/repo/image:tag"
        )
        assert region == "us-central1"

    def test_non_matching_url_returns_default(self) -> None:
        mgr = _make_vm_config()
        region = mgr.extract_registry_region("gcr.io/proj/image:latest")  # type: ignore[attr-defined]
        assert region == "asia-northeast1"  # default


class TestGenerateInstanceName:
    def test_name_contains_service_and_shard(self) -> None:
        mgr = _make_vm_config()
        name = mgr.generate_instance_name("my-service", "shard-001")  # type: ignore[attr-defined]
        assert "my-service" in name
        assert "shard" in name

    def test_shard_id_sanitised_to_lowercase_dashes(self) -> None:
        import re

        mgr = _make_vm_config()
        name = mgr.generate_instance_name("my-svc", "SHARD/UPPER")  # type: ignore[attr-defined]
        # shard_id portion is sanitised; service_name is taken as-is (first 20 chars)
        assert re.match(r"^[a-z0-9-]+$", name.split("-shard")[1] if "-shard" in name else name), (
            f"Shard portion not sanitised: {name}"
        )
        # Verify the shard portion contains no slash
        assert "/" not in name, f"Slash not removed: {name}"

    def test_name_uniqueness(self) -> None:
        mgr = _make_vm_config()
        names = {mgr.generate_instance_name("svc", "shard") for _ in range(5)}  # type: ignore[attr-defined]
        assert len(names) == 5, "Expected unique names each call"


class TestGetStatusPath:
    def test_empty_bucket_returns_empty(self) -> None:
        mgr = _make_vm_config()
        path = mgr.get_status_path(None, "status", "deploy-1", "shard-1")  # type: ignore[attr-defined]
        assert path == ""

    def test_valid_path_structure(self) -> None:
        mgr = _make_vm_config()
        path = mgr.get_status_path("my-bucket", "pfx", "dep-1", "shard-2")  # type: ignore[attr-defined]
        assert "my-bucket" in path
        assert "dep-1" in path
        assert "shard-2" in path


class TestIsZoneExhaustedError:
    def test_zone_pool_exhausted_matches(self) -> None:
        mgr = _make_vm_config()
        assert mgr.is_zone_exhausted_error("ZONE_RESOURCE_POOL_EXHAUSTED in zone asia-northeast1-a") is True  # type: ignore[attr-defined]

    def test_stockout_matches(self) -> None:
        mgr = _make_vm_config()
        assert mgr.is_zone_exhausted_error("stockout for requested machine type") is True  # type: ignore[attr-defined]

    def test_unrelated_error_does_not_match(self) -> None:
        mgr = _make_vm_config()
        assert mgr.is_zone_exhausted_error("API quota exceeded") is False  # type: ignore[attr-defined]

    def test_case_insensitive(self) -> None:
        mgr = _make_vm_config()
        assert mgr.is_zone_exhausted_error("Insufficient Resources available") is True  # type: ignore[attr-defined]


class TestIsRegionalQuotaError:
    def test_cpus_quota_matches(self) -> None:
        mgr = _make_vm_config()
        assert mgr.is_regional_quota_error("Quota exceeded for quota CPUS in region asia-northeast1") is True  # type: ignore[attr-defined]

    def test_ip_quota_matches(self) -> None:
        mgr = _make_vm_config()
        assert mgr.is_regional_quota_error("quota exceeded: IN_USE_ADDRESSES") is True  # type: ignore[attr-defined]

    def test_zone_exhausted_does_not_match(self) -> None:
        mgr = _make_vm_config()
        assert mgr.is_regional_quota_error("ZONE_RESOURCE_POOL_EXHAUSTED") is False  # type: ignore[attr-defined]

    def test_requires_both_quota_and_indicator(self) -> None:
        mgr = _make_vm_config()
        # "quota" present but no regional indicator
        assert mgr.is_regional_quota_error("quota message without indicator") is False  # type: ignore[attr-defined]
