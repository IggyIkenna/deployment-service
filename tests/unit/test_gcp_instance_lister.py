"""Tests for deployment_service.vm.gcp_instance_lister.

The lister routes through the UTL Compute Engine client
(``get_compute_engine_client`` → ``aggregated_list_instances``) rather than
importing ``google.cloud.compute_v1`` directly (cloud-interface SSOT / QG
STEP 5.10), so these tests mock that interface — which returns plain
``list[dict]`` rows of ``{"name", "status", "zone"}``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_client(rows: list[dict[str, object]]) -> MagicMock:
    """A mock ComputeEngineClient whose aggregated_list_instances returns ``rows``."""
    client = MagicMock()
    client.aggregated_list_instances.return_value = rows
    return client


def test_returns_running_vm_names() -> None:
    """RUNNING + TERMINATED rows → only RUNNING names returned."""
    from deployment_service.vm.gcp_instance_lister import list_running_vm_names

    client = _make_client(
        [
            {"name": "vm-a", "status": "RUNNING", "zone": "asia-northeast1-a"},
            {"name": "vm-b", "status": "RUNNING", "zone": "asia-northeast1-a"},
            {"name": "vm-c", "status": "TERMINATED", "zone": "asia-northeast1-b"},
        ]
    )

    with patch(
        "deployment_service.vm.gcp_instance_lister.get_compute_engine_client",
        return_value=client,
    ):
        result = list_running_vm_names("test-project")

    assert result == {"vm-a", "vm-b"}


def test_empty_list_returns_empty_set() -> None:
    """No instances → empty set (does not crash)."""
    from deployment_service.vm.gcp_instance_lister import list_running_vm_names

    with patch(
        "deployment_service.vm.gcp_instance_lister.get_compute_engine_client",
        return_value=_make_client([]),
    ):
        result = list_running_vm_names("test-project")

    assert result == set()


def test_api_error_returns_empty_set() -> None:
    """API exception → empty set returned + warning logged (never raises)."""
    from deployment_service.vm.gcp_instance_lister import list_running_vm_names

    client = MagicMock()
    client.aggregated_list_instances.side_effect = RuntimeError("GCP quota exceeded")
    mock_logger = MagicMock()

    with (
        patch(
            "deployment_service.vm.gcp_instance_lister.get_compute_engine_client",
            return_value=client,
        ),
        patch(
            "deployment_service.vm.gcp_instance_lister.logger",
            mock_logger,
        ),
    ):
        result = list_running_vm_names("test-project")

    assert result == set()
    mock_logger.warning.assert_called_once()
