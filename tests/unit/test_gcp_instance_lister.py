"""Tests for deployment_service.vm.gcp_instance_lister."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_scoped_list(names: list[str], status: str = "RUNNING") -> MagicMock:
    instances = []
    for name in names:
        inst = MagicMock()
        inst.status = status
        inst.name = name
        instances.append(inst)
    scoped = MagicMock()
    scoped.instances = instances
    return scoped


def test_returns_running_vm_names() -> None:
    """Aggregated response with RUNNING + TERMINATED → only RUNNING names returned."""
    from deployment_service.vm.gcp_instance_lister import list_running_vm_names

    running_scoped = _make_scoped_list(["vm-a", "vm-b"])
    stopped_scoped = _make_scoped_list(["vm-c"], status="TERMINATED")

    mock_client = MagicMock()
    mock_client.aggregated_list.return_value = [
        ("zones/asia-northeast1-a", running_scoped),
        ("zones/asia-northeast1-b", stopped_scoped),
    ]

    with patch(
        "deployment_service.vm.gcp_instance_lister.compute_v1.InstancesClient",
        return_value=mock_client,
    ):
        result = list_running_vm_names("test-project")

    assert result == {"vm-a", "vm-b"}


def test_empty_zone_skipped() -> None:
    """Zone with no instances does not crash."""
    from deployment_service.vm.gcp_instance_lister import list_running_vm_names

    empty_scoped = MagicMock()
    empty_scoped.instances = None

    mock_client = MagicMock()
    mock_client.aggregated_list.return_value = [
        ("zones/asia-northeast1-a", empty_scoped),
    ]

    with patch(
        "deployment_service.vm.gcp_instance_lister.compute_v1.InstancesClient",
        return_value=mock_client,
    ):
        result = list_running_vm_names("test-project")

    assert result == set()


def test_api_error_returns_empty_set() -> None:
    """API exception → empty set returned + warning logged (never raises)."""
    from deployment_service.vm.gcp_instance_lister import list_running_vm_names

    mock_client = MagicMock()
    mock_client.aggregated_list.side_effect = RuntimeError("GCP quota exceeded")
    mock_logger = MagicMock()

    with (
        patch(
            "deployment_service.vm.gcp_instance_lister.compute_v1.InstancesClient",
            return_value=mock_client,
        ),
        patch(
            "deployment_service.vm.gcp_instance_lister.logger",
            mock_logger,
        ),
    ):
        result = list_running_vm_names("test-project")

    assert result == set()
    mock_logger.warning.assert_called_once()
