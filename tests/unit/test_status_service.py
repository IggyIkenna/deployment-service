"""Tests for deployment_service.services.status_service."""

from __future__ import annotations

from unittest.mock import patch

import pytest


class TestStatusServiceInit:
    def test_init_no_project(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        assert svc.project_id is None

    def test_init_with_project(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService(project_id="my-proj")
        assert svc.project_id == "my-proj"


class TestGetDeploymentStatus:
    def test_returns_status_dict(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        result = svc.get_deployment_status("dep-001")
        assert "deployment_id" in result
        assert result["deployment_id"] == "dep-001"

    def test_returns_error_dict_on_exception(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        with patch.object(svc, "_fetch_deployment_status", side_effect=OSError("fail")):
            result = svc.get_deployment_status("dep-001")
        assert result["status"] == "unknown"
        assert "fail" in str(result.get("error", ""))


class TestListDeployments:
    def test_returns_empty_list_by_default(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        result = svc.list_deployments()
        assert result == []

    def test_filters_by_status(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        fake = [{"status": "running"}, {"status": "completed"}]
        with patch.object(svc, "_fetch_deployments_list", return_value=fake):
            result = svc.list_deployments(status_filter="running")
        assert len(result) == 1

    def test_returns_empty_on_exception(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        with patch.object(svc, "_fetch_deployments_list", side_effect=OSError("fail")):
            result = svc.list_deployments()
        assert result == []


class TestCancelDeployment:
    def test_cancel_success(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        result = svc.cancel_deployment("dep-001")
        assert result is True

    def test_cancel_failure_logged(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        with patch.object(svc, "_cancel_deployment_backend", return_value=False):
            result = svc.cancel_deployment("dep-001")
        assert result is False

    def test_cancel_returns_false_on_exception(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        with patch.object(svc, "_cancel_deployment_backend", side_effect=OSError("fail")):
            result = svc.cancel_deployment("dep-001")
        assert result is False

    def test_cancel_already_completed_raises(self):
        import click

        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        with patch.object(svc, "get_deployment_status", return_value={"status": "completed"}):
            with pytest.raises(click.ClickException):
                svc.cancel_deployment("dep-001", force=False)

    def test_cancel_completed_with_force(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        with patch.object(svc, "get_deployment_status", return_value={"status": "completed"}):
            result = svc.cancel_deployment("dep-001", force=True)
        assert result is True


class TestCancelDeploymentBackend:
    def test_returns_true(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        result = svc._cancel_deployment_backend("dep-001", force=False)
        assert result is True


class TestDisplayHierarchicalStatus:
    def test_displays_basic_status(self, capsys):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        svc.display_hierarchical_status("dep-001")
        captured = capsys.readouterr()
        assert "dep-001" in captured.out

    def test_displays_with_details_empty_shards(self, capsys):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        with patch.object(
            svc,
            "get_deployment_status",
            return_value={
                "status": "running",
                "created_at": "t",
                "updated_at": "t",
                "summary": {"total_shards": 0, "completed_shards": 0, "failed_shards": 0, "running_shards": 0},
                "shards": [],
            },
        ):
            svc.display_hierarchical_status("dep-001", show_details=True)
        captured = capsys.readouterr()
        assert "dep-001" in captured.out

    def test_displays_with_details_and_shards(self, capsys):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        with patch.object(
            svc,
            "get_deployment_status",
            return_value={
                "status": "running",
                "created_at": "t",
                "updated_at": "t",
                "summary": {"total_shards": 1, "completed_shards": 0, "failed_shards": 0, "running_shards": 1},
                "shards": [{"shard_id": "shard-0", "status": "running"}],
            },
        ):
            svc.display_hierarchical_status("dep-001", show_details=True)
        captured = capsys.readouterr()
        assert "shard-0" in captured.out


class TestRefreshDeploymentStatus:
    def test_refresh_returns_status(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        result = svc.refresh_deployment_status("dep-001")
        assert "deployment_id" in result

    def test_refresh_falls_back_on_error(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        with patch.object(svc, "_force_refresh_status", side_effect=OSError("fail")):
            result = svc.refresh_deployment_status("dep-001")
        assert result["deployment_id"] == "dep-001"

    def test_force_refresh_status(self):
        from deployment_service.services.status_service import StatusService

        svc = StatusService()
        result = svc._force_refresh_status("dep-001")
        assert "deployment_id" in result
