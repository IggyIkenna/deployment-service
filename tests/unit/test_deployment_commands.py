"""Tests for deployment_service.deployment_commands module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_module_constants_are_strings() -> None:
    """Verify module-level constants are populated from DeploymentConfig at import."""
    import deployment_service.deployment_commands as dc

    assert isinstance(dc.DEFAULT_PROJECT_ID, str)
    assert isinstance(dc.DEFAULT_REGION, str)
    assert isinstance(dc.DEFAULT_STATE_BUCKET, str)
    assert isinstance(dc.DEFAULT_MAX_CONCURRENT, int)
    assert isinstance(dc.MAX_CONCURRENT_HARD_LIMIT, int)


def test_get_default_config_gcp() -> None:
    """GCP branch returns project_id, region, state_bucket, service_account."""
    mock_cfg = MagicMock()
    mock_cfg.gcp_project_id = "test-project"
    mock_cfg.gcs_region = "us-central1"
    mock_cfg.service_account_email = "sa@test-project.iam.gserviceaccount.com"
    mock_cfg.effective_state_bucket = "deployment-state-test-project"

    with patch(
        "deployment_service.deployment_commands.DeploymentConfig",
        return_value=mock_cfg,
    ):
        from deployment_service.deployment_commands import get_default_config

        result = get_default_config("gcp")

    assert result["project_id"] == "test-project"
    assert result["region"] == "us-central1"
    assert result["state_bucket"] == "deployment-state-test-project"
    assert "sa@" in str(result["service_account"])


def test_get_default_config_gcp_no_service_account() -> None:
    """GCP branch generates a default service account if email is None."""
    mock_cfg = MagicMock()
    mock_cfg.gcp_project_id = "test-project"
    mock_cfg.gcs_region = "us-central1"
    mock_cfg.service_account_email = None
    mock_cfg.effective_state_bucket = "deployment-state-test-project"

    with patch(
        "deployment_service.deployment_commands.DeploymentConfig",
        return_value=mock_cfg,
    ):
        from deployment_service.deployment_commands import get_default_config

        result = get_default_config("gcp")

    assert str(result["service_account"]).startswith("instruments-service-cloud-run@")


def test_get_default_config_aws() -> None:
    """AWS branch returns account_id as project_id and correct bucket pattern."""
    mock_cfg = MagicMock()
    mock_cfg.aws_account_id = "123456789012"
    mock_cfg.aws_region = "us-east-1"

    with patch(
        "deployment_service.deployment_commands.DeploymentConfig",
        return_value=mock_cfg,
    ):
        from deployment_service.deployment_commands import get_default_config

        result = get_default_config("aws")

    assert result["project_id"] == "123456789012"
    assert result["region"] == "us-east-1"
    assert result["state_bucket"] == "deployment-orchestration-123456789012"
    assert result["service_account"] == ""
