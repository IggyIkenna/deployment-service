"""Smoke tests for deployment-service."""

import pytest
from click.testing import CliRunner

from deployment_service.__main__ import cli


def test_import_deployment_service() -> None:
    """Verify deployment_service package imports."""
    import deployment_service

    assert deployment_service.__version__ == "0.1.0"


def test_import_submodules() -> None:
    """Verify submodules are importable."""
    import deployment_service.orchestrator
    import deployment_service.catalog
    import deployment_service.config_loader
    import deployment_service.deployment

    assert deployment_service.orchestrator is not None
    assert deployment_service.catalog is not None
    assert deployment_service.config_loader is not None
    assert deployment_service.deployment is not None


def test_cli_status() -> None:
    """Verify CLI status command runs."""
    runner = CliRunner()
    result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0
    assert "status stub" in result.output
