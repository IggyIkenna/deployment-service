"""
Unit tests for the top-level CLI entry point (deployment_service.cli.main).

The CLI is a Click group defined in cli/main.py that registers subcommands from
cli/commands/*.  This file tests the group-level options, get_config_dir(), and
confirms each subcommand is wired into the group.

Covers:
- cli group --help and top-level options (--verbose, --cloud, --project-id, --config-dir)
- get_config_dir() utility: found / not found paths
- Each subcommand responds to --help
- main() entry point calls cli()
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import click
import pytest
from click.testing import CliRunner

# ---------------------------------------------------------------------------
# Import the CLI module — patch DeploymentConfig to avoid live config at import
# ---------------------------------------------------------------------------
_MOD = "deployment_service.cli.main"

with patch(f"{_MOD}.DeploymentConfig"):
    from deployment_service.cli import main as _cli_module
    from deployment_service.cli.main import cli as _cli_group


def _invoke(args: list[str], input_str: str | None = None) -> click.testing.Result:
    """Invoke the top-level CLI with network patches active."""
    runner = CliRunner()
    with (
        patch(f"{_MOD}.setup_events"),
        patch(f"{_MOD}.setup_tracing"),
        patch(f"{_MOD}.GracefulShutdownHandler"),
    ):
        result = runner.invoke(_cli_group, args, catch_exceptions=False, input=input_str)
    return result


# ---------------------------------------------------------------------------
# Top-level cli group
# ---------------------------------------------------------------------------


class TestCliGroup:
    """Tests for the root CLI group defined in cli/main.py."""

    @pytest.mark.unit
    @pytest.mark.cli
    def test_help_exits_zero(self) -> None:
        result = _invoke(["--help"])
        assert result.exit_code == 0
        assert "Unified Trading Deployment" in result.output

    @pytest.mark.unit
    @pytest.mark.cli
    def test_help_lists_subcommands(self) -> None:
        result = _invoke(["--help"])
        assert result.exit_code == 0
        for cmd in ("calculate", "cluster", "info", "list-services", "venues"):
            assert cmd in result.output

    @pytest.mark.unit
    @pytest.mark.cli
    def test_cloud_option_gcp(self) -> None:
        result = _invoke(["--cloud", "gcp", "--help"])
        assert result.exit_code == 0

    @pytest.mark.unit
    @pytest.mark.cli
    def test_cloud_option_aws(self) -> None:
        result = _invoke(["--cloud", "aws", "--help"])
        assert result.exit_code == 0

    @pytest.mark.unit
    @pytest.mark.cli
    def test_invalid_cloud_option_rejected(self) -> None:
        runner = CliRunner()
        with (
            patch(f"{_MOD}.setup_events"),
            patch(f"{_MOD}.setup_tracing"),
            patch(f"{_MOD}.GracefulShutdownHandler"),
        ):
            result = runner.invoke(_cli_group, ["--cloud", "azure", "list-services"])
        assert result.exit_code != 0

    @pytest.mark.unit
    @pytest.mark.cli
    def test_verbose_flag_accepted(self) -> None:
        result = _invoke(["--verbose", "--help"])
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# get_config_dir utility
# ---------------------------------------------------------------------------


class TestGetConfigDir:
    """Tests for the get_config_dir() function in cli/main.py."""

    @pytest.mark.unit
    @pytest.mark.cli
    def test_returns_existing_configs_dir(self, tmp_path: Path) -> None:
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        fake_cli_py = tmp_path / "deployment_service" / "cli" / "main.py"
        with patch.object(_cli_module, "__file__", str(fake_cli_py)):
            result = _cli_module.get_config_dir()
        assert result == configs_dir

    @pytest.mark.unit
    @pytest.mark.cli
    def test_raises_when_not_found(self, tmp_path: Path) -> None:
        with (
            patch.object(_cli_module, "__file__", str(tmp_path / "main.py")),
            patch("pathlib.Path.cwd", return_value=tmp_path),
            pytest.raises(click.ClickException, match="Could not find configs"),
        ):
            _cli_module.get_config_dir()


# ---------------------------------------------------------------------------
# Subcommand presence tests
# ---------------------------------------------------------------------------


class TestSubcommandPresence:
    """Verify that expected subcommands are registered in the group."""

    @pytest.mark.unit
    @pytest.mark.cli
    @pytest.mark.parametrize(
        "cmd",
        [
            "batch",
            "calculate",
            "cluster",
            "info",
            "list-services",
            "live",
            "schedule",
            "venues",
        ],
    )
    def test_subcommand_help(self, cmd: str) -> None:
        result = _invoke([cmd, "--help"])
        assert result.exit_code == 0, f"{cmd} --help failed: {result.output}"


# ---------------------------------------------------------------------------
# main() entry point
# ---------------------------------------------------------------------------


class TestMainEntryPoint:
    """Tests for the main() function in cli/main.py."""

    @pytest.mark.unit
    @pytest.mark.cli
    def test_main_invokes_cli(self) -> None:
        with (
            patch.object(_cli_module, "setup_events"),
            patch.object(_cli_module, "setup_tracing"),
            patch.object(_cli_module, "GracefulShutdownHandler"),
            patch.object(_cli_module, "cli") as mock_cli,
        ):
            _cli_module.main()
        mock_cli.assert_called_once()
