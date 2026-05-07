"""Unit tests for the ``sports-trigger`` CLI sub-commands.

Covers:
  * ``run --one-shot`` — single evaluation cycle, exits 0, never calls the
    blocking ``scheduler.run()`` poll loop (Cloud Run Job shape).
  * ``run`` (default) — blocking poll loop invoked (VM / Cloud Run service shape).
  * ``evaluate --dry-run`` vs ``evaluate --execute`` — flag flips the dry-run
    boolean on SportsTriggerScheduler, so the ``evaluate`` sub-command can
    actually fire (not just report) when operators want a catch-up dispatch.

No network / GCS / subprocess — SportsTriggerScheduler is patched at its
module-import site so the CLI layer is exercised in isolation. Follows the
shard-level failure-isolation contract (tests never touch cloud).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from deployment_service.cli.commands.sports_trigger import sports_trigger

_TARGET = "deployment_service.cli.commands.sports_trigger.SportsTriggerScheduler"


def _invoke(args: list[str], scheduler_mock: MagicMock) -> object:
    """Invoke sports_trigger group with SportsTriggerScheduler patched."""
    runner = CliRunner()
    with patch(_TARGET, return_value=scheduler_mock):
        return runner.invoke(sports_trigger, args, catch_exceptions=False)


# ---------------------------------------------------------------------------
# run --one-shot
# ---------------------------------------------------------------------------


class TestRunOneShot:
    """``sports-trigger run --one-shot`` — Cloud Run Job shape."""

    @pytest.mark.unit
    def test_one_shot_calls_run_once_not_run(self) -> None:
        """--one-shot must call scheduler.run_once() and NEVER scheduler.run()."""
        scheduler = MagicMock()
        scheduler.run_once.return_value = 3

        result = _invoke(["run", "--one-shot"], scheduler)

        assert result.exit_code == 0, result.output
        scheduler.run_once.assert_called_once_with()
        scheduler.run.assert_not_called()
        assert "One-shot run complete: 3 triggers fired" in result.output

    @pytest.mark.unit
    def test_one_shot_respects_dry_run_flag(self) -> None:
        """--one-shot composes with --dry-run (dispatched to scheduler ctor)."""
        scheduler = MagicMock()
        scheduler.run_once.return_value = 0

        with patch(_TARGET, return_value=scheduler) as ctor:
            runner = CliRunner()
            result = runner.invoke(
                sports_trigger,
                ["run", "--one-shot", "--dry-run"],
                catch_exceptions=False,
            )

        assert result.exit_code == 0, result.output
        kwargs = ctor.call_args.kwargs
        assert kwargs["dry_run"] is True

    @pytest.mark.unit
    def test_one_shot_custom_config_path_propagates(self) -> None:
        """--config forwarded to scheduler ctor."""
        scheduler = MagicMock()
        scheduler.run_once.return_value = 0

        with patch(_TARGET, return_value=scheduler) as ctor:
            runner = CliRunner()
            result = runner.invoke(
                sports_trigger,
                ["run", "--one-shot", "--config", "configs/custom.yaml"],
                catch_exceptions=False,
            )

        assert result.exit_code == 0, result.output
        kwargs = ctor.call_args.kwargs
        assert kwargs["config_path"] == "configs/custom.yaml"


# ---------------------------------------------------------------------------
# run (default — blocking loop)
# ---------------------------------------------------------------------------


class TestRunBlocking:
    """``sports-trigger run`` without --one-shot — long-lived loop shape."""

    @pytest.mark.unit
    def test_default_run_calls_blocking_loop(self) -> None:
        """Without --one-shot, scheduler.run() (blocking) must be invoked."""
        scheduler = MagicMock()

        result = _invoke(["run"], scheduler)

        assert result.exit_code == 0, result.output
        scheduler.run.assert_called_once_with()
        scheduler.run_once.assert_not_called()

    @pytest.mark.unit
    def test_poll_interval_forwarded_to_scheduler(self) -> None:
        """--poll-interval reaches SportsTriggerScheduler ctor kwarg."""
        scheduler = MagicMock()

        with patch(_TARGET, return_value=scheduler) as ctor:
            runner = CliRunner()
            result = runner.invoke(
                sports_trigger,
                ["run", "--poll-interval", "120"],
                catch_exceptions=False,
            )

        assert result.exit_code == 0, result.output
        kwargs = ctor.call_args.kwargs
        assert kwargs["poll_interval_seconds"] == 120


# ---------------------------------------------------------------------------
# evaluate --dry-run / --execute
# ---------------------------------------------------------------------------


class TestEvaluateDryRunExecute:
    """``sports-trigger evaluate`` — --dry-run / --execute flag pair."""

    @pytest.mark.unit
    def test_evaluate_defaults_to_dry_run(self) -> None:
        """No flag = dry-run True (safe default)."""
        scheduler = MagicMock()
        scheduler.run_once.return_value = 0

        with patch(_TARGET, return_value=scheduler) as ctor:
            runner = CliRunner()
            result = runner.invoke(sports_trigger, ["evaluate"], catch_exceptions=False)

        assert result.exit_code == 0, result.output
        kwargs = ctor.call_args.kwargs
        assert kwargs["dry_run"] is True

    @pytest.mark.unit
    def test_evaluate_execute_flips_dry_run_false(self) -> None:
        """--execute overrides default and flips dry_run=False (real dispatch)."""
        scheduler = MagicMock()
        scheduler.run_once.return_value = 2

        with patch(_TARGET, return_value=scheduler) as ctor:
            runner = CliRunner()
            result = runner.invoke(
                sports_trigger,
                ["evaluate", "--execute"],
                catch_exceptions=False,
            )

        assert result.exit_code == 0, result.output
        kwargs = ctor.call_args.kwargs
        assert kwargs["dry_run"] is False
        scheduler.run_once.assert_called_once_with()

    @pytest.mark.unit
    def test_evaluate_explicit_dry_run_flag_still_dry_run(self) -> None:
        """--dry-run keeps dry_run=True explicitly."""
        scheduler = MagicMock()
        scheduler.run_once.return_value = 0

        with patch(_TARGET, return_value=scheduler) as ctor:
            runner = CliRunner()
            result = runner.invoke(
                sports_trigger,
                ["evaluate", "--dry-run"],
                catch_exceptions=False,
            )

        assert result.exit_code == 0, result.output
        kwargs = ctor.call_args.kwargs
        assert kwargs["dry_run"] is True
