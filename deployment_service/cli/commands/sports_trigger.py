"""Sports trigger scheduler CLI commands."""

import click

from deployment_service.sports_trigger_scheduler import SportsTriggerScheduler


@click.group()
@click.pass_context
def sports_trigger(ctx: click.Context):
    """Sports fixture-aware trigger scheduler."""


@sports_trigger.command("run")
@click.option(
    "--config",
    "config_path",
    default="configs/sports-trigger-tiers.yaml",
    help="Path to trigger tier config YAML",
)
@click.option(
    "--poll-interval",
    default=300,
    type=int,
    help="Seconds between evaluation cycles (default: 300)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Log trigger commands without executing them",
)
@click.pass_context
def sports_trigger_run(
    ctx: click.Context,
    config_path: str,
    poll_interval: int,
    dry_run: bool,
):
    """Run the sports trigger scheduler (blocking loop).

    Reads fixture calendar from GCS, evaluates trigger tiers, and fires
    standard batch CLI invocations at fixture-proximate times.

    Designed to run as a long-lived process on a VM or Cloud Run service.
    """
    scheduler = SportsTriggerScheduler(
        config_path=config_path,
        poll_interval_seconds=poll_interval,
        dry_run=dry_run,
    )
    scheduler.run()


@sports_trigger.command("evaluate")
@click.option(
    "--config",
    "config_path",
    default="configs/sports-trigger-tiers.yaml",
    help="Path to trigger tier config YAML",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=True,
    help="Dry-run mode (default: true for evaluate)",
)
@click.pass_context
def sports_trigger_evaluate(
    ctx: click.Context,
    config_path: str,
    dry_run: bool,
):
    """Run a single evaluation cycle (non-blocking).

    Checks what triggers are due right now and reports them.
    Useful for debugging and testing the trigger logic.
    """
    scheduler = SportsTriggerScheduler(
        config_path=config_path,
        dry_run=dry_run,
    )
    fired = scheduler.run_once()
    click.echo(f"Evaluation complete: {fired} triggers fired")
