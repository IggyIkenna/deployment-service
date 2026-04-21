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
@click.option(
    "--one-shot",
    is_flag=True,
    default=False,
    help=(
        "Run a single evaluation cycle and exit (Cloud Run Job shape). "
        "When set, the blocking poll loop is skipped — the process exits "
        "after one iteration of run_once() so Cloud Scheduler can drive "
        "the cadence externally."
    ),
)
@click.pass_context
def sports_trigger_run(
    ctx: click.Context,
    config_path: str,
    poll_interval: int,
    dry_run: bool,
    one_shot: bool,
):
    """Run the sports trigger scheduler.

    Reads fixture calendar from GCS, evaluates trigger tiers, and fires
    standard batch CLI invocations at fixture-proximate times.

    Two operational shapes:
      * default — blocking poll loop (long-lived VM / Cloud Run service).
      * ``--one-shot`` — single evaluation cycle, exit 0 (Cloud Run Job
        driven by Cloud Scheduler every 5 min). State (last_run per tier)
        persists to GCS between invocations, so cadence is preserved.
    """
    scheduler = SportsTriggerScheduler(
        config_path=config_path,
        poll_interval_seconds=poll_interval,
        dry_run=dry_run,
    )
    if one_shot:
        fired = scheduler.run_once()
        click.echo(f"One-shot run complete: {fired} triggers fired")
        return
    scheduler.run()


@sports_trigger.command("evaluate")
@click.option(
    "--config",
    "config_path",
    default="configs/sports-trigger-tiers.yaml",
    help="Path to trigger tier config YAML",
)
@click.option(
    "--dry-run/--execute",
    default=True,
    help=(
        "Dry-run (default) logs the triggers that would fire without "
        "dispatching them. ``--execute`` runs the cycle for real — "
        "useful when manually kicking off a catch-up dispatch."
    ),
)
@click.pass_context
def sports_trigger_evaluate(
    ctx: click.Context,
    config_path: str,
    dry_run: bool,
):
    """Run a single evaluation cycle (non-blocking).

    Checks what triggers are due right now and reports them. Defaults to
    dry-run so operators can inspect pending triggers; pass ``--execute``
    to actually fire.
    """
    scheduler = SportsTriggerScheduler(
        config_path=config_path,
        dry_run=dry_run,
    )
    fired = scheduler.run_once()
    click.echo(f"Evaluation complete: {fired} triggers fired")
