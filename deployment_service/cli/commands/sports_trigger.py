"""Sports trigger scheduler CLI commands."""

import click

from deployment_service.deployment_config import DeploymentConfig
from deployment_service.sports_trigger_scheduler import SportsTriggerScheduler

# Root cause of the 2026-06-24 -> 2026-07-08 silent zero-dispatch incident
# (unified-trading-pm/plans/active/issues/
# sports_trigger_scheduler_cloud_dispatch_broken_2026_07_08.md): neither
# ``run`` nor ``evaluate`` ever passed ``backend=``/``workspace_root=``/
# ``cloud_run_config=`` to ``SportsTriggerScheduler(...)``, so every
# invocation silently fell back to the class default ``backend="local"``
# with an empty ``workspace_root`` — which subprocess-execs
# ``python -m instruments_service ...`` directly in whatever container is
# running the scheduler. The Cloud Run Job image (Dockerfile "sports-scheduler"
# stage, ``FROM api AS sports-scheduler``) ships ONLY deployment-service, so
# that subprocess call always failed with a caught FileNotFoundError,
# ``_dispatch_local`` returned False, 0 triggers ever dispatched, and — because
# ``PeriodicTierDispatcher`` only persists ``last_run[tier]`` when
# ``dispatched > 0`` — the state file went stale for 11-14 days while the
# Cloud Run Job itself kept exiting 0 (no failure signal anywhere).
#
# Fix: expose the backend explicitly. The Cloud Run Job (terraform:
# terraform/gcp/sports_scheduler_cron.tf) now passes ``--backend cloud``
# with the real, ALREADY-DEPLOYED per-service Cloud Run Jobs the T+1 batch
# reconciliation cron already dispatches into (see
# ``configs/sports-trigger-tiers.yaml`` ``cloud_run_job_name`` fields —
# ``uts-prod-instruments-service-t1-recon`` etc.), rather than a set of
# scheduler-specific jobs that were never provisioned.


def _cloud_run_config(project: str, region: str, service_account: str) -> dict[str, str]:
    """Build the ``cloud_run_config`` kwarg for ``SportsTriggerScheduler``.

    Falls back to ``DeploymentConfig`` for any flag left unset, so
    ``--backend cloud`` works with zero extra flags inside a container that
    already carries GCP_PROJECT_ID / GCS_REGION / SERVICE_ACCOUNT env vars.
    Constructed lazily (not at module import time) so env-var resolution
    happens per-invocation.
    """
    deployment_config = DeploymentConfig()
    return {
        "project_id": project or (deployment_config.project_id or ""),
        "region": region or deployment_config.gcs_region,
        "service_account_email": service_account or deployment_config.service_account_email,
    }


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
@click.option(
    "--backend",
    type=click.Choice(["local", "cloud"]),
    default="local",
    help=(
        "Dispatch backend for fired triggers. 'local' subprocess-execs the "
        "target service CLI directly — only viable if --workspace-root points "
        "at a directory with that service's checkout (e.g. a VM tarball "
        "workspace); it CANNOT work inside the deployment-service-only "
        "Cloud Run Job image. 'cloud' fires the configured "
        "cloud_run_job_name via the Cloud Run Jobs API — the real, "
        "already-deployed dispatch mechanism for the Cloud Run Job cron."
    ),
)
@click.option(
    "--workspace-root",
    default="",
    help="Root dir containing per-service checkouts, for --backend local (e.g. a VM tarball workspace).",
)
@click.option(
    "--cloud-run-project",
    default="",
    help="GCP project ID for --backend cloud (defaults to DeploymentConfig().project_id).",
)
@click.option(
    "--cloud-run-region",
    default="",
    help="GCP region for --backend cloud (defaults to DeploymentConfig().gcs_region).",
)
@click.option(
    "--cloud-run-service-account",
    default="",
    help="Service account email for --backend cloud (defaults to DeploymentConfig().service_account_email).",
)
@click.pass_context
def sports_trigger_run(
    ctx: click.Context,
    config_path: str,
    poll_interval: int,
    dry_run: bool,
    one_shot: bool,
    backend: str,
    workspace_root: str,
    cloud_run_project: str,
    cloud_run_region: str,
    cloud_run_service_account: str,
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
        backend=backend,
        workspace_root=workspace_root,
        cloud_run_config=(
            _cloud_run_config(cloud_run_project, cloud_run_region, cloud_run_service_account)
            if backend == "cloud"
            else None
        ),
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
@click.option(
    "--backend",
    type=click.Choice(["local", "cloud"]),
    default="local",
    help="Dispatch backend for --execute. See `sports-trigger run --help` for details.",
)
@click.option(
    "--workspace-root",
    default="",
    help="Root dir containing per-service checkouts, for --backend local.",
)
@click.option(
    "--cloud-run-project",
    default="",
    help="GCP project ID for --backend cloud (defaults to DeploymentConfig().project_id).",
)
@click.option(
    "--cloud-run-region",
    default="",
    help="GCP region for --backend cloud (defaults to DeploymentConfig().gcs_region).",
)
@click.option(
    "--cloud-run-service-account",
    default="",
    help="Service account email for --backend cloud (defaults to DeploymentConfig().service_account_email).",
)
@click.pass_context
def sports_trigger_evaluate(
    ctx: click.Context,
    config_path: str,
    dry_run: bool,
    backend: str,
    workspace_root: str,
    cloud_run_project: str,
    cloud_run_region: str,
    cloud_run_service_account: str,
):
    """Run a single evaluation cycle (non-blocking).

    Checks what triggers are due right now and reports them. Defaults to
    dry-run so operators can inspect pending triggers; pass ``--execute``
    to actually fire.
    """
    scheduler = SportsTriggerScheduler(
        config_path=config_path,
        dry_run=dry_run,
        backend=backend,
        workspace_root=workspace_root,
        cloud_run_config=(
            _cloud_run_config(cloud_run_project, cloud_run_region, cloud_run_service_account)
            if backend == "cloud"
            else None
        ),
    )
    fired = scheduler.run_once()
    click.echo(f"Evaluation complete: {fired} triggers fired")
