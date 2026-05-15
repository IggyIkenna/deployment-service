"""Cluster, batch, live, and schedule commands for deployment-service CLI."""

import click

from ..handlers.cluster_handler import ClusterHandler

# =============================================================================
# CLUSTER GROUP
# =============================================================================


@click.group()
@click.pass_context
def cluster(ctx: click.Context):
    """Cluster lifecycle management (bootstrap, teardown, status)."""


@cluster.command("bootstrap")
@click.option(
    "--cluster",
    "cluster_name",
    required=True,
    help="Cluster name (cefi, tradfi, defi, sports, prediction, full)",
)
@click.option(
    "--mode",
    default="mock",
    type=click.Choice(["live", "mock"]),
    help="Startup mode (default: mock)",
)
@click.option(
    "--cloud",
    default="local",
    type=click.Choice(["local", "gcp", "aws"]),
    help="Cloud provider (default: local)",
)
@click.option(
    "--client-id",
    "client_id",
    default=None,
    help=(
        "Optional client identifier. Loads ClientSubscription from "
        "configs/client_subscriptions/<client_id>.yaml and materialises per-service "
        "isolation (ISOLATED services get dedicated per-client instances)."
    ),
)
@click.pass_context
def cluster_bootstrap(
    ctx: click.Context,
    cluster_name: str,
    mode: str,
    cloud: str,
    client_id: str | None,
):
    """Bootstrap all services in a cluster."""
    handler = ClusterHandler(ctx)
    handler.handle_cluster_bootstrap(cluster_name, mode=mode, cloud=cloud, client_id=client_id)


@cluster.command("teardown")
@click.option("--cluster", "cluster_name", required=True, help="Cluster to teardown")
@click.pass_context
def cluster_teardown(ctx: click.Context, cluster_name: str):
    """Teardown all services in a cluster (reverse dependency order)."""
    handler = ClusterHandler(ctx)
    handler.handle_cluster_teardown(cluster_name)


@cluster.command("status")
@click.option("--cluster", "cluster_name", required=True, help="Cluster to check")
@click.pass_context
def cluster_status(ctx: click.Context, cluster_name: str):
    """Check health status of all services in a cluster."""
    handler = ClusterHandler(ctx)
    handler.handle_cluster_status(cluster_name)


@cluster.command("list")
@click.pass_context
def cluster_list(ctx: click.Context):
    """List available cluster definitions."""
    handler = ClusterHandler(ctx)
    handler.handle_cluster_list()


# =============================================================================
# BATCH GROUP
# =============================================================================


@click.group()
@click.pass_context
def batch(ctx: click.Context):
    """Batch processing operations (thermal batch via T1Orchestrator)."""


@batch.command("run")
@click.option("--cluster", "cluster_name", required=True, help="Cluster to run batch for")
@click.option("--as-of-date", required=True, help="Date to process (YYYY-MM-DD)")
@click.option("--service", help="Optional single service to run (defaults to all in cluster)")
@click.pass_context
def batch_run(ctx: click.Context, cluster_name: str, as_of_date: str, service: str | None):
    """Run a thermal batch for a cluster.

    Examples:

        deploy-shards batch run --cluster cefi --as-of-date 2026-03-21

        deploy-shards batch run --cluster cefi --as-of-date 2026-03-21
          --service instruments-service
    """
    handler = ClusterHandler(ctx)
    handler.handle_batch_run(cluster_name, as_of_date=as_of_date, service=service)


# =============================================================================
# LIVE GROUP
# =============================================================================


@click.group()
@click.pass_context
def live(ctx: click.Context):
    """Live service management (start, stop, status)."""


@live.command("start")
@click.option("--service", required=True, help="Service to start in live mode")
@click.option("--cluster", "cluster_name", default=None, help="Cluster context (auto-detected if omitted)")
@click.pass_context
def live_start(ctx: click.Context, service: str, cluster_name: str | None):
    """Start a service in live mode."""
    handler = ClusterHandler(ctx)
    handler.handle_live_start(service, cluster_name=cluster_name)


@live.command("stop")
@click.option("--service", required=True, help="Service to stop")
@click.pass_context
def live_stop(ctx: click.Context, service: str):
    """Stop a live service."""
    handler = ClusterHandler(ctx)
    handler.handle_live_stop(service)


@live.command("status")
@click.option("--cluster", "cluster_name", default=None, help="Cluster to filter by (shows all if omitted)")
@click.pass_context
def live_status(ctx: click.Context, cluster_name: str | None):
    """Check status of live services."""
    handler = ClusterHandler(ctx)
    handler.handle_live_status(cluster_name=cluster_name)


# =============================================================================
# SCHEDULE GROUP
# =============================================================================


@click.group()
@click.pass_context
def schedule(ctx: click.Context):
    """Batch schedule management (create, list, disable)."""


@schedule.command("create")
@click.option("--cluster", "cluster_name", required=True, help="Cluster to schedule")
@click.option("--cron", required=True, help="Cron expression (e.g. '0 6 * * *')")
@click.pass_context
def schedule_create(ctx: click.Context, cluster_name: str, cron: str):
    """Create a batch schedule for a cluster.

    Examples:

        deploy-shards schedule create --cluster cefi --cron "0 6 * * *"
    """
    handler = ClusterHandler(ctx)
    handler.handle_schedule_create(cluster_name, cron)


@schedule.command("list")
@click.pass_context
def schedule_list(ctx: click.Context):
    """List all configured batch schedules."""
    handler = ClusterHandler(ctx)
    handler.handle_schedule_list()


@schedule.command("disable")
@click.option("--cluster", "cluster_name", required=True, help="Cluster whose schedule to disable")
@click.pass_context
def schedule_disable(ctx: click.Context, cluster_name: str):
    """Disable a cluster's batch schedule."""
    handler = ClusterHandler(ctx)
    handler.handle_schedule_disable(cluster_name)


# =============================================================================
# EXPORT — list of top-level groups to register with cli.add_command()
# =============================================================================

cluster_commands: list[click.BaseCommand] = [
    cluster,
    batch,
    live,
    schedule,
]
