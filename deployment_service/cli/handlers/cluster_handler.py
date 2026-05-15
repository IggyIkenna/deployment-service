"""Cluster, batch, live, and schedule command handlers."""

import logging
from typing import cast

import click

from ...cluster import (
    BatchResult,
    ClusterOrchestrator,
    ClusterStatus,
    ServiceHealthStatus,
)
from ...deployment_config import DeploymentConfig

logger = logging.getLogger(__name__)

_handler_config = DeploymentConfig()


class ClusterHandler:
    """Handler for cluster lifecycle CLI commands."""

    def __init__(self, ctx: click.Context):
        """Initialize cluster handler.

        Args:
            ctx: Click context with configuration
        """
        self.ctx = ctx
        ctx_obj = cast("dict[str, object]", ctx.obj)
        self.config_dir = cast("str | None", ctx_obj.get("config_dir"))
        self.cloud_provider = cast(str, ctx_obj.get("cloud", "gcp"))

        workspace_root = _handler_config.workspace_root
        if not workspace_root:
            # Fall back to parent of config_dir
            if self.config_dir:
                from pathlib import Path

                workspace_root = str(Path(self.config_dir).parent.parent)
            else:
                workspace_root = "."

        mock_mode = _handler_config.is_mock_mode()

        self._orchestrator = ClusterOrchestrator(
            config_dir=self.config_dir or "configs",
            workspace_root=workspace_root,
            cloud_provider=self.cloud_provider,
            mock_mode=mock_mode,
        )

    # =========================================================================
    # CLUSTER COMMANDS
    # =========================================================================

    def handle_cluster_bootstrap(
        self,
        cluster_name: str,
        mode: str = "mock",
        cloud: str = "local",
        client_id: str | None = None,
    ) -> None:
        """Handle cluster bootstrap command.

        Args:
            cluster_name: Cluster to bootstrap
            mode: Startup mode (live, mock)
            cloud: Cloud provider override
            client_id: Optional client identifier. When set, the orchestrator
                loads the matching ClientSubscription from
                configs/client_subscriptions/<client_id>.yaml and materialises
                per-service isolation (ISOLATED services get dedicated instances
                with CLIENT_ID env var).
        """
        try:
            client_suffix = f" client={client_id}" if client_id else ""
            click.echo(f"Bootstrapping cluster '{cluster_name}' (mode={mode}, cloud={cloud}){client_suffix}...")

            if cloud != self.cloud_provider:
                self._orchestrator.cloud_provider = cloud

            status = self._orchestrator.bootstrap(cluster_name, mode=mode, client_id=client_id)
            self._display_cluster_status(status)

            if status.all_healthy:
                click.echo(f"\nCluster '{cluster_name}' bootstrapped successfully.")
            else:
                failed = [s.service_name for s in status.services if s.health != ServiceHealthStatus.HEALTHY]
                click.echo(f"\nCluster '{cluster_name}' partially started. Failed: {', '.join(failed)}")

        except FileNotFoundError as e:
            raise click.ClickException(f"Cluster not found: {e}") from e
        except ValueError as e:
            raise click.ClickException(f"Invalid cluster config: {e}") from e
        except (OSError, RuntimeError) as e:
            logger.error("Bootstrap failed: %s", e)
            raise click.ClickException(f"Bootstrap failed: {e}") from e

    def handle_cluster_teardown(self, cluster_name: str) -> None:
        """Handle cluster teardown command.

        Args:
            cluster_name: Cluster to teardown
        """
        try:
            click.echo(f"Tearing down cluster '{cluster_name}'...")
            self._orchestrator.teardown(cluster_name)
            click.echo(f"Cluster '{cluster_name}' torn down successfully.")

        except FileNotFoundError as e:
            raise click.ClickException(f"Cluster not found: {e}") from e
        except (OSError, RuntimeError) as e:
            logger.error("Teardown failed: %s", e)
            raise click.ClickException(f"Teardown failed: {e}") from e

    def handle_cluster_status(self, cluster_name: str) -> None:
        """Handle cluster status command.

        Args:
            cluster_name: Cluster to check
        """
        try:
            status = self._orchestrator.status(cluster_name)
            self._display_cluster_status(status)

        except FileNotFoundError as e:
            raise click.ClickException(f"Cluster not found: {e}") from e
        except (OSError, RuntimeError) as e:
            logger.error("Status check failed: %s", e)
            raise click.ClickException(f"Status check failed: {e}") from e

    def handle_cluster_list(self) -> None:
        """Handle cluster list command."""
        try:
            clusters = self._orchestrator.list_clusters()

            if not clusters:
                click.echo("No clusters found")
                return

            click.echo("Available clusters:")
            for name in clusters:
                try:
                    config = self._orchestrator.load_cluster(name)
                    click.echo(f"  {name:<15} {config.asset_group:<8} {config.description}")
                except (ValueError, FileNotFoundError) as e:
                    logger.warning("Failed to load cluster %s: %s", name, e)
                    click.echo(f"  {name:<15} (invalid config)")

        except (OSError, RuntimeError) as e:
            logger.error("List clusters failed: %s", e)
            raise click.ClickException(f"Failed to list clusters: {e}") from e

    # =========================================================================
    # BATCH COMMANDS
    # =========================================================================

    def handle_batch_run(
        self,
        cluster_name: str,
        as_of_date: str,
        service: str | None = None,
    ) -> None:
        """Handle batch run command.

        Args:
            cluster_name: Cluster to run batch for
            as_of_date: Date to process (YYYY-MM-DD)
            service: Optional single service to run
        """
        try:
            services = [service] if service else None
            click.echo(f"Running batch for cluster '{cluster_name}' as-of {as_of_date}...")

            result = self._orchestrator.run_batch(
                cluster_name=cluster_name,
                as_of_date=as_of_date,
                services=services,
            )
            self._display_batch_result(result)

        except FileNotFoundError as e:
            raise click.ClickException(f"Cluster not found: {e}") from e
        except ValueError as e:
            raise click.ClickException(f"Invalid batch parameters: {e}") from e
        except (OSError, RuntimeError) as e:
            logger.error("Batch run failed: %s", e)
            raise click.ClickException(f"Batch run failed: {e}") from e

    # =========================================================================
    # LIVE COMMANDS
    # =========================================================================

    def handle_live_start(
        self,
        service_name: str,
        cluster_name: str | None = None,
    ) -> None:
        """Handle live start command.

        Args:
            service_name: Service to start
            cluster_name: Optional cluster context
        """
        try:
            if cluster_name:
                cluster = self._orchestrator.load_cluster(cluster_name)
                if service_name not in cluster.services:
                    raise click.ClickException(f"Service '{service_name}' not in cluster '{cluster_name}'")

            click.echo(f"Starting live service '{service_name}'...")

            # Bootstrap just the single service
            status = self._orchestrator.bootstrap(
                cluster_name=cluster_name or self._find_cluster_for_service(service_name),
                mode="live",
            )

            svc_status = status.get_service(service_name)
            if svc_status and svc_status.running:
                click.echo(f"Service '{service_name}' started (pid={svc_status.pid})")
            else:
                error = svc_status.error_message if svc_status else "unknown error"
                click.echo(f"Failed to start '{service_name}': {error}")

        except FileNotFoundError as e:
            raise click.ClickException(f"Cluster not found: {e}") from e
        except (OSError, RuntimeError) as e:
            logger.error("Live start failed: %s", e)
            raise click.ClickException(f"Live start failed: {e}") from e

    def handle_live_stop(self, service_name: str) -> None:
        """Handle live stop command.

        Args:
            service_name: Service to stop
        """
        try:
            click.echo(f"Stopping live service '{service_name}'...")
            self._orchestrator._stop_service(service_name)
            click.echo(f"Service '{service_name}' stopped.")

        except (OSError, RuntimeError) as e:
            logger.error("Live stop failed: %s", e)
            raise click.ClickException(f"Live stop failed: {e}") from e

    def handle_live_status(self, cluster_name: str | None = None) -> None:
        """Handle live status command.

        Args:
            cluster_name: Optional cluster to filter by
        """
        try:
            if cluster_name:
                status = self._orchestrator.status(cluster_name)
                self._display_cluster_status(status)
            else:
                # Show all clusters
                clusters = self._orchestrator.list_clusters()
                for name in clusters:
                    try:
                        status = self._orchestrator.status(name)
                        if status.running_count > 0:
                            click.echo(f"\n--- Cluster: {name} ---")
                            self._display_cluster_status(status)
                    except (FileNotFoundError, ValueError) as e:
                        logger.warning("Failed to check status for %s: %s", name, e)

        except (OSError, RuntimeError) as e:
            logger.error("Live status failed: %s", e)
            raise click.ClickException(f"Live status failed: {e}") from e

    # =========================================================================
    # SCHEDULE COMMANDS
    # =========================================================================

    def handle_schedule_create(self, cluster_name: str, cron: str) -> None:
        """Handle schedule create command.

        Args:
            cluster_name: Cluster to schedule
            cron: Cron expression
        """
        try:
            schedule = self._orchestrator.create_schedule(cluster_name, cron)
            click.echo(f"Schedule created for cluster '{cluster_name}':")
            click.echo(f"  Cron: {schedule.cron}")
            click.echo(f"  Timezone: {schedule.timezone}")
            click.echo(f"  Enabled: {schedule.enabled}")

        except FileNotFoundError as e:
            raise click.ClickException(f"Cluster not found: {e}") from e
        except (OSError, RuntimeError) as e:
            logger.error("Schedule create failed: %s", e)
            raise click.ClickException(f"Schedule create failed: {e}") from e

    def handle_schedule_list(self) -> None:
        """Handle schedule list command."""
        try:
            schedules = self._orchestrator.list_schedules()

            if not schedules:
                click.echo("No schedules configured")
                return

            click.echo(f"{'Cluster':<20} {'Cron':<20} {'Enabled':<10} {'Timezone':<10}")
            click.echo("-" * 60)

            for schedule in schedules:
                enabled_str = "yes" if schedule.enabled else "no"
                click.echo(f"{schedule.cluster_name:<20} {schedule.cron:<20} {enabled_str:<10} {schedule.timezone:<10}")

        except (OSError, RuntimeError) as e:
            logger.error("Schedule list failed: %s", e)
            raise click.ClickException(f"Schedule list failed: {e}") from e

    def handle_schedule_disable(self, cluster_name: str) -> None:
        """Handle schedule disable command.

        Args:
            cluster_name: Cluster whose schedule to disable
        """
        try:
            success = self._orchestrator.disable_schedule(cluster_name)

            if success:
                click.echo(f"Schedule disabled for cluster '{cluster_name}'")
            else:
                click.echo(f"No schedule found for cluster '{cluster_name}'")

        except (OSError, RuntimeError) as e:
            logger.error("Schedule disable failed: %s", e)
            raise click.ClickException(f"Schedule disable failed: {e}") from e

    # =========================================================================
    # DISPLAY HELPERS
    # =========================================================================

    def _display_cluster_status(self, status: ClusterStatus) -> None:
        """Display cluster status in table format.

        Args:
            status: ClusterStatus to display
        """
        click.echo(f"\nCluster: {status.cluster_name}")
        click.echo(f"Running: {status.running_count}/{status.total_count}")
        click.echo()

        click.echo(f"{'Service':<40} {'Status':<12} {'PID':<10} {'Health':<12}")
        click.echo("-" * 74)

        for svc in status.services:
            running_str = "running" if svc.running else "stopped"
            pid_str = str(svc.pid) if svc.pid else "-"
            health_str = svc.health.value

            service_display = svc.service_name
            if len(service_display) > 40:
                service_display = service_display[:37] + "..."

            click.echo(f"{service_display:<40} {running_str:<12} {pid_str:<10} {health_str:<12}")

            if svc.error_message:
                click.echo(f"  Error: {svc.error_message}")

    def _display_batch_result(self, result: BatchResult) -> None:
        """Display batch result in table format.

        Args:
            result: BatchResult to display
        """
        click.echo(f"\nBatch Result: {result.cluster_name} (as-of {result.as_of_date})")
        click.echo(f"Overall: {'PASS' if result.all_succeeded else 'FAIL'}")
        click.echo(f"Duration: {result.total_duration_seconds:.1f}s")
        click.echo()

        click.echo(f"{'Service':<40} {'Result':<10} {'Duration':<12} {'Jobs':<10}")
        click.echo("-" * 72)

        for svc_result in result.service_results:
            result_str = "OK" if svc_result.success else "FAIL"
            duration_str = f"{svc_result.duration_seconds:.1f}s"
            jobs_str = f"{svc_result.jobs_completed}/{svc_result.jobs_completed + svc_result.jobs_failed}"

            service_display = svc_result.service_name
            if len(service_display) > 40:
                service_display = service_display[:37] + "..."

            click.echo(f"{service_display:<40} {result_str:<10} {duration_str:<12} {jobs_str:<10}")

            if svc_result.error_message:
                click.echo(f"  Error: {svc_result.error_message}")

        if result.failed_services:
            click.echo(f"\nFailed services: {', '.join(result.failed_services)}")

    def _find_cluster_for_service(self, service_name: str) -> str:
        """Find the first cluster containing a service.

        Args:
            service_name: Service to find

        Returns:
            Cluster name

        Raises:
            click.ClickException: If service not found in any cluster
        """
        for cluster_name in self._orchestrator.list_clusters():
            try:
                cluster = self._orchestrator.load_cluster(cluster_name)
                if service_name in cluster.services:
                    return cluster_name
            except (FileNotFoundError, ValueError):
                continue

        raise click.ClickException(f"Service '{service_name}' not found in any cluster. Use --cluster to specify.")
