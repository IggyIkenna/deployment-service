# SCHEMA_PROVENANCE_EXEMPT: Service-internal types — not cross-repo contracts. See QUALITY_GATE_BYPASS_AUDIT.md §2.17.
"""Cluster orchestrator — manages deployment groups (cefi, tradfi, defi, sports, prediction, full).

Loads cluster definitions from configs/clusters/*.yaml.
Provides bootstrap, teardown, and status operations.
Works at T2 (local) and T3-T6 (cloud) by delegating to compute backends.
"""

import logging
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import cast

import yaml
from unified_trading_library import log_event, setup_events

from .dependencies import DependencyGraph
from .deployment_config import DeploymentConfig
from .orchestrator import T1Orchestrator

logger = logging.getLogger(__name__)


class ServiceHealthStatus(Enum):
    """Health status of a service in a cluster."""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    STARTING = "starting"
    STOPPED = "stopped"
    UNKNOWN = "unknown"


class ClusterOperation(Enum):
    """Operations that can be performed on a cluster."""

    BOOTSTRAP = "bootstrap"
    TEARDOWN = "teardown"
    STATUS = "status"
    BATCH = "batch"


@dataclass
class ClusterConfig:
    """Parsed cluster YAML configuration."""

    name: str
    description: str
    category: str
    services: list[str]

    # Batch scheduling
    schedule_cron: str = ""
    schedule_as_of: str = "yesterday"
    schedule_timezone: str = "UTC"

    # Live mode settings
    health_check_interval_s: int = 30
    readiness_timeout_s: int = 120

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "category": self.category,
            "services": self.services,
            "schedule": {
                "cron": self.schedule_cron,
                "as_of": self.schedule_as_of,
                "timezone": self.schedule_timezone,
            },
            "live": {
                "health_check_interval_s": self.health_check_interval_s,
                "readiness_timeout_s": self.readiness_timeout_s,
            },
        }


@dataclass
class ServiceStatus:
    """Status of an individual service in a cluster."""

    service_name: str
    running: bool = False
    pid: int | None = None
    job_id: str = ""
    port: int = 0
    health: ServiceHealthStatus = ServiceHealthStatus.STOPPED
    started_at: datetime | None = None
    error_message: str = ""

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "service_name": self.service_name,
            "running": self.running,
            "pid": self.pid,
            "job_id": self.job_id,
            "port": self.port,
            "health": self.health.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "error_message": self.error_message,
        }


@dataclass
class ClusterStatus:
    """Aggregate status of all services in a cluster."""

    cluster_name: str
    operation: str = ""
    services: list[ServiceStatus] = field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def all_healthy(self) -> bool:
        """Check if all services are healthy."""
        return all(s.health == ServiceHealthStatus.HEALTHY for s in self.services)

    @property
    def running_count(self) -> int:
        """Count of running services."""
        return sum(1 for s in self.services if s.running)

    @property
    def total_count(self) -> int:
        """Total number of services."""
        return len(self.services)

    def get_service(self, name: str) -> ServiceStatus | None:
        """Get status for a specific service."""
        for svc in self.services:
            if svc.service_name == name:
                return svc
        return None

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "cluster_name": self.cluster_name,
            "operation": self.operation,
            "all_healthy": self.all_healthy,
            "running_count": self.running_count,
            "total_count": self.total_count,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "services": [s.to_dict() for s in self.services],
        }


@dataclass
class BatchServiceResult:
    """Result of a batch run for a single service."""

    service_name: str
    success: bool
    duration_seconds: float = 0.0
    error_message: str = ""
    jobs_completed: int = 0
    jobs_failed: int = 0

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "service_name": self.service_name,
            "success": self.success,
            "duration_seconds": self.duration_seconds,
            "error_message": self.error_message,
            "jobs_completed": self.jobs_completed,
            "jobs_failed": self.jobs_failed,
        }


@dataclass
class BatchResult:
    """Thermal batch outcome for a cluster."""

    cluster_name: str
    as_of_date: str
    service_results: list[BatchServiceResult] = field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def total_duration_seconds(self) -> float:
        """Total batch duration."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return sum(r.duration_seconds for r in self.service_results)

    @property
    def all_succeeded(self) -> bool:
        """Check if all services succeeded."""
        return all(r.success for r in self.service_results)

    @property
    def failed_services(self) -> list[str]:
        """List of services that failed."""
        return [r.service_name for r in self.service_results if not r.success]

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "cluster_name": self.cluster_name,
            "as_of_date": self.as_of_date,
            "all_succeeded": self.all_succeeded,
            "total_duration_seconds": self.total_duration_seconds,
            "failed_services": self.failed_services,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "service_results": [r.to_dict() for r in self.service_results],
        }


@dataclass
class ScheduleConfig:
    """Schedule configuration for a cluster."""

    cluster_name: str
    cron: str
    enabled: bool = True
    timezone: str = "UTC"
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "cluster_name": self.cluster_name,
            "cron": self.cron,
            "enabled": self.enabled,
            "timezone": self.timezone,
            "created_at": self.created_at.isoformat(),
        }


class ClusterOrchestrator:
    """Orchestrates cluster lifecycle — bootstrap, teardown, status, and batch operations.

    Loads cluster definitions from configs/clusters/*.yaml and coordinates
    service startup/shutdown respecting the dependency graph.
    """

    _events_initialized = False

    def __init__(
        self,
        config_dir: str,
        workspace_root: str,
        cloud_provider: str = "local",
        mock_mode: bool = True,
    ):
        """Initialize cluster orchestrator.

        Args:
            config_dir: Path to configs directory (contains clusters/ subdirectory)
            workspace_root: Path to workspace root (for locating service repos)
            cloud_provider: Cloud provider (local, gcp, aws)
            mock_mode: Whether to run in mock mode
        """
        if not self.__class__._events_initialized:
            setup_events(service_name="deployment-service", mode="batch", sink=None)
            self.__class__._events_initialized = True

        self.config_dir = config_dir
        self.workspace_root = workspace_root
        self.cloud_provider = cloud_provider
        self.mock_mode = mock_mode
        self._config = DeploymentConfig()
        self._clusters_dir = Path(config_dir) / "clusters"
        self._graph = DependencyGraph(config_dir)

        # Track running processes for teardown
        self._running_processes: dict[str, subprocess.Popen[bytes]] = {}

        # Track schedules in memory (would be persisted in production)
        self._schedules: dict[str, ScheduleConfig] = {}

    def load_cluster(self, name: str) -> ClusterConfig:
        """Load a cluster configuration from YAML.

        Args:
            name: Cluster name (e.g. 'cefi', 'tradfi', 'full')

        Returns:
            Parsed ClusterConfig

        Raises:
            FileNotFoundError: If cluster YAML does not exist
            ValueError: If cluster YAML is malformed
        """
        cluster_path = self._clusters_dir / f"{name}.yaml"
        if not cluster_path.exists():
            msg = f"Cluster config not found: {cluster_path}"
            raise FileNotFoundError(msg)

        with open(cluster_path) as f:
            raw = yaml.safe_load(f)

        if not isinstance(raw, dict):
            msg = f"Invalid cluster config format in {cluster_path}: expected mapping"
            raise ValueError(msg)

        raw_data = cast(dict[str, object], raw)
        services_raw = raw_data.get("services")
        if not isinstance(services_raw, list):
            msg = f"Cluster {name} has no 'services' list"
            raise ValueError(msg)

        schedule_raw = cast(dict[str, object], raw_data.get("schedule") or {})
        live_raw = cast(dict[str, object], raw_data.get("live") or {})

        return ClusterConfig(
            name=str(raw_data.get("cluster", name)),
            description=str(raw_data.get("description", "")),
            category=str(raw_data.get("category", "")),
            services=[str(s) for s in services_raw],
            schedule_cron=str(schedule_raw.get("cron", "")),
            schedule_as_of=str(schedule_raw.get("as_of", "yesterday")),
            schedule_timezone=str(schedule_raw.get("timezone", "UTC")),
            health_check_interval_s=int(live_raw.get("health_check_interval_s", 30)),
            readiness_timeout_s=int(live_raw.get("readiness_timeout_s", 120)),
        )

    def list_clusters(self) -> list[str]:
        """List available cluster names from configs/clusters/.

        Returns:
            Sorted list of cluster names (without .yaml extension)
        """
        if not self._clusters_dir.exists():
            return []

        return sorted(p.stem for p in self._clusters_dir.glob("*.yaml") if p.is_file())

    def bootstrap(
        self,
        cluster_name: str,
        mode: str = "live",
        versions: dict[str, str] | None = None,
    ) -> ClusterStatus:
        """Start all services in a cluster, respecting dependency order.

        For local: launches processes via subprocess.
        For cloud: delegates to Cloud Run / AWS backend (placeholder).

        Args:
            cluster_name: Name of the cluster to bootstrap
            mode: Startup mode ('live' or 'mock')
            versions: Optional version overrides per service

        Returns:
            ClusterStatus with per-service status
        """
        cluster = self.load_cluster(cluster_name)
        status = ClusterStatus(
            cluster_name=cluster_name,
            operation=ClusterOperation.BOOTSTRAP.value,
            started_at=datetime.now(UTC),
        )

        log_event(
            "cluster.bootstrap.started",
            cluster_name=cluster_name,
            mode=mode,
            service_count=len(cluster.services),
            cloud_provider=self.cloud_provider,
        )

        # Get dependency-ordered services
        ordered_services = self._get_ordered_services(cluster.services)

        for service_name in ordered_services:
            svc_status = self._start_service(
                service_name=service_name,
                cluster=cluster,
                mode=mode,
                version=versions.get(service_name) if versions else None,
            )
            status.services.append(svc_status)

            if not svc_status.running:
                logger.warning(
                    "Service %s failed to start, continuing with remaining services",
                    service_name,
                )

        status.completed_at = datetime.now(UTC)

        log_event(
            "cluster.bootstrap.completed",
            cluster_name=cluster_name,
            running_count=status.running_count,
            total_count=status.total_count,
            all_healthy=status.all_healthy,
        )

        return status

    def teardown(self, cluster_name: str) -> None:
        """Stop all services in a cluster (reverse dependency order).

        Args:
            cluster_name: Name of the cluster to teardown
        """
        cluster = self.load_cluster(cluster_name)

        log_event(
            "cluster.teardown.started",
            cluster_name=cluster_name,
            service_count=len(cluster.services),
        )

        # Reverse dependency order for teardown
        ordered_services = self._get_ordered_services(cluster.services)
        ordered_services.reverse()

        for service_name in ordered_services:
            self._stop_service(service_name)

        log_event(
            "cluster.teardown.completed",
            cluster_name=cluster_name,
        )

    def status(self, cluster_name: str) -> ClusterStatus:
        """Check health of all services in a cluster.

        Args:
            cluster_name: Name of the cluster to check

        Returns:
            ClusterStatus with per-service health
        """
        cluster = self.load_cluster(cluster_name)
        cluster_status = ClusterStatus(
            cluster_name=cluster_name,
            operation=ClusterOperation.STATUS.value,
        )

        for service_name in cluster.services:
            svc_status = self._check_service_health(service_name)
            cluster_status.services.append(svc_status)

        return cluster_status

    def run_batch(
        self,
        cluster_name: str,
        as_of_date: str,
        services: list[str] | None = None,
    ) -> BatchResult:
        """Run thermal batch using T1Orchestrator with the cluster's service list.

        Args:
            cluster_name: Name of the cluster
            as_of_date: Date to process (YYYY-MM-DD)
            services: Optional subset of services to run (defaults to all in cluster)

        Returns:
            BatchResult with per-service outcomes
        """
        cluster = self.load_cluster(cluster_name)
        batch_services = services or cluster.services

        result = BatchResult(
            cluster_name=cluster_name,
            as_of_date=as_of_date,
            started_at=datetime.now(UTC),
        )

        log_event(
            "cluster.batch.started",
            cluster_name=cluster_name,
            as_of_date=as_of_date,
            service_count=len(batch_services),
        )

        # Use T1Orchestrator for dependency-aware execution
        orchestrator = T1Orchestrator(
            config_dir=self.config_dir,
            project_id=self._config.gcp_project_id or None,
        )

        plan = orchestrator.create_daily_plan(
            target_date=as_of_date,
            category=cluster.category,
            services=batch_services,
        )

        # Execute the plan tier by tier
        tiers = orchestrator.get_execution_tiers(plan)
        for tier in tiers:
            for service_name in tier:
                svc_start = time.monotonic()
                svc_result = self._execute_batch_service(
                    service_name=service_name,
                    as_of_date=as_of_date,
                    category=cluster.category,
                    plan=plan,
                    orchestrator=orchestrator,
                )
                svc_result.duration_seconds = time.monotonic() - svc_start
                result.service_results.append(svc_result)

                # Propagate failure to downstream
                if not svc_result.success:
                    jobs = plan.get_jobs_by_service(service_name)
                    for job in jobs:
                        orchestrator.propagate_failure(plan, job.job_id)

        result.completed_at = datetime.now(UTC)

        log_event(
            "cluster.batch.completed",
            cluster_name=cluster_name,
            as_of_date=as_of_date,
            all_succeeded=result.all_succeeded,
            failed_services=result.failed_services,
            total_duration_seconds=result.total_duration_seconds,
        )

        return result

    def create_schedule(self, cluster_name: str, cron: str) -> ScheduleConfig:
        """Create a batch schedule for a cluster.

        Args:
            cluster_name: Name of the cluster
            cron: Cron expression (e.g. '0 6 * * *')

        Returns:
            Created ScheduleConfig
        """
        # Validate cluster exists
        self.load_cluster(cluster_name)

        schedule = ScheduleConfig(
            cluster_name=cluster_name,
            cron=cron,
            enabled=True,
        )
        self._schedules[cluster_name] = schedule

        log_event(
            "cluster.schedule.created",
            cluster_name=cluster_name,
            cron=cron,
        )

        return schedule

    def list_schedules(self) -> list[ScheduleConfig]:
        """List all configured schedules.

        Returns:
            List of ScheduleConfig objects
        """
        return list(self._schedules.values())

    def disable_schedule(self, cluster_name: str) -> bool:
        """Disable a cluster's batch schedule.

        Args:
            cluster_name: Name of the cluster

        Returns:
            True if schedule was found and disabled
        """
        schedule = self._schedules.get(cluster_name)
        if not schedule:
            return False

        schedule.enabled = False

        log_event(
            "cluster.schedule.disabled",
            cluster_name=cluster_name,
        )

        return True

    # =========================================================================
    # PRIVATE METHODS
    # =========================================================================

    def _get_ordered_services(self, services: list[str]) -> list[str]:
        """Order services by dependency graph.

        Args:
            services: List of service names

        Returns:
            Dependency-ordered list of services
        """
        execution_order = self._graph.get_execution_order()
        # Filter to only services in this cluster, preserving dependency order
        return [s for s in execution_order if s in services]

    def _start_service(
        self,
        service_name: str,
        cluster: ClusterConfig,
        mode: str,
        version: str | None = None,
    ) -> ServiceStatus:
        """Start a single service.

        Args:
            service_name: Service to start
            cluster: Cluster configuration
            mode: Startup mode
            version: Optional version override

        Returns:
            ServiceStatus for the started service
        """
        svc_status = ServiceStatus(
            service_name=service_name,
            health=ServiceHealthStatus.STARTING,
        )

        if self.cloud_provider == "local":
            svc_status = self._start_local_service(service_name, mode)
        else:
            # Cloud backends (GCP Cloud Run, AWS ECS) — placeholder
            logger.info(
                "Cloud bootstrap for %s on %s (not yet implemented)",
                service_name,
                self.cloud_provider,
            )
            svc_status.health = ServiceHealthStatus.UNKNOWN
            svc_status.error_message = f"Cloud provider {self.cloud_provider} not yet implemented"

        return svc_status

    def _start_local_service(self, service_name: str, mode: str) -> ServiceStatus:
        """Start a service locally via subprocess.

        Args:
            service_name: Service to start
            mode: Startup mode

        Returns:
            ServiceStatus for the started service
        """
        svc_status = ServiceStatus(
            service_name=service_name,
            health=ServiceHealthStatus.STARTING,
        )

        # Build the service directory path
        service_dir = Path(self.workspace_root) / service_name
        if not service_dir.exists():
            svc_status.health = ServiceHealthStatus.UNHEALTHY
            svc_status.error_message = f"Service directory not found: {service_dir}"
            logger.warning("Service directory not found: %s", service_dir)
            return svc_status

        # Check for CLI entry point
        cli_module = service_name.replace("-", "_")
        cmd = [
            str(service_dir / ".venv" / "bin" / "python"),
            "-m",
            cli_module,
            "--operation",
            "run",
            "--mode",
            mode,
        ]

        if self.mock_mode:
            env_overrides = {
                "CLOUD_MOCK_MODE": "true",
                "CLOUD_PROVIDER": "local",
            }
        else:
            env_overrides = {}

        try:
            # Inherit parent environment for subprocess; env=None inherits automatically.
            # Only override when we have mock-mode-specific vars.
            env: dict[str, str] | None = None
            if env_overrides:
                env = {**os.environ, **env_overrides}  # config-bootstrap: subprocess env merge
            process = subprocess.Popen(
                cmd,
                cwd=str(service_dir),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self._running_processes[service_name] = process

            svc_status.running = True
            svc_status.pid = process.pid
            svc_status.health = ServiceHealthStatus.HEALTHY
            svc_status.started_at = datetime.now(UTC)

            log_event(
                "cluster.service.started",
                service_name=service_name,
                pid=process.pid,
                mode=mode,
            )

        except FileNotFoundError as e:
            svc_status.health = ServiceHealthStatus.UNHEALTHY
            svc_status.error_message = f"Service executable not found: {e}"
            logger.warning("Failed to start %s: %s", service_name, e)
        except OSError as e:
            svc_status.health = ServiceHealthStatus.UNHEALTHY
            svc_status.error_message = f"OS error starting service: {e}"
            logger.warning("Failed to start %s: %s", service_name, e)

        return svc_status

    def _stop_service(self, service_name: str) -> None:
        """Stop a running service.

        Args:
            service_name: Service to stop
        """
        process = self._running_processes.pop(service_name, None)
        if process is None:
            logger.debug("Service %s not tracked as running", service_name)
            return

        try:
            # Graceful shutdown first
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                logger.warning("Service %s did not stop gracefully, sending SIGKILL", service_name)
                process.kill()
                process.wait(timeout=10)

            log_event(
                "cluster.service.stopped",
                service_name=service_name,
                pid=process.pid,
            )
        except OSError as e:
            logger.warning("Error stopping service %s: %s", service_name, e)

    def _check_service_health(self, service_name: str) -> ServiceStatus:
        """Check the health of a service.

        Args:
            service_name: Service to check

        Returns:
            ServiceStatus with current health
        """
        svc_status = ServiceStatus(service_name=service_name)

        process = self._running_processes.get(service_name)
        if process is None:
            svc_status.health = ServiceHealthStatus.STOPPED
            return svc_status

        # Check if process is still alive
        poll_result = process.poll()
        if poll_result is not None:
            # Process has exited
            svc_status.running = False
            svc_status.health = ServiceHealthStatus.STOPPED
            svc_status.error_message = f"Process exited with code {poll_result}"
            self._running_processes.pop(service_name, None)
        else:
            svc_status.running = True
            svc_status.pid = process.pid
            svc_status.health = ServiceHealthStatus.HEALTHY

        return svc_status

    def _execute_batch_service(
        self,
        service_name: str,
        as_of_date: str,
        category: str,
        plan: object,
        orchestrator: T1Orchestrator,
    ) -> BatchServiceResult:
        """Execute a batch run for a single service.

        Args:
            service_name: Service to execute
            as_of_date: Date to process
            category: Category (CEFI/TRADFI/DEFI)
            plan: Orchestration plan
            orchestrator: T1Orchestrator instance

        Returns:
            BatchServiceResult
        """
        result = BatchServiceResult(service_name=service_name, success=False)

        log_event(
            "cluster.batch.service.started",
            service_name=service_name,
            as_of_date=as_of_date,
            category=category,
        )

        if self.mock_mode:
            # In mock mode, simulate successful execution
            result.success = True
            result.jobs_completed = 1

            log_event(
                "cluster.batch.service.completed",
                service_name=service_name,
                as_of_date=as_of_date,
                mock_mode=True,
            )
            return result

        # Real execution via subprocess
        service_dir = Path(self.workspace_root) / service_name
        if not service_dir.exists():
            result.error_message = f"Service directory not found: {service_dir}"
            return result

        cli_module = service_name.replace("-", "_")
        cmd = [
            str(service_dir / ".venv" / "bin" / "python"),
            "-m",
            cli_module,
            "--operation",
            "run",
            "--mode",
            "batch",
            "--category",
            category,
        ]

        try:
            completed = subprocess.run(
                cmd,
                cwd=str(service_dir),
                capture_output=True,
                text=True,
                timeout=3600,
            )

            if completed.returncode == 0:
                result.success = True
                result.jobs_completed = 1
            else:
                result.error_message = (
                    completed.stderr[:500] if completed.stderr else "Non-zero exit"
                )
                result.jobs_failed = 1

        except subprocess.TimeoutExpired:
            result.error_message = "Service batch timed out after 3600s"
            result.jobs_failed = 1
        except FileNotFoundError as e:
            result.error_message = f"Service executable not found: {e}"
            result.jobs_failed = 1
        except OSError as e:
            result.error_message = f"OS error running batch: {e}"
            result.jobs_failed = 1

        log_event(
            "cluster.batch.service.completed",
            service_name=service_name,
            as_of_date=as_of_date,
            success=result.success,
            error_message=result.error_message,
        )

        return result
