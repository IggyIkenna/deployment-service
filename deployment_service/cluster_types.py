# SCHEMA_PROVENANCE_EXEMPT: Service-internal types — not cross-repo contracts. See QUALITY_GATE_BYPASS_AUDIT.md §2.17.
"""Cluster orchestrator data types — dataclasses and enums.

Extracted from ``cluster.py`` to keep files below the 900-line codex threshold.
Orchestrator logic lives in ``cluster.py``; this module holds only the plain
data containers (status snapshots, configs, batch results, schedule entries)
that the orchestrator returns and consumes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum


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
