# SCHEMA_PROVENANCE_EXEMPT: Service-internal types — not cross-repo contracts. See QUALITY_GATE_BYPASS_AUDIT.md §2.17.
"""
FastAPI route handlers for deployment orchestration.

Endpoints for cluster lifecycle, batch pipelines, live service management,
and T+1 scheduling. Delegates to T1Orchestrator, LiveDeployer, and cluster
config YAML files under configs/clusters/.
"""

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import yaml
from fastapi import APIRouter, HTTPException
from pydantic import AliasChoices, BaseModel, Field

from deployment_service.deployment_config import DeploymentConfig
from deployment_service.orchestrator import T1Orchestrator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/deployment", tags=["orchestration"])

_config = DeploymentConfig()

# ---------------------------------------------------------------------------
# Cluster config helpers
# ---------------------------------------------------------------------------

_CLUSTER_DIR = Path(__file__).resolve().parents[3] / "configs" / "clusters"


def _load_cluster_config(cluster_name: str) -> dict[str, object]:
    """Load a cluster YAML config by name.

    Raises HTTPException(404) if the cluster file does not exist.
    """
    path = _CLUSTER_DIR / f"{cluster_name}.yaml"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Cluster '{cluster_name}' not found")
    with open(path) as f:
        loaded_data = cast(dict[str, object], yaml.safe_load(f))
        data: dict[str, object] = loaded_data
    return data


def _list_cluster_names() -> list[str]:
    """Return sorted list of available cluster names (stem of each YAML)."""
    if not _CLUSTER_DIR.is_dir():
        return []
    return sorted(p.stem for p in _CLUSTER_DIR.glob("*.yaml"))


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class ClusterSummary(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI response body, not a domain contract
    name: str
    description: str
    asset_group: str
    service_count: int


class ServiceStatusEntry(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI response body, not a domain contract
    service: str
    status: str
    started_at: str | None = None


class ClusterStatusResponse(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI response body, not a domain contract
    cluster: str
    status: str
    services: list[ServiceStatusEntry]
    timestamp: str


class BootstrapRequest(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI request body, not a domain contract
    mode: str = "live"
    mock_mode: bool = True
    versions: dict[str, str] | None = None


class BatchRunRequest(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI request body, not a domain contract
    cluster: str
    as_of_date: str
    service: str | None = None
    asset_group: str | None = Field(
        default=None,
        validation_alias=AliasChoices("asset_group", "category"),
    )


class BatchResultResponse(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI response body, not a domain contract
    cluster: str
    as_of_date: str
    status: str
    total_jobs: int
    completed_jobs: int
    failed_jobs: int
    skipped_jobs: int
    completion_percent: float
    started_at: str
    finished_at: str | None = None


class ServiceStartRequest(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI request body, not a domain contract
    mode: str = "live"
    mock_mode: bool = True


class ServiceStatusResponse(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI response body, not a domain contract
    service: str
    status: str
    mode: str
    cluster: str | None = None
    started_at: str | None = None


class ScheduleCreateRequest(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI request body, not a domain contract
    cluster: str
    cron: str
    as_of: str = "yesterday"


class ScheduleResponse(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI response body, not a domain contract
    cluster: str
    cron: str
    as_of: str
    enabled: bool
    created_at: str


# ---------------------------------------------------------------------------
# In-memory state for schedule / service tracking
# ---------------------------------------------------------------------------

# Lightweight in-memory registries. A production deployment would persist
# these to GCS via StateManager, but for the API surface they are sufficient.

_active_services: dict[str, ServiceStatusResponse] = {}
_schedules: dict[str, ScheduleResponse] = {}
_batch_history: list[BatchResultResponse] = []


# ---------------------------------------------------------------------------
# Cluster operations
# ---------------------------------------------------------------------------


@router.get("/clusters")
async def list_clusters() -> list[ClusterSummary]:
    """List available deployment clusters."""
    summaries: list[ClusterSummary] = []
    for name in _list_cluster_names():
        try:
            cfg = _load_cluster_config(name)
            services = cfg.get("services", [])
            service_list: list[str] = services if isinstance(services, list) else []
            summaries.append(
                ClusterSummary(
                    name=str(cfg.get("cluster", name)),
                    description=str(cfg.get("description", "")),
                    asset_group=str(cfg.get("asset_group") or cfg.get("category", "")),
                    service_count=len(service_list),
                )
            )
        except HTTPException:
            # Skip files that fail to parse — don't crash the listing.
            logger.warning("Skipping unparseable cluster config: %s", name)
    return summaries


@router.get("/clusters/{cluster_name}/status")
async def get_cluster_status(cluster_name: str) -> ClusterStatusResponse:
    """Get status of all services in a cluster."""
    cfg = _load_cluster_config(cluster_name)
    services_raw = cfg.get("services", [])
    service_names: list[str] = services_raw if isinstance(services_raw, list) else []

    entries: list[ServiceStatusEntry] = []
    for svc in service_names:
        svc_str = str(svc)
        tracked = _active_services.get(svc_str)
        if tracked is not None:
            entries.append(
                ServiceStatusEntry(
                    service=svc_str,
                    status=tracked.status,
                    started_at=tracked.started_at,
                )
            )
        else:
            entries.append(ServiceStatusEntry(service=svc_str, status="stopped"))

    running_count = sum(1 for e in entries if e.status == "running")
    cluster_status = "running" if running_count == len(entries) and entries else "stopped"
    if 0 < running_count < len(entries):
        cluster_status = "partial"

    return ClusterStatusResponse(
        cluster=cluster_name,
        status=cluster_status,
        services=entries,
        timestamp=datetime.now(UTC).isoformat(),
    )


@router.post("/clusters/{cluster_name}/bootstrap")
async def bootstrap_cluster(cluster_name: str, request: BootstrapRequest) -> ClusterStatusResponse:
    """Start all services in a cluster."""
    cfg = _load_cluster_config(cluster_name)
    services_raw = cfg.get("services", [])
    service_names: list[str] = services_raw if isinstance(services_raw, list) else []

    now = datetime.now(UTC).isoformat()
    entries: list[ServiceStatusEntry] = []
    for svc in service_names:
        svc_str = str(svc)
        resp = ServiceStatusResponse(
            service=svc_str,
            status="running",
            mode=request.mode,
            cluster=cluster_name,
            started_at=now,
        )
        _active_services[svc_str] = resp
        entries.append(ServiceStatusEntry(service=svc_str, status="running", started_at=now))

    logger.info(
        "Bootstrapped cluster %s with %d services (mode=%s, mock=%s)",
        cluster_name,
        len(entries),
        request.mode,
        request.mock_mode,
    )

    return ClusterStatusResponse(
        cluster=cluster_name,
        status="running",
        services=entries,
        timestamp=now,
    )


@router.post("/clusters/{cluster_name}/teardown")
async def teardown_cluster(cluster_name: str) -> dict[str, str]:
    """Stop all services in a cluster."""
    cfg = _load_cluster_config(cluster_name)
    services_raw = cfg.get("services", [])
    service_names: list[str] = services_raw if isinstance(services_raw, list) else []

    stopped_count = 0
    for svc in service_names:
        svc_str = str(svc)
        if svc_str in _active_services:
            del _active_services[svc_str]
            stopped_count += 1

    logger.info("Tore down cluster %s — stopped %d services", cluster_name, stopped_count)
    return {"cluster": cluster_name, "status": "stopped", "stopped_services": str(stopped_count)}


# ---------------------------------------------------------------------------
# Batch operations
# ---------------------------------------------------------------------------


@router.post("/batch/run")
async def run_batch(request: BatchRunRequest) -> BatchResultResponse:
    """Run a thermal batch pipeline."""
    # Validate cluster exists
    cfg = _load_cluster_config(request.cluster)
    ag = request.asset_group or str(cfg.get("asset_group") or cfg.get("category", "ALL"))

    services_filter: list[str] | None = None
    if request.service is not None:
        services_filter = [request.service]

    now = datetime.now(UTC).isoformat()

    try:
        orchestrator = T1Orchestrator(
            config_dir=str(_CLUSTER_DIR.parent),
            project_id=_config.gcp_project_id or None,
        )
        plan = orchestrator.create_daily_plan(
            target_date=request.as_of_date,
            asset_group=ag,
            services=services_filter,
        )

        result = BatchResultResponse(
            cluster=request.cluster,
            as_of_date=request.as_of_date,
            status="completed" if plan.failed_jobs == 0 else "failed",
            total_jobs=plan.total_jobs,
            completed_jobs=plan.completed_jobs,
            failed_jobs=plan.failed_jobs,
            skipped_jobs=plan.skipped_jobs,
            completion_percent=plan.completion_percent,
            started_at=now,
            finished_at=datetime.now(UTC).isoformat(),
        )
    except (OSError, ValueError, RuntimeError) as e:
        logger.error("run_batch failed for cluster %s: %s", request.cluster, e)
        result = BatchResultResponse(
            cluster=request.cluster,
            as_of_date=request.as_of_date,
            status="failed",
            total_jobs=0,
            completed_jobs=0,
            failed_jobs=0,
            skipped_jobs=0,
            completion_percent=0.0,
            started_at=now,
            finished_at=datetime.now(UTC).isoformat(),
        )

    _batch_history.append(result)
    return result


@router.get("/batch/history")
async def batch_history(cluster: str, limit: int = 10) -> list[BatchResultResponse]:
    """Get batch run history."""
    filtered = [b for b in _batch_history if b.cluster == cluster]
    return filtered[-limit:]


# ---------------------------------------------------------------------------
# Live service operations
# ---------------------------------------------------------------------------


@router.post("/live/{service_name}/start")
async def start_service(service_name: str, request: ServiceStartRequest) -> ServiceStatusResponse:
    """Start a single service."""
    now = datetime.now(UTC).isoformat()
    resp = ServiceStatusResponse(
        service=service_name,
        status="running",
        mode=request.mode,
        started_at=now,
    )
    _active_services[service_name] = resp
    logger.info("Started service %s (mode=%s, mock=%s)", service_name, request.mode, request.mock_mode)
    return resp


@router.post("/live/{service_name}/stop")
async def stop_service(service_name: str) -> dict[str, str]:
    """Stop a single service."""
    if service_name not in _active_services:
        raise HTTPException(status_code=404, detail=f"Service '{service_name}' is not running")
    del _active_services[service_name]
    logger.info("Stopped service %s", service_name)
    return {"service": service_name, "status": "stopped"}


@router.get("/live/status")
async def fleet_status(cluster: str | None = None) -> list[ServiceStatusResponse]:
    """Get status of all running services."""
    if cluster is None:
        return list(_active_services.values())
    return [s for s in _active_services.values() if s.cluster == cluster]


# ---------------------------------------------------------------------------
# Schedule operations
# ---------------------------------------------------------------------------


@router.post("/schedule")
async def create_schedule(request: ScheduleCreateRequest) -> ScheduleResponse:
    """Create a T+1 batch schedule."""
    # Validate cluster exists
    _load_cluster_config(request.cluster)

    now = datetime.now(UTC).isoformat()
    schedule = ScheduleResponse(
        cluster=request.cluster,
        cron=request.cron,
        as_of=request.as_of,
        enabled=True,
        created_at=now,
    )
    _schedules[request.cluster] = schedule
    logger.info("Created schedule for cluster %s: cron=%s", request.cluster, request.cron)
    return schedule


@router.get("/schedule")
async def list_schedules() -> list[ScheduleResponse]:
    """List active schedules."""
    return [s for s in _schedules.values() if s.enabled]


@router.delete("/schedule/{cluster_name}")
async def disable_schedule(cluster_name: str) -> dict[str, str]:
    """Disable a schedule."""
    schedule = _schedules.get(cluster_name)
    if schedule is None:
        raise HTTPException(status_code=404, detail=f"No schedule found for cluster '{cluster_name}'")
    _schedules[cluster_name] = schedule.model_copy(update={"enabled": False})
    logger.info("Disabled schedule for cluster %s", cluster_name)
    return {"cluster": cluster_name, "status": "disabled"}
