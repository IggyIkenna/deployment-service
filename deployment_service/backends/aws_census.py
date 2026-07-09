# Epic: observability_master
# Lifecycle: permanent
"""Read-only AWS compute census — the deployment-observability AWS seam.

The unified deployment inventory (deployment-api ``GET /api/deployments/inventory``)
needs a **read-only list** of the live AWS estate: every backfill-class EC2 instance
(running + recently terminated) and every recent AWS Batch Fargate job. The
deploy-side ``AWSEC2Backend`` / ``AWSBatchBackend`` classes describe instances/jobs
by *id* (they are constructed with deploy config — subnet/SG/queue), which is the
wrong shape for a census. This module is the read-only twin: it lists by tag/queue
and returns plain dataclasses the inventory maps to ``DeploymentItem``.

Cloud-agnostic boundary: boto3 is reached ONLY through the SAME deferred
``_ensure_boto3()`` import boundary the ``aws_ec2`` / ``aws_batch`` backends use
(the sanctioned AWS control-plane seam) — never an inline ``import boto3`` in a
deployment-api route. boto3 is an optional ``deployment-service[aws]`` extra, so the
import is deferred; on a non-AWS install / credential-free CI the census degrades to
an empty list (logged, never raised) so the inventory falls back to its GCP items.

The discovery filters mirror ``vm_zombie_watchdog_aws.py`` (the live AWS census the
zombie watchdog already runs): backfill EC2 instances carry ``Name`` / ``asset-group``
tags, Batch jobs live on the ``unified-trading-job-queue``.

SSOT: ``plans/active/deployment_observability_parity_live_batch_paper_2026_06_22.md``
Phase 5 + ``codex/05-infrastructure/deployment-observability.md``.
"""

from __future__ import annotations

import importlib.util
import logging
import types
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    import boto3 as _boto3_module  # noqa: TID251,RUF100 — TYPE_CHECKING-only guard; boto3 optional dep never imported at runtime

logger = logging.getLogger(__name__)

# A boto3 paginated/describe response element is an untyped nested dict. We narrow
# the boto3 client to these typed Protocols at construction so every downstream read
# is basedpyright-clean (the boto3 client itself is untyped — ``Unknown``).
_RawResponse = dict[str, object]


class _Paginator(Protocol):
    """The boto3 paginator surface the census uses (yields raw response pages)."""

    def paginate(self, **kwargs: object) -> Iterator[_RawResponse]: ...


class _Ec2Client(Protocol):
    """The read-only EC2 client surface the census uses."""

    def get_paginator(self, operation_name: str) -> _Paginator: ...


class _BatchClient(Protocol):
    """The read-only AWS Batch client surface the census uses."""

    def get_paginator(self, operation_name: str) -> _Paginator: ...

    def describe_jobs(self, *, jobs: list[str]) -> _RawResponse: ...


class _EcsClient(Protocol):
    """The read-only ECS client surface the census uses."""

    def get_paginator(self, operation_name: str) -> _Paginator: ...

    def describe_services(self, *, cluster: str, services: list[str]) -> _RawResponse: ...


# AWS estate defaults (mirror vm_zombie_watchdog_aws.py — all GCS/S3 data + compute
# live in ap-northeast-1, the AWS twin of GCP asia-northeast1).
DEFAULT_AWS_REGION = "ap-northeast-1"

# The default AWS Batch job queue (mirrors AWSBatchBackend's default).
DEFAULT_BATCH_JOB_QUEUE = "unified-trading-job-queue"

# The prod ECS clusters the deployment-observability census reads (mirrors the
# ``cluster:`` field in ``deployment-service/configs/aws/*.yaml`` + the Terraform
# ``aws_ecs_cluster.unified_trading`` name ``unified-trading-{environment}``).
DEFAULT_ECS_CLUSTERS: tuple[str, ...] = ("uts-defi-prod", "unified-trading-prod")

# How many recently-terminated EC2 instances + finished Batch jobs to surface (a
# census is "live + recent", not the whole history) — Batch list_jobs is paged per
# status; EC2 describe_instances returns terminated instances AWS still retains
# (~1h after termination), which is exactly the "recently-terminated" window.
_EC2_STATES = ("pending", "running", "shutting-down", "stopping", "stopped", "terminated")
_BATCH_STATUSES = ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "SUCCEEDED", "FAILED")


def _ensure_boto3() -> types.ModuleType:
    """Deferred boto3 import — the sanctioned deployment AWS control-plane boundary.

    Mirrors ``aws_ec2._ensure_boto3`` / ``aws_batch._ensure_boto3``. boto3 is the
    optional ``deployment-service[aws]`` extra; a missing install raises ImportError
    which the census callers catch and degrade to an empty list.
    """
    if importlib.util.find_spec("boto3") is None:
        raise ImportError(
            "boto3 is required for the AWS census. Install with: uv pip install 'deployment-service[aws]'"
        )
    import boto3  # noqa: imports-inside-functions,TID251,RUF100 — intentional lazy load, boto3 optional dep

    return boto3


def _make_client(region: str, service_name: str) -> object:
    """Construct a boto3 service client, returned as ``object`` (the untyped boundary).

    ``boto3.client`` carries ~400 per-service overloads that all return ``Unknown``;
    funnelling construction through this single ``-> object`` boundary lets the callers
    ``cast`` to a typed Protocol once (basedpyright-clean) instead of every call site
    re-deriving the unknown overload.
    """
    boto3 = cast("_boto3_module", _ensure_boto3())
    return cast("object", boto3.client(service_name, region_name=region))  # pyright: ignore[reportUnknownMemberType]


@dataclass(frozen=True)
class AwsInstanceCensus:
    """One EC2 instance in the read-only census (the inventory's VM analogue).

    Attributes:
        name: The instance ``Name`` tag (the deployment name the classifier reads),
            falling back to the instance id when untagged.
        instance_id: The EC2 instance id (``i-...``).
        state: The raw EC2 instance-state name (``running`` / ``terminated`` / ...).
        asset_group: The ``asset-group`` tag value, or ``""`` when absent.
        launch_time: The instance launch time (UTC), or ``None`` if unparseable.
    """

    name: str
    instance_id: str
    state: str
    asset_group: str
    launch_time: datetime | None


@dataclass(frozen=True)
class AwsBatchJobCensus:
    """One AWS Batch (Fargate) job in the read-only census (the Cloud-Run analogue).

    Attributes:
        name: The Batch ``jobName`` (the deployment name the classifier reads).
        job_id: The Batch job id.
        status: The raw AWS Batch status (``RUNNING`` / ``SUCCEEDED`` / ``FAILED`` / ...).
        asset_group: The ``asset-group`` tag value, or ``""`` when absent.
        created_at: The job ``createdAt`` (UTC), or ``None``.
        stopped_at: The job ``stoppedAt`` (UTC), or ``None`` when not finished.
        exit_code: The container exit code of a finished job, or ``None``.
        status_reason: The ``statusReason`` of a failed job, or ``""``.
    """

    name: str
    job_id: str
    status: str
    asset_group: str
    created_at: datetime | None
    stopped_at: datetime | None
    exit_code: int | None
    status_reason: str


@dataclass(frozen=True)
class AwsEcsServiceCensus:
    """One ECS/Fargate service in the read-only census (always-on, not run-to-completion).

    Attributes:
        name: The ECS ``serviceName`` (the deployment name the classifier reads).
        cluster: The owning ECS cluster name (``uts-defi-prod`` / ``unified-trading-prod``).
        desired_count: The service's desired task count (0 = intentionally scaled to zero).
        running_count: The service's currently running task count.
        task_definition_revision: The active task-definition revision number, or ``None``
            if the ARN's trailing segment is not a revision integer.
        created_at: The service's ``createdAt`` (UTC), or ``None`` if unparseable.
        updated_at: The primary deployment's ``updatedAt`` (UTC) — the most recent
            deployment activity — or ``None`` if there is no primary deployment.
    """

    name: str
    cluster: str
    desired_count: int
    running_count: int
    task_definition_revision: int | None
    created_at: datetime | None
    updated_at: datetime | None


def _as_dict(obj: object) -> dict[str, object]:
    """Narrow an untyped boto3 response element to ``dict[str, object]`` (boundary cast).

    The boto3 client is untyped; its paginator pages / describe responses are nested
    ``Any``. Casting each element to a typed dict at the boundary keeps every
    downstream ``.get`` typed (basedpyright-clean) without scattering ``# pyright:
    ignore`` across the read paths.
    """
    return cast("dict[str, object]", obj) if isinstance(obj, dict) else {}


def _as_list(obj: object) -> list[object]:
    """Narrow an untyped boto3 response list element to ``list[object]`` (boundary cast)."""
    return cast("list[object]", obj) if isinstance(obj, list) else []


def _tag_value(tags: object, key: str) -> str:
    """Read an EC2-shaped tag list (``[{"Key":..,"Value":..}]``) for ``key``, or ``""``."""
    for tag_obj in _as_list(tags):
        tag = _as_dict(tag_obj)
        if tag.get("Key") == key:
            return str(tag.get("Value", ""))
    return ""


def _epoch_ms_to_utc(value: object) -> datetime | None:
    """AWS Batch ``createdAt``/``stoppedAt`` are epoch-millis ints → UTC datetime."""
    if not isinstance(value, (int, float)):
        return None
    try:
        return datetime.fromtimestamp(value / 1000.0, UTC)
    except (ValueError, OSError, OverflowError):
        return None


def list_ec2_census(region: str = DEFAULT_AWS_REGION) -> list[AwsInstanceCensus]:
    """List backfill-class EC2 instances (running + recently-terminated), read-only.

    Pages ``describe_instances`` filtered to the lifecycle states a census cares about
    (excludes nothing terminal — AWS retains terminated instances ~1h, which is the
    "recently-terminated" window). Returns one ``AwsInstanceCensus`` per instance,
    keyed on the ``Name`` tag (the deployment name).

    Honest degradation: any boto3 import / API / credential error is logged and yields
    an empty list — never raises — so the inventory degrades to its GCP items.
    """
    try:
        ec2 = cast("_Ec2Client", _make_client(region, "ec2"))
        paginator = ec2.get_paginator("describe_instances")
        result: list[AwsInstanceCensus] = []
        for page_obj in paginator.paginate(Filters=[{"Name": "instance-state-name", "Values": list(_EC2_STATES)}]):
            page = _as_dict(page_obj)
            for reservation_obj in _as_list(page.get("Reservations")):
                reservation = _as_dict(reservation_obj)
                for inst_obj in _as_list(reservation.get("Instances")):
                    inst = _as_dict(inst_obj)
                    tags = inst.get("Tags")
                    instance_id = str(inst.get("InstanceId", ""))
                    name = _tag_value(tags, "Name") or instance_id
                    launch = inst.get("LaunchTime")
                    launch_time: datetime | None = None
                    if isinstance(launch, datetime):
                        launch_time = launch if launch.tzinfo is not None else launch.replace(tzinfo=UTC)
                    state = _as_dict(inst.get("State")).get("Name", "unknown")
                    result.append(
                        AwsInstanceCensus(
                            name=name,
                            instance_id=instance_id,
                            state=str(state),
                            asset_group=_tag_value(tags, "asset-group"),
                            launch_time=launch_time,
                        )
                    )
        return result
    except Exception as exc:
        logger.warning("AWS EC2 census failed (degrading to empty list): %s", exc)
        return []


def list_batch_census(
    region: str = DEFAULT_AWS_REGION,
    job_queue: str = DEFAULT_BATCH_JOB_QUEUE,
) -> list[AwsBatchJobCensus]:
    """List recent AWS Batch (Fargate) jobs on the queue, read-only (Cloud-Run analogue).

    ``Batch.list_jobs`` returns ids per (queue, status); per id we then ``describe_jobs``
    to read ``createdAt`` / ``stoppedAt`` / the container ``exitCode`` / ``statusReason``.
    Returns one ``AwsBatchJobCensus`` per job, keyed on ``jobName``.

    Honest degradation: any error is logged and yields an empty list — never raises.
    """
    try:
        batch = cast("_BatchClient", _make_client(region, "batch"))

        job_ids: list[str] = []
        for status in _BATCH_STATUSES:
            paginator = batch.get_paginator("list_jobs")
            for page_obj in paginator.paginate(jobQueue=job_queue, jobStatus=status):
                for summary_obj in _as_list(_as_dict(page_obj).get("jobSummaryList")):
                    job_id = _as_dict(summary_obj).get("jobId")
                    if job_id:
                        job_ids.append(str(job_id))
        if not job_ids:
            return []

        result: list[AwsBatchJobCensus] = []
        # describe_jobs accepts up to 100 ids per call.
        for i in range(0, len(job_ids), 100):
            response = _as_dict(batch.describe_jobs(jobs=job_ids[i : i + 100]))
            for job_obj in _as_list(response.get("jobs")):
                job = _as_dict(job_obj)
                container = _as_dict(job.get("container"))
                raw_exit = container.get("exitCode")
                exit_code = int(raw_exit) if isinstance(raw_exit, (int, float)) else None
                tags = _as_dict(job.get("tags"))
                raw_ag = tags.get("asset-group", "")
                asset_group = str(raw_ag) if raw_ag else ""
                result.append(
                    AwsBatchJobCensus(
                        name=str(job.get("jobName", job.get("jobId", ""))),
                        job_id=str(job.get("jobId", "")),
                        status=str(job.get("status", "UNKNOWN")),
                        asset_group=asset_group,
                        created_at=_epoch_ms_to_utc(job.get("createdAt")),
                        stopped_at=_epoch_ms_to_utc(job.get("stoppedAt")),
                        exit_code=exit_code,
                        status_reason=str(job.get("statusReason", "")),
                    )
                )
        return result
    except Exception as exc:
        logger.warning("AWS Batch census failed (degrading to empty list): %s", exc)
        return []


def _ecs_datetime(value: object) -> datetime | None:
    """ECS ``createdAt``/``updatedAt`` are boto3 ``datetime`` objects → UTC datetime."""
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return None


def _task_definition_revision(task_definition_arn: str) -> int | None:
    """The trailing ``:{revision}`` of a task-definition ARN, or ``None`` if unparseable."""
    tail = task_definition_arn.rsplit(":", 1)[-1] if ":" in task_definition_arn else ""
    return int(tail) if tail.isdigit() else None


def _parse_ecs_service(svc: dict[str, object], cluster: str) -> AwsEcsServiceCensus:
    """Parse one ``describe_services`` response element into an ``AwsEcsServiceCensus``."""
    deployments = _as_list(svc.get("deployments"))
    primary = _as_dict(deployments[0]) if deployments else {}
    raw_desired = svc.get("desiredCount")
    raw_running = svc.get("runningCount")
    return AwsEcsServiceCensus(
        name=str(svc.get("serviceName", "")),
        cluster=cluster,
        desired_count=int(raw_desired) if isinstance(raw_desired, (int, float)) else 0,
        running_count=int(raw_running) if isinstance(raw_running, (int, float)) else 0,
        task_definition_revision=_task_definition_revision(str(svc.get("taskDefinition", ""))),
        created_at=_ecs_datetime(svc.get("createdAt")),
        updated_at=_ecs_datetime(primary.get("updatedAt")),
    )


def _list_ecs_service_arns(ecs: _EcsClient, cluster: str) -> list[str]:
    """Page ``list_services`` for one cluster → every service ARN (running or scaled-to-zero)."""
    paginator = ecs.get_paginator("list_services")
    arns: list[str] = []
    for page_obj in paginator.paginate(cluster=cluster):
        arns.extend(str(a) for a in _as_list(_as_dict(page_obj).get("serviceArns")))
    return arns


def list_ecs_census(
    region: str = DEFAULT_AWS_REGION,
    clusters: tuple[str, ...] = DEFAULT_ECS_CLUSTERS,
) -> list[AwsEcsServiceCensus]:
    """List every ECS/Fargate service across the prod clusters, read-only.

    Pages ``list_services`` per cluster for service ARNs, then ``describe_services``
    (batched ≤10 per call, the ECS API limit) for desiredCount/runningCount/
    taskDefinition/deployment timestamps. A service is returned exactly as listed —
    including at 0 running (or 0 desired) tasks — never filtered here, so an
    intentional scale-to-zero stays visible instead of vanishing (WS-B Open-Q7: the
    service sub-taxonomy task derives ``serving``/``scaled-to-zero``/``dead``/
    ``degraded`` from these counts; this census only supplies the raw numbers).

    Honest degradation + shard-level failure isolation: a client-construction failure
    degrades to an empty list; a single cluster's list/describe failure is logged and
    that cluster's services are skipped — never raises — so one bad cluster never
    blocks the others or the GCP inventory.
    """
    try:
        ecs = cast("_EcsClient", _make_client(region, "ecs"))
    except Exception as exc:
        logger.warning("AWS ECS census client unavailable (degrading to empty list): %s", exc)
        return []

    result: list[AwsEcsServiceCensus] = []
    for cluster in clusters:
        try:
            service_arns = _list_ecs_service_arns(ecs, cluster)
            for i in range(0, len(service_arns), 10):
                response = _as_dict(ecs.describe_services(cluster=cluster, services=service_arns[i : i + 10]))
                for svc_obj in _as_list(response.get("services")):
                    result.append(_parse_ecs_service(_as_dict(svc_obj), cluster))
        except Exception as exc:
            logger.warning("AWS ECS census failed for cluster %s (skipping cluster): %s", cluster, exc)
    return result


__all__ = [
    "DEFAULT_AWS_REGION",
    "DEFAULT_BATCH_JOB_QUEUE",
    "DEFAULT_ECS_CLUSTERS",
    "AwsBatchJobCensus",
    "AwsEcsServiceCensus",
    "AwsInstanceCensus",
    "list_batch_census",
    "list_ec2_census",
    "list_ecs_census",
]
