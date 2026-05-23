# SCHEMA_PROVENANCE_EXEMPT: Service-internal API types — not cross-repo contracts. See QUALITY_GATE_BYPASS_AUDIT.md §2.17.
"""
FastAPI route handlers for ML experiment and model browsing.

Provides read-only endpoints for listing experiments from the
``ml-training-artifacts`` bucket and models from the ``ml-models-store`` bucket.
Both bucket names are resolved via the canonical SSOT
``unified_trading_library.cloud_interface.bucket_naming.resolve_bucket_name``
(reading ``deployment-service/configs/cloud-providers.yaml``) — see
Bucket-name SSOT (b+). Local helpers ``_artifacts_bucket()`` / ``_models_bucket()``
keep delegating through ``get_bucket_name(...)`` for now; the resolver migration is
tracked under the ml_artefact_path_resolver_consumer_sweep_2026_05_12 issue.
"""

from __future__ import annotations

import json
import logging
import re
from typing import cast

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict
from unified_trading_library import get_bucket_name, get_storage_client

from deployment_service.deployment_config import DeploymentConfig

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ml", tags=["ml-experiments"])

_config = DeploymentConfig()


# ---------------------------------------------------------------------------
# Response models (frozen Pydantic BaseModel — service-internal)
# ---------------------------------------------------------------------------


class ExperimentSummary(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI response body, not a domain contract
    model_config = ConfigDict(frozen=True)

    experiment_id: str
    has_metrics: bool


class ExperimentDetail(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI response body, not a domain contract
    model_config = ConfigDict(frozen=True)

    experiment_id: str
    metrics: dict[str, object]


class ModelSummary(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI response body, not a domain contract
    model_config = ConfigDict(frozen=True)

    model_id: str
    latest_training_period: str | None = None


class ModelMetadataResponse(
    BaseModel,
):  # CORRECT-LOCAL — FastAPI response body, not a domain contract
    model_config = ConfigDict(frozen=True)

    model_id: str
    metadata: dict[str, object]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _artifacts_bucket() -> str:
    """Return the ml-training-artifacts bucket name."""
    return get_bucket_name("ml_artifacts", project_id=_config.gcp_project_id)


def _models_bucket() -> str:
    """Return the ml-models-store bucket name."""
    return get_bucket_name("ml_models", project_id=_config.gcp_project_id)


# ---------------------------------------------------------------------------
# GET /api/ml/experiments — list experiment directories
# ---------------------------------------------------------------------------


@router.get("/experiments")
async def list_experiments(limit: int = 100) -> list[ExperimentSummary]:
    """List experiment directories from the ml-training-artifacts GCS bucket."""
    bucket = _artifacts_bucket()
    try:
        storage_client = get_storage_client(project_id=_config.gcp_project_id)
        blobs = storage_client.list_blobs(bucket=bucket, prefix="experiments/")

        # Collect unique experiment IDs and whether they have metrics.json
        experiment_map: dict[str, bool] = {}
        pattern = re.compile(r"^experiments/([^/]+)/")
        for blob in blobs:
            blob_name: str = blob.name if hasattr(blob, "name") else str(blob)
            match = pattern.match(blob_name)
            if match:
                exp_id = match.group(1)
                if exp_id not in experiment_map:
                    experiment_map[exp_id] = False
                if blob_name.endswith("/metrics.json"):
                    experiment_map[exp_id] = True
            if len(experiment_map) >= limit:
                break

        return [
            ExperimentSummary(experiment_id=eid, has_metrics=has_metrics)
            for eid, has_metrics in sorted(experiment_map.items(), reverse=True)
        ][:limit]
    except (OSError, ValueError, RuntimeError) as e:
        logger.error("list_experiments failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


# ---------------------------------------------------------------------------
# GET /api/ml/experiments/{experiment_id} — get experiment details
# ---------------------------------------------------------------------------


@router.get("/experiments/{experiment_id}")
async def get_experiment(experiment_id: str) -> ExperimentDetail:
    """Get experiment details by reading metrics.json from GCS."""
    bucket = _artifacts_bucket()
    gcs_path = f"experiments/{experiment_id}/metrics.json"
    try:
        storage_client = get_storage_client(project_id=_config.gcp_project_id)
        raw_bytes = storage_client.download_bytes(bucket=bucket, blob_path=gcs_path)
        metrics_data = cast(dict[str, object], json.loads(raw_bytes.decode("utf-8")))
        metrics: dict[str, object] = metrics_data
        return ExperimentDetail(experiment_id=experiment_id, metrics=metrics)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Experiment '{experiment_id}' not found") from e
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as e:
        logger.error("get_experiment failed for %s: %s", experiment_id, e)
        raise HTTPException(status_code=500, detail=str(e)) from e


# ---------------------------------------------------------------------------
# GET /api/ml/models — list models from model_registry/
# ---------------------------------------------------------------------------


@router.get("/models")
async def list_models(limit: int = 100) -> list[ModelSummary]:
    """List models from the ml-models-store GCS bucket model_registry/ prefix."""
    bucket = _models_bucket()
    try:
        storage_client = get_storage_client(project_id=_config.gcp_project_id)

        # Try reading the manifest first (faster than scanning)
        try:
            manifest_bytes = storage_client.download_bytes(bucket=bucket, blob_path="model_registry/manifest.json")
            manifest_data = cast(dict[str, object], json.loads(manifest_bytes.decode("utf-8")))
            manifest: dict[str, object] = manifest_data
            models_dict = manifest.get("models", {})
            if isinstance(models_dict, dict):
                results: list[ModelSummary] = []
                for model_id, model_info in models_dict.items():
                    latest_period: str | None = None
                    if isinstance(model_info, dict):
                        raw_period = model_info.get("latest_training_period")
                        if isinstance(raw_period, str):
                            latest_period = raw_period
                    results.append(
                        ModelSummary(
                            model_id=str(model_id),
                            latest_training_period=latest_period,
                        )
                    )
                return sorted(results, key=lambda m: m.model_id)[:limit]
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            pass  # Fall through to GCS scan

        # Fallback: scan model_registry/metadata/ for model IDs
        blobs = storage_client.list_blobs(bucket=bucket, prefix="model_registry/metadata/")
        model_ids: set[str] = set()
        pattern = re.compile(r"^model_registry/metadata/([^/]+)/")
        for blob in blobs:
            blob_name = blob.name if hasattr(blob, "name") else str(blob)
            match = pattern.match(blob_name)
            if match:
                model_ids.add(match.group(1))
            if len(model_ids) >= limit:
                break

        return [ModelSummary(model_id=mid, latest_training_period=None) for mid in sorted(model_ids)][:limit]
    except (OSError, ValueError, RuntimeError) as e:
        logger.error("list_models failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


# ---------------------------------------------------------------------------
# GET /api/ml/models/{model_id}/metadata — read metadata.json for a model
# ---------------------------------------------------------------------------


@router.get("/models/{model_id}/metadata")
async def get_model_metadata(model_id: str, training_period: str | None = None) -> ModelMetadataResponse:
    """Read metadata.json for a specific model from the ml-models-store bucket.

    If training_period is not specified, reads the manifest to find the latest period.
    """
    bucket = _models_bucket()
    try:
        storage_client = get_storage_client(project_id=_config.gcp_project_id)

        # Resolve training period if not provided
        if training_period is None:
            training_period = _resolve_latest_period(storage_client, bucket, model_id)
            if training_period is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"No training periods found for model '{model_id}'",
                )

        gcs_path = f"model_registry/metadata/{model_id}/training-period-{training_period}/metadata.json"
        raw_bytes = storage_client.download_bytes(bucket=bucket, blob_path=gcs_path)
        metadata_data = cast(dict[str, object], json.loads(raw_bytes.decode("utf-8")))
        metadata: dict[str, object] = metadata_data
        return ModelMetadataResponse(model_id=model_id, metadata=metadata)
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=f"Metadata not found for model '{model_id}' (period={training_period})",
        ) from e
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as e:
        logger.error("get_model_metadata failed for %s: %s", model_id, e)
        raise HTTPException(status_code=500, detail=str(e)) from e


def _resolve_latest_period(
    storage_client: object,
    bucket: str,
    model_id: str,
) -> str | None:
    """Find the latest training period for a model from manifest or GCS scan."""
    # Try manifest first
    try:
        manifest_bytes = storage_client.download_bytes(  # pyright: ignore[reportUnknownMemberType]
            bucket=bucket, blob_path="model_registry/manifest.json"
        )
        manifest_data = cast(dict[str, object], json.loads(manifest_bytes.decode("utf-8")))  # pyright: ignore[reportUnknownMemberType]
        manifest: dict[str, object] = manifest_data
        models_dict = manifest.get("models", {})
        if isinstance(models_dict, dict) and model_id in models_dict:
            model_info = models_dict[model_id]
            if isinstance(model_info, dict):
                period = model_info.get("latest_training_period")
                if isinstance(period, str) and period:
                    return period
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        pass

    # Fallback: scan GCS
    try:
        prefix = f"model_registry/metadata/{model_id}/"
        blobs = storage_client.list_blobs(bucket=bucket, prefix=prefix)  # pyright: ignore[reportUnknownMemberType]
        periods: set[str] = set()
        pattern = re.compile(r"training-period-(\d{4}-\d{2})")
        for blob in blobs:
            blob_name = blob.name if hasattr(blob, "name") else str(blob)
            match = pattern.search(blob_name)
            if match:
                periods.add(match.group(1))
        return max(periods) if periods else None
    except (FileNotFoundError, OSError, ValueError, RuntimeError):
        return None
