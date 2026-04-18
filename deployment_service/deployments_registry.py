# SCHEMA_PROVENANCE_EXEMPT: Service-internal registry state model — not a cross-repo contract. See QUALITY_GATE_BYPASS_AUDIT.md §2.17.
"""
VM Deployment Registry — GCS-backed active + archive table.

SSOT for "what VM jobs are currently running and what did the last week look like".

Layout on GCS:
    gs://<bucket>/deployments/active/<deployment_id>.json   while status=running
    gs://<bucket>/deployments/archive/<YYYY-MM-DD>/<deployment_id>.json
                                                            after completed/failed

This registry is written by the VM-side helper (`scripts/vm/deployment_heartbeat.py`)
and read by `deployment-api`'s `GET /api/deployments` endpoint.

All schema fields below match the VM-side schema contract documented in the
Gate G1 plan.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from unified_trading_library import StorageClient

logger = logging.getLogger(__name__)


DEFAULT_BUCKET = "deployment-scripts-central-element-323112"
ACTIVE_PREFIX = "deployments/active/"
ARCHIVE_PREFIX = "deployments/archive/"


@dataclass
class DeploymentRegistryEntry:  # CORRECT-LOCAL: service-internal registry model
    """One row of the VM deployments registry.

    Schema matches JSON written to GCS. Every field is typed — no `object`.
    """

    deployment_id: str
    vm_name: str
    category: str
    task: str
    mode: str
    start_date: str
    end_date: str
    status: str  # running | completed | failed
    started_at: str
    last_heartbeat_at: str
    completed_at: str | None
    exit_code: int | None
    rows_in: int
    rows_out: int
    rows_error: int
    events_emitted: int
    log_uri: str
    extras: dict[str, str] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def from_json(cls, payload: str) -> DeploymentRegistryEntry:
        data = json.loads(payload)
        extras_raw = data.get("extras", {})
        extras = (
            {str(k): str(v) for k, v in extras_raw.items()} if isinstance(extras_raw, dict) else {}
        )
        return cls(
            deployment_id=str(data["deployment_id"]),
            vm_name=str(data["vm_name"]),
            category=str(data["category"]),
            task=str(data["task"]),
            mode=str(data["mode"]),
            start_date=str(data["start_date"]),
            end_date=str(data["end_date"]),
            status=str(data["status"]),
            started_at=str(data["started_at"]),
            last_heartbeat_at=str(data["last_heartbeat_at"]),
            completed_at=data.get("completed_at"),
            exit_code=data.get("exit_code"),
            rows_in=int(data.get("rows_in", 0) or 0),
            rows_out=int(data.get("rows_out", 0) or 0),
            rows_error=int(data.get("rows_error", 0) or 0),
            events_emitted=int(data.get("events_emitted", 0) or 0),
            log_uri=str(data.get("log_uri", "")),
            extras=extras,
        )


class _StorageClientLike(Protocol):
    """Subset of StorageClient used here — makes in-memory fakes easy in tests."""

    def upload_string(self, bucket: str, key: str, body: str) -> None: ...

    def download_string(self, bucket: str, key: str) -> str: ...

    def list_keys(self, bucket: str, prefix: str) -> list[str]: ...

    def delete_object(self, bucket: str, key: str) -> None: ...


class DeploymentsRegistry:
    """GCS-backed VM deployments registry.

    Backed by UTL's `StorageClient` in production. For tests, pass any object
    satisfying `_StorageClientLike` (e.g., `InMemoryStorageClient` below).
    """

    def __init__(
        self,
        bucket: str = DEFAULT_BUCKET,
        storage: _StorageClientLike | None = None,
    ) -> None:
        self._bucket = bucket
        self._storage: _StorageClientLike = storage if storage is not None else _default_storage()

    # ---- mutating ---------------------------------------------------------

    def register(self, entry: DeploymentRegistryEntry) -> None:
        """Write the initial ACTIVE record for a new deployment."""
        key = f"{ACTIVE_PREFIX}{entry.deployment_id}.json"
        self._storage.upload_string(self._bucket, key, entry.to_json())
        logger.info(
            "registered deployment %s (%s, %s) in gs://%s/%s",
            entry.deployment_id,
            entry.category,
            entry.mode,
            self._bucket,
            key,
        )

    def heartbeat(self, entry: DeploymentRegistryEntry) -> None:
        """Overwrite the ACTIVE record with updated counters + heartbeat time."""
        key = f"{ACTIVE_PREFIX}{entry.deployment_id}.json"
        self._storage.upload_string(self._bucket, key, entry.to_json())

    def complete(self, entry: DeploymentRegistryEntry) -> None:
        """Move a deployment from ACTIVE to ARCHIVE/<YYYY-MM-DD>/."""
        if entry.status not in ("completed", "failed"):
            raise ValueError(f"complete() requires terminal status, got {entry.status!r}")
        completed_at = entry.completed_at or _utcnow_iso()
        archive_date = completed_at[:10]  # YYYY-MM-DD
        archive_key = f"{ARCHIVE_PREFIX}{archive_date}/{entry.deployment_id}.json"
        active_key = f"{ACTIVE_PREFIX}{entry.deployment_id}.json"

        self._storage.upload_string(self._bucket, archive_key, entry.to_json())
        self._storage.delete_object(self._bucket, active_key)
        logger.info(
            "archived deployment %s (status=%s, exit_code=%s) to gs://%s/%s",
            entry.deployment_id,
            entry.status,
            entry.exit_code,
            self._bucket,
            archive_key,
        )

    # ---- reading ----------------------------------------------------------

    def list_active(self) -> list[DeploymentRegistryEntry]:
        keys = self._storage.list_keys(self._bucket, ACTIVE_PREFIX)
        result: list[DeploymentRegistryEntry] = []
        for key in keys:
            if not key.endswith(".json"):
                continue
            try:
                raw = self._storage.download_string(self._bucket, key)
                result.append(DeploymentRegistryEntry.from_json(raw))
            except (ValueError, KeyError, json.JSONDecodeError) as exc:
                logger.warning("skipping malformed active entry %s: %s", key, exc)
        return result

    def list_recent_archive(self, days: int = 7) -> list[DeploymentRegistryEntry]:
        """Return archive entries from the last N days (inclusive)."""
        today = datetime.now(UTC).date()
        result: list[DeploymentRegistryEntry] = []
        for offset in range(days):
            day = today - timedelta(days=offset)
            prefix = f"{ARCHIVE_PREFIX}{day.isoformat()}/"
            keys = self._storage.list_keys(self._bucket, prefix)
            for key in keys:
                if not key.endswith(".json"):
                    continue
                try:
                    raw = self._storage.download_string(self._bucket, key)
                    result.append(DeploymentRegistryEntry.from_json(raw))
                except (ValueError, KeyError, json.JSONDecodeError) as exc:
                    logger.warning("skipping malformed archive entry %s: %s", key, exc)
        return result

    def get(self, deployment_id: str) -> DeploymentRegistryEntry | None:
        """Return an entry by id — checks active first, then recent archive."""
        active_key = f"{ACTIVE_PREFIX}{deployment_id}.json"
        try:
            raw = self._storage.download_string(self._bucket, active_key)
            return DeploymentRegistryEntry.from_json(raw)
        except (FileNotFoundError, KeyError, ValueError):
            pass

        for entry in self.list_recent_archive(days=14):
            if entry.deployment_id == deployment_id:
                return entry
        return None


# ---- helpers --------------------------------------------------------------


def _utcnow_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _default_storage() -> _StorageClientLike:
    """Get the UTL-backed StorageClient wrapped to match `_StorageClientLike`.

    Lazy import: UTL pulls in UAC transitively, which callers may not want at
    module load time (e.g., unit tests using InMemoryStorageClient).
    """
    from unified_trading_library import get_storage_client

    utl_client = get_storage_client()
    return _UTLStorageAdapter(utl_client)


class _UTLStorageAdapter:
    """Adapts UTL's StorageClient (upload_bytes/list_blobs/...) to the Protocol used here."""

    def __init__(self, client: StorageClient) -> None:
        self._client = client

    def upload_string(self, bucket: str, key: str, body: str) -> None:
        self._client.upload_bytes(
            bucket=bucket,
            blob_path=key,
            data=body.encode("utf-8"),
            content_type="application/json",
        )

    def download_string(self, bucket: str, key: str) -> str:
        data = self._client.download_bytes(bucket=bucket, blob_path=key)
        return data.decode("utf-8")

    def list_keys(self, bucket: str, prefix: str) -> list[str]:
        return [blob.name for blob in self._client.list_blobs(bucket=bucket, prefix=prefix)]

    def delete_object(self, bucket: str, key: str) -> None:
        self._client.delete_blob(bucket=bucket, blob_path=key)


# ---- in-memory fake for tests --------------------------------------------


class InMemoryStorageClient:
    """Dict-backed fake implementing `_StorageClientLike`. For unit tests only."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], str] = {}

    def upload_string(self, bucket: str, key: str, body: str) -> None:
        self._store[(bucket, key)] = body

    def download_string(self, bucket: str, key: str) -> str:
        if (bucket, key) not in self._store:
            raise FileNotFoundError(f"gs://{bucket}/{key}")
        return self._store[(bucket, key)]

    def list_keys(self, bucket: str, prefix: str) -> list[str]:
        return sorted(k for (b, k) in self._store if b == bucket and k.startswith(prefix))

    def delete_object(self, bucket: str, key: str) -> None:
        self._store.pop((bucket, key), None)


__all__ = [
    "ACTIVE_PREFIX",
    "ARCHIVE_PREFIX",
    "DEFAULT_BUCKET",
    "DeploymentRegistryEntry",
    "DeploymentsRegistry",
    "InMemoryStorageClient",
]
