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
from typing import Protocol, cast

from unified_trading_library import StorageClient, UnifiedCloudConfig, get_storage_client

logger = logging.getLogger(__name__)


def _resolve_default_bucket() -> str:
    """Derive the registry bucket from UnifiedCloudConfig.

    Registry discovery runs before most config loads (VM startup / heartbeat
    CLI), so callers can still override via the ``bucket=`` constructor arg.
    The canonical pattern is ``deployment-scripts-<gcp_project_id>``.
    UnifiedCloudConfig reads the project id from the environment via pydantic
    AliasChoices — it owns the value post-init. Documented bootstrap exception
    per codex §bootstrap-phase.
    """
    try:
        cfg = UnifiedCloudConfig()
    except (ValueError, RuntimeError, OSError):  # shard-level isolation
        return ""
    proj = getattr(cfg, "gcp_project_id", "") or ""
    return f"deployment-scripts-{proj}" if proj else ""


def vm_log_stream_uri(vm_name: str, project_id: str | None = None) -> str:
    """Return the canonical GCS URI for a VM's live log stream.

    Live logs are streamed to this path every 30s by heartbeat_daemon.py.
    This prefix has a 14-day delete lifecycle.

    Returns: gs://deployment-scripts-{project}/vm-logs/{vm}/run.log

    This function IS the SSOT for the VM live-log path shape — callers must
    use it instead of constructing the URI themselves (the analogue of the
    bucket-naming SSOT pattern). The scheme literal here is intentional.
    """
    bucket = f"deployment-scripts-{project_id}" if project_id else DEFAULT_BUCKET
    return f"gs://{bucket}/vm-logs/{vm_name}/run.log"  # noqa: gs-uri  — canonical VM live-log path SSOT


def vm_log_archive_uri(vm_name: str, timestamp: str, project_id: str | None = None) -> str:
    """Return the canonical GCS URI for a VM's archived logs.

    Archived logs persist indefinitely (no lifecycle rule).
    The timestamp should be in YYYYMMDD_HHMM format.

    Returns: gs://deployment-scripts-{project}/log-archive/snapshot_{timestamp}/{vm}/

    This function IS the SSOT for the VM archive-log path shape — callers must
    use it instead of constructing the URI themselves (the analogue of the
    bucket-naming SSOT pattern). The scheme literal here is intentional.
    """
    bucket = f"deployment-scripts-{project_id}" if project_id else DEFAULT_BUCKET
    return f"gs://{bucket}/log-archive/snapshot_{timestamp}/{vm_name}/"  # noqa: gs-uri  — canonical VM archive-log path SSOT


def vm_serial_console_archive_uri(vm_name: str, timestamp: str, project_id: str | None = None) -> str:
    """Return the canonical GCS URI for a VM's archived serial console output.

    Serial console is captured via GCP API and persists indefinitely.
    The timestamp should be in YYYYMMDD_HHMM format.

    Returns: gs://deployment-scripts-{project}/log-archive/snapshot_{timestamp}/{vm}/serial-console.txt
    """
    base = vm_log_archive_uri(vm_name, timestamp, project_id)
    return f"{base}serial-console.txt"


def vm_run_log_archive_uri(vm_name: str, timestamp: str, project_id: str | None = None) -> str:
    """Return the canonical GCS URI for a VM's archived run.log.

    Archived run.log persists indefinitely (no lifecycle rule).
    The timestamp should be in YYYYMMDD_HHMM format.

    Returns: gs://deployment-scripts-{project}/log-archive/snapshot_{timestamp}/{vm}/run.log
    """
    base = vm_log_archive_uri(vm_name, timestamp, project_id)
    return f"{base}run.log"


def vm_serial_rolling_uri(vm_name: str, date_stamp: str, project_id: str | None = None) -> str:
    """Return the canonical GCS URI for a long-lived VM's rolling serial capture.

    Distinct from the snapshot-style serial path (vm_serial_console_archive_uri):
    rolling captures run on a daily schedule for LONG_LIVED_LIVE / SCHEDULED_RECURRING VMs
    to preserve serial output before the ring buffer wraps.

    date_stamp should be YYYYMMDD format (daily granularity).

    Returns: gs://deployment-scripts-{project}/log-archive/serial-rolling/{date}/{vm}/serial-console.txt
    """
    bucket = f"deployment-scripts-{project_id}" if project_id else DEFAULT_BUCKET
    return f"gs://{bucket}/log-archive/serial-rolling/{date_stamp}/{vm_name}/serial-console.txt"  # noqa: gs-uri  — canonical rolling serial path SSOT


# Deployment registry bucket — derived from UnifiedCloudConfig.gcp_project_id.
# Empty string at import time = no GCP_PROJECT_ID set (e.g. unit tests); callers
# must pass ``bucket=`` explicitly in that case.
DEFAULT_BUCKET = _resolve_default_bucket()
ACTIVE_PREFIX = "deployments/active/"
ARCHIVE_PREFIX = "deployments/archive/"


@dataclass
class DeploymentRegistryEntry:  # CORRECT-LOCAL: service-internal registry model
    """One row of the VM deployments registry.

    Schema matches JSON written to GCS. Every field is typed — no `object`.
    """

    deployment_id: str
    vm_name: str
    asset_group: str
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
        row = asdict(self)
        # Legacy registry rows used ``category``; mirror for GCS readers not yet on ``asset_group``.
        row["category"] = row["asset_group"]
        return json.dumps(row, sort_keys=True)

    @classmethod
    def from_json(cls, payload: str) -> DeploymentRegistryEntry:
        data = cast(dict[str, object], json.loads(payload))
        extras_raw = cast(dict[str, object], data.get("extras", {}))
        extras = {str(k): str(v) for k, v in extras_raw.items()}
        return cls(
            deployment_id=str(data["deployment_id"]),
            vm_name=str(data["vm_name"]),
            asset_group=str(data.get("asset_group") or data.get("category") or ""),
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
            entry.asset_group,
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

    # ---- reaping ----------------------------------------------------------

    def reap_stale(
        self,
        max_age_hours: int = 6,
        running_vm_names: set[str] | None = None,
        now: datetime | None = None,
    ) -> list[DeploymentRegistryEntry]:
        """Archive orphan ``active/`` entries — VMs gone or heartbeat dead.

        An entry is considered stale when **either**:
          * ``running_vm_names`` is not ``None`` and ``entry.vm_name`` is NOT
            in that set (the GCE VM is gone / deleted / stopped), OR
          * ``now - last_heartbeat_at > max_age_hours`` (the heartbeat thread
            died but the VM may still be up — hard-kill or pre-emption).

        Each reaped entry is archived with ``status="failed"``,
        ``exit_code=125``, ``completed_at=now``, and
        ``extras["reap_reason"]`` set to ``"vm_not_running"`` or
        ``"heartbeat_stale"``. Returns the list of reaped entries.

        Clock-skew tolerance: heartbeats less than 5 minutes old are never
        considered stale even if ``running_vm_names`` says the VM is gone —
        avoids racing with still-booting VMs whose first heartbeat has not
        landed yet.
        """
        current = now or datetime.now(UTC)
        reaped: list[DeploymentRegistryEntry] = []
        skew_tolerance = timedelta(minutes=5)
        max_age = timedelta(hours=max_age_hours)

        for entry in self.list_active():
            last_hb = _parse_iso_utc(entry.last_heartbeat_at)
            if last_hb is None:
                logger.warning(
                    "reap_stale: skipping %s — unparseable last_heartbeat_at=%r",
                    entry.deployment_id,
                    entry.last_heartbeat_at,
                )
                continue
            age = current - last_hb
            if age < skew_tolerance:
                continue  # fresh heartbeat — never stale

            reason: str | None = None
            if running_vm_names is not None and entry.vm_name not in running_vm_names:
                reason = "vm_not_running"
            elif age > max_age:
                reason = "heartbeat_stale"

            if reason is None:
                continue

            completed_at = current.strftime("%Y-%m-%dT%H:%M:%SZ")
            entry.status = "failed"
            entry.exit_code = 125
            entry.completed_at = completed_at
            entry.last_heartbeat_at = completed_at
            extras = dict(entry.extras)
            extras["reap_reason"] = reason
            extras["reaped_at"] = completed_at
            entry.extras = extras

            try:
                self.complete(entry)
                reaped.append(entry)
                logger.info(
                    "reap_stale: archived %s (vm=%s, reason=%s, age=%s)",
                    entry.deployment_id,
                    entry.vm_name,
                    reason,
                    age,
                )
            except (OSError, ValueError, RuntimeError) as exc:  # shard-level isolation
                logger.warning(
                    "reap_stale: failed to archive %s: %s",
                    entry.deployment_id,
                    exc,
                )

        return reaped


def is_entry_stale(
    entry: DeploymentRegistryEntry,
    *,
    now: datetime,
    max_age_hours: int = 6,
    running_vm_names: set[str] | None = None,
) -> bool:
    """Classify a single registry entry as stale (reap-eligible) or fresh.

    Pure function — mirrors the logic inside ``reap_stale`` but exposed so
    callers (dry-run previews, admin inspectors) can ask without mutating
    state. Returns ``True`` iff the entry would be reaped by a corresponding
    ``reap_stale`` call at the same ``now``.
    """
    last_hb = _parse_iso_utc(entry.last_heartbeat_at)
    if last_hb is None:
        return False
    age = now - last_hb
    if age < timedelta(minutes=5):
        return False
    if running_vm_names is not None and entry.vm_name not in running_vm_names:
        return True
    return age > timedelta(hours=max_age_hours)


# ---- helpers --------------------------------------------------------------


def _utcnow_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso_utc(value: str | None) -> datetime | None:
    """Parse an ISO-8601 UTC timestamp (``YYYY-MM-DDTHH:MM:SSZ``) tolerantly.

    Returns ``None`` for empty / malformed input so callers can log-and-skip
    rather than raise — matches the shard-level isolation policy used by
    ``reap_stale``. ``Z`` suffix is accepted; internally we convert to the
    ``+00:00`` form ``datetime.fromisoformat`` recognises across Py3.10+.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _default_storage() -> _StorageClientLike:
    """Get the UTL-backed StorageClient wrapped to match `_StorageClientLike`."""
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
            raise FileNotFoundError(f"storage object not found at {bucket}/{key}")
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
    "vm_log_archive_uri",
    "vm_log_stream_uri",
    "vm_run_log_archive_uri",
    "vm_serial_console_archive_uri",
    "vm_serial_rolling_uri",
]
