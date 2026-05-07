# SCHEMA_PROVENANCE_EXEMPT: Service-internal types — not cross-repo contracts.
"""Periodic-tier cadence state + parsing helpers for sports trigger scheduler.

Extracted from ``sports_trigger_scheduler`` to keep that module under the
900-line codex cap. Two concerns live here:

  1. ``PeriodicTierState`` — GCS-backed `last_run` map for periodic tiers.
     Single JSON blob, overwrite on every persist. Chosen for parity with
     ``DeploymentsRegistry`` — keeps ops surface at one backend (no Firestore
     dependency added). SSOT: codex/02-data/sports-scheduling-and-sharding.md §8.

  2. YAML coercion / source-key resolution helpers used by the periodic
     dispatcher to parse strings out of ``configs/sports-trigger-tiers.yaml``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Protocol

from unified_trading_library import StorageClient, get_storage_client

from .deployment_config import DeploymentConfig

logger = logging.getLogger(__name__)

DEFAULT_STATE_KEY = "sports_scheduler_state/scheduler.json"


class SchedulerStateStorage(Protocol):
    """Tiny storage shim — matches ``InMemoryStorageClient`` from deployments_registry."""

    def upload_string(self, bucket: str, key: str, body: str) -> None: ...

    def download_string(self, bucket: str, key: str) -> str: ...


class _UTLStateAdapter:
    """Adapts UTL ``StorageClient`` to the ``SchedulerStateStorage`` Protocol."""

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


def _default_state_storage() -> SchedulerStateStorage:
    return _UTLStateAdapter(get_storage_client())


def resolve_state_bucket() -> str:
    """Default scheduler-state bucket — ``deployment-scripts-<project_id>``.

    Mirrors ``DeploymentsRegistry.DEFAULT_BUCKET`` convention. Derived from
    ``DeploymentConfig.project_id`` so it tracks the active environment.
    """
    return f"deployment-scripts-{DeploymentConfig().project_id}"


class PeriodicTierState:
    """GCS-backed ``last_run`` map for periodic tiers."""

    def __init__(
        self,
        *,
        bucket: str,
        key: str = DEFAULT_STATE_KEY,
        storage: SchedulerStateStorage | None = None,
    ) -> None:
        self._bucket = bucket
        self._key = key
        self._storage: SchedulerStateStorage = (
            storage if storage is not None else _default_state_storage()
        )
        self._last_run: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        try:
            raw = self._storage.download_string(self._bucket, self._key)
        except Exception as exc:
            # First-boot case: scheduler.json doesn't exist yet → GCS raises
            # google.cloud.storage.exceptions.InvalidResponse (404), UTL's
            # local provider raises FileNotFoundError, in-memory raises
            # KeyError. Treat ANY download failure as "no state yet" so the
            # daemon can start and write the first state file on its first
            # persist. Logging is INFO — this is the happy-path bootstrap.
            logger.info(
                "No existing scheduler state at gs://%s/%s (%s) — starting fresh",
                self._bucket,
                self._key,
                type(exc).__name__,
            )
            self._last_run = {}
            return
        try:
            data: object = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning(
                "Malformed scheduler state at gs://%s/%s: %s — starting fresh",
                self._bucket,
                self._key,
                exc,
            )
            self._last_run = {}
            return
        if not isinstance(data, dict):
            self._last_run = {}
            return
        entries = data.get("last_run", {})
        if not isinstance(entries, dict):
            self._last_run = {}
            return
        self._last_run = {str(k): str(v) for k, v in entries.items()}

    def get_last_run(self, tier_name: str) -> datetime | None:
        raw = self._last_run.get(tier_name)
        if raw is None:
            return None
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None

    def set_last_run(self, tier_name: str, when: datetime, *, persist: bool = True) -> None:
        self._last_run[tier_name] = when.isoformat()
        if persist:
            self._persist()

    def _persist(self) -> None:
        body = json.dumps({"last_run": dict(self._last_run)}, sort_keys=True)
        try:
            self._storage.upload_string(self._bucket, self._key, body)
        except (OSError, ValueError) as exc:
            logger.warning(
                "Failed to persist scheduler state to gs://%s/%s: %s", self._bucket, self._key, exc
            )

    def snapshot(self) -> dict[str, str]:
        """Return a copy of the internal last-run map (for tests / introspection)."""
        return dict(self._last_run)


# ---------------------------------------------------------------------------
# YAML value coercion + source-key resolution used by the periodic dispatcher
# ---------------------------------------------------------------------------


def as_int(value: object, *, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, (float, str)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    return default


def as_float(value: object, *, default: float) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    return default


def resolve_source_key(svc: dict[str, object]) -> str:
    """Pick the UAC data-source key for a reference-tier service.

    Preference order:
      1. explicit ``data_source`` field on the service config
      2. ``--sports-entity`` → adapter mapping
         (TRANSFERS→transfermarkt; XG→understat; WEATHER→open_meteo;
         others default to api_football)
      3. fallback: "api_football" (broadest denominator)
    """
    explicit = svc.get("data_source")
    if isinstance(explicit, str) and explicit:
        return explicit
    args_raw = svc.get("args", {})
    args: dict[str, object] = args_raw if isinstance(args_raw, dict) else {}
    entity = args.get("--sports-entity")
    if isinstance(entity, str):
        if entity == "TRANSFERS":
            return "transfermarkt"
        if entity == "XG":
            return "understat"
        if entity == "WEATHER":
            return "open_meteo"
    return "api_football"


__all__ = [
    "DEFAULT_STATE_KEY",
    "PeriodicTierState",
    "SchedulerStateStorage",
    "as_float",
    "as_int",
    "resolve_source_key",
    "resolve_state_bucket",
]
