"""
Unit tests for deployments_registry + the deployment_heartbeat helper.

All tests use `InMemoryStorageClient` so no GCS calls are made.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from types import ModuleType

import pytest


def _load_registry_module() -> ModuleType:
    """Load deployments_registry.py directly so we bypass deployment_service/__init__.py.

    The package __init__ pulls in UAC's catalog which has a pre-existing
    ticker-registry import error unrelated to this module. Loading the file
    directly keeps these unit tests honest + isolated.
    """
    import sys

    path = (
        pathlib.Path(__file__).resolve().parents[2]
        / "deployment_service"
        / "deployments_registry.py"
    )
    module_name = "_deployments_registry_under_test"
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # Dataclasses inspect sys.modules[__name__] during decoration, so register first.
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_registry_module = _load_registry_module()
ACTIVE_PREFIX = _registry_module.ACTIVE_PREFIX
ARCHIVE_PREFIX = _registry_module.ARCHIVE_PREFIX
DEFAULT_BUCKET = _registry_module.DEFAULT_BUCKET
DeploymentRegistryEntry = _registry_module.DeploymentRegistryEntry
DeploymentsRegistry = _registry_module.DeploymentsRegistry
InMemoryStorageClient = _registry_module.InMemoryStorageClient


@pytest.fixture
def storage() -> InMemoryStorageClient:
    return InMemoryStorageClient()


@pytest.fixture
def registry(storage: InMemoryStorageClient) -> DeploymentsRegistry:
    return DeploymentsRegistry(bucket=DEFAULT_BUCKET, storage=storage)


def _make_entry(**overrides: object) -> DeploymentRegistryEntry:
    base: dict[str, object] = {
        "deployment_id": "dep-abc-123",
        "vm_name": "canonical-migration-cefi-20260418-042359",
        "category": "CEFI",
        "task": "canonical-migration",
        "mode": "dry",
        "start_date": "2024-06-01",
        "end_date": "2024-06-30",
        "status": "running",
        "started_at": "2026-04-18T04:23:59Z",
        "last_heartbeat_at": "2026-04-18T04:23:59Z",
        "completed_at": None,
        "exit_code": None,
        "rows_in": 0,
        "rows_out": 0,
        "rows_error": 0,
        "events_emitted": 1,
        "log_uri": "gs://bucket/vm-logs/x/run.log",
    }
    base.update(overrides)
    return DeploymentRegistryEntry(**base)  # type: ignore[arg-type]


def test_register_writes_to_active(
    registry: DeploymentsRegistry, storage: InMemoryStorageClient
) -> None:
    entry = _make_entry()
    registry.register(entry)
    keys = storage.list_keys(DEFAULT_BUCKET, ACTIVE_PREFIX)
    assert keys == [f"{ACTIVE_PREFIX}{entry.deployment_id}.json"]
    stored = json.loads(storage.download_string(DEFAULT_BUCKET, keys[0]))
    assert stored["status"] == "running"
    assert stored["rows_in"] == 0


def test_heartbeat_overwrites_active_entry(
    registry: DeploymentsRegistry, storage: InMemoryStorageClient
) -> None:
    entry = _make_entry()
    registry.register(entry)
    entry.rows_in = 100
    entry.rows_out = 99
    entry.last_heartbeat_at = "2026-04-18T04:28:59Z"
    registry.heartbeat(entry)
    key = f"{ACTIVE_PREFIX}{entry.deployment_id}.json"
    stored = json.loads(storage.download_string(DEFAULT_BUCKET, key))
    assert stored["rows_in"] == 100
    assert stored["rows_out"] == 99
    assert stored["last_heartbeat_at"] == "2026-04-18T04:28:59Z"
    # Still only one active entry — heartbeat overwrites, never dupes.
    assert storage.list_keys(DEFAULT_BUCKET, ACTIVE_PREFIX) == [key]


def test_complete_moves_from_active_to_archive(
    registry: DeploymentsRegistry, storage: InMemoryStorageClient
) -> None:
    entry = _make_entry()
    registry.register(entry)
    entry.status = "completed"
    entry.exit_code = 0
    entry.completed_at = "2026-04-18T05:00:00Z"
    registry.complete(entry)

    active = storage.list_keys(DEFAULT_BUCKET, ACTIVE_PREFIX)
    assert active == [], "active entry must be deleted after complete()"
    archive = storage.list_keys(DEFAULT_BUCKET, ARCHIVE_PREFIX)
    assert archive == [f"{ARCHIVE_PREFIX}2026-04-18/{entry.deployment_id}.json"]
    stored = json.loads(storage.download_string(DEFAULT_BUCKET, archive[0]))
    assert stored["status"] == "completed"
    assert stored["exit_code"] == 0


def test_complete_rejects_non_terminal_status(registry: DeploymentsRegistry) -> None:
    entry = _make_entry()
    registry.register(entry)
    entry.status = "running"  # still not terminal
    with pytest.raises(ValueError):
        registry.complete(entry)


def test_failed_deployment_routes_to_archive(
    registry: DeploymentsRegistry, storage: InMemoryStorageClient
) -> None:
    entry = _make_entry(deployment_id="dep-fail-1")
    registry.register(entry)
    entry.status = "failed"
    entry.exit_code = 1
    entry.rows_error = 42
    entry.completed_at = "2026-04-18T06:00:00Z"
    registry.complete(entry)
    archive_key = f"{ARCHIVE_PREFIX}2026-04-18/dep-fail-1.json"
    stored = json.loads(storage.download_string(DEFAULT_BUCKET, archive_key))
    assert stored["status"] == "failed"
    assert stored["rows_error"] == 42


def test_list_active_skips_malformed(
    registry: DeploymentsRegistry, storage: InMemoryStorageClient
) -> None:
    good = _make_entry(deployment_id="good")
    registry.register(good)
    # Injecting a bad payload should not blow up list_active()
    storage.upload_string(DEFAULT_BUCKET, f"{ACTIVE_PREFIX}broken.json", "{not json")
    entries = registry.list_active()
    assert [e.deployment_id for e in entries] == ["good"]


def test_list_recent_archive_covers_window(
    registry: DeploymentsRegistry, storage: InMemoryStorageClient
) -> None:
    # Seed 3 archive entries across 3 different days, all within 7-day window.
    today = datetime.now(UTC).date()
    for offset in range(3):
        day = (today - timedelta(days=offset)).isoformat()
        entry = _make_entry(
            deployment_id=f"dep-{offset}",
            status="completed",
            exit_code=0,
            completed_at=f"{day}T05:00:00Z",
        )
        storage.upload_string(
            DEFAULT_BUCKET,
            f"{ARCHIVE_PREFIX}{day}/{entry.deployment_id}.json",
            entry.to_json(),
        )
    entries = registry.list_recent_archive(days=7)
    ids = sorted(e.deployment_id for e in entries)
    assert ids == ["dep-0", "dep-1", "dep-2"]


def test_get_returns_active_then_archive(
    registry: DeploymentsRegistry, storage: InMemoryStorageClient
) -> None:
    live = _make_entry(deployment_id="live-1")
    registry.register(live)
    # Put an unrelated archive entry from today so list_recent_archive has data.
    today = datetime.now(UTC).date().isoformat()
    archived = _make_entry(
        deployment_id="arch-1",
        status="completed",
        exit_code=0,
        completed_at=f"{today}T00:00:00Z",
    )
    storage.upload_string(
        DEFAULT_BUCKET,
        f"{ARCHIVE_PREFIX}{today}/{archived.deployment_id}.json",
        archived.to_json(),
    )

    assert registry.get("live-1") is not None
    assert registry.get("live-1").deployment_id == "live-1"  # type: ignore[union-attr]
    assert registry.get("arch-1") is not None
    assert registry.get("arch-1").deployment_id == "arch-1"  # type: ignore[union-attr]
    assert registry.get("nope") is None


def test_round_trip_json_preserves_fields() -> None:
    entry = _make_entry(rows_in=5, rows_out=4, rows_error=1, events_emitted=17)
    restored = DeploymentRegistryEntry.from_json(entry.to_json())
    assert restored == entry


# ----- heartbeat.py subcommand integration --------------------------------


def _iter_registered(
    storage: InMemoryStorageClient, bucket: str = DEFAULT_BUCKET
) -> Iterator[DeploymentRegistryEntry]:
    for key in storage.list_keys(bucket, ACTIVE_PREFIX):
        yield DeploymentRegistryEntry.from_json(storage.download_string(bucket, key))


def test_heartbeat_cli_register_then_complete(
    storage: InMemoryStorageClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Load the heartbeat CLI directly. We inject:
    #   - a pre-built DeploymentsRegistry bound to our in-memory storage
    #   - no-op _init_events / _emit so the test doesn't touch Pub/Sub.
    import sys

    spec = importlib.util.spec_from_file_location(
        "deployment_heartbeat_under_test",
        str(
            pathlib.Path(__file__).resolve().parents[2]
            / "scripts"
            / "vm"
            / "deployment_heartbeat.py"
        ),
    )
    assert spec is not None and spec.loader is not None

    # Pre-install a stub `deployment_service.deployments_registry` module so
    # the CLI's top-level `from deployment_service.deployments_registry import …`
    # resolves without triggering the broken `deployment_service/__init__.py`.
    stub_pkg_name = "deployment_service"
    stub_reg_name = "deployment_service.deployments_registry"
    if stub_pkg_name not in sys.modules:
        stub_pkg = ModuleType(stub_pkg_name)
        stub_pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules[stub_pkg_name] = stub_pkg
    sys.modules[stub_reg_name] = _registry_module

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET, storage=storage)
    monkeypatch.setattr(module, "_init_events", lambda: None)
    monkeypatch.setattr(module, "_emit", lambda *a, **k: None)
    monkeypatch.setattr(module, "DeploymentsRegistry", lambda bucket: registry)

    rc = module.main(
        [
            "register",
            "--id",
            "cli-1",
            "--name",
            "vm-x",
            "--category",
            "CEFI",
            "--task",
            "smoke",
            "--mode",
            "smoke",
            "--start-date",
            "2024-06-01",
            "--end-date",
            "2024-06-01",
            "--log-uri",
            "gs://bucket/vm-logs/vm-x/run.log",
        ]
    )
    assert rc == 0
    entries = list(_iter_registered(storage))
    assert len(entries) == 1
    assert entries[0].deployment_id == "cli-1"
    assert entries[0].status == "running"

    rc = module.main(
        [
            "complete",
            "--id",
            "cli-1",
            "--exit-code",
            "0",
            "--rows-in",
            "10",
            "--rows-out",
            "10",
            "--rows-error",
            "0",
        ]
    )
    assert rc == 0
    # Active must be empty now; archive must contain the entry.
    assert storage.list_keys(DEFAULT_BUCKET, ACTIVE_PREFIX) == []
    archived = storage.list_keys(DEFAULT_BUCKET, ARCHIVE_PREFIX)
    assert len(archived) == 1
    final = DeploymentRegistryEntry.from_json(storage.download_string(DEFAULT_BUCKET, archived[0]))
    assert final.status == "completed"
    assert final.exit_code == 0
    assert final.rows_in == 10
