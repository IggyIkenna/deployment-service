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

    path = pathlib.Path(__file__).resolve().parents[2] / "deployment_service" / "deployments_registry.py"
    module_name = "_deployments_registry_under_test"
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # Dataclasses inspect sys.modules[__name__] during decoration, so register first.
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_bom_module() -> ModuleType:
    """Load bom.py directly (stdlib-only at module level) — same isolation rationale."""
    import sys

    path = pathlib.Path(__file__).resolve().parents[2] / "deployment_service" / "bom.py"
    module_name = "_bom_for_registry_tests"
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_registry_module = _load_registry_module()
_bom_module = _load_bom_module()
ACTIVE_PREFIX = _registry_module.ACTIVE_PREFIX
ARCHIVE_PREFIX = _registry_module.ARCHIVE_PREFIX
DEFAULT_BUCKET = _registry_module.DEFAULT_BUCKET
DeploymentRegistryEntry = _registry_module.DeploymentRegistryEntry
DeploymentsRegistry = _registry_module.DeploymentsRegistry
InMemoryStorageClient = _registry_module.InMemoryStorageClient
vm_log_stream_uri = _registry_module.vm_log_stream_uri
vm_log_archive_uri = _registry_module.vm_log_archive_uri
vm_serial_console_archive_uri = _registry_module.vm_serial_console_archive_uri
vm_run_log_archive_uri = _registry_module.vm_run_log_archive_uri
vm_serial_rolling_uri = _registry_module.vm_serial_rolling_uri


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
        "asset_group": "CEFI",
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


def test_register_writes_to_active(registry: DeploymentsRegistry, storage: InMemoryStorageClient) -> None:
    entry = _make_entry()
    registry.register(entry)
    keys = storage.list_keys(DEFAULT_BUCKET, ACTIVE_PREFIX)
    assert keys == [f"{ACTIVE_PREFIX}{entry.deployment_id}.json"]
    stored = json.loads(storage.download_string(DEFAULT_BUCKET, keys[0]))
    assert stored["status"] == "running"
    assert stored["rows_in"] == 0


def test_heartbeat_overwrites_active_entry(registry: DeploymentsRegistry, storage: InMemoryStorageClient) -> None:
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


def test_complete_moves_from_active_to_archive(registry: DeploymentsRegistry, storage: InMemoryStorageClient) -> None:
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


def test_failed_deployment_routes_to_archive(registry: DeploymentsRegistry, storage: InMemoryStorageClient) -> None:
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


def test_list_active_skips_malformed(registry: DeploymentsRegistry, storage: InMemoryStorageClient) -> None:
    good = _make_entry(deployment_id="good")
    registry.register(good)
    # Injecting a bad payload should not blow up list_active()
    storage.upload_string(DEFAULT_BUCKET, f"{ACTIVE_PREFIX}broken.json", "{not json")
    entries = registry.list_active()
    assert [e.deployment_id for e in entries] == ["good"]


def test_list_recent_archive_covers_window(registry: DeploymentsRegistry, storage: InMemoryStorageClient) -> None:
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


def test_get_returns_active_then_archive(registry: DeploymentsRegistry, storage: InMemoryStorageClient) -> None:
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


# ----- bill-of-materials (BoM) provenance fields ---------------------------

_BOM_DIGEST = "sha256:" + "a" * 64
_BOM_BASE_DIGEST = "sha256:" + "b" * 64


def test_bom_fields_round_trip() -> None:
    entry = _make_entry(
        image_digest=_BOM_DIGEST,
        git_commit="deadbee",
        dep_versions={
            "unified-trading-library": "0.5.0",
            "unified-api-contracts": "0.6.0",
            "base_image_digest": _BOM_BASE_DIGEST,
        },
    )
    restored = DeploymentRegistryEntry.from_json(entry.to_json())
    assert restored == entry
    assert restored.image_digest == _BOM_DIGEST
    assert restored.git_commit == "deadbee"
    assert restored.dep_versions["unified-trading-library"] == "0.5.0"
    assert restored.dep_versions["base_image_digest"] == _BOM_BASE_DIGEST


def test_legacy_row_without_bom_fields_loads_with_defaults() -> None:
    # A pre-BoM GCS row (written before 2026-06-10) — none of the BoM keys exist.
    legacy = {
        "deployment_id": "dep-legacy-1",
        "vm_name": "vm-legacy",
        "category": "CEFI",  # pre-asset_group rows used category only
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
    restored = DeploymentRegistryEntry.from_json(json.dumps(legacy))
    assert restored.deployment_id == "dep-legacy-1"
    assert restored.asset_group == "CEFI"
    assert restored.image_digest == ""
    assert restored.git_commit == ""
    assert restored.dep_versions == {}


def test_register_persists_bom_fields(registry: DeploymentsRegistry, storage: InMemoryStorageClient) -> None:
    entry = _make_entry(
        image_digest=_BOM_DIGEST,
        git_commit="abc1234",
        dep_versions={"unified-trading-library": "0.5.0"},
    )
    registry.register(entry)
    stored = json.loads(storage.download_string(DEFAULT_BUCKET, f"{ACTIVE_PREFIX}{entry.deployment_id}.json"))
    assert stored["image_digest"] == _BOM_DIGEST
    assert stored["git_commit"] == "abc1234"
    assert stored["dep_versions"] == {"unified-trading-library": "0.5.0"}


# ----- D.1 host metric vector (deployment_obs_backend_kinds_health_2026_07_09) ---


def test_host_metrics_fields_round_trip() -> None:
    entry = _make_entry(
        cpu_pct=42.5,
        mem_pct=67.25,
        mem_slope=1.5,
        disk_pct=88.0,
        io_write_rate_bytes_sec=1024.0,
        net_recv_rate_bytes_sec=2048.0,
    )
    restored = DeploymentRegistryEntry.from_json(entry.to_json())
    assert restored == entry
    assert restored.cpu_pct == 42.5
    assert restored.mem_slope == 1.5
    assert restored.net_recv_rate_bytes_sec == 2048.0


def test_legacy_row_without_host_metrics_loads_with_zero_defaults() -> None:
    # A pre-D.1 GCS row (written before 2026-07-09) — none of the host-metric keys exist.
    legacy = {
        "deployment_id": "dep-legacy-2",
        "vm_name": "vm-legacy",
        "asset_group": "CEFI",
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
    restored = DeploymentRegistryEntry.from_json(json.dumps(legacy))
    assert restored.deployment_id == "dep-legacy-2"
    assert restored.cpu_pct == 0.0
    assert restored.mem_pct == 0.0
    assert restored.mem_slope == 0.0
    assert restored.disk_pct == 0.0
    assert restored.io_write_rate_bytes_sec == 0.0
    assert restored.net_recv_rate_bytes_sec == 0.0


# ----- heartbeat.py subcommand integration --------------------------------


def _iter_registered(storage: InMemoryStorageClient, bucket: str = DEFAULT_BUCKET) -> Iterator[DeploymentRegistryEntry]:
    for key in storage.list_keys(bucket, ACTIVE_PREFIX):
        yield DeploymentRegistryEntry.from_json(storage.download_string(bucket, key))


def test_heartbeat_cli_register_then_complete(storage: InMemoryStorageClient, monkeypatch: pytest.MonkeyPatch) -> None:
    # Load the heartbeat CLI directly. We inject:
    #   - a pre-built DeploymentsRegistry bound to our in-memory storage
    #   - no-op _init_events / _emit so the test doesn't touch Pub/Sub.
    import sys

    spec = importlib.util.spec_from_file_location(
        "deployment_heartbeat_under_test",
        str(pathlib.Path(__file__).resolve().parents[2] / "scripts" / "vm" / "deployment_heartbeat.py"),
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
    # The CLI also imports the BoM resolver — satisfy it with the direct-loaded module.
    sys.modules["deployment_service.bom"] = _bom_module

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET, storage=storage)
    monkeypatch.setattr(module, "_init_events", lambda: None)
    monkeypatch.setattr(module, "_emit", lambda *a, **k: None)
    monkeypatch.setattr(module, "DeploymentsRegistry", lambda bucket: registry)
    # Deterministic BoM — resolve_deployment_bom(config=None) would otherwise
    # lazily import DeploymentConfig through the stub package and fail.
    stub_bom = _bom_module.DeploymentBom(
        image_digest=_BOM_DIGEST,
        git_commit="cafebab",
        dep_versions={"unified-trading-library": "0.5.0"},
    )
    monkeypatch.setattr(module, "resolve_deployment_bom", lambda config=None: stub_bom)

    rc = module.main(
        [
            "register",
            "--id",
            "cli-1",
            "--name",
            "vm-x",
            "--asset-group",
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
    # BoM stamped at register time.
    assert entries[0].image_digest == _BOM_DIGEST
    assert entries[0].git_commit == "cafebab"
    assert entries[0].dep_versions == {"unified-trading-library": "0.5.0"}

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
    # BoM survives the heartbeat→complete round-trip (registry.get → from_json).
    assert final.image_digest == _BOM_DIGEST
    assert final.git_commit == "cafebab"
    assert final.dep_versions == {"unified-trading-library": "0.5.0"}


# ----- reap_stale / is_entry_stale / _parse_iso_utc ------------------------

_parse_iso_utc = _registry_module._parse_iso_utc
is_entry_stale = _registry_module.is_entry_stale


def test_parse_iso_utc_happy_path() -> None:
    parsed = _parse_iso_utc("2026-04-18T04:23:59Z")
    assert parsed is not None
    assert parsed.year == 2026 and parsed.month == 4 and parsed.day == 18
    assert parsed.tzinfo is not None


def test_parse_iso_utc_returns_none_for_empty_or_malformed() -> None:
    assert _parse_iso_utc(None) is None
    assert _parse_iso_utc("") is None
    assert _parse_iso_utc("not-a-date") is None


def test_is_entry_stale_fresh_heartbeat_returns_false() -> None:
    now = datetime.now(UTC)
    entry = _make_entry(
        last_heartbeat_at=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    assert is_entry_stale(entry, now=now, max_age_hours=6) is False


def test_is_entry_stale_stale_heartbeat_returns_true() -> None:
    now = datetime.now(UTC)
    old = now - timedelta(hours=7)
    entry = _make_entry(
        last_heartbeat_at=old.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    assert is_entry_stale(entry, now=now, max_age_hours=6) is True


def test_is_entry_stale_unparseable_heartbeat_returns_false() -> None:
    now = datetime.now(UTC)
    entry = _make_entry(last_heartbeat_at="garbage")
    assert is_entry_stale(entry, now=now, max_age_hours=6) is False


def test_is_entry_stale_vm_not_in_running_set_returns_true() -> None:
    now = datetime.now(UTC)
    # Recent heartbeat but VM not in running_vm_names → still stale (vm_not_running).
    # NB: within skew-tolerance (5min) it must NOT be stale regardless.
    old = now - timedelta(minutes=10)
    entry = _make_entry(
        vm_name="gone-vm",
        last_heartbeat_at=old.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    assert is_entry_stale(entry, now=now, max_age_hours=24, running_vm_names={"other-vm"}) is True
    # Same scenario within skew tolerance — must be fresh.
    fresh = now - timedelta(minutes=1)
    entry2 = _make_entry(
        vm_name="gone-vm",
        last_heartbeat_at=fresh.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    assert is_entry_stale(entry2, now=now, max_age_hours=24, running_vm_names={"other-vm"}) is False


def test_reap_stale_archives_stale_entries(registry: DeploymentsRegistry, storage: InMemoryStorageClient) -> None:
    now = datetime.now(UTC)
    old_hb = (now - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%SZ")
    fresh_hb = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    stale = _make_entry(deployment_id="stale-1", last_heartbeat_at=old_hb)
    fresh = _make_entry(deployment_id="fresh-1", last_heartbeat_at=fresh_hb)
    registry.register(stale)
    registry.register(fresh)

    reaped = registry.reap_stale(max_age_hours=6, now=now)
    reaped_ids = {e.deployment_id for e in reaped}
    assert reaped_ids == {"stale-1"}

    # Fresh entry must still be active.
    active_keys = storage.list_keys(DEFAULT_BUCKET, ACTIVE_PREFIX)
    assert active_keys == [f"{ACTIVE_PREFIX}fresh-1.json"]

    # Stale entry must be archived with reap metadata.
    archive_keys = storage.list_keys(DEFAULT_BUCKET, ARCHIVE_PREFIX)
    assert len(archive_keys) == 1
    stored = json.loads(storage.download_string(DEFAULT_BUCKET, archive_keys[0]))
    assert stored["status"] == "failed"
    assert stored["exit_code"] == 125
    assert stored["extras"]["reap_reason"] == "heartbeat_stale"


def test_reap_stale_uses_vm_name_signal(registry: DeploymentsRegistry, storage: InMemoryStorageClient) -> None:
    now = datetime.now(UTC)
    old_hb = (now - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    entry = _make_entry(deployment_id="gone-1", vm_name="gone-vm", last_heartbeat_at=old_hb)
    registry.register(entry)

    reaped = registry.reap_stale(max_age_hours=24, running_vm_names={"other-vm"}, now=now)
    assert [e.deployment_id for e in reaped] == ["gone-1"]
    archive_keys = storage.list_keys(DEFAULT_BUCKET, ARCHIVE_PREFIX)
    stored = json.loads(storage.download_string(DEFAULT_BUCKET, archive_keys[0]))
    assert stored["extras"]["reap_reason"] == "vm_not_running"


def test_reap_stale_skips_unparseable_heartbeat(registry: DeploymentsRegistry, storage: InMemoryStorageClient) -> None:
    entry = _make_entry(deployment_id="bad-hb", last_heartbeat_at="garbage")
    registry.register(entry)
    reaped = registry.reap_stale(max_age_hours=6, now=datetime.now(UTC))
    assert reaped == []
    # Active entry must be untouched.
    assert storage.list_keys(DEFAULT_BUCKET, ACTIVE_PREFIX) == [f"{ACTIVE_PREFIX}bad-hb.json"]


def test_vm_log_stream_uri_canonical_shape() -> None:
    """Pin the canonical live stream path format: vm-logs/{vm}/run.log."""
    assert vm_log_stream_uri("my-vm", "test-project") == "gs://deployment-scripts-test-project/vm-logs/my-vm/run.log"
    assert vm_log_stream_uri("other-vm", "prod-123") == "gs://deployment-scripts-prod-123/vm-logs/other-vm/run.log"
    # When project_id not provided, falls back to DEFAULT_BUCKET (whose value
    # depends on the active UnifiedCloudConfig — empty in fully sterile envs,
    # populated when GCP_PROJECT_ID is set via ADC / env).
    assert vm_log_stream_uri("test-vm") == f"gs://{DEFAULT_BUCKET}/vm-logs/test-vm/run.log"


def test_vm_log_archive_uri_canonical_shape() -> None:
    """Pin the canonical archive path format: log-archive/snapshot_{ts}/{vm}/."""
    assert (
        vm_log_archive_uri("my-vm", "20260527_0830", "test-project")
        == "gs://deployment-scripts-test-project/log-archive/snapshot_20260527_0830/my-vm/"
    )
    assert (
        vm_log_archive_uri("other-vm", "20260101_1200", "prod-456")
        == "gs://deployment-scripts-prod-456/log-archive/snapshot_20260101_1200/other-vm/"
    )
    # When project_id not provided, falls back to DEFAULT_BUCKET (env-dependent).
    assert (
        vm_log_archive_uri("test-vm", "20260527_1000")
        == f"gs://{DEFAULT_BUCKET}/log-archive/snapshot_20260527_1000/test-vm/"
    )


def test_vm_serial_console_archive_uri_canonical_shape() -> None:
    """Pin the canonical serial console archive path: .../{vm}/serial-console.txt."""
    assert (
        vm_serial_console_archive_uri("my-vm", "20260527_0830", "test-project")
        == "gs://deployment-scripts-test-project/log-archive/snapshot_20260527_0830/my-vm/serial-console.txt"
    )
    assert (
        vm_serial_console_archive_uri("other-vm", "20260101_1200", "prod-456")
        == "gs://deployment-scripts-prod-456/log-archive/snapshot_20260101_1200/other-vm/serial-console.txt"
    )


def test_vm_run_log_archive_uri_canonical_shape() -> None:
    """Pin the canonical run.log archive path: .../{vm}/run.log."""
    assert (
        vm_run_log_archive_uri("my-vm", "20260527_0830", "test-project")
        == "gs://deployment-scripts-test-project/log-archive/snapshot_20260527_0830/my-vm/run.log"
    )
    assert (
        vm_run_log_archive_uri("other-vm", "20260101_1200", "prod-456")
        == "gs://deployment-scripts-prod-456/log-archive/snapshot_20260101_1200/other-vm/run.log"
    )


def test_vm_serial_rolling_uri_canonical_shape() -> None:
    """Pin the serial-rolling path: log-archive/serial-rolling/{date}/{vm}/serial-console.txt."""
    assert (
        vm_serial_rolling_uri("strategy-live-20260530", "20260530", "test-project")
        == "gs://deployment-scripts-test-project/log-archive/serial-rolling/20260530/strategy-live-20260530/serial-console.txt"
    )
    assert (
        vm_serial_rolling_uri("mtds-live-defi-abc", "20260101", "prod-456")
        == "gs://deployment-scripts-prod-456/log-archive/serial-rolling/20260101/mtds-live-defi-abc/serial-console.txt"
    )
    # Distinct from the one-shot snapshot path (different prefix: serial-rolling vs snapshot_{ts}).
    assert "serial-rolling" in vm_serial_rolling_uri("vm", "20260530", "pid")
    assert "snapshot_" not in vm_serial_rolling_uri("vm", "20260530", "pid")
