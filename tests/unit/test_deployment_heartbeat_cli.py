"""
Unit tests for the ``deployment_heartbeat.py`` VM-side CLI.

The registry itself (``DeploymentsRegistry`` / ``DeploymentRegistryEntry`` / URI
helpers) relocated to UTL — see
``unified-trading-library/tests/unit/test_deployment_registry.py``. This file keeps
only the CLI integration test, which exercises deployment-service's own
``scripts/vm/deployment_heartbeat.py`` end to end.

All tests use `InMemoryStorageClient` so no GCS calls are made.
"""

from __future__ import annotations

import importlib.util
import pathlib
from types import ModuleType

import pytest
from unified_trading_library import (
    ACTIVE_PREFIX,
    ARCHIVE_PREFIX,
    DEFAULT_BUCKET,
    DeploymentRegistryEntry,
    DeploymentsRegistry,
    InMemoryStorageClient,
)


def _load_bom_module() -> ModuleType:
    """Load bom.py directly (stdlib-only at module level) — same isolation rationale.

    The package __init__ pulls in UAC's catalog which has a pre-existing
    ticker-registry import error unrelated to this module. Loading the file
    directly keeps these unit tests honest + isolated.
    """
    import sys

    path = pathlib.Path(__file__).resolve().parents[2] / "deployment_service" / "bom.py"
    module_name = "_bom_for_registry_tests"
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_bom_module = _load_bom_module()

_BOM_DIGEST = "sha256:" + "a" * 64


@pytest.fixture
def storage() -> InMemoryStorageClient:
    return InMemoryStorageClient()


# ----- heartbeat.py subcommand integration --------------------------------


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

    # Pre-install a stub `deployment_service` package so the CLI's other
    # `deployment_service.*` imports (bom, deployment_classification) resolve
    # without triggering the broken `deployment_service/__init__.py`.
    stub_pkg_name = "deployment_service"
    if stub_pkg_name not in sys.modules:
        stub_pkg = ModuleType(stub_pkg_name)
        stub_pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules[stub_pkg_name] = stub_pkg
    # The CLI also imports the BoM resolver — satisfy it with the direct-loaded module.
    sys.modules["deployment_service.bom"] = _bom_module

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET, storage=storage)
    monkeypatch.setattr(module, "_init_events", lambda config: None)
    monkeypatch.setattr(module, "_emit", lambda *a, **k: None)
    # Same "don't touch Pub/Sub" intent as the two mocks above — _build_run_summary_publisher
    # is a separate call site `cmd_complete` invokes directly (not through `_emit`).
    monkeypatch.setattr(module, "_build_run_summary_publisher", lambda config: None)
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
    entries = [
        DeploymentRegistryEntry.from_json(storage.download_string(DEFAULT_BUCKET, key))
        for key in storage.list_keys(DEFAULT_BUCKET, ACTIVE_PREFIX)
    ]
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
