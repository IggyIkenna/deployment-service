"""Unit tests for the heartbeat-sidecar reliability CLI entry point (DP-VM-008).

Credential-free: this module is self-contained (no cli.py imports — see
heartbeat_sidecar_reliability_cli.py's docstring), so each test patches
``heartbeat_sidecar_reliability_cli``'s own names directly. The watchdog-module
deferred import is patched out via ``_default_threshold_resolver`` so tests never
depend on ``google.cloud.compute_v1`` being importable / on the VM-side script.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from deployment_service.data_pipeline_monitors import _gcs
from deployment_service.data_pipeline_monitors import heartbeat_sidecar_reliability_cli as cli
from tests.unit.test_data_pipeline_monitors import LOG_BUCKET, FakeStorage

_VM = "af-backfill-20260717-151237"
_KILL_ISO = "2026-07-18T09:18:00+00:00"


def _heartbeat_blob(vm: str) -> str:
    return _gcs.HEARTBEAT_BLOB.format(vm=vm)


def _patch_common(monkeypatch, storage: FakeStorage) -> None:
    monkeypatch.setattr(cli, "get_storage_client", lambda: storage)
    monkeypatch.setattr(cli, "_log_bucket", lambda: LOG_BUCKET)
    monkeypatch.setattr(cli, "_default_threshold_resolver", lambda: lambda _vm: 10.0)


def test_main_genuinely_stale_returns_zero(monkeypatch):
    kill_dt = datetime.fromisoformat(_KILL_ISO)
    write_epoch = int((kill_dt - timedelta(minutes=25)).timestamp())
    storage = FakeStorage({(LOG_BUCKET, _heartbeat_blob(_VM)): (f"{write_epoch}\n-1\nrunning".encode(), None)})
    _patch_common(monkeypatch, storage)
    rc = cli.main(["--vm-name", _VM, "--kill-time", _KILL_ISO])
    assert rc == 0


def test_main_reliability_gap_returns_nonzero(monkeypatch):
    kill_dt = datetime.fromisoformat(_KILL_ISO)
    write_epoch = int((kill_dt - timedelta(seconds=16)).timestamp())
    storage = FakeStorage({(LOG_BUCKET, _heartbeat_blob(_VM)): (f"{write_epoch}\n-1\nrunning".encode(), None)})
    _patch_common(monkeypatch, storage)
    rc = cli.main(["--vm-name", _VM, "--kill-time", _KILL_ISO])
    assert rc == 1


def test_main_no_blob_returns_zero(monkeypatch):
    _patch_common(monkeypatch, FakeStorage({}))
    rc = cli.main(["--vm-name", _VM, "--kill-time", _KILL_ISO])
    assert rc == 0


def test_main_stall_minutes_override(monkeypatch):
    # Blob written 5min before kill; default 10min threshold would call this
    # GENUINELY_STALE-safe-side (age < threshold => RELIABILITY_GAP), but a
    # --stall-minutes 1 override should flip it to GENUINELY_STALE (age >= 1min).
    kill_dt = datetime.fromisoformat(_KILL_ISO)
    write_epoch = int((kill_dt - timedelta(minutes=5)).timestamp())
    storage = FakeStorage({(LOG_BUCKET, _heartbeat_blob(_VM)): (f"{write_epoch}\n-1\nrunning".encode(), None)})
    _patch_common(monkeypatch, storage)
    rc = cli.main(["--vm-name", _VM, "--kill-time", _KILL_ISO, "--stall-minutes", "1"])
    assert rc == 0


def test_main_batch_json_mode(monkeypatch, tmp_path):
    kill_dt = datetime.fromisoformat(_KILL_ISO)
    stale_vm = "cefi-hyperliquid-2023-20260623-113700"
    gap_vm = _VM
    storage = FakeStorage(
        {
            (LOG_BUCKET, _heartbeat_blob(stale_vm)): (
                f"{int((kill_dt - timedelta(minutes=25)).timestamp())}\n-1\nrunning".encode(),
                None,
            ),
            (LOG_BUCKET, _heartbeat_blob(gap_vm)): (
                f"{int((kill_dt - timedelta(seconds=16)).timestamp())}\n-1\nrunning".encode(),
                None,
            ),
        }
    )
    _patch_common(monkeypatch, storage)
    manifest = tmp_path / "killed_vms.json"
    manifest.write_text(
        json.dumps(
            [
                {"vm_name": stale_vm, "kill_time": _KILL_ISO},
                {"vm_name": gap_vm, "kill_time": _KILL_ISO},
            ]
        ),
        encoding="utf-8",
    )
    rc = cli.main(["--killed-vms-json", str(manifest)])
    assert rc == 1  # gap_vm drags the batch to nonzero


def test_main_batch_json_per_entry_threshold_override(monkeypatch, tmp_path):
    kill_dt = datetime.fromisoformat(_KILL_ISO)
    write_epoch = int((kill_dt - timedelta(minutes=5)).timestamp())
    storage = FakeStorage({(LOG_BUCKET, _heartbeat_blob(_VM)): (f"{write_epoch}\n-1\nrunning".encode(), None)})
    _patch_common(monkeypatch, storage)
    manifest = tmp_path / "killed_vms.json"
    manifest.write_text(
        json.dumps([{"vm_name": _VM, "kill_time": _KILL_ISO, "stall_threshold_min": 1.0}]), encoding="utf-8"
    )
    rc = cli.main(["--killed-vms-json", str(manifest)])
    assert rc == 0  # 5min age clears the entry's own 1min override -> genuinely stale


def test_main_requires_vm_name_and_kill_time_together():
    with pytest.raises(SystemExit):
        cli.main(["--vm-name", _VM])


def test_main_requires_some_mode():
    with pytest.raises(SystemExit):
        cli.main([])
