"""Unit tests for the ad-hoc check-vm CLI entry point (DP-VM-006).

Credential-free: this module is self-contained (no cli.py imports — see
check_vm_cli.py's docstring), so each test patches ``check_vm_cli``'s own
names directly.
"""

from __future__ import annotations

import pytest

from deployment_service.data_pipeline_monitors import check_vm_cli
from tests.unit.test_data_pipeline_monitors import LOG_BUCKET, FakeStorage

_VM = "mtds-solana-drift-backfill"
_ZONE = "asia-northeast1-c"


def test_main_alive(monkeypatch):
    storage = FakeStorage({})
    monkeypatch.setattr(check_vm_cli, "get_storage_client", lambda: storage)
    monkeypatch.setattr(check_vm_cli, "_log_bucket", lambda: LOG_BUCKET)
    monkeypatch.setattr(check_vm_cli, "_find_running_vm", lambda _vm: (_VM, _ZONE))
    rc = check_vm_cli.main(["--vm-name", _VM])
    assert rc == 0


def test_main_stall_returns_nonzero(monkeypatch):
    storage = FakeStorage(
        {(LOG_BUCKET, f"vm-logs/{_VM}/run.log"): (b"2020-01-01 00:00:00,000 INFO last line before the OOM\n", None)}
    )
    monkeypatch.setattr(check_vm_cli, "get_storage_client", lambda: storage)
    monkeypatch.setattr(check_vm_cli, "_log_bucket", lambda: LOG_BUCKET)
    monkeypatch.setattr(check_vm_cli, "_find_running_vm", lambda _vm: (_VM, _ZONE))
    rc = check_vm_cli.main(["--vm-name", _VM])
    assert rc == 1


def test_main_not_running_is_benign(monkeypatch):
    storage = FakeStorage({})
    monkeypatch.setattr(check_vm_cli, "get_storage_client", lambda: storage)
    monkeypatch.setattr(check_vm_cli, "_log_bucket", lambda: LOG_BUCKET)
    monkeypatch.setattr(check_vm_cli, "_find_running_vm", lambda _vm: None)
    rc = check_vm_cli.main(["--vm-name", _VM])
    assert rc == 0


def test_main_missing_vm_name_errors():
    with pytest.raises(SystemExit):
        check_vm_cli.main([])
