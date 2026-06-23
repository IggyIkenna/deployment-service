"""Tests for VM event emission compliance (heartbeat_cli.py).

Verifies that:
  - heartbeat_cli.main() initialises the event sink via setup_events()
  - HeartbeatDaemon is constructed with the correct DEPLOYMENT_STARTED /
    DEPLOYMENT_COMPLETED / DEPLOYMENT_FAILED event-name constants
  - run_lifecycle() context manager is entered (STEP 5.63 compliance)
  - _vm_payload() includes all required wire-format fields

These tests provide coverage for deployment_service/vm/heartbeat_cli.py
which was previously at 0% (97 lines uncovered).
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REQUIRED_ARGV = [
    "--id",
    "deploy-test-uuid",
    "--name",
    "test-vm-001",
    "--asset-group",
    "defi",
    "--task",
    "features-backfill",
    "--mode",
    "backfill",
    "--start-date",
    "2026-01-01",
    "--end-date",
    "2026-01-31",
    "--log-uri",
    "gs://bucket/vm-logs/test-vm-001/run.log",
    "--local-log",
    "/tmp/test-backfill.log",
    "--exit-status-file",
    "/tmp/test-exit-status",
    "--stall-breadcrumb",
    "/tmp/test-stall",
    "--watchdog-file",
    "/tmp/test-watchdog",
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_main_calls_setup_events() -> None:
    """main() calls setup_events() — verifies event sink is initialised."""
    import deployment_service.vm.heartbeat_cli as hc

    setup_ev = MagicMock()
    daemon_mock = MagicMock()
    daemon_mock.run.return_value = 0

    @contextmanager
    def _fake_run_lifecycle(**_: Any):
        yield

    with (
        patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"}),
        patch("deployment_service.vm.heartbeat_cli.setup_events", setup_ev),
        patch("deployment_service.vm.heartbeat_cli.PubSubEventSink"),
        patch("deployment_service.vm.heartbeat_cli.run_lifecycle", _fake_run_lifecycle),
        patch("deployment_service.vm.heartbeat_cli.HeartbeatDaemon", return_value=daemon_mock),
        patch("deployment_service.vm.heartbeat_cli.DeploymentsRegistry"),
        patch("deployment_service.vm.heartbeat_cli.get_storage_client"),
    ):
        rc = hc.main(_REQUIRED_ARGV)

    setup_ev.assert_called_once()
    assert rc == 0


def test_main_constructs_daemon_with_deployment_events() -> None:
    """HeartbeatDaemon is created with DEPLOYMENT_STARTED/COMPLETED/FAILED constants."""
    from unified_trading_library import (
        DEPLOYMENT_COMPLETED,
        DEPLOYMENT_FAILED,
        DEPLOYMENT_STARTED,
    )

    import deployment_service.vm.heartbeat_cli as hc

    daemon_mock = MagicMock()
    daemon_mock.run.return_value = 0
    daemon_ctor = MagicMock(return_value=daemon_mock)

    @contextmanager
    def _fake_run_lifecycle(**_: Any):
        yield

    with (
        patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"}),
        patch("deployment_service.vm.heartbeat_cli.setup_events"),
        patch("deployment_service.vm.heartbeat_cli.PubSubEventSink"),
        patch("deployment_service.vm.heartbeat_cli.run_lifecycle", _fake_run_lifecycle),
        patch("deployment_service.vm.heartbeat_cli.HeartbeatDaemon", daemon_ctor),
        patch("deployment_service.vm.heartbeat_cli.DeploymentsRegistry"),
        patch("deployment_service.vm.heartbeat_cli.get_storage_client"),
    ):
        hc.main(_REQUIRED_ARGV)

    daemon_ctor.assert_called_once()
    call_kwargs = daemon_ctor.call_args.kwargs
    assert call_kwargs["started_event"] == DEPLOYMENT_STARTED
    assert call_kwargs["completed_event"] == DEPLOYMENT_COMPLETED
    assert call_kwargs["failed_event"] == DEPLOYMENT_FAILED
    # Freshness-ceiling wiring (data_pipeline_hardening_self_monitoring 2026-06-22):
    # the GCS run.log staleness ceiling MUST be forwarded to the daemon so a
    # slow-but-live log can't freeze its GCS copy. Regression guard for the freeze.
    assert call_kwargs["upload_max_staleness_sec"] == 90


def test_run_lifecycle_is_entered() -> None:
    """run_lifecycle() context manager is entered — STEP 5.63 compliance."""
    import deployment_service.vm.heartbeat_cli as hc

    entered = False

    @contextmanager
    def _tracking_lifecycle(**_: Any):
        nonlocal entered
        entered = True
        yield

    daemon_mock = MagicMock()
    daemon_mock.run.return_value = 0

    with (
        patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"}),
        patch("deployment_service.vm.heartbeat_cli.setup_events"),
        patch("deployment_service.vm.heartbeat_cli.PubSubEventSink"),
        patch("deployment_service.vm.heartbeat_cli.run_lifecycle", _tracking_lifecycle),
        patch("deployment_service.vm.heartbeat_cli.HeartbeatDaemon", return_value=daemon_mock),
        patch("deployment_service.vm.heartbeat_cli.DeploymentsRegistry"),
        patch("deployment_service.vm.heartbeat_cli.get_storage_client"),
    ):
        hc.main(_REQUIRED_ARGV)

    assert entered, "run_lifecycle() was not entered — STEP 5.63 compliance broken"


def test_vm_payload_required_fields() -> None:
    """_vm_payload() output includes all required wire-format fields."""
    from unified_trading_library import HeartbeatEntry

    from deployment_service.vm.heartbeat_cli import _vm_payload

    entry = HeartbeatEntry(
        deployment_id="test-uuid",
        service_name="test-vm",
        status="running",
        started_at="2026-01-01T00:00:00Z",
        last_heartbeat_at="2026-01-01T00:01:00Z",
        metadata={
            "vm_name": "test-vm",
            "asset_group": "defi",
            "task": "features-backfill",
            "mode": "backfill",
            "start_date": "2026-01-01",
            "end_date": "2026-01-31",
            "log_uri": "gs://bucket/logs/run.log",
        },
        counters={"rows_in": 100, "rows_out": 95, "rows_error": 0, "events_emitted": 5},
    )

    payload = _vm_payload(entry)

    required = {
        "deployment_id",
        "vm_name",
        "asset_group",
        "task",
        "mode",
        "start_date",
        "end_date",
        "status",
        "rows_in",
        "rows_out",
        "rows_error",
        "events_emitted",
        "exit_code",
        "log_uri",
    }
    missing = required - set(payload.keys())
    assert not missing, f"_vm_payload missing fields: {missing}"
    assert payload["asset_group"] == "defi"
    assert payload["rows_in"] == 100


def test_entry_to_registry_roundtrip() -> None:
    """_entry_to_registry / _registry_to_entry preserve all fields."""
    from unified_trading_library import HeartbeatEntry

    from deployment_service.vm.heartbeat_cli import _entry_to_registry, _registry_to_entry

    entry = HeartbeatEntry(
        deployment_id="roundtrip-uuid",
        service_name="roundtrip-vm",
        status="completed",
        started_at="2026-01-01T00:00:00Z",
        last_heartbeat_at="2026-01-01T01:00:00Z",
        completed_at="2026-01-01T01:00:00Z",
        exit_code=0,
        counters={"rows_in": 200, "rows_out": 200, "rows_error": 0, "events_emitted": 10},
        metadata={
            "vm_name": "roundtrip-vm",
            "asset_group": "cefi",
            "task": "mdps-backfill",
            "mode": "full",
            "start_date": "2026-01-01",
            "end_date": "2026-01-31",
            "log_uri": "gs://bucket/logs/run.log",
        },
    )

    reg = _entry_to_registry(entry)
    assert reg.deployment_id == "roundtrip-uuid"
    assert reg.asset_group == "cefi"
    assert reg.rows_in == 200
    assert reg.exit_code == 0

    restored = _registry_to_entry(reg)
    assert restored.deployment_id == "roundtrip-uuid"
    assert restored.counters["rows_in"] == 200
