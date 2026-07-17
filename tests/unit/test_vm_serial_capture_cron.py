"""Unit tests for vm_serial_capture_cron.py.

All tests mock the GCP Compute and Storage clients — no real GCP calls.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from unified_trading_library import vm_serial_rolling_uri

# ---------------------------------------------------------------------------
# Helper: build a minimal mock instance
# ---------------------------------------------------------------------------


def _make_instance(name: str, status: str = "RUNNING") -> MagicMock:
    inst = MagicMock()
    inst.name = name
    inst.status = status
    return inst


def _make_scoped(instances: list[MagicMock]) -> MagicMock:
    scoped = MagicMock()
    scoped.instances = instances
    return scoped


# ---------------------------------------------------------------------------
# Tests for _build_target_prefixes
# ---------------------------------------------------------------------------


def test_build_target_prefixes_returns_long_lived_and_scheduled() -> None:
    """Only LONG_LIVED_LIVE and SCHEDULED_RECURRING prefixes are returned."""
    from unified_api_contracts.canonical.crosscutting import LifecycleClass

    from scripts.vm.vm_serial_capture_cron import _build_target_prefixes

    prefixes = _build_target_prefixes()
    assert isinstance(prefixes, frozenset)
    assert len(prefixes) > 0

    # Spot-check: known LONG_LIVED_LIVE prefixes are included.
    assert "strategy-live-" in prefixes
    assert "mtds-live-defi-" in prefixes
    assert "agent-orch-vm-defi-" in prefixes

    # EPHEMERAL_BATCH prefixes must NOT appear.
    assert "cefi-mr-" not in prefixes
    assert "tradfi-bf-" not in prefixes


def test_is_target_vm_matches() -> None:
    """VMs with matching prefixes are identified as targets."""
    from scripts.vm.vm_serial_capture_cron import _is_target_vm

    prefixes = frozenset({"strategy-live-", "mtds-live-defi-"})
    assert _is_target_vm("strategy-live-20260530", prefixes)
    assert _is_target_vm("mtds-live-defi-abc123", prefixes)
    assert not _is_target_vm("cefi-mr-20260530", prefixes)
    assert not _is_target_vm("tradfi-bf-20260530", prefixes)


# ---------------------------------------------------------------------------
# Tests for capture_serial_for_long_lived_vms (dry-run)
# ---------------------------------------------------------------------------


def test_dry_run_counts_target_vms() -> None:
    """Dry-run returns 0 (success) and logs matching VMs without uploading."""
    from scripts.vm.vm_serial_capture_cron import capture_serial_for_long_lived_vms

    fake_zones = [
        (
            "zones/asia-northeast1-c",
            _make_scoped(
                [
                    _make_instance("strategy-live-20260530"),  # LONG_LIVED_LIVE — should match
                    _make_instance("cefi-mr-20260530"),  # EPHEMERAL_BATCH — should not match
                    _make_instance("strategy-live-staging", "TERMINATED"),  # not RUNNING — skip
                ]
            ),
        ),
        ("zones/asia-northeast1-b", _make_scoped([])),
    ]

    with (
        patch("scripts.vm.vm_serial_capture_cron.compute_v1.InstancesClient") as mock_cc,
        patch("scripts.vm.vm_serial_capture_cron.storage_exists", return_value=False),
        patch("scripts.vm.vm_serial_capture_cron.upload_to_storage"),
    ):
        mock_cc.return_value.aggregated_list.return_value = fake_zones
        result = capture_serial_for_long_lived_vms("test-project", dry_run=True)

    assert result == 0


def test_skips_already_captured() -> None:
    """VMs whose GCS object already exists are counted as skipped, not re-uploaded."""
    from scripts.vm.vm_serial_capture_cron import capture_serial_for_long_lived_vms

    fake_zones = [
        (
            "zones/asia-northeast1-c",
            _make_scoped(
                [
                    _make_instance("mtds-live-defi-abc"),
                ]
            ),
        ),
    ]

    with (
        patch("scripts.vm.vm_serial_capture_cron.compute_v1.InstancesClient") as mock_cc,
        patch("scripts.vm.vm_serial_capture_cron.storage_exists", return_value=True) as mock_exists,
        patch("scripts.vm.vm_serial_capture_cron.upload_to_storage") as mock_upload,
    ):
        mock_cc.return_value.aggregated_list.return_value = fake_zones
        result = capture_serial_for_long_lived_vms("test-project", dry_run=False)

    assert result == 0
    mock_upload.assert_not_called()
    mock_exists.assert_called_once()


def test_upload_on_new_vm() -> None:
    """Serial content is uploaded for a matching VM when no prior capture exists."""
    from scripts.vm.vm_serial_capture_cron import capture_serial_for_long_lived_vms

    fake_zones = [
        (
            "zones/asia-northeast1-c",
            _make_scoped(
                [
                    _make_instance("strategy-live-20260530"),
                ]
            ),
        ),
    ]

    mock_serial = MagicMock()
    mock_serial.contents = "Linux vm kernel output..."

    with (
        patch("scripts.vm.vm_serial_capture_cron.compute_v1.InstancesClient") as mock_cc,
        patch("scripts.vm.vm_serial_capture_cron.storage_exists", return_value=False),
        patch("scripts.vm.vm_serial_capture_cron.upload_to_storage") as mock_upload,
    ):
        mock_cc.return_value.aggregated_list.return_value = fake_zones
        mock_cc.return_value.get_serial_port_output.return_value = mock_serial
        result = capture_serial_for_long_lived_vms("test-project", dry_run=False)

    assert result == 0
    mock_upload.assert_called_once()
    # Verify the bytes and bucket/path were passed (contents is a str, so encode() is applied)
    call_args = mock_upload.call_args
    assert call_args.args[2] == b"Linux vm kernel output..."


def test_error_on_serial_fetch_returns_nonzero() -> None:
    """A serial-fetch exception is logged and the function returns 1."""
    from scripts.vm.vm_serial_capture_cron import capture_serial_for_long_lived_vms

    fake_zones = [
        (
            "zones/asia-northeast1-c",
            _make_scoped(
                [
                    _make_instance("strategy-live-20260530"),
                ]
            ),
        ),
    ]

    with (
        patch("scripts.vm.vm_serial_capture_cron.compute_v1.InstancesClient") as mock_cc,
        patch("scripts.vm.vm_serial_capture_cron.storage_exists", return_value=False),
        patch("scripts.vm.vm_serial_capture_cron.upload_to_storage"),
    ):
        mock_cc.return_value.aggregated_list.return_value = fake_zones
        mock_cc.return_value.get_serial_port_output.side_effect = RuntimeError("API unavailable")
        result = capture_serial_for_long_lived_vms("test-project", dry_run=False)

    assert result == 1


# ---------------------------------------------------------------------------
# Tests for main()
# ---------------------------------------------------------------------------


def test_main_dry_run_succeeds() -> None:
    """main() with --dry-run exits 0 without real GCP calls."""
    with (
        patch("scripts.vm.vm_serial_capture_cron.UnifiedCloudConfig") as mock_cfg,
        patch("scripts.vm.vm_serial_capture_cron.capture_serial_for_long_lived_vms") as mock_cap,
    ):
        mock_cfg.return_value.gcp_project_id = "test-project"
        mock_cap.return_value = 0
        from scripts.vm.vm_serial_capture_cron import main

        result = main(["--dry-run"])
    assert result == 0
    mock_cap.assert_called_once_with("test-project", dry_run=True)


def test_main_explicit_project_id() -> None:
    """main() uses --project-id when provided, bypassing UnifiedCloudConfig."""
    with patch("scripts.vm.vm_serial_capture_cron.capture_serial_for_long_lived_vms") as mock_cap:
        mock_cap.return_value = 0
        from scripts.vm.vm_serial_capture_cron import main

        result = main(["--project-id", "my-project"])
    assert result == 0
    mock_cap.assert_called_once_with("my-project", dry_run=False)
