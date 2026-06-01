"""Unit tests for validate_vm_prefix_mapping.py."""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest

# Add scripts/vm to path so the module can be imported without a package.
_SCRIPTS_VM = Path(__file__).parents[2] / "scripts" / "vm"
if str(_SCRIPTS_VM) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_VM))


# ---------------------------------------------------------------------------
# Helpers to build a minimal fake watchdog module so validate_vm_prefix_mapping
# can import VM_PREFIX_TO_BUCKET without the real google.cloud.compute_v1 dep.
# ---------------------------------------------------------------------------


def _make_fake_watchdog(prefix_map: dict) -> ModuleType:
    mod = ModuleType("vm_zombie_watchdog")
    mod.PROJECT_ID = "test-project-123"  # type: ignore[attr-defined]
    mod.VM_PREFIX_TO_BUCKET = prefix_map  # type: ignore[attr-defined]
    return mod


class _FakeSpec:
    """Minimal stand-in for VmPrefixSpec when bucket is not None."""

    def __init__(self, bucket: str | None) -> None:
        self.bucket = bucket


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_all_heartbeat_only_exits_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """All None-bucket entries: no GCS calls, exit 0."""
    fake_map = {"prefix-a-": None, "prefix-b-": None}
    fake_mod = _make_fake_watchdog(fake_map)
    monkeypatch.setitem(sys.modules, "vm_zombie_watchdog", fake_mod)

    import validate_vm_prefix_mapping as vmp

    monkeypatch.delitem(sys.modules, "validate_vm_prefix_mapping", raising=False)

    with patch.object(vmp, "_bucket_exists", return_value=True) as mock_check:
        # Direct call via simulated argv.
        with (
            patch("sys.argv", ["validate_vm_prefix_mapping.py"]),
            patch.dict(
                "sys.modules",
                {"vm_zombie_watchdog": fake_mod},
            ),
        ):
            vmp.VM_PREFIX_TO_BUCKET = fake_map  # type: ignore[attr-defined]
            vmp.PROJECT_ID = "test-project-123"  # type: ignore[attr-defined]
            rc = vmp.main()
        mock_check.assert_not_called()
    assert rc == 0


def test_existing_bucket_exits_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-None bucket that exists in GCS → exit 0."""
    import validate_vm_prefix_mapping as vmp

    fake_map: dict = {"myvm-": _FakeSpec("my-real-bucket")}
    vmp.VM_PREFIX_TO_BUCKET = fake_map  # type: ignore[attr-defined]
    vmp.PROJECT_ID = "test-project-123"  # type: ignore[attr-defined]

    with (
        patch.object(vmp, "_bucket_exists", return_value=True),
        patch("sys.argv", ["validate_vm_prefix_mapping.py"]),
    ):
        rc = vmp.main()
    assert rc == 0


def test_missing_bucket_exits_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-None bucket missing in GCS → exit 1 (orphan)."""
    import validate_vm_prefix_mapping as vmp

    fake_map: dict = {"ghost-prefix-": _FakeSpec("bucket-that-does-not-exist")}
    vmp.VM_PREFIX_TO_BUCKET = fake_map  # type: ignore[attr-defined]
    vmp.PROJECT_ID = "test-project-123"  # type: ignore[attr-defined]

    with (
        patch.object(vmp, "_bucket_exists", return_value=False),
        patch("sys.argv", ["validate_vm_prefix_mapping.py"]),
    ):
        rc = vmp.main()
    assert rc == 1


def test_dry_run_no_gcs_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    """--dry-run: prints prefixes, makes zero GCS calls, exits 0."""
    import validate_vm_prefix_mapping as vmp

    fake_map: dict = {"myvm-": _FakeSpec("some-bucket"), "hb-": None}
    vmp.VM_PREFIX_TO_BUCKET = fake_map  # type: ignore[attr-defined]
    vmp.PROJECT_ID = "test-project-123"  # type: ignore[attr-defined]

    with (
        patch.object(vmp, "_bucket_exists") as mock_check,
        patch("sys.argv", ["validate_vm_prefix_mapping.py", "--dry-run"]),
    ):
        rc = vmp.main()
    mock_check.assert_not_called()
    assert rc == 0


def test_mixed_ok_and_orphan(monkeypatch: pytest.MonkeyPatch) -> None:
    """One existing + one missing bucket: exit 1, orphan reported."""
    import validate_vm_prefix_mapping as vmp

    fake_map: dict = {
        "good-prefix-": _FakeSpec("good-bucket"),
        "bad-prefix-": _FakeSpec("missing-bucket"),
        "hb-only-": None,
    }
    vmp.VM_PREFIX_TO_BUCKET = fake_map  # type: ignore[attr-defined]
    vmp.PROJECT_ID = "test-project-123"  # type: ignore[attr-defined]

    def _exists(_project: str, bucket: str) -> bool:
        return bucket == "good-bucket"

    with (
        patch.object(vmp, "_bucket_exists", side_effect=_exists),
        patch("sys.argv", ["validate_vm_prefix_mapping.py"]),
    ):
        rc = vmp.main()
    assert rc == 1
