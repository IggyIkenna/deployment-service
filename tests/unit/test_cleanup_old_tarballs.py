"""Unit tests for scripts/vm/cleanup_old_tarballs.py.

Tests:
  - _parse_tarballs: SHA-versioned + simple patterns
  - cleanup_name_versioned: keeps N most-recent, deletes older ones (dry-run)
  - cleanup_noncurrent_versions: skips live versions, skips recent noncurrent, deletes old ones (dry-run)
  - main() argparse: --dry-run, --keep, --noncurrent flags accepted
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final
from unittest.mock import patch

SCRIPT_PATH: Final[Path] = Path(__file__).parent.parent.parent / "scripts" / "vm" / "cleanup_old_tarballs.py"


def _load_module() -> object:
    spec = importlib.util.spec_from_file_location("cleanup_old_tarballs", SCRIPT_PATH)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["cleanup_old_tarballs"] = mod  # type: ignore[assignment]
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


_mod = _load_module()
_parse_tarballs = _mod._parse_tarballs  # type: ignore[attr-defined]
_cleanup_name_versioned = _mod.cleanup_name_versioned  # type: ignore[attr-defined]
_cleanup_noncurrent = _mod.cleanup_noncurrent_versions  # type: ignore[attr-defined]
_delete_tarball_pair = _mod._delete_tarball_pair  # type: ignore[attr-defined]
_collect_pins_for_project = _mod.collect_pins_for_project  # type: ignore[attr-defined]
_main = _mod.main  # type: ignore[attr-defined]
TarballEntry = _mod.TarballEntry  # type: ignore[attr-defined]

from deployment_service.vm.tarball_pins import (  # noqa: E402
    InUsePinsUnavailableError,
    TarballPin,
)


def _make_ls_l_row(gcs_path: str, age_hours: float = 1.0) -> tuple[datetime, str]:
    mtime = datetime.now(UTC) - timedelta(hours=age_hours)
    return mtime, gcs_path


class TestParseTarballs:
    def test_sha_versioned_pattern(self) -> None:
        rows = [
            _make_ls_l_row("gs://bucket/code/my-svc@abc12345.tar.gz", 10),
            _make_ls_l_row("gs://bucket/code/my-svc@def67890.tar.gz", 5),
        ]
        with patch("cleanup_old_tarballs._gsutil_ls_l", return_value=rows):
            entries = _parse_tarballs("gs://bucket/code/")
        assert len(entries) == 2
        assert all(e["service"] == "my-svc" for e in entries)
        assert all(e["has_sha"] for e in entries)

    def test_simple_pattern_parsed_as_no_sha(self) -> None:
        rows = [_make_ls_l_row("gs://bucket/code/my-svc-code.tar.gz", 1)]
        with patch("cleanup_old_tarballs._gsutil_ls_l", return_value=rows):
            entries = _parse_tarballs("gs://bucket/code/")
        assert len(entries) == 1
        assert entries[0]["service"] == "my-svc"
        assert not entries[0]["has_sha"]

    def test_non_tarball_files_ignored(self) -> None:
        rows = [
            _make_ls_l_row("gs://bucket/code/some-script.sh", 1),
            _make_ls_l_row("gs://bucket/code/my-svc-code.tar.gz", 1),
        ]
        with patch("cleanup_old_tarballs._gsutil_ls_l", return_value=rows):
            entries = _parse_tarballs("gs://bucket/code/")
        assert len(entries) == 1


class TestCleanupNameVersioned:
    def _make_entries(self, service: str, ages_hours: list[float]) -> list[object]:
        return [
            {
                "gcs_path": f"gs://bucket/code/{service}@sha{i}.tar.gz",
                "service": service,
                "sha": f"sha{i}",
                "mtime": datetime.now(UTC) - timedelta(hours=h),
                "has_sha": True,
            }
            for i, h in enumerate(ages_hours)
        ]

    def test_keeps_n_most_recent_dry_run(self) -> None:
        entries = self._make_entries("svc-a", [1, 5, 10, 20, 40, 80])  # 6 entries, keep 3

        with (
            patch("cleanup_old_tarballs._parse_tarballs", return_value=entries),
            patch("cleanup_old_tarballs._delete_tarball_pair", return_value=True) as mock_del,
        ):
            result = _cleanup_name_versioned("bucket", keep=3, dry_run=True)

        assert result["svc-a"] == 3  # 6 - 3 = 3 deleted
        assert mock_del.call_count == 3

    def test_no_sha_entries_are_skipped(self) -> None:
        simple_entries = [
            {
                "gcs_path": "gs://bucket/code/svc-code.tar.gz",
                "service": "svc",
                "mtime": datetime.now(UTC),
                "has_sha": False,
            }
        ]
        with (
            patch("cleanup_old_tarballs._parse_tarballs", return_value=simple_entries),
            patch("cleanup_old_tarballs._delete_tarball_pair") as mock_del,
        ):
            result = _cleanup_name_versioned("bucket", keep=3, dry_run=True)

        assert result == {}  # no SHA-versioned entries → nothing deleted
        mock_del.assert_not_called()

    def test_within_keep_limit_nothing_deleted(self) -> None:
        entries = self._make_entries("svc-b", [1, 5])  # 2 entries, keep 5

        with (
            patch("cleanup_old_tarballs._parse_tarballs", return_value=entries),
            patch("cleanup_old_tarballs._delete_tarball_pair") as mock_del,
        ):
            result = _cleanup_name_versioned("bucket", keep=5, dry_run=True)

        assert "svc-b" not in result
        mock_del.assert_not_called()

    # ── Pin-aware retention — the 2026-07-20 regression guards ──────────────

    def test_pinned_and_running_tarball_is_preserved(self) -> None:
        """A pinned tarball survives even when it has aged far past --keep.

        This is THE incident: at --keep 5 a high-velocity repo like UAC evicts a
        long-running fleet's pin within days, purely on mtime rank.
        """
        entries = self._make_entries("unified-api-contracts-code", [1, 5, 10, 20, 40, 80])
        # sha5 is the OLDEST (80h) — dead last in the ranking, and pinned.
        pins = frozenset({TarballPin(tarball_name="unified-api-contracts-code", sha="sha5")})

        # Baseline: with no pins the aged-out trio (incl. sha5) IS deleted.
        with (
            patch("cleanup_old_tarballs._parse_tarballs", return_value=entries),
            patch("cleanup_old_tarballs._delete_tarball_pair", return_value=True) as mock_baseline,
        ):
            baseline = _cleanup_name_versioned("bucket", keep=3, dry_run=True)
        baseline_paths = [c.args[0] for c in mock_baseline.call_args_list]
        assert baseline["unified-api-contracts-code"] == 3
        assert any("@sha5.tar.gz" in p for p in baseline_paths), "baseline must reproduce the incident"

        # With the pin present, that same tarball is exempt.
        with (
            patch("cleanup_old_tarballs._parse_tarballs", return_value=entries),
            patch("cleanup_old_tarballs._delete_tarball_pair", return_value=True) as mock_del,
        ):
            result_pinned = _cleanup_name_versioned("bucket", keep=3, dry_run=True, pins=pins)

        assert result_pinned["unified-api-contracts-code"] == 2, "the pinned tarball is exempt"
        deleted_paths = [c.args[0] for c in mock_del.call_args_list]
        assert not any("@sha5.tar.gz" in p for p in deleted_paths), "pinned tarball must never be deleted"

    def test_pin_match_is_prefix_tolerant(self) -> None:
        """A short recorded sha still protects the full-length object (and vice versa)."""
        entries = self._make_entries("svc-a", [1, 5, 10, 20])
        entries[3]["sha"] = "acd8714c811027910cc18e730f8b38a6deef7822"  # type: ignore[index]
        pins = frozenset({TarballPin(tarball_name="svc-a", sha="acd8714c")})  # 8-char pin

        with (
            patch("cleanup_old_tarballs._parse_tarballs", return_value=entries),
            patch("cleanup_old_tarballs._delete_tarball_pair", return_value=True) as mock_del,
        ):
            result = _cleanup_name_versioned("bucket", keep=3, dry_run=True, pins=pins)

        assert result == {} or result.get("svc-a", 0) == 0
        mock_del.assert_not_called()

    def test_unpinned_and_old_is_still_deleted(self) -> None:
        """Protection must not become a blanket amnesty — retention still works."""
        entries = self._make_entries("svc-a", [1, 5, 10, 20, 40])
        pins = frozenset({TarballPin(tarball_name="other-svc-code", sha="sha4")})  # different service

        with (
            patch("cleanup_old_tarballs._parse_tarballs", return_value=entries),
            patch("cleanup_old_tarballs._delete_tarball_pair", return_value=True) as mock_del,
        ):
            result = _cleanup_name_versioned("bucket", keep=3, dry_run=True, pins=pins)

        assert result["svc-a"] == 2
        assert mock_del.call_count == 2


class TestTarballManifestPairDeletion:
    def test_manifest_deleted_together_with_tarball_manifest_first(self) -> None:
        """Tarball + sibling manifest share a fate, and the manifest goes FIRST.

        Order matters: interrupting after the tarball would leave an ORPHAN
        MANIFEST — a pin that still resolves but whose code is gone, which is the
        silent failure shape that bricked the fleet.
        """
        with patch("cleanup_old_tarballs._delete_object", return_value=True) as mock_del:
            ok = _delete_tarball_pair("gs://bucket/code/uac-code@abc123.tar.gz", dry_run=True)

        assert ok
        deleted = [c.args[0] for c in mock_del.call_args_list]
        assert deleted == [
            "gs://bucket/code/uac-code@abc123.manifest.json",
            "gs://bucket/code/uac-code@abc123.tar.gz",
        ]


class TestCleanupNoncurrentVersions:
    def _ls_rows(self, paths_ages: list[tuple[str, float]]) -> list[tuple[datetime, str]]:
        return [(datetime.now(UTC) - timedelta(hours=h), p) for p, h in paths_ages]

    def test_skips_live_versions(self) -> None:
        rows = self._ls_rows(
            [
                ("gs://bucket/code/svc-code.tar.gz", 100),  # no # → live
            ]
        )
        with (
            patch("cleanup_old_tarballs._gsutil_ls_l", return_value=rows),
            patch("cleanup_old_tarballs._delete_object") as mock_del,
        ):
            count = _cleanup_noncurrent("bucket", max_age_days=7, dry_run=True)
        assert count == 0
        mock_del.assert_not_called()

    def test_skips_recent_noncurrent(self) -> None:
        rows = self._ls_rows(
            [
                ("gs://bucket/code/svc-code.tar.gz#123456", 10),  # noncurrent, 10h old → within 7d
            ]
        )
        with (
            patch("cleanup_old_tarballs._gsutil_ls_l", return_value=rows),
            patch("cleanup_old_tarballs._delete_object") as mock_del,
        ):
            count = _cleanup_noncurrent("bucket", max_age_days=7, dry_run=True)
        assert count == 0
        mock_del.assert_not_called()

    def test_deletes_old_noncurrent(self) -> None:
        rows = self._ls_rows(
            [
                ("gs://bucket/code/svc-code.tar.gz#123456", 24 * 10),  # 10 days old → delete
            ]
        )
        with (
            patch("cleanup_old_tarballs._gsutil_ls_l", return_value=rows),
            patch("cleanup_old_tarballs._delete_object", return_value=True) as mock_del,
        ):
            count = _cleanup_noncurrent("bucket", max_age_days=7, dry_run=True)
        assert count == 1
        mock_del.assert_called_once()


class TestMainArgParser:
    def test_dry_run_name_versioned(self) -> None:
        with (
            patch("cleanup_old_tarballs.collect_pins_for_project", return_value=frozenset()),
            patch("cleanup_old_tarballs.cleanup_name_versioned", return_value={}) as mock_fn,
            patch("cleanup_old_tarballs.cleanup_noncurrent_versions") as mock_nc,
        ):
            rc = _main(["--project", "my-proj", "--keep", "3", "--dry-run"])
        assert rc == 0
        mock_fn.assert_called_once()
        call_kwargs_positional = mock_fn.call_args[0]
        assert call_kwargs_positional[1] == 3  # keep=3
        assert call_kwargs_positional[2] is True  # dry_run=True
        mock_nc.assert_not_called()

    def test_noncurrent_mode(self) -> None:
        with (
            patch("cleanup_old_tarballs.cleanup_noncurrent_versions", return_value=0) as mock_nc,
            patch("cleanup_old_tarballs.cleanup_name_versioned") as mock_fn,
        ):
            rc = _main(["--project", "my-proj", "--noncurrent", "--max-age-days", "14", "--dry-run"])
        assert rc == 0
        mock_nc.assert_called_once()
        mock_fn.assert_not_called()

    def test_default_bucket_derived_from_project(self) -> None:
        with (
            patch("cleanup_old_tarballs.collect_pins_for_project", return_value=frozenset()),
            patch("cleanup_old_tarballs.cleanup_name_versioned", return_value={}) as mock_fn,
        ):
            _main(["--project", "my-proj-123", "--dry-run"])
        bucket_arg = mock_fn.call_args[0][0]
        assert bucket_arg == "deployment-scripts-my-proj-123"

    def test_pins_are_passed_through_to_the_sweep(self) -> None:
        pins = frozenset({TarballPin(tarball_name="uac-code", sha="abc123")})
        with (
            patch("cleanup_old_tarballs.collect_pins_for_project", return_value=pins),
            patch("cleanup_old_tarballs.cleanup_name_versioned", return_value={}) as mock_fn,
        ):
            rc = _main(["--project", "my-proj", "--dry-run"])
        assert rc == 0
        assert mock_fn.call_args.kwargs["pins"] == pins

    def test_fails_closed_and_deletes_nothing_when_pins_unknown(self) -> None:
        """If live state can't be read, delete NOTHING — never assume "nothing runs"."""
        with (
            patch(
                "cleanup_old_tarballs.collect_pins_for_project",
                side_effect=InUsePinsUnavailableError("compute API down"),
            ),
            patch("cleanup_old_tarballs.cleanup_name_versioned") as mock_fn,
        ):
            rc = _main(["--project", "my-proj"])
        assert rc == 1, "a non-zero exit surfaces the skipped sweep"
        mock_fn.assert_not_called()


class TestCollectPinsForProject:
    def test_compute_api_failure_raises_rather_than_returning_empty(self) -> None:
        """The fail-closed contract: an API error must NOT look like "no VMs running"."""
        with patch(
            "cleanup_old_tarballs.list_running_vm_names_strict",
            side_effect=RuntimeError("403"),
        ):
            try:
                _collect_pins_for_project("proj", "bucket", grace_days=14)
            except InUsePinsUnavailableError:
                return
        raise AssertionError("expected InUsePinsUnavailableError")
