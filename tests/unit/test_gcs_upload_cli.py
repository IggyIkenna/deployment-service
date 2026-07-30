"""Unit tests for ``deployment_service.vm.gcs_upload_cli``.

Covers the base upload path plus the ``--verify`` round-trip byte-compare
(vm_startup_scripts_no_auto_rollout_to_gcs_2026_07_19.md — "verify post-upload
rather than assuming the gsutil cp succeeded silently"). No real GCS: mocks
``StorageClient.upload_file`` (the SDK boundary) and ``StorageClient.client``
(the raw UTL client ``_verify_upload`` reads ``download_bytes`` from).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

from deployment_service.cloud.storage_client import StorageClient
from deployment_service.vm.gcs_upload_cli import main


def _write(tmp_path: Path, name: str, content: bytes) -> Path:
    p = tmp_path / name
    p.write_bytes(content)
    return p


def test_upload_success_no_verify(tmp_path: Path) -> None:
    f = _write(tmp_path, "setup-data-pipeline-vm.sh", b"#!/usr/bin/env bash\necho hi\n")
    with patch.object(StorageClient, "upload_file", return_value="gs://bkt/vm/setup-data-pipeline-vm.sh"):
        rc = main(["--bucket", "bkt", "--prefix", "vm", str(f)])
    assert rc == 0


def test_upload_failure_nonzero_exit(tmp_path: Path) -> None:
    f = _write(tmp_path, "setup-data-pipeline-vm.sh", b"content")
    with patch.object(StorageClient, "upload_file", return_value=None):
        rc = main(["--bucket", "bkt", "--prefix", "vm", str(f)])
    assert rc == 1


def test_skip_nonexistent_file(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.sh"
    with patch.object(StorageClient, "upload_file", return_value="gs://bkt/vm/x"):
        rc = main(["--bucket", "bkt", "--prefix", "vm", str(missing)])
    # No files actually uploaded (skipped, not a failure) -> success.
    assert rc == 0


def test_verify_passes_on_byte_identical_roundtrip(tmp_path: Path) -> None:
    content = b"#!/usr/bin/env bash\necho verify-me\n"
    f = _write(tmp_path, "vm-exec-with-gcs-tee.sh", content)
    fake_client = MagicMock()
    fake_client.download_bytes.return_value = content
    with (
        patch.object(StorageClient, "upload_file", return_value="gs://bkt/vm/vm-exec-with-gcs-tee.sh"),
        patch.object(StorageClient, "client", new_callable=PropertyMock, return_value=fake_client),
    ):
        rc = main(["--bucket", "bkt", "--prefix", "vm", "--verify", str(f)])
    assert rc == 0
    fake_client.download_bytes.assert_called_once_with(bucket="bkt", blob_path="vm/vm-exec-with-gcs-tee.sh")


def test_verify_fails_on_byte_mismatch(tmp_path: Path) -> None:
    f = _write(tmp_path, "vm-exec-with-gcs-tee.sh", b"local bytes")
    fake_client = MagicMock()
    fake_client.download_bytes.return_value = b"DIFFERENT remote bytes"  # simulates a partial/stale upload
    with (
        patch.object(StorageClient, "upload_file", return_value="gs://bkt/vm/vm-exec-with-gcs-tee.sh"),
        patch.object(StorageClient, "client", new_callable=PropertyMock, return_value=fake_client),
    ):
        rc = main(["--bucket", "bkt", "--prefix", "vm", "--verify", str(f)])
    assert rc == 1


def test_verify_fails_when_download_raises(tmp_path: Path) -> None:
    f = _write(tmp_path, "vm-exec-with-gcs-tee.sh", b"content")
    fake_client = MagicMock()
    fake_client.download_bytes.side_effect = OSError("blob not found")
    with (
        patch.object(StorageClient, "upload_file", return_value="gs://bkt/vm/vm-exec-with-gcs-tee.sh"),
        patch.object(StorageClient, "client", new_callable=PropertyMock, return_value=fake_client),
    ):
        rc = main(["--bucket", "bkt", "--prefix", "vm", "--verify", str(f)])
    assert rc == 1


def test_no_verify_flag_skips_roundtrip_check_entirely(tmp_path: Path) -> None:
    """Back-compat: without --verify, a byte-mismatched round-trip (were one to
    exist) is never even checked — matches today's behavior exactly."""
    f = _write(tmp_path, "setup-data-pipeline-vm.sh", b"content")
    fake_client = MagicMock()
    with (
        patch.object(StorageClient, "upload_file", return_value="gs://bkt/vm/setup-data-pipeline-vm.sh"),
        patch.object(StorageClient, "client", new_callable=PropertyMock, return_value=fake_client),
    ):
        rc = main(["--bucket", "bkt", "--prefix", "vm", str(f)])
    assert rc == 0
    fake_client.download_bytes.assert_not_called()
