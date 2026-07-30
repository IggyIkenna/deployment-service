#!/usr/bin/env python3
"""CLI — upload local files to GCS via UTL's ADC-backed StorageClient.

Exists because ``gsutil``/``gcloud storage`` resolve credentials from the CLI's
configured ACTIVE ACCOUNT, not from Application Default Credentials — in an
interactive AO slot the active account is often a short-lived Workload-Identity-
Federation service account (``github-actions-deploy@...``) whose token expires
and cannot be refreshed unattended, while ADC (a long-lived refresh-token-backed
user credential) keeps working. Routing tarball/launcher uploads through
``deployment_service.cloud.storage_client.StorageClient`` (backed by
``unified_trading_library.get_storage_client()``) sidesteps the broken CLI
credential entirely. See
plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.

Invoked by ``scripts/vm/gcs_upload_via_adc.py`` (thin shim, mirrors
``scripts/vm/heartbeat_daemon.py`` -> ``deployment_service.vm.heartbeat_cli``).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import cast

from deployment_service.cloud.storage_client import StorageClient

logger = logging.getLogger("gcs_upload_cli")


def _verify_upload(client: StorageClient, bucket: str, blob_path: str, local_path: Path) -> bool:
    """Re-download the just-uploaded blob and byte-compare it to the local source.

    ``upload_file`` returning a URI only means the SDK call didn't raise — it does
    NOT prove the bytes GCS now serves match what was sent (partial upload, retry
    landing a stale object, etc.). Hash/etag compare via an actual round-trip read
    closes that gap (vm_startup_scripts_no_auto_rollout_to_gcs_2026_07_19.md —
    "verify post-upload rather than assuming the gsutil cp succeeded silently").
    Returns True iff the round-tripped bytes are byte-identical to the local file.
    """
    raw_client = client.client
    if raw_client is None:
        return False
    try:
        remote_bytes = raw_client.download_bytes(bucket=bucket, blob_path=blob_path)
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("Verify download failed for gs://%s/%s: %s", bucket, blob_path, exc)
        return False
    return remote_bytes == local_path.read_bytes()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True, help="Destination GCS bucket (no gs:// prefix)")
    parser.add_argument("--prefix", default="", help="Destination blob-path prefix (no leading/trailing slash)")
    parser.add_argument(
        "--project",
        default=None,
        help=(
            "GCP project ID. DeploymentConfig only reads the canonical GCP_PROJECT_ID/PROJECT_ID env vars, "
            "which an interactive slot may not have set even though gcloud's local config does — pass it "
            "explicitly (the caller resolves it via `gcloud config get-value project`) rather than relying on env."
        ),
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help=(
            "After each upload, re-download the blob and byte-compare it to the local file — "
            "fails loudly on a mismatch instead of trusting the SDK call's return value alone."
        ),
    )
    parser.add_argument("files", nargs="+", help="Local file paths to upload")
    raw_args = parser.parse_args(argv)

    # argparse.Namespace attrs are Any — cast each field so the rest of main() is typed.
    bucket = str(cast(object, raw_args.bucket))
    prefix = str(cast(object, raw_args.prefix)).strip("/")
    project = cast(str | None, raw_args.project)
    verify = bool(cast(object, raw_args.verify))
    files = cast("list[str]", raw_args.files)

    client = StorageClient(project_id=project, provider="gcs")

    # This is a CLI entry point (invoked from create-code-tarballs.sh) — stdout/stderr IS
    # the interface the calling shell script reads, not a service lifecycle event. Same
    # exemption base-service.sh already grants **/cli/main.py and **/__main__.py.
    failures: list[str] = []
    for f in files:
        path = Path(f)
        if not path.is_file():
            print(f"SKIP {f} — not a file", file=sys.stderr)  # noqa: qg-print
            continue
        blob_path = f"{prefix}/{path.name}" if prefix else path.name
        cloud_path = f"gs://{bucket}/{blob_path}"  # noqa: gs-uri — this CLI IS the upload boundary
        uri = client.upload_file(str(path), cloud_path)
        if uri is None:
            print(f"FAILED {path.name} -> {cloud_path}", file=sys.stderr)  # noqa: qg-print
            failures.append(f)
            continue
        if verify and not _verify_upload(client, bucket, blob_path, path):
            print(f"VERIFY_FAILED {path.name} -> {cloud_path} (round-trip byte mismatch)", file=sys.stderr)  # noqa: qg-print
            failures.append(f)
            continue
        print(f"Uploaded {path.name} -> {uri}{' (verified)' if verify else ''}")  # noqa: qg-print

    if failures:
        print(f"{len(failures)} upload(s) failed: {failures}", file=sys.stderr)  # noqa: qg-print
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
