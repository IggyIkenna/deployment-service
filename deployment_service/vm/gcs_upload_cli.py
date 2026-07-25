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
    parser.add_argument("files", nargs="+", help="Local file paths to upload")
    raw_args = parser.parse_args(argv)

    # argparse.Namespace attrs are Any — cast each field so the rest of main() is typed.
    bucket = str(cast(object, raw_args.bucket))
    prefix = str(cast(object, raw_args.prefix)).strip("/")
    project = cast(str | None, raw_args.project)
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
        else:
            print(f"Uploaded {path.name} -> {uri}")  # noqa: qg-print

    if failures:
        print(f"{len(failures)} upload(s) failed: {failures}", file=sys.stderr)  # noqa: qg-print
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
