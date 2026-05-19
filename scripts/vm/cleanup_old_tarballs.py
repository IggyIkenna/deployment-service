#!/usr/bin/env python3
"""Prune old code tarballs from GCS deployment-scripts bucket.

Two modes:
  1. **Name-versioned** (primary): tarballs named `<service>@<sha>.tar.gz` or
     `<service>-code-<sha>.tar.gz` accumulate per service.  Keep the N most
     recent per service (ordered by GCS object mtime); delete the rest.

  2. **GCS-noncurrent-versions** (secondary, --noncurrent): if the bucket has
     object versioning enabled/suspended, noncurrent versions of
     `<service>-code.tar.gz` accumulate silently.  Delete all noncurrent
     versions older than --max-age-days (default 7).

Current state (2026-05-15): the bucket uses single-file-per-service naming
(`<service>-code.tar.gz`) with versioning Suspended.  No cleanup is needed
today.  This script is the canonical cleanup tool for when either:
  (a) SHA-versioned naming is adopted per vm-tarball-deployment.md SSOT, or
  (b) Bucket versioning is re-enabled to track history.

SSOT: codex/05-infrastructure/vm-tarball-deployment.md

Usage:
    python cleanup_old_tarballs.py --project central-element-323112 --keep 5 --dry-run
    python cleanup_old_tarballs.py --project central-element-323112 --keep 5
    python cleanup_old_tarballs.py --project central-element-323112 --noncurrent --max-age-days 7 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import re
import subprocess
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import TypedDict, cast

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_GCS_LS_DATE = re.compile(r"^\s*\d+\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\s+(gs://.+)$")

# Tarball filename patterns:
#   sha-versioned:  <service>@<sha>.tar.gz  OR  <service>-code-<sha>.tar.gz
#   simple:         <service>-code.tar.gz  (no sha — single-version per service)
_SHA_PATTERN = re.compile(r"^(.+?)(?:@[a-f0-9]+|-code-[a-f0-9]+)\.tar\.gz$")
_SIMPLE_PATTERN = re.compile(r"^(.+?)-code\.tar\.gz$")


class TarballEntry(TypedDict):
    gcs_path: str
    service: str
    mtime: datetime
    has_sha: bool


def _gsutil_ls_l(prefix: str, all_versions: bool = False) -> list[tuple[datetime, str]]:
    """Run `gsutil ls -l [--all-versions]` and return [(mtime, gcs_path)] pairs."""
    cmd = ["gsutil", "ls", "-l"]
    if all_versions:
        cmd.append("-a")
    cmd.append(prefix)
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        logger.warning("gsutil ls failed: %s", result.stderr.strip())
        return []
    rows: list[tuple[datetime, str]] = []
    for line in result.stdout.splitlines():
        m = _GCS_LS_DATE.match(line)
        if not m:
            continue
        ts = datetime.fromisoformat(m.group(1).replace("Z", "+00:00"))
        rows.append((ts, m.group(2).strip()))
    return rows


def _parse_tarballs(prefix: str) -> list[TarballEntry]:
    """List all .tar.gz objects under prefix; parse service name + mtime."""
    rows = _gsutil_ls_l(prefix)
    entries: list[TarballEntry] = []
    for mtime, gcs_path in rows:
        filename = gcs_path.rsplit("/", 1)[-1]
        if not filename.endswith(".tar.gz"):
            continue
        sha_m = _SHA_PATTERN.match(filename)
        if sha_m:
            service = sha_m.group(1)
            entries.append(TarballEntry(gcs_path=gcs_path, service=service, mtime=mtime, has_sha=True))
            continue
        simple_m = _SIMPLE_PATTERN.match(filename)
        if simple_m:
            service = simple_m.group(1)
            entries.append(TarballEntry(gcs_path=gcs_path, service=service, mtime=mtime, has_sha=False))
    return entries


def _delete_object(gcs_path: str, dry_run: bool) -> bool:
    if dry_run:
        logger.info("[DRY-RUN] would delete %s", gcs_path)
        return True
    result = subprocess.run(["gsutil", "rm", gcs_path], capture_output=True, text=True, check=False)
    if result.returncode == 0:
        logger.info("deleted %s", gcs_path)
        return True
    logger.warning("failed to delete %s: %s", gcs_path, result.stderr.strip())
    return False


def cleanup_name_versioned(bucket: str, keep: int, dry_run: bool) -> dict[str, int]:
    """Delete old SHA-versioned tarballs; keep N most recent per service."""
    entries = _parse_tarballs(f"gs://{bucket}/code/")

    # Only process SHA-versioned entries — single-version files are untouched
    sha_entries = [e for e in entries if e["has_sha"]]
    if not sha_entries:
        logger.info("No SHA-versioned tarballs found under gs://%s/code/ — nothing to clean up", bucket)
        return {}

    by_service: dict[str, list[TarballEntry]] = defaultdict(list)
    for entry in sha_entries:
        by_service[entry["service"]].append(entry)

    deleted: dict[str, int] = {}
    for service, service_entries in sorted(by_service.items()):
        sorted_entries = sorted(service_entries, key=lambda e: e["mtime"], reverse=True)
        to_keep = sorted_entries[:keep]
        to_delete = sorted_entries[keep:]
        if not to_delete:
            logger.info("service=%s: %d tarballs, %d to keep, 0 to delete", service, len(sorted_entries), keep)
            continue
        logger.info(
            "service=%s: %d tarballs, keeping %d most recent, deleting %d",
            service,
            len(sorted_entries),
            len(to_keep),
            len(to_delete),
        )
        count = 0
        for entry in to_delete:
            if _delete_object(entry["gcs_path"], dry_run):
                count += 1
        deleted[service] = count
    return deleted


def cleanup_noncurrent_versions(bucket: str, max_age_days: int, dry_run: bool) -> int:
    """Delete noncurrent GCS object versions older than max_age_days.

    Only effective when bucket versioning is Enabled or Suspended.
    Safe to run when versioning is off — gsutil ls -a will list only live objects.
    """
    cutoff = datetime.now(UTC) - timedelta(days=max_age_days)
    rows = _gsutil_ls_l(f"gs://{bucket}/code/", all_versions=True)

    # gsutil ls -la adds #<generation> suffix for noncurrent versions
    # e.g.: gs://bucket/code/svc-code.tar.gz#1234567890
    noncurrent_count = 0
    for mtime, gcs_path in rows:
        if "#" not in gcs_path:
            continue  # Live version — skip
        if mtime >= cutoff:
            continue  # Recent enough — keep
        logger.info("noncurrent version (age=%s): %s", datetime.now(UTC) - mtime, gcs_path)
        if _delete_object(gcs_path, dry_run):
            noncurrent_count += 1
    return noncurrent_count


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--bucket",
        default="",
        help="GCS bucket name (default: deployment-scripts-{project})",
    )
    parser.add_argument(
        "--keep", type=int, default=5, help="Number of most-recent tarballs to keep per service (name-versioned mode)"
    )
    parser.add_argument(
        "--noncurrent",
        action="store_true",
        help="Clean up GCS noncurrent object versions instead of name-versioned tarballs",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=7,
        help="Max age (days) for noncurrent versions before deletion (--noncurrent mode)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report but do not delete")
    args = parser.parse_args(argv)

    project: str = cast(str, args.project)
    bucket: str = cast(str, args.bucket) or f"deployment-scripts-{project}"
    keep: int = cast(int, args.keep)
    max_age_days: int = cast(int, args.max_age_days)
    dry_run: bool = cast(bool, args.dry_run)
    noncurrent: bool = cast(bool, args.noncurrent)

    logger.info(
        "bucket=gs://%s  dry_run=%s  mode=%s", bucket, dry_run, "noncurrent" if noncurrent else "name-versioned"
    )

    if noncurrent:
        deleted = cleanup_noncurrent_versions(bucket, max_age_days, dry_run)
        logger.info("noncurrent cleanup complete: %d version(s) deleted", deleted)
    else:
        deleted_by_service = cleanup_name_versioned(bucket, keep, dry_run)
        total = sum(deleted_by_service.values())
        logger.info(
            "name-versioned cleanup complete: %d service(s) processed, %d tarball(s) deleted",
            len(deleted_by_service),
            total,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
