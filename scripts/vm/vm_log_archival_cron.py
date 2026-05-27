#!/usr/bin/env python3
"""Daily VM log archival cron job.

Copies live VM logs from the 14-day TTL prefix (vm-logs/) to a durable 
archive prefix (log-archive/rolling/) that has no lifecycle rule.

Runs daily via Cloud Scheduler + Cloud Run Job, preserving VM logs beyond
the 14-day window for forensics and compliance.

Usage:
    python vm_log_archival_cron.py [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime

from google.cloud import storage
from unified_trading_library import UnifiedCloudConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)


def archive_vm_logs(project_id: str, dry_run: bool = False) -> int:
    """Archive VM logs from live stream to durable storage.
    
    Copies all objects from gs://deployment-scripts-{project}/vm-logs/
    to gs://deployment-scripts-{project}/log-archive/rolling/{date}/
    
    Returns number of objects archived.
    """
    bucket_name = f"deployment-scripts-{project_id}"
    source_prefix = "vm-logs/"
    
    # Use date-stamped archive to preserve history
    date_stamp = datetime.now(UTC).strftime("%Y%m%d")
    dest_prefix = f"log-archive/rolling/{date_stamp}/"
    
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    
    archived_count = 0
    error_count = 0
    
    # List all objects in vm-logs/
    blobs = bucket.list_blobs(prefix=source_prefix)
    
    for blob in blobs:
        # Skip directory markers
        if blob.name.endswith('/'):
            continue
            
        # Derive destination path
        relative_path = blob.name[len(source_prefix):]
        dest_name = f"{dest_prefix}{relative_path}"
        
        if dry_run:
            logger.info("[DRY-RUN] Would copy %s to %s", blob.name, dest_name)
            archived_count += 1
            continue
        
        try:
            # Check if destination already exists (idempotent)
            dest_blob = bucket.blob(dest_name)
            if dest_blob.exists():
                logger.debug("Already archived: %s", dest_name)
                continue
            
            # Server-side copy (fast, no download)
            bucket.copy_blob(blob, bucket, dest_name)
            logger.info("Archived: %s -> %s", blob.name, dest_name)
            archived_count += 1
            
        except Exception as exc:
            logger.error("Failed to archive %s: %s", blob.name, exc)
            error_count += 1
    
    logger.info(
        "Archival complete: %d objects archived, %d errors%s",
        archived_count,
        error_count,
        " (DRY-RUN)" if dry_run else ""
    )
    
    # Return non-zero if any errors occurred
    return 1 if error_count > 0 else 0


def main(argv: list[str] | None = None) -> int:
    """Entry point for Cloud Run Job execution."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be archived without making changes"
    )
    parser.add_argument(
        "--project-id",
        help="GCP project ID (defaults to UnifiedCloudConfig)"
    )
    
    args = parser.parse_args(argv)
    
    # Resolve project ID
    if args.project_id:
        project_id = args.project_id
    else:
        try:
            config = UnifiedCloudConfig()
            project_id = config.gcp_project_id
        except Exception as exc:
            logger.error("Failed to resolve project ID: %s", exc)
            return 1
    
    if not project_id:
        logger.error("No project ID available")
        return 1
    
    logger.info(
        "Starting VM log archival for project %s%s",
        project_id,
        " (DRY-RUN)" if args.dry_run else ""
    )
    
    try:
        return archive_vm_logs(project_id, dry_run=args.dry_run)
    except Exception as exc:
        logger.error("Archival failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())