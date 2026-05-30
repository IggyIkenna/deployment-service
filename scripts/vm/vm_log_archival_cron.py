#!/usr/bin/env python3
"""Daily VM log archival cron job.

Copies live VM logs from the 14-day TTL prefix (vm-logs/) to a durable
archive prefix (log-archive/rolling/) that has no lifecycle rule.

Also captures serial console output for LONG_LIVED_LIVE / SCHEDULED_RECURRING
VMs on the same daily schedule — one-shot capture at kill time is insufficient
for long-running VMs once the GCE serial ring buffer wraps.

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

from google.cloud import compute_v1, storage
from unified_trading_library import UnifiedCloudConfig

from deployment_service.deployments_registry import vm_serial_rolling_uri

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


# Prefixes whose lifecycle_class is LONG_LIVED_LIVE or SCHEDULED_RECURRING in
# vm_zombie_watchdog.py VM_PREFIX_TO_BUCKET. Duplicated here because the watchdog
# is a standalone script (not a module) and the daily cron runs in Cloud Run.
# SSOT: deployment-service/scripts/vm/vm_zombie_watchdog.py VM_PREFIX_TO_BUCKET.
# Keep in sync when adding new long-lived prefixes to the watchdog.
_LONG_LIVED_VM_PREFIXES: tuple[str, ...] = (
    # LONG_LIVED_LIVE
    "strategy-paper-",
    "strategy-live-",
    "greeks-compute-live-",
    "greeks-compute-batch-",
    "defi-paper-",
    "defi-backtest-",
    "defi-recursive-",
    "deployment-dashboard-vm",
    "mtds-live-cefi-",
    "mtds-live-defi-",
    "mtds-live-tradfi-",
    "mtds-live-sports-",
    "mtds-live-prediction-",
    "mdps-features-live-cefi-",
    "mdps-features-live-defi-",
    "mdps-features-live-tradfi-",
    "mdps-features-live-sports-",
    "mdps-features-live-prediction-",
    "agent-orch-planning-vm-",
    "agent-orch-vm-defi-",
    "agent-orch-vm-cefi-",
    "agent-orch-vm-tradfi-",
    "agent-orch-vm-sports-",
    "agent-orch-vm-prediction-",
    "agent-orch-vm-ml-",
    "agent-orch-vm-trading-core-",
    "agent-orch-vm-operator-ops-",
    "agent-orch-vm-cross-cutting-",
    "agent-orch-vm-orchestrator-",
    # SCHEDULED_RECURRING
    "disaster-drill-cron-",
    "dr-drill-cutover-",
    "honest-coverage-",
    "tradfi-fwd-daily-cron-",
    "cefi-fwd-daily-cron-",
)


def capture_long_lived_serial(project_id: str, dry_run: bool = False) -> int:
    """Capture serial console for all RUNNING long-lived / scheduled-recurring VMs.

    Stores to log-archive/serial-rolling/{date}/{vm}/serial-console.txt — no lifecycle
    rule, persists indefinitely. Runs daily so serial history survives ring-buffer wraps.

    Returns number of serial captures written (or would-write in dry-run).
    """
    date_stamp = datetime.now(UTC).strftime("%Y%m%d")
    storage_client = storage.Client(project=project_id)
    compute_client = compute_v1.InstancesClient()

    request = compute_v1.AggregatedListInstancesRequest(project=project_id)
    long_lived_vms: list[tuple[str, str]] = []
    for zone, scoped in compute_client.aggregated_list(request=request):
        if not scoped.instances:
            continue
        for inst in scoped.instances:
            if inst.status != "RUNNING":
                continue
            if inst.name.startswith(_LONG_LIVED_VM_PREFIXES):
                long_lived_vms.append((inst.name, zone.replace("zones/", "")))

    logger.info(
        "Found %d long-lived RUNNING VMs for serial capture%s",
        len(long_lived_vms),
        " (DRY-RUN)" if dry_run else "",
    )

    captured = 0
    for vm_name, zone in long_lived_vms:
        dest_uri = vm_serial_rolling_uri(vm_name, date_stamp, project_id)
        if dry_run:
            logger.info("[DRY-RUN] Would capture serial for %s -> %s", vm_name, dest_uri)
            captured += 1
            continue

        try:
            serial_output = compute_client.get_serial_port_output(
                project=project_id, zone=zone, instance=vm_name
            )
            if not serial_output.contents:
                logger.info("No serial output for %s (normal for freshly booted VMs)", vm_name)
                continue

            # Parse canonical GCS URI into bucket + blob path
            # URI shape: gs://{bucket}/log-archive/serial-rolling/{date}/{vm}/serial-console.txt
            uri_parts = dest_uri[len("gs://"):].split("/", 1)
            dest_bucket_name, dest_blob_path = uri_parts[0], uri_parts[1]

            dest_blob = storage_client.bucket(dest_bucket_name).blob(dest_blob_path)
            dest_blob.upload_from_string(serial_output.contents, content_type="text/plain")
            logger.info("Captured serial for %s -> %s", vm_name, dest_uri)
            captured += 1

        except Exception as exc:
            logger.error("Failed serial capture for %s: %s", vm_name, exc)

    logger.info(
        "Serial capture complete: %d VMs captured%s",
        captured,
        " (DRY-RUN)" if dry_run else "",
    )
    return captured


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
        " (DRY-RUN)" if args.dry_run else "",
    )

    exit_code = 0
    try:
        exit_code |= archive_vm_logs(project_id, dry_run=args.dry_run)
    except Exception as exc:
        logger.error("Log archival failed: %s", exc)
        exit_code = 1

    try:
        capture_long_lived_serial(project_id, dry_run=args.dry_run)
    except Exception as exc:
        logger.error("Serial capture failed: %s", exc)
        exit_code = 1

    return exit_code


if __name__ == "__main__":
    sys.exit(main())