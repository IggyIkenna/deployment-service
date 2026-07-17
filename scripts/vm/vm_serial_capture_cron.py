#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Periodic serial console capture for LONG_LIVED_LIVE and SCHEDULED_RECURRING VMs.

Runs on a schedule (every 6 hours) to capture the GCE serial console before
the ring buffer wraps. A one-shot capture at kill time (backup-vm-logs.sh)
misses early boot output once the ~1 MB ring buffer overwrites it; this cron
supplements that with periodic snapshots for long-lived VMs.

Only LONG_LIVED_LIVE and SCHEDULED_RECURRING lifecycle classes are targeted —
EPHEMERAL_BATCH and EPHEMERAL_EXPERIMENT VMs are short-lived enough that the
pre-kill hook captures everything relevant.

Archive path (SSOT: unified_trading_library.deployment_registry.vm_serial_rolling_uri):
    gs://deployment-scripts-{project}/log-archive/serial-rolling/{YYYYMMDD}/{vm}/serial-console.txt

Idempotency: date is truncated to the calendar day, so reruns on the same day
are no-ops (the existing GCS object is detected and skipped).

Usage:
    python -m scripts.vm.vm_serial_capture_cron [--dry-run] [--project-id <id>]
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime

from google.cloud import compute_v1
from unified_api_contracts.canonical.crosscutting import LifecycleClass, VmPrefixSpec
from unified_trading_library import UnifiedCloudConfig, storage_exists, upload_to_storage, vm_serial_rolling_uri

from scripts.vm.vm_zombie_watchdog import VM_PREFIX_TO_BUCKET

_TARGET_LIFECYCLE_CLASSES: frozenset[LifecycleClass] = frozenset(
    {
        LifecycleClass.LONG_LIVED_LIVE,
        LifecycleClass.SCHEDULED_RECURRING,
    }
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def _build_target_prefixes() -> frozenset[str]:
    """Return VM name prefixes for LONG_LIVED_LIVE or SCHEDULED_RECURRING lifecycle classes."""
    return frozenset(
        prefix
        for prefix, spec in VM_PREFIX_TO_BUCKET.items()
        if isinstance(spec, VmPrefixSpec) and spec.lifecycle_class in _TARGET_LIFECYCLE_CLASSES
    )


def _is_target_vm(vm_name: str, target_prefixes: frozenset[str]) -> bool:
    return any(vm_name.startswith(p) for p in target_prefixes)


def capture_serial_for_long_lived_vms(
    project_id: str,
    dry_run: bool = False,
) -> int:
    """Capture serial console output for all matching running VMs.

    Returns 0 on full success, 1 if any capture errors occurred.
    """
    now = datetime.now(UTC)
    # Truncate to calendar day for idempotent path within each daily window.
    date_stamp = now.strftime("%Y%m%d")

    target_prefixes = _build_target_prefixes()
    logger.info("Target prefixes (%d classified)", len(target_prefixes))

    compute_client = compute_v1.InstancesClient()

    request = compute_v1.AggregatedListInstancesRequest(project=project_id)

    captured = 0
    skipped_existing = 0
    error_count = 0

    for zone, scoped in compute_client.aggregated_list(request=request):
        if not scoped.instances:
            continue
        for inst in scoped.instances:
            if inst.status != "RUNNING":
                continue
            vm_name = inst.name
            if not _is_target_vm(vm_name, target_prefixes):
                continue

            zone_name = zone.replace("zones/", "")
            serial_uri = vm_serial_rolling_uri(vm_name, date_stamp, project_id)

            if dry_run:
                logger.info("[DRY-RUN] Would capture serial for %s -> %s", vm_name, serial_uri)
                captured += 1
                continue

            # Parse bucket + blob path from gs:// URI.
            uri_no_scheme = serial_uri[5:]  # strip "gs://"
            slash = uri_no_scheme.index("/")
            bucket_name = uri_no_scheme[:slash]
            object_path = uri_no_scheme[slash + 1 :]

            if storage_exists(bucket_name, object_path):
                logger.debug("Already captured %s on %s — skipping", vm_name, date_stamp)
                skipped_existing += 1
                continue

            try:
                serial_response = compute_client.get_serial_port_output(
                    project=project_id,
                    zone=zone_name,
                    instance=vm_name,
                )
                if serial_response.contents:
                    serial_bytes = (
                        serial_response.contents.encode()
                        if isinstance(serial_response.contents, str)
                        else serial_response.contents
                    )
                    upload_to_storage(bucket_name, object_path, serial_bytes)
                    logger.info("Captured serial for %s -> %s", vm_name, serial_uri)
                    captured += 1
                else:
                    logger.warning("Empty serial output for %s — skipping upload", vm_name)
            except Exception as exc:
                logger.error("Failed to capture serial for %s: %s", vm_name, exc)
                error_count += 1

    logger.info(
        "Periodic serial capture complete: %d captured, %d already-existed, %d errors%s",
        captured,
        skipped_existing,
        error_count,
        " (DRY-RUN)" if dry_run else "",
    )
    return 1 if error_count > 0 else 0


def main(argv: list[str] | None = None) -> int:
    """Entry point for Cloud Run Job execution."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Preview without making changes")
    parser.add_argument("--project-id", help="GCP project ID (defaults to UnifiedCloudConfig)")
    args = parser.parse_args(argv)

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
        "Starting periodic serial capture for project %s%s",
        project_id,
        " (DRY-RUN)" if args.dry_run else "",
    )

    try:
        return capture_serial_for_long_lived_vms(project_id, dry_run=args.dry_run)
    except Exception as exc:
        logger.error("Periodic serial capture failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
