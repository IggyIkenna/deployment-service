#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""
VM-side deployment heartbeat + lifecycle event helper.

Invoked as a subprocess from `vm-exec-with-gcs-tee.sh`:

    python deployment_heartbeat.py register \\
        --id <uuid> --name <vm_name> --asset-group <CEFI> \\
        --task <canonical-migration> --mode <dry|full> \\
        --start-date <YYYY-MM-DD> --end-date <YYYY-MM-DD> \\
        --log-uri <gs://...>

    python deployment_heartbeat.py heartbeat --id <uuid> \\
        [--rows-in N] [--rows-out N] [--rows-error N] [--events-emitted N]

    python deployment_heartbeat.py complete --id <uuid> \\
        --exit-code <rc> [--rows-in N] [--rows-out N] [--rows-error N] \\
        [--events-emitted N] [--status completed|failed]

All three commands:
- Update the GCS-backed registry entry (active/<id>.json, or archive/<day>/<id>.json on complete).
- Emit the matching DEPLOYMENT_STARTED / DEPLOYMENT_PROGRESS / DEPLOYMENT_COMPLETED /
  DEPLOYMENT_FAILED event via UTL `log_event` so Pub/Sub + event sinks see it.

Fails LOUD on errors — never swallows exceptions. VM ops can see the cause in
the tee'd log.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime

from unified_trading_library import (
    DEFAULT_BUCKET,
    DEPLOYMENT_COMPLETED,
    DEPLOYMENT_FAILED,
    DEPLOYMENT_PROGRESS,
    DEPLOYMENT_STARTED,
    RUN_LEDGER_RECORDED,
    DeploymentRegistryEntry,
    DeploymentsRegistry,
    PubSubEventSink,  # pyright: ignore[reportPrivateImportUsage]
    PubSubFlatEventPublisher,
    log_event,
    setup_events,
)

# deployment-service is installed in the VM's tarball venv — importing here
# gives us the BoM resolver + classification SSOT for free.
from deployment_service.bom import resolve_deployment_bom
from deployment_service.deployment_classification import (
    UnclassifiedDeploymentError,
    umbrella_for_vm_name,
)
from deployment_service.deployment_config import DeploymentConfig

logger = logging.getLogger("deployment_heartbeat")


def _resolve_umbrella(vm_name: str) -> str:
    """Resolve a VM name's deployment umbrella (LIVE/BATCH/PAPER/EXPERIMENT) string.

    Reads ``vm_zombie_watchdog.VM_PREFIX_TO_BUCKET`` (co-located in scripts/vm/) and
    dispatches through the classification SSOT ``umbrella_for_vm_name`` so the
    DEPLOYMENT_* event carries the umbrella the alerting-service router splits on
    (LIVE → #uts-live-alerts, BATCH → #data-pipeline-alerts). Returns "" when the
    prefix is unregistered (the alert still routes to the batch default — never a crash
    on the heartbeat path; the unregistered-prefix gap is caught by the watchdog guard
    test, not by silently dropping the lifecycle event).
    """
    # Local imports: this resolver is only on the heartbeat path, and the
    # vm_zombie_watchdog script is VM-side / not import-safe at module load.
    import importlib
    import importlib.util
    from typing import cast

    from unified_api_contracts import VmPrefixSpec

    if importlib.util.find_spec("vm_zombie_watchdog") is None:
        logger.warning("vm_zombie_watchdog not on path; DEPLOYMENT_* event will carry no umbrella")
        return ""
    watchdog = importlib.import_module("vm_zombie_watchdog")
    registry = cast("dict[str, VmPrefixSpec | None]", watchdog.VM_PREFIX_TO_BUCKET)
    try:
        return umbrella_for_vm_name(vm_name, registry).value
    except UnclassifiedDeploymentError as _exc:
        logger.warning("umbrella unresolved for vm_name=%s: %s", vm_name, _exc)
        return ""


def _utcnow_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _init_events(config: DeploymentConfig) -> None:
    """Initialise event sink in batch mode — publish to Pub/Sub topic
    ``deployment-events`` so deployment-ui + any subscriber sees the
    DEPLOYMENT_STARTED/PROGRESS/COMPLETED/FAILED lifecycle in real time.

    UTL's ``setup_events(mode='batch')`` requires ``sink=`` explicitly. A
    previous heartbeat version called setup_events without a sink which
    RuntimeError'd and silently lost events; combined with the missing
    deployment-service tarball (fixed 2026-04-18, deployment-service
    241940d) this meant zero batch-VM observability. This function fixes
    bug #2.

    Resolves project id + topic via the typed ``DeploymentConfig`` (same
    path as ``deployment_service/vm/heartbeat_cli.py``) — previously a bare
    ``os.environ.get`` here independently duplicated the same defaults,
    a real drift risk flagged by the 2026-07-27 audit
    (deployment_durable_operational_data_bigquery_2026_07_21.md PR-1).
    """
    project_id = config.gcp_project_id
    if not project_id:
        # Fallback so we don't crash the VM; events degrade gracefully to
        # stdout via UTL's local mode.
        logger.warning("GCP_PROJECT_ID unset; falling back to mode=local (events only in stdout)")
        setup_events(service_name="vm-deployment-heartbeat", mode="local")
        return

    sink = PubSubEventSink(
        project_id=project_id,
        topic=config.deployment_events_topic,
        service_name="vm-deployment-heartbeat",
    )
    setup_events(service_name="vm-deployment-heartbeat", mode="batch", sink=sink)


def _build_run_summary_publisher(config: DeploymentConfig) -> PubSubFlatEventPublisher | None:
    """Flat-schema (BQ-bound) run-summary publisher, or None with no project.

    This CLI has no host-metrics sampler (unlike heartbeat_cli.py's
    HostMetricsSampler), so it publishes a run-summary row on ``complete``
    only — never a resource-sample event, honestly reflecting that this
    caller never samples host metrics.
    """
    project_id = config.gcp_project_id
    if not project_id:
        return None
    return PubSubFlatEventPublisher(project_id=project_id, topic=config.run_ledger_events_topic)


def _run_summary_payload(entry: DeploymentRegistryEntry) -> dict[str, object]:
    """Flat run_ledger row from the registry entry at completion time."""
    return {
        "deployment_id": entry.deployment_id,
        "vm_name": entry.vm_name,
        "asset_group": entry.asset_group,
        "task": entry.task,
        "mode": entry.mode,
        "status": entry.status,
        "exit_code": entry.exit_code,
        "started_at": entry.started_at,
        "completed_at": entry.completed_at,
        "rows_in": entry.rows_in,
        "rows_out": entry.rows_out,
        "rows_error": entry.rows_error,
        "events_emitted": entry.events_emitted,
        "log_uri": entry.log_uri,
        "image_digest": entry.image_digest,
        "git_commit": entry.git_commit,
        "cpu_pct": entry.cpu_pct,
        "mem_pct": entry.mem_pct,
        "disk_pct": entry.disk_pct,
        "workload_alive": entry.workload_alive,
    }


def _emit(
    event: str,
    severity: str,
    entry: DeploymentRegistryEntry,
    extra: dict[str, object] | None = None,
) -> None:
    payload: dict[str, object] = {
        "deployment_id": entry.deployment_id,
        "vm_name": entry.vm_name,
        "asset_group": entry.asset_group,
        # umbrella + cloud drive the alerting-service router channel split (LIVE →
        # #uts-live-alerts, BATCH → #data-pipeline-alerts). umbrella="" → batch default.
        "umbrella": _resolve_umbrella(entry.vm_name),
        "cloud": "GCP",
        "task": entry.task,
        "mode": entry.mode,
        "start_date": entry.start_date,
        "end_date": entry.end_date,
        "status": entry.status,
        "rows_in": entry.rows_in,
        "rows_out": entry.rows_out,
        "rows_error": entry.rows_error,
        "events_emitted": entry.events_emitted,
        "exit_code": entry.exit_code,
        "log_uri": entry.log_uri,
        "image_digest": entry.image_digest,
        "git_commit": entry.git_commit,
        "dep_versions": entry.dep_versions,
    }
    if extra:
        payload.update(extra)
    log_event(event_name=event, severity=severity, details=payload)


# ----- subcommands ---------------------------------------------------------


def cmd_register(args: argparse.Namespace, registry: DeploymentsRegistry) -> int:
    now = _utcnow_iso()
    # BoM: stamp provenance (image digest / git commit / dep versions) at
    # launch — heartbeat/complete preserve it via registry.get() round-trips.
    bom = resolve_deployment_bom()
    entry = DeploymentRegistryEntry(
        deployment_id=args.id,
        vm_name=args.name,
        asset_group=args.asset_group,
        task=args.task,
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        status="running",
        started_at=now,
        last_heartbeat_at=now,
        completed_at=None,
        exit_code=None,
        rows_in=0,
        rows_out=0,
        rows_error=0,
        events_emitted=1,
        log_uri=args.log_uri,
        image_digest=bom.image_digest,
        git_commit=bom.git_commit,
        dep_versions=dict(bom.dep_versions),
    )
    registry.register(entry)
    _emit(DEPLOYMENT_STARTED, "INFO", entry)
    logger.info("DEPLOYMENT_STARTED %s", entry.deployment_id)
    return 0


def cmd_heartbeat(args: argparse.Namespace, registry: DeploymentsRegistry) -> int:
    entry = registry.get(args.id)
    if entry is None:
        logger.error("heartbeat: deployment %s not found in active registry", args.id)
        return 2
    if args.rows_in is not None:
        entry.rows_in = int(args.rows_in)
    if args.rows_out is not None:
        entry.rows_out = int(args.rows_out)
    if args.rows_error is not None:
        entry.rows_error = int(args.rows_error)
    if args.events_emitted is not None:
        entry.events_emitted = int(args.events_emitted)
    entry.last_heartbeat_at = _utcnow_iso()
    registry.heartbeat(entry)
    _emit(DEPLOYMENT_PROGRESS, "INFO", entry)
    return 0


def cmd_complete(
    args: argparse.Namespace,
    registry: DeploymentsRegistry,
    run_summary_publisher: PubSubFlatEventPublisher | None,
) -> int:
    entry = registry.get(args.id)
    if entry is None:
        logger.error("complete: deployment %s not found in registry", args.id)
        return 2
    if args.rows_in is not None:
        entry.rows_in = int(args.rows_in)
    if args.rows_out is not None:
        entry.rows_out = int(args.rows_out)
    if args.rows_error is not None:
        entry.rows_error = int(args.rows_error)
    if args.events_emitted is not None:
        entry.events_emitted = int(args.events_emitted)
    entry.exit_code = int(args.exit_code)
    status = args.status or ("completed" if entry.exit_code == 0 else "failed")
    entry.status = status
    now = _utcnow_iso()
    entry.last_heartbeat_at = now
    entry.completed_at = now
    registry.complete(entry)
    event = DEPLOYMENT_COMPLETED if status == "completed" else DEPLOYMENT_FAILED
    severity = "INFO" if status == "completed" else "ERROR"
    _emit(event, severity, entry)
    logger.info("%s %s (exit_code=%s)", event, entry.deployment_id, entry.exit_code)

    # Additive flat-schema run-summary publish (best-effort — must not fail
    # the CLI's exit code; the DEPLOYMENT_COMPLETED/FAILED event above already
    # fired and the registry archive already happened).
    if run_summary_publisher is not None:
        try:
            run_summary_publisher.publish(_run_summary_payload(entry))
        except Exception as exc:
            logger.warning("%s publish failed: %s", RUN_LEDGER_RECORDED, exc)

    return 0


# ----- CLI ----------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="VM deployment heartbeat helper")
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="Registry bucket (default: %(default)s)")
    sub = p.add_subparsers(dest="operation", required=True)

    reg = sub.add_parser("register", help="Create a new ACTIVE registry entry + DEPLOYMENT_STARTED")
    reg.add_argument("--id", required=True)
    reg.add_argument("--name", required=True)
    reg.add_argument("--asset-group", required=True)
    reg.add_argument("--task", required=True)
    reg.add_argument("--mode", required=True, choices=["dry", "full", "backfill", "forward-poll", "smoke"])
    reg.add_argument("--start-date", required=True)
    reg.add_argument("--end-date", required=True)
    reg.add_argument("--log-uri", required=True)

    hb = sub.add_parser("heartbeat", help="Update counters + DEPLOYMENT_PROGRESS event")
    hb.add_argument("--id", required=True)
    hb.add_argument("--rows-in", type=int)
    hb.add_argument("--rows-out", type=int)
    hb.add_argument("--rows-error", type=int)
    hb.add_argument("--events-emitted", type=int)

    comp = sub.add_parser("complete", help="Archive entry + DEPLOYMENT_COMPLETED/FAILED event")
    comp.add_argument("--id", required=True)
    comp.add_argument("--exit-code", type=int, required=True)
    comp.add_argument("--status", choices=["completed", "failed"])
    comp.add_argument("--rows-in", type=int)
    comp.add_argument("--rows-out", type=int)
    comp.add_argument("--rows-error", type=int)
    comp.add_argument("--events-emitted", type=int)

    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = _build_parser()
    args = parser.parse_args(argv)

    config = DeploymentConfig()
    _init_events(config)
    registry = DeploymentsRegistry(bucket=args.bucket)

    if args.operation == "register":
        return cmd_register(args, registry)
    if args.operation == "heartbeat":
        return cmd_heartbeat(args, registry)
    if args.operation == "complete":
        return cmd_complete(args, registry, _build_run_summary_publisher(config))
    parser.error(f"unknown operation: {args.operation}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
