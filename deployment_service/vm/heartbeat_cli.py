#!/usr/bin/env python3
"""VM-side heartbeat CLI — thin wrapper around UTL's HeartbeatDaemon.

All generic lifecycle plumbing (daemon loop, counter parsing, GCS uploader,
signal-file protocol) lives in ``unified_trading_library.lifecycle``. This
module owns only:

  - VM-specific CLI arg shape (--id/--name/--asset-group/--task/--mode/
    --start-date/--end-date/--log-uri/--local-log/--exit-status-file/
    --stall-breadcrumb/--watchdog-file/--heartbeat-interval-sec/
    --upload-interval-sec/--bucket)
  - ``_RegistryAdapter`` translating the generic ``HeartbeatStore`` protocol
    to ``deployment_service.deployments_registry.DeploymentsRegistry``
  - Event-payload builder preserving the VM deployment wire format
    (deployment_id / vm_name / asset_group / task / mode / start_date /
    end_date / status / row counters / exit_code / log_uri)

Invoked by ``scripts/vm/vm-exec-with-gcs-tee.sh`` on every VM. Signature is
backwards-compatible with ``scripts/vm/heartbeat_daemon.py`` @ 8007de0 —
existing wrapper shell scripts need no change.

Fails LOUD on imports — no try/except around UTL / registry.
"""

from __future__ import annotations

import argparse
import logging
import os
import pathlib
import sys
from typing import cast

from unified_trading_library import (
    DEPLOYMENT_COMPLETED,
    DEPLOYMENT_FAILED,
    DEPLOYMENT_PROGRESS,
    DEPLOYMENT_STARTED,
    HeartbeatDaemon,
    HeartbeatEntry,
    PubSubEventSink,  # pyright: ignore[reportPrivateImportUsage]
    SignalProtocol,
    get_storage_client,
    run_lifecycle,
    setup_events,
)

from deployment_service.deployments_registry import (
    DEFAULT_BUCKET,
    DeploymentRegistryEntry,
    DeploymentsRegistry,
)

logger = logging.getLogger("heartbeat_daemon")


# ---------------------------------------------------------------------------
# DeploymentsRegistry <-> HeartbeatStore adapter
# ---------------------------------------------------------------------------


def _entry_to_registry(entry: HeartbeatEntry) -> DeploymentRegistryEntry:
    md = entry.metadata
    counters = entry.counters
    return DeploymentRegistryEntry(
        deployment_id=entry.deployment_id,
        vm_name=str(md.get("vm_name", entry.service_name)),
        asset_group=str(md.get("asset_group") or md.get("category", "")),
        task=str(md.get("task", "")),
        mode=str(md.get("mode", "")),
        start_date=str(md.get("start_date", "")),
        end_date=str(md.get("end_date", "")),
        status=entry.status,
        started_at=entry.started_at,
        last_heartbeat_at=entry.last_heartbeat_at,
        completed_at=entry.completed_at,
        exit_code=entry.exit_code,
        rows_in=int(counters.get("rows_in", 0)),
        rows_out=int(counters.get("rows_out", 0)),
        rows_error=int(counters.get("rows_error", 0)),
        events_emitted=int(counters.get("events_emitted", 0)),
        log_uri=str(md.get("log_uri", "")),
    )


def _registry_to_entry(reg: DeploymentRegistryEntry) -> HeartbeatEntry:
    return HeartbeatEntry(
        deployment_id=reg.deployment_id,
        service_name=reg.vm_name,
        status=reg.status,
        started_at=reg.started_at,
        last_heartbeat_at=reg.last_heartbeat_at,
        completed_at=reg.completed_at,
        exit_code=reg.exit_code,
        counters={
            "rows_in": reg.rows_in,
            "rows_out": reg.rows_out,
            "rows_error": reg.rows_error,
            "events_emitted": reg.events_emitted,
        },
        metadata={
            "vm_name": reg.vm_name,
            "asset_group": reg.asset_group,
            "task": reg.task,
            "mode": reg.mode,
            "start_date": reg.start_date,
            "end_date": reg.end_date,
            "log_uri": reg.log_uri,
        },
    )


class _RegistryAdapter:
    """Makes ``DeploymentsRegistry`` satisfy the ``HeartbeatStore`` Protocol."""

    def __init__(self, registry: DeploymentsRegistry) -> None:
        self._registry = registry

    def register(self, entry: HeartbeatEntry) -> None:
        self._registry.register(_entry_to_registry(entry))

    def heartbeat(self, entry: HeartbeatEntry) -> None:
        self._registry.heartbeat(_entry_to_registry(entry))

    def complete(self, entry: HeartbeatEntry) -> None:
        self._registry.complete(_entry_to_registry(entry))

    def get(self, deployment_id: str) -> HeartbeatEntry | None:
        reg = self._registry.get(deployment_id)
        if reg is None:
            return None
        return _registry_to_entry(reg)


# ---------------------------------------------------------------------------
# VM-specific event payload (preserves wire format)
# ---------------------------------------------------------------------------


def _vm_payload(entry: HeartbeatEntry) -> dict[str, object]:
    md = entry.metadata
    counters = entry.counters
    return {
        "deployment_id": entry.deployment_id,
        "vm_name": md.get("vm_name", entry.service_name),
        "asset_group": md.get("asset_group") or md.get("category", ""),
        "task": md.get("task", ""),
        "mode": md.get("mode", ""),
        "start_date": md.get("start_date", ""),
        "end_date": md.get("end_date", ""),
        "status": entry.status,
        "rows_in": counters.get("rows_in", 0),
        "rows_out": counters.get("rows_out", 0),
        "rows_error": counters.get("rows_error", 0),
        "events_emitted": counters.get("events_emitted", 0),
        "exit_code": entry.exit_code,
        "log_uri": md.get("log_uri", ""),
    }


# ---------------------------------------------------------------------------
# Events bootstrap
# ---------------------------------------------------------------------------


def _init_events() -> None:
    """Initialise the UTL event sink targeting ``deployment-events``."""
    project_id = os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        logger.warning("GCP_PROJECT_ID unset; falling back to mode=local (events only in stdout)")
        from unified_trading_library import (  # noqa: imports-inside-functions
            LocalFsEventSink,
        )

        local_sink = LocalFsEventSink(
            path=pathlib.Path("/tmp/vm-heartbeat-events.jsonl"),
            service_name="vm-heartbeat-daemon",
        )
        setup_events(service_name="vm-heartbeat-daemon", mode="local", sink=local_sink)
        return
    topic = os.environ.get("DEPLOYMENT_EVENTS_TOPIC", "deployment-events")
    sink = PubSubEventSink(
        project_id=project_id,
        topic=topic,
        service_name="vm-heartbeat-daemon",
    )
    setup_events(service_name="vm-heartbeat-daemon", mode="batch", sink=sink)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="VM deployment heartbeat + uploader daemon")
    p.add_argument("--id", required=True, help="Deployment UUID")
    p.add_argument("--name", required=True, help="VM name")
    p.add_argument("--asset-group", required=True)
    p.add_argument("--task", required=True)
    p.add_argument(
        "--mode", required=True, choices=["dry", "full", "backfill", "forward-poll", "smoke"]
    )
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--log-uri", required=True, help="gs://bucket/key for the tailed log")  # noqa: gs-uri
    p.add_argument("--local-log", required=True, help="Path on disk being teed into")
    p.add_argument(
        "--exit-status-file",
        required=True,
        help="Path the wrapper writes the final CMD_PID rc to",
    )
    p.add_argument(
        "--stall-breadcrumb",
        required=True,
        help="Path the wrapper touches on stall detection",
    )
    p.add_argument(
        "--watchdog-file",
        required=True,
        help="Path this daemon touches every iter so the shell can verify it's alive",
    )
    p.add_argument(
        "--heartbeat-interval-sec",
        type=int,
        default=int(os.environ.get("HEARTBEAT_INTERVAL_SEC", "60")),
    )
    p.add_argument(
        "--upload-interval-sec",
        type=int,
        default=int(os.environ.get("UPLOAD_INTERVAL_SEC", "30")),
    )
    p.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help="Registry bucket (default: %(default)s)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    """Heartbeat daemon entry-point — wrapped in `run_lifecycle` for STEP 5.63.

    The body is wrapped in `with run_lifecycle(service_name="vm-heartbeat-daemon")`
    so the workspace QG `setup_events()` ↔ `run_lifecycle()` / `ServiceBootstrap()`
    pairing rule (STEP 5.63) is satisfied. `run_lifecycle` emits
    `VM_HEARTBEAT_DAEMON_RUN_STARTED` / `_RUN_COMPLETED` / `_RUN_FAILED` around
    the daemon's main loop. `_init_events()` initialises the underlying event
    sink before `run_lifecycle` enters, so the lifecycle events route through
    the configured sink (PubSub or local fallback).
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = _build_parser()
    raw_args = parser.parse_args(argv)

    # Pull each field as str/int with an explicit cast so the rest of main()
    # is fully typed (argparse.Namespace attrs are Any).
    def _s(name: str) -> str:
        return str(cast(object, getattr(raw_args, name)))

    def _i(name: str) -> int:
        return int(cast(int | str, getattr(raw_args, name)))

    deployment_id = _s("id")
    vm_name = _s("name")
    asset_group = _s("asset_group")
    task = _s("task")
    mode = _s("mode")
    start_date = _s("start_date")
    end_date = _s("end_date")
    log_uri = _s("log_uri")
    local_log_str = _s("local_log")
    exit_status_str = _s("exit_status_file")
    stall_str = _s("stall_breadcrumb")
    watchdog_str = _s("watchdog_file")
    heartbeat_interval = _i("heartbeat_interval_sec")
    upload_interval = _i("upload_interval_sec")
    bucket = _s("bucket")

    _init_events()

    with run_lifecycle(
        service_name="vm-heartbeat-daemon",
        details={
            "deployment_id": deployment_id,
            "vm_name": vm_name,
            "asset_group": asset_group,
            "task": task,
            "mode": mode,
        },
    ):
        registry = DeploymentsRegistry(bucket=bucket)
        store = _RegistryAdapter(registry)
        storage_client = get_storage_client()

        entry = HeartbeatEntry(
            deployment_id=deployment_id,
            service_name=vm_name,
            status="pending",
            started_at="",
            last_heartbeat_at="",
            metadata={
                "vm_name": vm_name,
                "asset_group": asset_group,
                "task": task,
                "mode": mode,
                "start_date": start_date,
                "end_date": end_date,
                "log_uri": log_uri,
            },
        )

        signals = SignalProtocol(
            exit_status_file=pathlib.Path(exit_status_str),
            stall_breadcrumb=pathlib.Path(stall_str),
            watchdog_file=pathlib.Path(watchdog_str),
        )

        daemon = HeartbeatDaemon(
            entry=entry,
            store=store,
            storage_client=storage_client,
            local_log=pathlib.Path(local_log_str),
            remote_log_uri=log_uri,
            signals=signals,
            started_event=DEPLOYMENT_STARTED,
            progress_event=DEPLOYMENT_PROGRESS,
            completed_event=DEPLOYMENT_COMPLETED,
            failed_event=DEPLOYMENT_FAILED,
            heartbeat_interval_sec=heartbeat_interval,
            upload_interval_sec=upload_interval,
            payload_builder=_vm_payload,
        )
        return daemon.run()


if __name__ == "__main__":
    sys.exit(main())
