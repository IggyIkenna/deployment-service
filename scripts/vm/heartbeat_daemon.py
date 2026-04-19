#!/usr/bin/env python3
"""
Persistent VM-side heartbeat + GCS log uploader daemon.

Replaces the two cold-start subprocess loops in ``vm-exec-with-gcs-tee.sh``:
  1. ``timeout 30 python deployment_heartbeat.py heartbeat ...`` every 60s.
  2. ``gsutil cp $LOCAL_LOG $GCS_LOG_URI`` every 30s.

Each of those forked a fresh process per tick, paying full Python/gsutil
init cost plus gRPC auth + channel setup. At 100+ concurrent VMs this piles
up into auth-token mint rate limits and multi-minute heartbeat latency.

This daemon imports UTL once, keeps the Pub/Sub + GCS clients warm, and
runs both loops as threads inside a single long-lived Python process.

Lifecycle (driven by vm-exec-with-gcs-tee.sh):
  1. Shell launches daemon in background with the same ``register``-shaped
     args (+ ``--local-log`` + ``--exit-status-file`` + ``--stall-breadcrumb``).
  2. Daemon calls ``register`` (DEPLOYMENT_STARTED + registry entry).
  3. Daemon loops:
       - heartbeat thread: every HEARTBEAT_INTERVAL_SEC, parse counters from
         LOCAL_LOG, call ``heartbeat`` (DEPLOYMENT_PROGRESS + registry update).
       - uploader thread: every UPLOAD_INTERVAL_SEC, overwrite GCS_LOG_URI
         with LOCAL_LOG contents (skipped when file size unchanged).
       - watchdog file ``/tmp/vm-exec-PID.daemon_alive`` touched every iter.
  4. Shell writes exit status into ``--exit-status-file`` when CMD_PID exits
     and SIGTERMs the daemon.
  5. Daemon SIGTERM handler: final counter parse + read exit status file +
     stall breadcrumb, emit DEPLOYMENT_COMPLETED or DEPLOYMENT_FAILED, final
     GCS upload, exit.

Iteration failures are isolated (shard-level failure isolation): a transient
Pub/Sub or GCS error is logged and the loop continues. Only SIGTERM /
SIGINT triggers shutdown.

Fails LOUD on imports (no try/except around UTL) per workspace rule.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import pathlib
import re
import signal
import sys
import threading
from datetime import UTC, datetime
from types import FrameType
from urllib.parse import urlparse

from unified_trading_library import (
    DEPLOYMENT_COMPLETED,
    DEPLOYMENT_FAILED,
    DEPLOYMENT_PROGRESS,
    DEPLOYMENT_STARTED,
    PubSubEventSink,  # pyright: ignore[reportPrivateImportUsage]
    get_storage_client,
    log_event,
    setup_events,
)

from deployment_service.deployments_registry import (
    DEFAULT_BUCKET,
    DeploymentRegistryEntry,
    DeploymentsRegistry,
)

logger = logging.getLogger("heartbeat_daemon")


# ---------------------------------------------------------------------------
# Counter parsing (ported from vm-exec-with-gcs-tee.sh parse_counters)
# ---------------------------------------------------------------------------


_COUNTER_KEYS: tuple[str, ...] = ("rows_in", "rows_out", "rows_error", "events_emitted")
_COUNTERS_DICT_RE = re.compile(r"counters\s*=\s*(\{[^}]+\})")
_EVENT_MARKER_RE = re.compile(r"INFO Event:")


def parse_counters(log_path: str | pathlib.Path) -> dict[str, int]:
    """Scan the last 2000 lines of LOCAL_LOG for counters=... and key=value pairs.

    Aggregates via MAX across every match, so a transient warning burst that
    displaces the most recent per-day summary line does not zero out rows_in.
    """
    path = pathlib.Path(log_path)
    out: dict[str, int] = {k: 0 for k in _COUNTER_KEYS}
    try:
        text = path.read_text(errors="ignore")
    except OSError:
        return out
    tail = "\n".join(text.splitlines()[-2000:])

    # JSON-ish "counters={...}" — take max across all matches.
    for m in _COUNTERS_DICT_RE.finditer(tail):
        try:
            data = json.loads(m.group(1).replace("'", '"'))
        except (json.JSONDecodeError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        for k in _COUNTER_KEYS:
            if k in data:
                with contextlib.suppress(TypeError, ValueError):
                    out[k] = max(out[k], int(data[k]))

    # Loose key=value pairs — take max across all matches.
    for key in _COUNTER_KEYS:
        kv_re = re.compile(rf"{key}\s*=\s*(\d+)")
        for m in kv_re.finditer(tail):
            out[key] = max(out[key], int(m.group(1)))

    # events_emitted fallback — tally `INFO Event:` lines.
    if out["events_emitted"] == 0:
        out["events_emitted"] = len(_EVENT_MARKER_RE.findall(tail))
    return out


# ---------------------------------------------------------------------------
# GCS URI helpers
# ---------------------------------------------------------------------------


def split_gcs_uri(uri: str) -> tuple[str, str]:
    """Split ``gs://bucket/some/path/run.log`` into ``('bucket', 'some/path/run.log')``."""
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc:
        raise ValueError(f"expected gs://bucket/key URI, got {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


# ---------------------------------------------------------------------------
# Events + time
# ---------------------------------------------------------------------------


def _utcnow_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _init_events() -> None:
    """Initialise event sink in batch mode targeting ``deployment-events`` topic."""
    project_id = os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        logger.warning("GCP_PROJECT_ID unset; falling back to mode=local (events only in stdout)")
        setup_events(service_name="vm-heartbeat-daemon", mode="local")
        return
    topic = os.environ.get("DEPLOYMENT_EVENTS_TOPIC", "deployment-events")
    sink = PubSubEventSink(
        project_id=project_id,
        topic=topic,
        service_name="vm-heartbeat-daemon",
    )
    setup_events(service_name="vm-heartbeat-daemon", mode="batch", sink=sink)


def _emit(
    event: str,
    severity: str,
    entry: DeploymentRegistryEntry,
    extra: dict[str, object] | None = None,
) -> None:
    payload: dict[str, object] = {
        "deployment_id": entry.deployment_id,
        "vm_name": entry.vm_name,
        "category": entry.category,
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
    }
    if extra:
        payload.update(extra)
    log_event(event_name=event, severity=severity, details=payload)


# ---------------------------------------------------------------------------
# Daemon
# ---------------------------------------------------------------------------


class HeartbeatDaemon:
    """Owns the registry entry, two loop threads, and shutdown sequence."""

    def __init__(
        self,
        *,
        deployment_id: str,
        vm_name: str,
        category: str,
        task: str,
        mode: str,
        start_date: str,
        end_date: str,
        log_uri: str,
        local_log: pathlib.Path,
        exit_status_file: pathlib.Path,
        stall_breadcrumb: pathlib.Path,
        watchdog_file: pathlib.Path,
        heartbeat_interval_sec: int,
        upload_interval_sec: int,
        registry_bucket: str,
    ) -> None:
        self.deployment_id = deployment_id
        self.vm_name = vm_name
        self.category = category
        self.task = task
        self.mode = mode
        self.start_date = start_date
        self.end_date = end_date
        self.log_uri = log_uri
        self.local_log = local_log
        self.exit_status_file = exit_status_file
        self.stall_breadcrumb = stall_breadcrumb
        self.watchdog_file = watchdog_file
        self.heartbeat_interval_sec = heartbeat_interval_sec
        self.upload_interval_sec = upload_interval_sec

        # Warm, cached clients — created once, reused by every iter.
        self.registry = DeploymentsRegistry(bucket=registry_bucket)
        self.storage_client = get_storage_client()
        self.log_bucket, self.log_key = split_gcs_uri(log_uri)

        self._shutdown = threading.Event()
        self._last_upload_size = -1

    # ---- register ---------------------------------------------------------

    def register(self) -> DeploymentRegistryEntry:
        now = _utcnow_iso()
        entry = DeploymentRegistryEntry(
            deployment_id=self.deployment_id,
            vm_name=self.vm_name,
            category=self.category,
            task=self.task,
            mode=self.mode,
            start_date=self.start_date,
            end_date=self.end_date,
            status="running",
            started_at=now,
            last_heartbeat_at=now,
            completed_at=None,
            exit_code=None,
            rows_in=0,
            rows_out=0,
            rows_error=0,
            events_emitted=1,
            log_uri=self.log_uri,
        )
        self.registry.register(entry)
        _emit(DEPLOYMENT_STARTED, "INFO", entry)
        logger.info("DEPLOYMENT_STARTED %s", entry.deployment_id)
        return entry

    # ---- loop threads -----------------------------------------------------

    def _touch_watchdog(self, source: str) -> None:
        try:
            self.watchdog_file.write_text(f"{source} ts={_utcnow_iso()} pid={os.getpid()}\n")
        except OSError as exc:
            logger.warning("watchdog touch failed: %s", exc)

    def _heartbeat_once(self) -> None:
        counters = parse_counters(self.local_log)
        entry = self.registry.get(self.deployment_id)
        if entry is None:
            logger.error("heartbeat: deployment %s not found in registry", self.deployment_id)
            return
        entry.rows_in = counters["rows_in"]
        entry.rows_out = counters["rows_out"]
        entry.rows_error = counters["rows_error"]
        entry.events_emitted = counters["events_emitted"]
        entry.last_heartbeat_at = _utcnow_iso()
        self.registry.heartbeat(entry)
        _emit(DEPLOYMENT_PROGRESS, "INFO", entry)

    def _heartbeat_loop(self) -> None:
        logger.info("heartbeat loop started interval=%ss", self.heartbeat_interval_sec)
        while not self._shutdown.wait(self.heartbeat_interval_sec):
            self._touch_watchdog("heartbeat")
            try:
                self._heartbeat_once()
            except Exception as exc:  # noqa: BLE001 — shard-level isolation
                logger.warning("heartbeat iteration failed: %s", exc)

    def _upload_once(self) -> None:
        if not self.local_log.exists():
            return
        try:
            stat = self.local_log.stat()
        except OSError as exc:
            logger.warning("upload: stat %s failed: %s", self.local_log, exc)
            return
        if stat.st_size == self._last_upload_size:
            return  # no new bytes since last upload — skip
        data = self.local_log.read_bytes()
        self.storage_client.upload_bytes(
            bucket=self.log_bucket,
            blob_path=self.log_key,
            data=data,
            content_type="text/plain",
        )
        self._last_upload_size = stat.st_size

    def _uploader_loop(self) -> None:
        logger.info(
            "uploader loop started interval=%ss target=%s",
            self.upload_interval_sec,
            self.log_uri,
        )
        while not self._shutdown.wait(self.upload_interval_sec):
            self._touch_watchdog("uploader")
            try:
                self._upload_once()
            except Exception as exc:  # noqa: BLE001 — shard-level isolation
                logger.warning("upload iteration failed: %s", exc)

    # ---- shutdown ---------------------------------------------------------

    def _read_exit_status(self) -> int:
        """Read the rc the shell wrote after CMD_PID exited.

        Missing file / unparseable body → rc=1 so we never emit a false
        DEPLOYMENT_COMPLETED on a daemon that was SIGKILLed before the
        shell ever wrote the status.
        """
        if not self.exit_status_file.exists():
            logger.warning("exit status file %s missing; assuming rc=1", self.exit_status_file)
            return 1
        try:
            body = self.exit_status_file.read_text().strip()
            return int(body)
        except (OSError, ValueError) as exc:
            logger.warning("exit status read failed (%s); assuming rc=1", exc)
            return 1

    def complete(self) -> int:
        """Emit DEPLOYMENT_COMPLETED or DEPLOYMENT_FAILED + final upload."""
        rc = self._read_exit_status()
        status = "completed" if rc == 0 else "failed"

        # Stall breadcrumb overrides — wrapper synthesised the rc from a SIGKILL.
        if self.stall_breadcrumb.exists():
            status = "failed"
            if rc == 0:
                rc = 124  # same rc GNU timeout uses on wall-clock expiry

        counters = parse_counters(self.local_log)
        entry = self.registry.get(self.deployment_id)
        if entry is None:
            logger.error("complete: deployment %s not found in registry", self.deployment_id)
            return rc
        entry.rows_in = counters["rows_in"]
        entry.rows_out = counters["rows_out"]
        entry.rows_error = counters["rows_error"]
        entry.events_emitted = counters["events_emitted"]
        entry.exit_code = rc
        entry.status = status
        now = _utcnow_iso()
        entry.last_heartbeat_at = now
        entry.completed_at = now
        self.registry.complete(entry)
        event = DEPLOYMENT_COMPLETED if status == "completed" else DEPLOYMENT_FAILED
        severity = "INFO" if status == "completed" else "ERROR"
        extra: dict[str, object] = {}
        if self.stall_breadcrumb.exists():
            extra["stall_breadcrumb"] = self.stall_breadcrumb.read_text(errors="ignore").strip()
        _emit(event, severity, entry, extra=extra if extra else None)
        logger.info("%s %s (exit_code=%s)", event, entry.deployment_id, entry.exit_code)

        # Final GCS upload — always, even if same size. Operators want the
        # last byte on disk to be the last byte in GCS.
        try:
            if self.local_log.exists():
                self.storage_client.upload_bytes(
                    bucket=self.log_bucket,
                    blob_path=self.log_key,
                    data=self.local_log.read_bytes(),
                    content_type="text/plain",
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("final upload failed: %s", exc)
        return rc

    def _signal_handler(self, signum: int, _frame: FrameType | None) -> None:
        logger.info("received signal %s — initiating shutdown", signum)
        self._shutdown.set()

    def run(self) -> int:
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        self.register()
        self._touch_watchdog("startup")

        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="heartbeat-loop", daemon=True
        )
        uploader_thread = threading.Thread(
            target=self._uploader_loop, name="uploader-loop", daemon=True
        )
        heartbeat_thread.start()
        uploader_thread.start()

        # Block main thread until shutdown is signalled.
        while not self._shutdown.is_set():
            # Signal-wake: threading.Event.wait releases on SIGTERM handler.
            self._shutdown.wait(timeout=60.0)

        # Give loops a moment to exit cleanly.
        heartbeat_thread.join(timeout=5.0)
        uploader_thread.join(timeout=5.0)

        return self.complete()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="VM deployment heartbeat + uploader daemon")
    p.add_argument("--id", required=True, help="Deployment UUID")
    p.add_argument("--name", required=True, help="VM name")
    p.add_argument("--category", required=True)
    p.add_argument("--task", required=True)
    p.add_argument(
        "--mode", required=True, choices=["dry", "full", "backfill", "forward-poll", "smoke"]
    )
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--log-uri", required=True, help="gs://bucket/key for the tailed log")
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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = _build_parser()
    args = parser.parse_args(argv)

    _init_events()

    daemon = HeartbeatDaemon(
        deployment_id=args.id,
        vm_name=args.name,
        category=args.category,
        task=args.task,
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        log_uri=args.log_uri,
        local_log=pathlib.Path(args.local_log),
        exit_status_file=pathlib.Path(args.exit_status_file),
        stall_breadcrumb=pathlib.Path(args.stall_breadcrumb),
        watchdog_file=pathlib.Path(args.watchdog_file),
        heartbeat_interval_sec=args.heartbeat_interval_sec,
        upload_interval_sec=args.upload_interval_sec,
        registry_bucket=args.bucket,
    )
    return daemon.run()


if __name__ == "__main__":
    sys.exit(main())
