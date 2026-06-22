"""Shared, cloud-agnostic GCS read helpers for the data-pipeline fleet monitors.

All reads go through ``unified_trading_library.get_storage_client`` (the unified
cloud interface) — never ``google.cloud`` / ``boto3`` directly. The helpers are
pure functions over an injected ``StorageClient`` so the watchers are
unit-testable with a mock storage client (QG runs credential-free + block-network).
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

from unified_trading_library import StorageClient

# Canonical VM live-log paths (mirror deployment_service.deployments_registry):
#   gs://deployment-scripts-{pid}/vm-logs/{vm}/run.log
#   gs://deployment-scripts-{pid}/vm-logs/{vm}/EXIT_STATUS
RUN_LOG_BLOB = "vm-logs/{vm}/run.log"
EXIT_STATUS_BLOB = "vm-logs/{vm}/EXIT_STATUS"
HEARTBEAT_BLOB = "vm-heartbeat/{vm}.txt"

# vm-exec-with-gcs-tee.sh emits a final ``[vm-exec] command exited rc=<n>`` line
# into run.log, and writes the integer rc to the EXIT_STATUS blob. Either is the
# durable terminal exit code that survives VM self-delete.
_RC_LINE_RE = re.compile(r"command exited rc=(-?\d+)")
# A WORKER_STALLED kill maps rc -> 124/137; the tee wrapper also records the cause.
_STALL_RE = re.compile(r"WORKER_STALLED|reason=WORKER_STALLED")


def blob_age_minutes(storage_client: StorageClient, bucket: str, blob_path: str) -> float | None:
    """Age in minutes of a blob's last modification, or ``None`` if missing/unreadable.

    Uses ``get_blob_metadata`` (cloud-agnostic) and the ISO ``last_modified`` field.
    Never raises — a transient read error reads as "unknown" (``None``), so the
    caller decides the safe verdict rather than crashing the sweep.
    """
    try:
        meta = storage_client.get_blob_metadata(bucket, blob_path)
    except Exception:
        return None
    if meta is None or not meta.last_modified:
        return None
    try:
        modified = datetime.fromisoformat(meta.last_modified)
    except ValueError:
        return None
    if modified.tzinfo is None:
        modified = modified.replace(tzinfo=UTC)
    return (datetime.now(UTC) - modified.astimezone(UTC)).total_seconds() / 60.0


def read_text(storage_client: StorageClient, bucket: str, blob_path: str) -> str | None:
    """Download a blob as UTF-8 text, or ``None`` if missing/unreadable.

    Never raises — a missing blob (the VM never wrote it) reads as ``None``.
    """
    try:
        if not storage_client.blob_exists(bucket, blob_path):
            return None
        return storage_client.download_bytes(bucket, blob_path).decode("utf-8", errors="replace")
    except Exception:
        return None


def read_terminal_exit_code(storage_client: StorageClient, bucket: str, vm_name: str) -> int | None:
    """Return the terminal ``exit_code`` of a (possibly self-deleted) VM run.

    Reads the persisted ``EXIT_STATUS`` blob FIRST (the integer rc that the
    ``vm-exec-with-gcs-tee.sh`` wrapper writes + that survives the VM's
    ``VM_SHUTDOWN_ON_COMPLETION`` self-delete). Falls back to parsing the final
    ``command exited rc=<n>`` line out of ``run.log`` when the EXIT_STATUS blob is
    absent.  Returns ``None`` only when neither source carries a code (the run
    never terminated, or the durable log was never written) — the caller treats
    ``None`` distinctly from a 0, never as success.
    """
    raw = read_text(storage_client, bucket, EXIT_STATUS_BLOB.format(vm=vm_name))
    if raw is not None:
        stripped = raw.strip()
        if stripped:
            try:
                return int(stripped.splitlines()[0].strip())
            except ValueError:
                pass

    log = read_text(storage_client, bucket, RUN_LOG_BLOB.format(vm=vm_name))
    if not log:
        return None
    # Scan the tail for the LAST rc= marker (a run may print intermediate ones).
    last: int | None = None
    for match in _RC_LINE_RE.finditer(log):
        last = int(match.group(1))
    return last


def run_log_shows_stall(storage_client: StorageClient, bucket: str, vm_name: str) -> bool:
    """True when the durable run.log records a WORKER_STALLED kill (rc=124 class)."""
    log = read_text(storage_client, bucket, RUN_LOG_BLOB.format(vm=vm_name))
    if not log:
        return False
    return bool(_STALL_RE.search(log))
