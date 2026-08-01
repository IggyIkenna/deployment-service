"""Shared, cloud-agnostic GCS read helpers for the data-pipeline fleet monitors.

All reads go through ``unified_trading_library.get_storage_client`` (the unified
cloud interface) — never ``google.cloud`` / ``boto3`` directly. The helpers are
pure functions over an injected ``StorageClient`` so the watchers are
unit-testable with a mock storage client (QG runs credential-free + block-network).
"""

from __future__ import annotations

import json
import logging
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import cast

from unified_trading_library import StorageClient

logger = logging.getLogger(__name__)

# Bounded wait for a single GCS SDK call inside a per-VM fleet-sweep hot loop.
#
# **Why not simply pass a caller-supplied ``timeout=`` straight to the SDK
# call?** The UTL ``StorageClient`` abstraction
# (``unified_trading_library.cloud_interface.abstractions.StorageClient``,
# implemented by ``GCSStorageClient`` in ``cloud_interface/providers/gcp.py``)
# does NOT expose a ``timeout`` kwarg on ``blob_exists``/``download_bytes`` to
# callers — the GCP provider hardcodes its OWN internal timeout
# (``download_bytes`` -> ``blob.download_as_bytes(timeout=600, ...)``;
# ``blob_exists`` -> ``blob.exists()`` with no override at all, whatever the
# native SDK's own default is). Read directly from source before writing this
# fix (dp_exit_code_monitor_cron_dead_2026_07_23.md's suggested "just pass
# timeout=" assumed a kwarg this interface does not actually expose). Note
# too that ``download_bytes``'s hardcoded 600s ALONE already exceeds this
# Cloud Run job's own 300s ``timeoutSeconds`` budget, so even that call's
# eventual internal timeout could never fire before Cloud Run kills the whole
# task first. Extending the shared UTL interface to accept a caller-supplied
# timeout would be the ideal long-term fix but is a cross-repo change to a T1
# shared library, out of this single-repo fix's scope.
#
# So this helper bounds the call from the CALLER side instead, on a plain
# ``threading.Thread(daemon=True)`` — the same bounded-wait /
# abandon-and-log MECHANISM already shipped THIS WEEK for the identical
# untimed-GCS-read failure class in market-tick-data-service
# (mtds_defi_migration_cell_stall_untimed_gcs_read_2026_07_22.md), adapted
# from that fix's N-way ``ThreadPoolExecutor`` poll down to a single-call
# join (this file has one blocking call per invocation, not a fan-out).
# Deliberately a raw daemon ``Thread`` rather than a
# ``concurrent.futures.ThreadPoolExecutor``: an executor's worker is joined
# by a process-wide ``atexit`` hook at interpreter shutdown REGARDLESS of
# ``shutdown(wait=False)`` on that one pool instance — the exact gotcha the
# MTDS fix above had to separately work around with an explicit
# ``os._exit()``. A plain daemon thread is never joined by anything, so an
# abandoned one can never block this process's exit.
DEFAULT_GCS_CALL_TIMEOUT_SECONDS = 30.0


def _call_with_timeout[T](
    fn: Callable[[], T],
    *,
    timeout_seconds: float,
    op_label: str,
) -> T:
    """Run ``fn`` on a daemon thread, bounded to ``timeout_seconds``.

    Raises ``TimeoutError`` — logged as a distinct WARNING here, BEFORE
    raising, so a wedged blob is diagnosable rather than silently folding
    into the same "not found" outcome a genuinely-absent blob produces —
    when ``fn`` has not completed within the bound. Re-raises whatever ``fn``
    itself raised on a normal (non-timeout) failure, unchanged, so a caller
    that already wraps this in a broad ``except Exception`` (e.g.
    :func:`read_text`) sees no behavior change on that path.
    """
    result: list[T] = []
    error: list[Exception] = []

    def _runner() -> None:
        try:
            result.append(fn())
        except Exception as exc:
            error.append(exc)

    thread = threading.Thread(target=_runner, name=f"gcs-timeout:{op_label}", daemon=True)
    thread.start()
    thread.join(timeout_seconds)
    if thread.is_alive():
        logger.warning(
            "_gcs: %s exceeded the %.0fs bounded-call timeout — abandoning the stalled GCS "
            "call (the thread is left running as a daemon so it can never block process exit) "
            "and treating this call as failed",
            op_label,
            timeout_seconds,
        )
        raise TimeoutError(f"{op_label} exceeded {timeout_seconds:.0f}s")
    if error:
        raise error[0]
    return result[0]


# Canonical VM live-log paths (mirror unified_trading_library.deployment_registry):
#   gs://deployment-scripts-{pid}/vm-logs/{vm}/run.log
#   gs://deployment-scripts-{pid}/vm-logs/{vm}/EXIT_STATUS
RUN_LOG_BLOB = "vm-logs/{vm}/run.log"
EXIT_STATUS_BLOB = "vm-logs/{vm}/EXIT_STATUS"
HEARTBEAT_BLOB = "vm-heartbeat/{vm}.txt"
# Written by the backfill-VM shutdown-script when GCE triggers a spot preemption.
# Presence ⇒ the monitor classifies the termination as a benign relaunch (not CRITICAL).
PREEMPTED_BLOB = "vm-logs/{vm}/PREEMPTED"
# Written by the LAUNCHER (``lc_write_launch_params`` in
# ``scripts/vm/lib/launcher_common.sh``) right before ``gcloud compute instances
# create`` — {"launcher": "<script>", "env": {"K": "V", ...}}. Lets a preemption
# relaunch reproduce the EXACT venues/START_DATE/concurrency/lease the terminated
# VM was launched with, instead of falling back to the launcher's bare defaults.
LAUNCH_PARAMS_BLOB = "vm-logs/{vm}/LAUNCH_PARAMS.json"
# Written by the VM's OWN run loop (UTL ``record_vm_progress`` hooked into
# ``ManifestWriter.record_captured``) as each day's shards land — the durable
# progress checkpoint that lets a SPOT-preemption relaunch RESUME from the last
# completed date instead of replaying the original START_DATE from genesis. This
# closes the force-run day-one-replay bug (``spot-vms-for-backfill.md`` §
# "Preemption recovery MUST resume from PROGRESS"): ``{"last_completed_date":
# "YYYY-MM-DD", "monotonic": true, "vm_name": "...", "updated": "<iso>"}``.
# ``monotonic=false`` means the run recorded dates out of order (venue-outer
# iteration) so the single global frontier is NOT a safe skip-ahead point — the
# reader only overrides START_DATE when monotonic (see read_progress_checkpoint).
PROGRESS_BLOB = "vm-logs/{vm}/PROGRESS.json"

# Per-monitor-sweep "last-run" sentinel (the cron-watches-cron + deadman signal).
# Each fleet-monitor / meta-watcher sweep writes ``vm-census/<mode>-last-run.json``
# at the END of a successful sweep. Layer-1 (``check_cron_fired``) reads its
# freshness in-band; Layer-2 (the out-of-band deadman poster) reads it too. The
# bucket is the durable log bucket (``deployment-scripts-<project>``) — same place
# as CENSUS_BLOB / _FIRST_SEEN_BLOB.
MONITOR_LAST_RUN_BLOB = "vm-census/{mode}-last-run.json"


def write_monitor_last_run(
    storage_client: StorageClient,
    log_bucket: str,
    mode: str,
    *,
    ok: bool,
    counts: dict[str, int] | None = None,
) -> None:
    """Write the ``vm-census/<mode>-last-run.json`` sentinel for a monitor sweep.

    Records the UTC timestamp + ok/fail + per-sweep counts so a downstream watcher
    (Layer-1 ``check_cron_fired`` in-band; Layer-2 deadman out-of-band) can detect a
    monitor cron that STOPPED FIRING (a stale/absent sentinel). Best-effort — a
    persist failure must never crash the sweep (the sweep already did its real work).
    """
    payload = {
        "mode": mode,
        "ts": datetime.now(UTC).isoformat(),
        "ok": bool(ok),
        "counts": counts or {},
    }
    blob_path = MONITOR_LAST_RUN_BLOB.format(mode=mode)
    try:
        storage_client.upload_bytes(
            log_bucket,
            blob_path,
            json.dumps(payload, sort_keys=True).encode("utf-8"),
            content_type="application/json",
        )
    except Exception as exc:
        logger.warning("write_monitor_last_run(%s) failed: %s", mode, exc)


def read_monitor_last_run(
    storage_client: StorageClient,
    log_bucket: str,
    mode: str,
) -> dict[str, object] | None:
    """Read + parse the ``vm-census/<mode>-last-run.json`` sentinel, or ``None``.

    ``None`` ⇒ the sentinel is absent (the monitor never wrote it / has never run)
    or unparseable. A caller treats ``None`` as the fail-safe "stale" verdict.
    """
    raw = read_text(storage_client, log_bucket, MONITOR_LAST_RUN_BLOB.format(mode=mode))
    if not raw:
        return None
    try:
        loaded = cast("object", json.loads(raw))
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(loaded, dict):
        return None
    data = cast("dict[str, object]", loaded)
    return {str(k): v for k, v in data.items()}


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

    **HARD-LEARNED (2026-06-22):** the unified ``StorageClient.get_blob_metadata``
    leaves ``last_modified=None`` for objects in ``deployment-scripts-*`` (the
    GCS metadata field is simply not populated on this read path). Relying on it
    alone made EVERY running VM read ``age=None`` → the heartbeat watcher
    classified 40/43 live VMs as ``EVENT_LOOP_STARVED`` (a false-alert flood that
    was as useless as silence — the operator's "zero real alerts" symptom). So
    when ``last_modified`` is absent we fall back to the blob's CONTENT timestamp
    via :func:`heartbeat_blob_age_minutes` for the heartbeat blob shape. SSOT:
    ``data_pipeline_hardening_self_monitoring_2026_06_22.md`` Phase 2 BUG2.
    """
    try:
        meta = storage_client.get_blob_metadata(bucket, blob_path)
    except Exception:
        meta = None
    if meta is not None and meta.last_modified:
        try:
            modified = datetime.fromisoformat(meta.last_modified)
        except ValueError:
            modified = None
        if modified is not None:
            if modified.tzinfo is None:
                modified = modified.replace(tzinfo=UTC)
            return (datetime.now(UTC) - modified.astimezone(UTC)).total_seconds() / 60.0
    # last_modified missing/unparseable → fall back to the blob's CONTENT epoch
    # (the heartbeat sidecar writes `<epoch_seconds>` as its first line). This is
    # the RELIABLE liveness signal when the storage-client metadata is bare.
    return _content_epoch_age_minutes(storage_client, bucket, blob_path)


def _content_epoch_age_minutes(storage_client: StorageClient, bucket: str, blob_path: str) -> float | None:
    """Age in minutes from a blob's CONTENT timestamp, or ``None``.

    Handles the TWO content shapes that live in ``deployment-scripts-*`` (where the
    storage-client ``last_modified`` metadata reads bare — see :func:`blob_age_minutes`):

    * **epoch sidecar** — ``vm_heartbeat_sidecar.sh`` writes ``<epoch_seconds>\\n<rc>\\n<status>``
      to ``vm-heartbeat/{vm}.txt``; the first line is a Unix epoch integer.
    * **JSON sentinel/census** — ``write_monitor_last_run`` (``{mode}-last-run.json``) and the
      zombie-watchdog census (``watchdog-census.json``) write ``{"ts": "<ISO>", ...}``; the first
      char is ``{`` so the epoch parse fails → read the ISO ``ts`` field instead.

    Returns ``None`` when the blob is missing or carries neither a leading epoch nor a JSON
    ``ts``. **HARD-LEARNED (2026-06-23):** when only the epoch shape was handled, every JSON
    sentinel read ``age=None`` → the out-of-band deadman paged all 3 fleet monitors
    "sentinel stale: missing (never ran)" on every run despite the sweeps writing fresh
    sentinels (the bug surfaced the moment the deadman stopped crashing before this check).
    """
    raw = read_text(storage_client, bucket, blob_path)
    if not raw:
        return None
    stripped = raw.strip()
    if not stripped:
        return None
    first = stripped.splitlines()[0].strip()
    # Shape 1: epoch sidecar — first line is a positive Unix epoch integer.
    try:
        epoch = int(first)
    except ValueError:
        epoch = 0
    if epoch > 0:
        return (datetime.now(UTC).timestamp() - float(epoch)) / 60.0
    # Shape 2: JSON sentinel/census — read the ISO ``ts`` field as the freshness instant.
    return _json_ts_age_minutes(stripped)


def _json_ts_age_minutes(raw: str) -> float | None:
    """Age in minutes from a JSON blob's ISO ``ts`` field, or ``None`` if absent/unparseable."""
    try:
        loaded = cast("object", json.loads(raw))
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(loaded, dict):
        return None
    data = cast("dict[str, object]", loaded)
    iso = data.get("ts")
    if not isinstance(iso, str) or not iso:
        return None
    try:
        when = datetime.fromisoformat(iso)
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return (datetime.now(UTC) - when.astimezone(UTC)).total_seconds() / 60.0


def heartbeat_blob_age_minutes(storage_client: StorageClient, bucket: str, vm_name: str) -> float | None:
    """Age in minutes of a VM's durable heartbeat for ``vm_name``.

    Reads ``vm-heartbeat/{vm}.txt`` via the content-epoch-aware
    :func:`blob_age_minutes` (which falls back to the blob's first-line epoch when
    the storage-client ``last_modified`` is bare). ``None`` ⇒ the VM has emitted
    NO heartbeat blob at all — the genuine total-silence signal the watcher's
    ``EVENT_LOOP_STARVED`` verdict is meant for.
    """
    return blob_age_minutes(storage_client, bucket, HEARTBEAT_BLOB.format(vm=vm_name))


def heartbeat_blob_write_epoch(storage_client: StorageClient, bucket: str, vm_name: str) -> float | None:
    """Return the RAW Unix epoch the heartbeat sidecar last wrote for ``vm_name``.

    Unlike :func:`heartbeat_blob_age_minutes` (age relative to
    ``datetime.now(UTC)`` — i.e. "how stale is this blob right now"), this returns
    the write INSTANT itself so a caller can compute age relative to an ARBITRARY
    reference time. The post-mortem heartbeat-sidecar reliability check
    (``heartbeat_sidecar_reliability.py``) needs the blob's age AT THE VM's KILL
    TIME, not at analysis time — the blob (``vm-heartbeat/{vm}.txt``, in the
    durable ``deployment-scripts-*`` bucket) has no delete lifecycle, so it is
    still readable long after the VM itself is gone, and its content is frozen at
    whatever the sidecar last wrote before the VM was deleted.

    Parses the same first-line-epoch shape as the epoch-sidecar branch of
    :func:`_content_epoch_age_minutes` (``<epoch_seconds>\\n<rc>\\n<status>``).
    Returns ``None`` when the blob is missing, empty, or its first line isn't a
    positive integer (e.g. a VM that never ran the sidecar).
    """
    raw = read_text(storage_client, bucket, HEARTBEAT_BLOB.format(vm=vm_name))
    if not raw:
        return None
    stripped = raw.strip()
    if not stripped:
        return None
    first = stripped.splitlines()[0].strip()
    try:
        epoch = int(first)
    except ValueError:
        return None
    return float(epoch) if epoch > 0 else None


# Leading ISO-ish timestamp on a Python-logging run.log line:
#   ``2026-06-22 22:07:09,814 INFO ...``  (date + space + HH:MM:SS[,ms])
_LOG_TS_RE = re.compile(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})")

# The VM-life PIPELINE_HEARTBEAT marker the data VM echoes into the tee'd run.log
# every 60s (setup-data-pipeline-vm.sh `_launch_with_tee`). Carries the emit
# instant as ``ts=<ISO8601 Z>``. This is the WORKER-life signal — decoupled from
# the always-fresh infra ``vm-heartbeat`` sidecar blob, which ticks even when the
# data worker is dead (the 2026-06-22 "zero alerts" blind spot: every VM read
# ALIVE off the infra sidecar so a never-heartbeating worker never alerted).
_PIPELINE_HB_MARKER_RE = re.compile(r"PIPELINE_HEARTBEAT\b.*?\bts=(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})")


def pipeline_heartbeat_age_minutes(storage_client: StorageClient, bucket: str, vm_name: str) -> float | None:
    """Age in minutes since the VM's last ``PIPELINE_HEARTBEAT`` run.log marker.

    Parses the FRESHEST ``PIPELINE_HEARTBEAT ... ts=<ISO>`` marker out of the
    VM's tee'd ``run.log`` (the VM-life 60s emitter wired into the tee'd command).
    This is the data-WORKER liveness signal: it is emitted only while the worker's
    tee'd bash is alive, so it is DECOUPLED from the generic infra ``vm-heartbeat``
    sidecar (which the platform writes every 60s regardless of worker health). A
    running data VM whose worker process tree has died / was never launched stops
    advancing this marker → the watcher's ``DP_VM_NO_HEARTBEAT`` verdict.

    Returns ``None`` when the log is absent or carries no parseable marker (the
    caller treats ``None`` as "no PIPELINE_HEARTBEAT seen yet" — within grace it
    is benign, past grace it is the silent-VM alert).
    """
    log = read_text(storage_client, bucket, RUN_LOG_BLOB.format(vm=vm_name))
    if not log:
        return None
    last: datetime | None = None
    for match in _PIPELINE_HB_MARKER_RE.finditer(log):
        try:
            parsed = datetime.fromisoformat(match.group(1))
        except ValueError:
            continue
        last = parsed.replace(tzinfo=UTC)
    if last is None:
        return None
    return (datetime.now(UTC) - last).total_seconds() / 60.0


def run_log_age_minutes(storage_client: StorageClient, bucket: str, vm_name: str) -> float | None:
    """Age in minutes since the VM's ``run.log`` last advanced, or ``None``.

    This is the PROGRESS signal (CLAUDE.md 2026-06-22 hung-process rule): a VM can
    keep its heartbeat blob fresh (the bash sidecar never stops) yet make ZERO
    actual progress for hours (an unbounded-HTTP/scrape stall). A frozen run.log
    past the stall threshold is that hang — so the watcher cross-checks run.log
    freshness alongside heartbeat-blob freshness.

    The storage-client ``last_modified`` is bare on this bucket (see
    :func:`blob_age_minutes`), so freshness is derived from the LAST embedded
    ``YYYY-MM-DD HH:MM:SS`` timestamp in the log TAIL (the worker's own log lines
    advance only while it makes progress). ``None`` when the log is absent or
    carries no parseable timestamp (the caller treats ``None`` as "no progress
    signal", never as fresh).
    """
    log = read_text(storage_client, bucket, RUN_LOG_BLOB.format(vm=vm_name))
    if not log:
        return None
    # Scan the tail for the LAST embedded timestamp (the freshest log line).
    last: datetime | None = None
    for match in _LOG_TS_RE.finditer(log):
        try:
            parsed = datetime.fromisoformat(f"{match.group(1)}T{match.group(2)}")
        except ValueError:
            continue
        last = parsed.replace(tzinfo=UTC)
    if last is None:
        return None
    return (datetime.now(UTC) - last).total_seconds() / 60.0


@dataclass(frozen=True)
class RunLogSignals:
    """Both time-based signals derived from a single run.log download.

    Avoids the double-download OOM (incident 2026-06-23): the old sweep called
    ``pipeline_heartbeat_age_minutes()`` AND ``run_log_age_minutes()`` for each VM,
    each downloading the FULL ``run.log`` independently.  With 55 running VMs that
    was 110 full run.log downloads per 5-min tick; long-running backfill VMs
    accumulate many-MB logs → total memory easily exceeded the 2Gi Cloud Run job
    limit → ``Container terminated on signal 9`` (OOM) on every sweep execution.
    """

    pipeline_heartbeat_age_min: float | None
    run_log_age_min: float | None


def run_log_signals(storage_client: StorageClient, bucket: str, vm_name: str) -> RunLogSignals:
    """Download ``run.log`` ONCE and extract both the pipeline-heartbeat and log-mtime ages.

    Replaces separate ``pipeline_heartbeat_age_minutes()`` + ``run_log_age_minutes()``
    calls in the sweep loop — halving the run.log downloads per tick from 2xN to N.
    """
    log = read_text(storage_client, bucket, RUN_LOG_BLOB.format(vm=vm_name))
    if not log:
        return RunLogSignals(pipeline_heartbeat_age_min=None, run_log_age_min=None)

    now = datetime.now(UTC)

    # pipeline heartbeat age — last PIPELINE_HEARTBEAT ts= marker
    hb_last: datetime | None = None
    for match in _PIPELINE_HB_MARKER_RE.finditer(log):
        try:
            parsed = datetime.fromisoformat(match.group(1))
        except ValueError:
            continue
        hb_last = parsed.replace(tzinfo=UTC)
    hb_age: float | None = (now - hb_last).total_seconds() / 60.0 if hb_last is not None else None

    # run.log progress age — last embedded YYYY-MM-DD HH:MM:SS timestamp
    log_last: datetime | None = None
    for match in _LOG_TS_RE.finditer(log):
        try:
            parsed = datetime.fromisoformat(f"{match.group(1)}T{match.group(2)}")
        except ValueError:
            continue
        log_last = parsed.replace(tzinfo=UTC)
    log_age: float | None = (now - log_last).total_seconds() / 60.0 if log_last is not None else None

    return RunLogSignals(pipeline_heartbeat_age_min=hb_age, run_log_age_min=log_age)


def read_text(
    storage_client: StorageClient,
    bucket: str,
    blob_path: str,
    *,
    timeout_seconds: float = DEFAULT_GCS_CALL_TIMEOUT_SECONDS,
) -> str | None:
    """Download a blob as UTF-8 text, or ``None`` if missing/unreadable.

    Never raises — a missing blob (the VM never wrote it) reads as ``None``,
    and so does a genuine read error.

    **Bounded** (2026-07-23, dp_exit_code_monitor_cron_dead_2026_07_23.md):
    each underlying GCS call (``blob_exists`` / ``download_bytes``) is
    wrapped in :func:`_call_with_timeout`, so a wedged/stalled connection on
    ONE blob fails fast (``timeout_seconds``, default
    :data:`DEFAULT_GCS_CALL_TIMEOUT_SECONDS` = 30s) instead of hanging the
    caller's entire per-VM fleet-sweep loop past its own Cloud Run job
    timeout — the confirmed failure mode that hung
    ``uts-prod-dp-exit-code-monitor`` on every execution for ~83h straight.
    A timeout is logged as a distinct WARNING (inside the helper, before this
    function folds it into the same ``None`` "unreadable" outcome every
    other read failure already produces) — never silently swallowed, so a
    future occurrence is diagnosable from the Cloud Run logs.
    """
    try:
        exists = _call_with_timeout(
            lambda: storage_client.blob_exists(bucket, blob_path),
            timeout_seconds=timeout_seconds,
            op_label=f"blob_exists({bucket}/{blob_path})",
        )
        if not exists:
            return None
        raw = _call_with_timeout(
            lambda: storage_client.download_bytes(bucket, blob_path),
            timeout_seconds=timeout_seconds,
            op_label=f"download_bytes({bucket}/{blob_path})",
        )
        return raw.decode("utf-8", errors="replace")
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


# A run.log line is "interesting" for an alert snippet when it carries an
# error/warn/traceback/OOM/rc-failure marker. Case-insensitive; matches the
# common shapes the VM wrappers + UTL classify-and-emit produce.
_ERROR_LINE_RE = re.compile(
    r"(error|exception|traceback|fail|fatal|critical|killed|oom|out of memory|rc=(?!0\b)\d+|exit_code=(?!0\b)\d+|warn)",
    re.IGNORECASE,
)


def error_snippet_from_run_log(
    storage_client: StorageClient,
    bucket: str,
    vm_name: str,
    *,
    max_error_lines: int = 12,
    tail_lines: int = 25,
    max_chars: int = 2400,
) -> str | None:
    """Extract an actionable error/warn snippet from a VM's durable run.log.

    Returns a compact text block built from (a) the LAST ``max_error_lines``
    lines matching ``_ERROR_LINE_RE`` (error/exception/traceback/OOM/non-zero
    rc/warn), and (b) the final ``tail_lines`` lines of the log (so the operator
    always sees how the run ended even when no line matched the error regex).
    ``None`` when the run.log is missing/empty (a self-deleted VM that never
    wrote one) — the caller omits the snippet rather than attaching an empty
    block. Capped at ``max_chars`` (the Slack code-block + the formatter's own
    3000-char ceiling). Never raises — a read failure reads as ``None``.

    The blob (``vm-logs/{vm}/run.log``) is the GCS-tee'd copy that survives the
    VM's ``VM_SHUTDOWN_ON_COMPLETION`` self-delete, so this works even for a
    backfill VM that is already gone by the time the exit-code sweep classifies it.
    """
    log = read_text(storage_client, bucket, RUN_LOG_BLOB.format(vm=vm_name))
    if not log:
        return None
    lines = log.splitlines()
    error_lines = [ln for ln in lines if _ERROR_LINE_RE.search(ln)][-max_error_lines:]
    tail = lines[-tail_lines:]
    parts: list[str] = []
    if error_lines:
        parts.append("── error/warn lines ──\n" + "\n".join(error_lines))
    if tail:
        parts.append("── run.log tail ──\n" + "\n".join(tail))
    snippet = "\n".join(parts).strip()
    if not snippet:
        return None
    if len(snippet) > max_chars:
        snippet = snippet[-max_chars:]
    return snippet


@dataclass(frozen=True)
class RecentLogSummary:
    """The deployment-observability popover's log summary — count + last line.

    Attributes:
        recent_error_count: How many of the last ``tail_lines`` run.log lines
            matched ``_ERROR_LINE_RE`` (error/exception/traceback/OOM/non-zero
            rc/warn) — the SAME regex ``error_snippet_from_run_log`` uses, so
            the popover's count agrees with the Slack-alert snippet.
        last_log_line: The final non-empty line of the log, or ``None`` when
            the log is missing/empty.
    """

    recent_error_count: int
    last_log_line: str | None


def recent_log_summary(
    storage_client: StorageClient,
    bucket: str,
    vm_name: str,
    *,
    tail_lines: int = 200,
) -> RecentLogSummary:
    """Recent error count + last log line for the deployment-observability popover.

    Reuses the SAME durable run.log blob + ``_ERROR_LINE_RE`` classifier
    ``error_snippet_from_run_log`` already reads for Slack alerts — no new
    GCS path, no new CloudWatch dependency (WS-D.0 principle 4's one scoped
    exception is Lambda, not VM run.log). A missing/empty log degrades to
    ``RecentLogSummary(0, None)`` — never raises, so a popover open for a
    self-deleted or not-yet-started VM shows an honest empty state.

    On-demand, single-target read (popover-triggered, not a bulk sweep) — the
    same acceptable read pattern the existing snippet functions already use.
    """
    log = read_text(storage_client, bucket, RUN_LOG_BLOB.format(vm=vm_name))
    if not log:
        return RecentLogSummary(recent_error_count=0, last_log_line=None)
    lines = log.splitlines()[-tail_lines:]
    error_count = sum(1 for ln in lines if _ERROR_LINE_RE.search(ln))
    non_empty = [ln for ln in lines if ln.strip()]
    last_line = non_empty[-1] if non_empty else None
    return RecentLogSummary(recent_error_count=error_count, last_log_line=last_line)


def run_log_console_url(bucket: str, vm_name: str) -> str:
    """Return the GCS-console deep-link to a VM's durable run.log object.

    Lets an alert carry a one-click link to the FULL log (the snippet is only the
    error/warn lines + tail). Pure string-builder — no I/O.
    """
    return f"https://console.cloud.google.com/storage/browser/_details/{bucket}/{RUN_LOG_BLOB.format(vm=vm_name)}"


def run_log_shows_stall(storage_client: StorageClient, bucket: str, vm_name: str) -> bool:
    """True when the durable run.log records a WORKER_STALLED kill (rc=124 class)."""
    log = read_text(storage_client, bucket, RUN_LOG_BLOB.format(vm=vm_name))
    if not log:
        return False
    return bool(_STALL_RE.search(log))


def read_launch_params(storage_client: StorageClient, bucket: str, vm_name: str) -> dict[str, str] | None:
    """Read the env vars ``vm_name`` was launched with, from ``LAUNCH_PARAMS.json``.

    Written by the launcher itself (``lc_write_launch_params``) at VM-creation
    time — this is what lets a SPOT-preemption relaunch reproduce the EXACT
    venues/START_DATE/concurrency/lease the terminated VM was launched with
    (never a blind relaunch onto the launcher's bare defaults, which for a
    launcher like ``launch-cefi-sharded-backfill.sh`` would mean the FULL
    genesis-to-now venue universe instead of the narrow tail that was actually
    running).

    Returns ``None`` when the blob is absent/unparseable — an older VM launched
    before this capture shipped, or a launcher that doesn't call the helper yet
    (only the cefi/tardis launchers do, as of this writing). The caller falls
    back to invoking the launcher with only the ambient env; the launcher's own
    ``tardis_concurrency_guard`` (or equivalent) still gates the relaunch either
    way — this function only affects WHAT gets relaunched, never whether it's
    allowed to.
    """
    raw = read_text(storage_client, bucket, LAUNCH_PARAMS_BLOB.format(vm=vm_name))
    if not raw:
        return None
    try:
        loaded = cast("object", json.loads(raw))
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(loaded, dict):
        return None
    data = cast("dict[str, object]", loaded)
    env_obj = data.get("env")
    if not isinstance(env_obj, dict):
        return None
    out: dict[str, str] = {}
    for key, value in cast("dict[str, object]", env_obj).items():
        if isinstance(value, str):
            out[str(key)] = value
    return out


# A resume checkpoint's ``last_completed_date`` must be a bare ISO calendar date.
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def read_progress_checkpoint(storage_client: StorageClient, bucket: str, vm_name: str) -> dict[str, str] | None:
    """Read the resume checkpoint ``vm_name`` wrote as it completed each day.

    Written by the VM's own run loop (UTL ``record_vm_progress`` hooked into
    ``ManifestWriter.record_captured``) to ``PROGRESS.json`` — the durable fix for
    the SPOT-preemption day-one-replay bug (``spot-vms-for-backfill.md`` §
    "Preemption recovery MUST resume from PROGRESS, never replay START_DATE"). A
    preemption relaunch reads this and RESUMES from ``last_completed_date`` instead
    of replaying the terminated VM's original ``START_DATE`` (which, for a
    ``--force`` run whose presence-skip is disabled, restarts the backfill at day
    one on every preemption — forever).

    Returns ``{"last_completed_date": "YYYY-MM-DD", "monotonic": "true"|"false"}``
    or ``None`` when the blob is absent/unparseable or carries no valid ISO date —
    an older VM, a pipeline that never emits the checkpoint, or a run that never
    completed a day. The caller then falls back to today's behavior (force → PAGE,
    non-force → verbatim replay). Never raises.

    ``monotonic`` is normalised to the string ``"true"`` ONLY when the blob's value
    is explicitly ``true``; anything else (a non-monotonic run, or an absent flag)
    becomes ``"false"`` so the relauncher fails safe — it will NOT skip START_DATE
    forward on a run whose recorded dates were out of order (which would drop the
    undone dates behind the frontier).

    **Accepted per-launcher naming exception (documented 2026-08-01,
    `infra_satellite_ao_dispatch_batch1_2026_07_26.md` "Close the two fleet-monitor
    blind spots"):** the ``canonical-migration-*-cdlap`` VMs (the ``*-candle-apply``
    categories in ``launch-canonical-migration-vm.sh``) write a REAL, working
    checkpoint — just not at this literal ``PROGRESS.json`` path. Their checkpoint
    lives at ``vm-logs/{vm}/MIGRATION_PROGRESS-shard{shard_index}.json``
    (``migrate_candle_canonical_2026_07.py``'s ``_CHECKPOINT_BLOB_TPL``) and carries
    a STRUCTURALLY DIFFERENT schema — ``last_processed_line_index``/
    ``processed_count``/``shard_index``/``shard_of`` (a 0-based line offset into
    ONE shard's deterministic object enumeration), not a calendar date. This
    function deliberately does NOT special-case that filename/schema: the two
    checkpoint mechanisms are not interchangeable (there is no date to extract from
    a line-index checkpoint), so "generalizing" this reader would require a second,
    parallel consumption path in ``relaunch_backfill_vm.py`` for a resume style that
    isn't date-based at all — disproportionate to the actual gap, because the
    resume already works WITHOUT this function's help: ``RelaunchPreemptedVm``
    relaunches the SAME ``vm_name`` (via ``VM_NAME_OVERRIDE``, captured in
    ``LAUNCH_PARAMS.json``), and ``migrate_candle_canonical_2026_07.py`` reads its
    OWN checkpoint (``checkpoint_uri_for_shard`` + ``read_migration_checkpoint``)
    keyed on that same ``vm_name`` — independent of whether this function ever sees
    it. The observable effect of this exception is purely cosmetic: a
    ``DP_VM_PREEMPTED``/``DP_VM_PREEMPTED_RECOVERED`` finding for one of these VMs
    never carries a ``progress_checkpoint`` detail even though a real checkpoint
    exists on disk — never a resume-safety gap. SSOT:
    `codex/05-infrastructure/spot-vms-for-backfill.md` § "Per-launcher-family
    conformance".
    """
    raw = read_text(storage_client, bucket, PROGRESS_BLOB.format(vm=vm_name))
    if not raw:
        return None
    try:
        loaded = cast("object", json.loads(raw))
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(loaded, dict):
        return None
    data = cast("dict[str, object]", loaded)
    last = data.get("last_completed_date")
    if not isinstance(last, str) or not _ISO_DATE_RE.match(last.strip()):
        return None
    mono = data.get("monotonic")
    monotonic = mono is True or str(mono).strip().lower() == "true"
    return {"last_completed_date": last.strip(), "monotonic": "true" if monotonic else "false"}


def is_vm_preempted(
    storage_client: StorageClient,
    bucket: str,
    vm_name: str,
    *,
    timeout_seconds: float = DEFAULT_GCS_CALL_TIMEOUT_SECONDS,
) -> bool:
    """True when the PREEMPTED signal blob exists for ``vm_name``.

    A spot backfill VM writes ``vm-logs/{vm}/PREEMPTED`` in its shutdown-script when
    GCE sends the preemption signal (30-second window before instance deletion).
    Presence of this blob tells the exit-code fleet monitor the termination was a
    benign SPOT preemption — not a crash or silent failure — so no
    ``DP_VM_GONE_NO_CAPTURE`` CRITICAL should fire. Never raises (a read error
    conservatively returns ``False`` so the monitor falls through to the normal
    classification path).

    **Bounded** (2026-07-23, same fix as :func:`read_text`): this is a sibling
    ``blob_exists`` call in ``exit_code_fleet_monitor.sweep()``'s own per-VM
    hot loop that does NOT funnel through ``read_text`` — a wedged connection
    here would hang the sweep exactly the same way, so it gets the identical
    :func:`_call_with_timeout` bound + distinct-WARNING-on-timeout logging.
    """
    try:
        return _call_with_timeout(
            lambda: storage_client.blob_exists(bucket, PREEMPTED_BLOB.format(vm=vm_name)),
            timeout_seconds=timeout_seconds,
            op_label=f"blob_exists({bucket}/{PREEMPTED_BLOB.format(vm=vm_name)})",
        )
    except Exception:
        return False


# ── no-capture-reason classification (DP-VM-002 false-positive killer) ────────
#
# A terminated VM with a FLAT consolidated/per-VM captured count is NOT
# necessarily a silent failure. The run.log distinguishes three benign cases
# from a genuine silent-zero (the 2026-06-23 false-positive flood diagnosis):
#
#   (a) PROGRESS    — the run actually WROTE rows ("Wrote N rows …" / record_captured /
#       "captured" climbing). The consolidated count can lag the writer's own
#       shard, so a flat consolidated count + a "wrote rows" run.log = benign.
#   (b) HONEST_ABSENCE — the source legitimately returned nothing (settled market →
#       "0 trades", off-season, "all venues already covered", record_empty,
#       enrichment-only "all entities already captured / fetching []"). The writer
#       recorded honest absence, which is correct behaviour, NOT a failure.
#   (c) RATE_LIMITED — an API-Football / HTTP-429 throttle wrote a partial result
#       (exit 0 but "Too many requests" / "HTTP 429" / DP_SOURCE_RATE_LIMITED).
#       This is real-transient → backoff-retry, NOT a silent-zero.
#
# Anything else (no progress + no honest-absence + no rate-limit signal) is a
# GENUINE silent zero (auth fail / 0-universe / unexpected empty) → still alerts.
class NoCaptureReason(StrEnum):
    """Why a terminated VM's captured count stayed flat (run.log-derived)."""

    PROGRESS = "progress"  # run.log shows rows were written → benign (consolidated lag)
    HONEST_ABSENCE = "honest_absence"  # source legitimately empty / already-complete → benign
    RATE_LIMITED = "rate_limited"  # HTTP-429 / API-Football throttle → backoff-retry, not silent
    SILENT = "silent"  # no benign signal → genuine silent-zero → ALERT


# Rows were genuinely written this run — the writer's own shard climbed even if the
# CONSOLIDATED index hasn't merged yet. Matches the MTDS/IS handler write logs
# ("Wrote N rows to gs://…", "Wrote N oracle price records …", "record_captured",
# "N captured").
#   - ``Wrote N … rows|records``: broadened from "rows" only — the DeFi oracle-prices
#     writer logs "Wrote 4 oracle price RECORDS …" (operator 2026-06-24 false positive:
#     defi-fwd-oracle-prices-poll wrote 4-7 records yet classified SILENT because the
#     pattern required the word "rows").
_PROGRESS_RE = re.compile(
    r"\bWrote\s+\d+\b[^\n]{0,60}\b(rows?|records?)\b"
    r"|\brecord_captured\b|\bCATALOGUE_PROMOTED\b|\bcaptured=(?!0\b)\d+"
    # migrate_cefi_content_instrument_id_catalogue_2026_07_17.py: a content-canonicalisation
    # dry-run/audit script that structurally never writes the availability manifest, so its
    # own progress/summary vocabulary (would_patch/already_canonical_skipped in its stats=
    # dict, or its "SCRIPT 1 CONTENT MIGRATION SUMMARY" banner) is the only durable signal
    # that the run actually did something — without this it false-pages DP_VM_GONE_NO_CAPTURE
    # on every completed run (cefi_content_migration_vm_wedged_worker_2026_07_23).
    r"|\bwould_patch\b|\balready_canonical_skipped\b|SCRIPT 1 CONTENT MIGRATION SUMMARY",
    re.IGNORECASE,
)
# The source legitimately had nothing / the shard was already complete / the VM
# honestly recorded the expected-but-absent universe (sentinels). Matches
# honest-absence + enrichment-only "nothing to capture" run.log shapes (settled
# market, off-season, all-already-captured, record_empty).
_HONEST_ABSENCE_RE = re.compile(
    r"honest.?absence|record_empty|all (entities|expected sentinels|venues) already (captured|covered)"
    r"|already captured\b|0 trades\b|off.?season|EXPECTED_PAUSED_LEAGUE|EXPECTED_NO_PROVIDER_COVERAGE"
    r"|fetching \[\]|Nothing to do\b|Skipping .* already captured|no fixtures"
    # MTDS idempotent-skip pre-flight: a re-run found the (venue, date) already fully
    # captured and correctly skipped re-fetching → captured 0→0 is benign already-done,
    # NOT a silent zero (venue_fetch.py:248). Without this the classifier false-positived
    # DP_VM_GONE_NO_CAPTURE on every resumed/idempotent backfill VM (operator 2026-06-24).
    r"|all requested data_types fully covered|fully covered \(atoms|atoms ⊆ captured"
    # The VM honestly enumerated its universe and recorded expected-but-absent SENTINELS
    # (the Tier-3 per-instrument sentinel fan-out → expected_unattempted) — a silent zero
    # (auth fail / never-launched) never reaches the fan-out. captured=0 here is honest
    # absence, not a silent failure (operator 2026-06-24: cefi-bybit-2021-light).
    r"|per-instrument sentinel fan-out"
    # A cross-venue arb DETECTOR (service-only output, manifest-exempt) ran fine and found
    # no quotable pairs → rows_written=0 is the correct empty result, not a silent capture
    # failure (operator 2026-06-24: prediction-arb-detector).
    r"|no cross-venue pairs matched|ARB_DETECT_TICK\b"
    # The WRITER recorded a 4-state honest-absence / sentinel row (capture_status
    # empty_confirmed / expected_unattempted): a backfill that hit a genuinely empty
    # (venue, date) cell writes THESE, not captured rows, so a flat captured 0→0 is
    # honest absence, not a silent zero. A crashed / auth-failed / never-launched VM
    # never reaches the 4-state write. operator 2026-06-27: sports-ref-v3-1 (zero-fixture
    # 2022 dates → empty_confirmed) + instr-backfill-tradfi-ice (EU-seeding →
    # expected_unattempted) flat-captured runs that false-fired DP_SOURCE_RATE_LIMITED
    # and would otherwise have fallen through to GONE_NO_CAPTURE.
    r"|empty_confirmed\b|expected_unattempted\b|[Zz]ero-fixture fast path",
    re.IGNORECASE,
)
# A rate-limit THROTTLE event (real-transient → backoff-retry tier, NOT silent-zero).
# Tightened 2026-06-27 (DP_SOURCE_RATE_LIMITED false-positive flood: sports-ref-v3-1 +
# instr-backfill-tradfi-ice were clean rc=0 runs with NO 429): the prior pattern matched
# the BARE substring `rate.?limit(ed)?` — so any benign rate-limiter CONFIG / budget /
# telemetry line tripped it — and even its OWN event name `DP_SOURCE_RATE_LIMITED` (a
# self-match on a heartbeat / prior-alert / comment echo in the run.log). Require a genuine
# 429 / "too many requests" / "quota exceeded", or a "rate limit" immediately qualified by
# an error / retry context — never the bare config mention or the self-reference.
_RATE_LIMIT_RE = re.compile(
    r"Too many requests"
    r"|HTTP[ /]?429\b|\b429\b[^\n]{0,40}(rate|limit|throttl|too many)"
    r"|rate.?limit(ed)?[^\n]{0,30}(exceeded|hit|reached|retry|back.?off|429)"
    r"|quota exceeded|throttl(ed|ing)\b",
    re.IGNORECASE,
)


def classify_no_capture_reason(log: str | None) -> NoCaptureReason:
    """Classify WHY a flat-captured run is flat, from its run.log text. Pure.

    Precedence (most-actionable first):
      RATE_LIMITED  — a throttle wrote partial (backoff-retry, not silent) wins, so a
                      rate-limited run is never mistaken for benign honest-absence.
      PROGRESS      — rows were written (consolidated count merely lags).
      HONEST_ABSENCE — source legitimately empty / shard already complete.
      SILENT        — none of the above → genuine silent-zero → the caller ALERTS.

    ``None`` / empty log ⇒ SILENT (no benign evidence → fail toward alerting, the
    safe direction; a self-deleted VM that never wrote a run.log is suspect).
    """
    if not log:
        return NoCaptureReason.SILENT
    if _RATE_LIMIT_RE.search(log):
        return NoCaptureReason.RATE_LIMITED
    if _PROGRESS_RE.search(log):
        return NoCaptureReason.PROGRESS
    if _HONEST_ABSENCE_RE.search(log):
        return NoCaptureReason.HONEST_ABSENCE
    return NoCaptureReason.SILENT


def no_capture_reason_from_run_log(storage_client: StorageClient, bucket: str, vm_name: str) -> NoCaptureReason:
    """Read a terminated VM's durable run.log + classify why captured stayed flat.

    The GCS-tee'd run.log survives the VM's ``VM_SHUTDOWN_ON_COMPLETION`` self-delete,
    so this works even for an already-gone backfill VM. ``None`` log ⇒ SILENT.
    """
    log = read_text(storage_client, bucket, RUN_LOG_BLOB.format(vm=vm_name))
    return classify_no_capture_reason(log)
