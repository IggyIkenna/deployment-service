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
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import cast

from unified_trading_library import StorageClient

logger = logging.getLogger(__name__)

# Canonical VM live-log paths (mirror deployment_service.deployments_registry):
#   gs://deployment-scripts-{pid}/vm-logs/{vm}/run.log
#   gs://deployment-scripts-{pid}/vm-logs/{vm}/EXIT_STATUS
RUN_LOG_BLOB = "vm-logs/{vm}/run.log"
EXIT_STATUS_BLOB = "vm-logs/{vm}/EXIT_STATUS"
HEARTBEAT_BLOB = "vm-heartbeat/{vm}.txt"

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
# ("Wrote N rows to gs://…", "record_captured", "N captured").
_PROGRESS_RE = re.compile(
    r"\bWrote\s+\d+\s+\w*\s*rows?\b|\brecord_captured\b|\bCATALOGUE_PROMOTED\b|\bcaptured=(?!0\b)\d+",
    re.IGNORECASE,
)
# The source legitimately had nothing / the shard was already complete. Matches
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
    r"|all requested data_types fully covered|fully covered \(atoms|atoms ⊆ captured",
    re.IGNORECASE,
)
# A rate-limit throttle (real-transient → backoff-retry tier, NOT silent-zero).
# Matches API-Football / HTTP-429 / DP_SOURCE_RATE_LIMITED run.log shapes.
_RATE_LIMIT_RE = re.compile(
    r"Too many requests|HTTP 429\b|\b429\b.*(rate|limit)|rate.?limit(ed)?|DP_SOURCE_RATE_LIMITED"
    r"|requests per (minute|day)|quota exceeded",
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
