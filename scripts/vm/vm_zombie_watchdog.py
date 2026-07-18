#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""External liveness watchdog for backfill VMs.

Polls every backfill-class VM and kills any that match a zombie signature:
  1. Has been RUNNING for >= MIN_AGE_MINUTES
  2. AND either:
     a. Heartbeat blob (gs://.../vm-heartbeat/{vm}.txt) is missing OR mtime
        > HEARTBEAT_STALE_MINUTES old
     b. OR per-vm manifest shard mtime > SHARD_STALE_MINUTES old (across the
        relevant data bucket — tradfi/cefi/sports/etc.)

Designed to run as a long-lived poll loop on a small e2-small VM, every 5 min.

Why this catches the 2026-04-29 cefi zombie failure mode:
  Twelve cefi VMs lost their network namespace (link-local 169.254.169.254
  unreachable, dial tcp: network is unreachable). The in-VM watchdog
  (STALL_TIMEOUT_SEC=600 in vm-exec-with-gcs-tee.sh) couldn't help because
  it can't communicate when the VM has no network. External polling does
  not depend on the VM's network at all — checks only the VM's GCS
  fingerprint (heartbeat blob + manifest shard).

SSOT: this file lives in deployment-service/scripts/vm/vm_zombie_watchdog.py.
The launcher (launch-vm-zombie-watchdog.sh) `gsutil cp`s it to
gs://deployment-scripts-{project}/scripts/vm_zombie_watchdog.py at launch
time so the watchdog VM bootstraps from the repo copy.

**Catch-all coverage (2026-05-06):** every RUNNING VM is watched,
regardless of name prefix. Unknown-prefix VMs fall back to heartbeat-only
(safe — setup-data-pipeline-vm.sh starts the heartbeat sidecar for every
VM, including one-off scripts and migrations). This closes the
"added launcher, forgot to update VM_PREFIX_TO_BUCKET, VM zombies
forever" footgun (5 prefixes silently un-watched in 2026-05-05 alone).

VM_PREFIX_TO_BUCKET is now a *richer-signal* opt-in: prefixes listed
below ALSO get checked for per-VM manifest-shard write progress
(``_index/per_vm/{vm_name}.parquet``), which detects "VM still alive
+ heartbeating but writing NOTHING AT ALL" failures (network
partitions, hung adapters, etc.). New launchers don't NEED a dict
entry to be watched — only when the richer signal is desired.

SCOPE LIMIT — this is a LIVENESS watchdog, NOT a correctness one. The
shard check is an mtime stat on ONE per-VM blob, so it is deliberately
ENTITY-AGNOSTIC: it cannot tell whether the VM produced the entity it
was ASKED for. A VM launched ``--entity FIXTURES`` that writes only
``fixture_lineups``/``fixture_stats``/``fixture_events``/``player_stats``
keeps this shard mtime fresh and scores ``alive`` — correctly by this
module's contract, and WRONGLY if read as "useful work happened".
Measured 2026-07-18: exactly that, for 3.5h, zero ``entity=fixtures``
written (the write gate ignored ``redo_all``).

Entity-scoping it here is the wrong layer: it would turn a cheap
metadata stat into a download+parse of every VM's manifest parquet on
every fleet sweep. "Did the run produce the REQUESTED artifact?" is a
PER-RUN verification and belongs to whoever launched the job. SSOT:
``codex/12-agent-workflow/async-wait-and-poll-discipline.md`` §
"Backfill progress = the TARGET ARTIFACT, entity-scoped".

Daemon opt-out: long-lived VMs without a deadline (manifest-consolidator
poll loops, sports-scheduler, the watchdog itself, etc.) must label
themselves ``--labels=...,tier=daemon`` to skip the catch-all sweep.
Without that label, a daemon whose heartbeat sidecar pauses past
``--heartbeat-stale`` (default 15 min) WILL be killed. The watchdog
itself is also opted out via ``purpose=vm-zombie-watchdog`` (specific
fallback so it can't reap itself even if its launcher omits the tier
label).

Naming convention: see unified-trading-pm/cursor-configs/CLAUDE.md §
"VM Naming Convention".

Heartbeat blob path:
  gs://deployment-scripts-{project_id}/vm-heartbeat/{vm_name}.txt

The blob format is `<unix_epoch>\n<python_pid>\n<status>` and is rewritten
every 60s by a side-loop in setup-data-pipeline-vm.sh. VMs whose launchers
bypass setup-data-pipeline-vm.sh (e.g. the perp-funding one-off) will not
have a heartbeat — they fall through to the SHARD_STALE check.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime

from google.cloud import (
    compute_v1,  # noqa: TID251,RUF100 — zombie watchdog is a standalone VM daemon with direct GCP SDK access; no UTL wrapper for aggregated_list_instances at this scope
)
from requests.adapters import HTTPAdapter
from unified_api_contracts import LifecycleClass
from unified_trading_library import StorageClient, get_storage_client, upload_to_storage
from unified_trading_library.cloud_interface import (  # noqa: qg-deep-import (gcs_copy_object not in UTL top-level; root import raises ImportError at runtime)
    gcs_copy_object,
)

# VM_PREFIX_TO_BUCKET was moved into the deployment_service PACKAGE (2026-07-13) so
# it ships in the wheel and is importable from the deployment-api image where the
# data-pipeline fleet monitors run — it was structurally absent while it lived in
# this unpackaged scripts/ file (ModuleNotFoundError sweep crashes, monitoring-deadman
# alert 2026-07-12). This script is still deployed standalone to the watchdog VM
# (which pip-installs deployment_service), and guard tests read
# ``vm_zombie_watchdog.VM_PREFIX_TO_BUCKET``, so the SSOT is re-exported here.
from deployment_service.vm_prefix_registry import VM_PREFIX_TO_BUCKET

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
HEARTBEAT_BUCKET = f"deployment-scripts-{PROJECT_ID}"
CENSUS_BLOB_PATH = "vm-census/watchdog-census.json"


# urllib3 pool size for the AuthorizedSession on each Google client. The
# default of 10 overflows under our 16-thread x 50-prefix workload —
# 'Connection pool is full, discarding connection' warnings AND silent
# op.result() polling failures (observed 2026-05-05). 64 matches the
# phantom-audit 2*workers convention with headroom.
POOL_SIZE = 64


def _bump_pool_size(session, size: int = POOL_SIZE) -> None:
    """Replace the default HTTPAdapter on a requests.Session-like object.

    Note: monkey-patching requests.adapters.DEFAULT_POOLSIZE doesn't work —
    HTTPAdapter.__init__ captures DEFAULT_POOLSIZE as a default kwarg at
    class-definition time (Python early-bound default args), so module-level
    reassignment is silently ignored. We must mount a fresh adapter.
    """
    adapter = HTTPAdapter(pool_connections=size, pool_maxsize=size)
    session.mount("https://", adapter)
    session.mount("http://", adapter)


@dataclass
class WatchdogVerdict:
    vm_name: str
    zone: str
    age_minutes: float
    heartbeat_age_min: float | None  # None if missing
    shard_age_min: float | None  # None if no bucket / no shard
    verdict: str
    # one of:
    #   'alive'                       — VM is healthy, no action
    #   'too_young'                   — VM age < min_age, skip this cycle
    #   'zombie_no_heartbeat'         — heartbeat + shard both missing, VM age > 30m
    #   'zombie_stale_heartbeat'      — heartbeat older than threshold
    #   'zombie_stale_shard'          — heartbeat unimplemented + shard older than threshold
    #   'zombie_finished_not_shutdown' — workload wrote EXIT_STATUS file, VM still RUNNING
    #                                    after grace period. Catches:
    #                                      (a) launchers missing VM_SHUTDOWN_ON_COMPLETION=true
    #                                      (b) self-delete subshell that failed silently
    #                                      (c) post-mortem mode VMs left after debugging

    def is_zombie(self) -> bool:
        return self.verdict.startswith("zombie")


# Catch-all opt-out. A VM is excluded from the heartbeat watcher when EITHER:
#   * Its ``tier`` label is ``daemon`` (canonical: long-lived poll loops with
#     no fixed deadline — manifest-consolidator-, sports-scheduler-, etc.).
#   * Its ``purpose`` label matches one of ``DAEMON_PURPOSE_OPT_OUT`` (legacy /
#     specific exemptions, primarily the watchdog itself so it doesn't reap
#     itself even before its launcher adds ``tier=daemon``).
#
# Daemons must self-declare via labels — there's no reliable name-pattern
# heuristic for "this VM is allowed to idle" because heartbeat staleness
# during legitimate idle waits is exactly the failure mode that the
# rich-signal (per-VM shard write) catches for backfill VMs. New daemons
# should add ``--labels=...,tier=daemon`` to their gcloud launcher.
# Tiers that mark a VM as long-lived (poll loop / scheduler / watchdog) and
# therefore exempt from heartbeat staleness. ``daemon`` is canonical; older
# launchers used ``scheduler`` for sports-scheduler-* and that's still in use.
DAEMON_TIER_LABELS: frozenset[str] = frozenset({"daemon", "scheduler"})
DAEMON_PURPOSE_OPT_OUT: frozenset[str] = frozenset(
    {
        "vm-zombie-watchdog",  # watchdog itself — must not reap self
    }
)

# Per-prefix idle-time overrides: (heartbeat_stale_min, shard_stale_min).
# Overrides the global --heartbeat-stale / --shard-stale CLI args for matching
# VM name prefixes. Long-lived pipeline VMs need larger windows; fast backfills
# can use tighter ones for quicker zombie detection.
#
# Lookup: sorted by prefix length descending so longer (more-specific) prefixes
# win (e.g. "cefi-fwd-" beats "cefi-"). Uses same longest-match logic as
# VM_PREFIX_TO_BUCKET.
PREFIX_IDLE_THRESHOLDS: dict[str, tuple[float, float]] = {
    # Live-pipeline VMs: long-lived continuous writers; allow wider idle window
    "mtds-live-": (30.0, 240.0),
    "mdps-features-live-": (30.0, 240.0),
    "features-xc-": (30.0, 240.0),
    "replay-": (30.0, 240.0),
    # Forward-poll VMs: poll-loop cadence ~60s; 30min heartbeat window is safe
    "cefi-fwd-": (30.0, 180.0),
    "defi-fwd-": (30.0, 180.0),
    "aster-fwd-": (30.0, 180.0),
    "sfi-fwd-": (30.0, 180.0),
    "footystats-fwd-": (30.0, 180.0),
    # Fast API-football backfills: per-league row; tighter threshold
    "af-backfill-": (10.0, 60.0),
    "af-audit-": (10.0, 60.0),
    "af-recover-": (10.0, 60.0),
}


def _resolve_idle_thresholds(vm_name: str, global_hb_stale: float, global_shard_stale: float) -> tuple[float, float]:
    """Return (heartbeat_stale_min, shard_stale_min) for this VM name.

    Uses longest-prefix match from PREFIX_IDLE_THRESHOLDS; falls back to the
    global CLI-arg thresholds if no override is configured.
    """
    for prefix, (hb, shard) in sorted(PREFIX_IDLE_THRESHOLDS.items(), key=lambda x: -len(x[0])):
        if vm_name.startswith(prefix):
            return hb, shard
    return global_hb_stale, global_shard_stale


def _send_zombie_notification(webhook_url: str, zombies: list[WatchdogVerdict]) -> None:
    """POST a JSON zombie-alert payload to ``webhook_url`` (e.g. Slack incoming webhook).

    Silently suppresses network errors — notifications are best-effort; a failed
    POST must never prevent the watchdog from killing zombies.
    """
    import json
    import urllib.request

    payload = {
        "text": f":rotating_light: Zombie watchdog: {len(zombies)} zombie(s) detected",
        "zombies": [
            {
                "vm": v.vm_name,
                "zone": v.zone,
                "age_min": round(v.age_minutes, 1),
                "reason": v.verdict,
                "heartbeat_age_min": round(v.heartbeat_age_min, 1) if v.heartbeat_age_min is not None else None,
                "shard_age_min": round(v.shard_age_min, 1) if v.shard_age_min is not None else None,
            }
            for v in zombies
        ],
    }
    try:
        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info("zombie notification sent to webhook: status=%d", resp.status)
    except Exception as exc:
        logger.warning("zombie notification failed (best-effort, continuing): %s", exc)


def _is_daemon(labels: object) -> bool:
    """True if VM labels mark it as a long-lived daemon (heartbeat-watch opt-out)."""
    if not labels:
        return False
    if labels.get("tier") in DAEMON_TIER_LABELS:
        return True
    if labels.get("purpose") in DAEMON_PURPOSE_OPT_OUT:
        return True
    return False


def _list_watchable_vms(compute_client: compute_v1.InstancesClient) -> list[tuple[str, str, bool]]:
    """List every RUNNING VM that should be watched. Returns ``[(name, zone, has_known_prefix), ...]``.

    Catch-all by default: every running VM is returned, regardless of whether
    its name prefix appears in :data:`VM_PREFIX_TO_BUCKET`. ``has_known_prefix``
    tells the caller whether the richer per-VM shard-write signal is available
    for that VM (rich signal) or whether we have to fall back to heartbeat-only
    (still safe, since ``setup-data-pipeline-vm.sh`` starts the heartbeat
    sidecar for every VM regardless of task / prefix).

    Opt-out: VMs labelled ``purpose ∈ DAEMON_OPT_OUT_LABELS`` are skipped —
    they're long-lived daemons (the watchdog itself, manifest-consolidator
    poll loops, etc.) where heartbeat staleness during legitimate idle waits
    would produce false-positive zombie kills.

    Why catch-all + opt-out beats prefix-allowlist (2026-05-06 incident
    pattern): adding a launcher without remembering to update
    ``VM_PREFIX_TO_BUCKET`` here used to silently make the new VMs invisible
    to the watchdog. Five prefixes were missed in 2026-05-05 alone. Catch-all
    closes that footgun; new launchers only need to set the opt-out label
    explicitly when their VM is genuinely long-lived without a deadline.
    """
    request = compute_v1.AggregatedListInstancesRequest(project=PROJECT_ID)
    prefixes = tuple(VM_PREFIX_TO_BUCKET.keys())
    out: list[tuple[str, str, bool]] = []
    for zone, scoped in compute_client.aggregated_list(request=request):
        if not scoped.instances:
            continue
        for inst in scoped.instances:
            if inst.status != "RUNNING":
                continue
            # Opt-out: explicit daemon labels (tier=daemon | purpose ∈ {watchdog}).
            if _is_daemon(inst.labels):
                continue
            has_known_prefix = inst.name.startswith(prefixes)
            z = zone.replace("zones/", "")
            out.append((inst.name, z, has_known_prefix))
    return out


def _list_backfill_vms(compute_client: compute_v1.InstancesClient) -> list[tuple[str, str]]:
    """Back-compat shim — name retained so older callers don't break.

    Delegates to :func:`_list_watchable_vms` and drops the third tuple
    field. New code should call :func:`_list_watchable_vms` directly so it
    can branch on the rich-signal vs heartbeat-only distinction.
    """
    return [(name, zone) for (name, zone, _) in _list_watchable_vms(compute_client)]


def _blob_age_minutes(bucket: storage.Bucket, blob_name: str) -> float | None:  # noqa: F821 — google.cloud.storage type-only ref; not imported at runtime (TID251-banned) and scripts/ is outside the basedpyright scope
    """Return age in minutes of a blob, or None if missing."""
    blob = bucket.blob(blob_name)
    try:
        if not blob.exists():
            return None
        blob.reload()
        if blob.updated is None:
            return None
        return (datetime.now(UTC) - blob.updated).total_seconds() / 60.0
    except Exception:
        return None


def _vm_age_minutes(compute_client: compute_v1.InstancesClient, vm_name: str, zone: str) -> float:
    inst = compute_client.get(project=PROJECT_ID, zone=zone, instance=vm_name)
    created = datetime.fromisoformat(inst.creation_timestamp)
    return (datetime.now(UTC) - created.astimezone(UTC)).total_seconds() / 60.0


def _evaluate_vm(
    compute_client: compute_v1.InstancesClient,
    storage_client: StorageClient,
    vm_name: str,
    zone: str,
    min_age: float,
    heartbeat_stale: float,
    shard_stale: float,
    finished_grace: float,
) -> WatchdogVerdict:
    age = _vm_age_minutes(compute_client, vm_name, zone)
    if age < min_age:
        return WatchdogVerdict(vm_name, zone, age, None, None, "too_young")

    heartbeat_stale, shard_stale = _resolve_idle_thresholds(vm_name, heartbeat_stale, shard_stale)

    hb_bucket = storage_client.bucket(HEARTBEAT_BUCKET)

    # FIRST — check if the workload wrote an EXIT_STATUS file. If yes the
    # workload is finished (cleanly or otherwise). The VM should have
    # self-deleted via VM_SHUTDOWN_ON_COMPLETION=true; if it's still RUNNING
    # after a grace period, the launcher omitted that flag OR the self-delete
    # subshell failed OR it's a post-mortem-mode VM left for debugging.
    # Either way, it's a zombie consuming idle compute.
    # Reference incident 2026-05-06: mdps-backfill-cefi-... ran rc=0 then sat
    # RUNNING for 12h (launcher omitted shutdown flag).  This check is the
    # catch-net for that whole class of bug.
    exit_status_age = _blob_age_minutes(hb_bucket, f"vm-logs/{vm_name}/EXIT_STATUS")
    if exit_status_age is not None and exit_status_age > finished_grace:
        return WatchdogVerdict(vm_name, zone, age, None, None, "zombie_finished_not_shutdown")

    hb_age = _blob_age_minutes(hb_bucket, f"vm-heartbeat/{vm_name}.txt")

    shard_age: float | None = None
    for prefix, spec in VM_PREFIX_TO_BUCKET.items():
        if vm_name.startswith(prefix) and spec and spec.bucket:
            shard_age = _blob_age_minutes(
                storage_client.bucket(spec.bucket),
                f"_index/per_vm/{vm_name}.parquet",
            )
            break

    # Verdict logic:
    #   Heartbeat is the primary signal — if missing or stale → zombie.
    #   If heartbeat is unimplemented (None for both heartbeat AND shard older
    #   than 2x shard_stale, fall back to shard-only).
    if hb_age is None and shard_age is None and age > 30:
        return WatchdogVerdict(vm_name, zone, age, None, None, "zombie_no_heartbeat")
    if hb_age is not None and hb_age > heartbeat_stale:
        return WatchdogVerdict(vm_name, zone, age, hb_age, shard_age, "zombie_stale_heartbeat")
    if hb_age is None and shard_age is not None and shard_age > shard_stale:
        return WatchdogVerdict(vm_name, zone, age, None, shard_age, "zombie_stale_shard")
    return WatchdogVerdict(vm_name, zone, age, hb_age, shard_age, "alive")


def _backup_vm_logs_before_kill(vm_name: str, zone: str) -> None:
    """Backup VM logs (run.log + serial console) before deletion.

    Archives to gs://deployment-scripts-{project}/log-archive/snapshot_{ts}/
    for forensics. Best-effort — raises on failure but caller should proceed
    with delete anyway (logs are nice-to-have, not a kill blocker).

    Uses the canonical helper paths from unified_trading_library/deployment_registry.py.
    """
    from datetime import datetime

    from unified_trading_library import (
        vm_log_stream_uri,
        vm_run_log_archive_uri,
        vm_serial_console_archive_uri,
    )

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M")

    # 1. Archive run.log (if it exists) via server-side copy
    src_uri = vm_log_stream_uri(vm_name, PROJECT_ID)
    dst_uri = vm_run_log_archive_uri(vm_name, timestamp, PROJECT_ID)

    try:
        src_bucket = src_uri.split("/")[2]
        src_path = "/".join(src_uri.split("/")[3:])
        if get_storage_client().blob_exists(src_bucket, src_path):
            gcs_copy_object(src_uri, dst_uri)
            logger.info("Archived run.log for %s to %s", vm_name, dst_uri)
    except Exception as exc:
        logger.debug("Failed to archive run.log for %s: %s", vm_name, exc)

    # 2. Capture serial console via compute API
    try:
        compute_client = compute_v1.InstancesClient()
        serial_output = compute_client.get_serial_port_output(project=PROJECT_ID, zone=zone, instance=vm_name)
        if serial_output.contents:
            serial_uri = vm_serial_console_archive_uri(vm_name, timestamp, PROJECT_ID)
            serial_bucket = serial_uri.split("/")[2]
            serial_path = "/".join(serial_uri.split("/")[3:])
            serial_bytes = (
                serial_output.contents.encode() if isinstance(serial_output.contents, str) else serial_output.contents
            )
            upload_to_storage(serial_bucket, serial_path, serial_bytes)
            logger.info("Archived serial console for %s to %s", vm_name, serial_uri)
    except Exception as exc:
        logger.debug("Failed to capture serial console for %s: %s", vm_name, exc)


def _kill_vm(compute_client: compute_v1.InstancesClient, vm_name: str, zone: str) -> bool:
    """Issue a delete and treat best-effort.

    The delete is counted as a kill iff the API call itself returned (the
    Compute API has accepted the delete and the VM is going away). The
    follow-up `op.result()` poll is best-effort — if it raises (transient
    connection-pool issue, network blip, eventual-consistency lag), the
    delete is still in-flight, so the kill counts. Previously we returned
    False on poll-raise, which under-counted real kills (observed
    2026-05-05: watchdog reported `killed 0/4` while all 4 VMs deleted).

    Pre-kill hook (2026-05-27): backup VM logs before deletion to preserve
    forensics. Captures run.log + serial console to durable archive.
    """
    # Pre-kill hook: backup logs before deletion (best-effort — don't block kill on backup failure)
    try:
        _backup_vm_logs_before_kill(vm_name, zone)
    except Exception as exc:
        logger.warning("Pre-kill log backup failed for %s (proceeding with delete): %s", vm_name, exc)

    try:
        op = compute_client.delete(project=PROJECT_ID, zone=zone, instance=vm_name)
    except Exception as exc:
        logger.warning("kill API call failed for %s in %s: %s", vm_name, zone, exc)
        return False

    try:
        op.result(timeout=120)
    except Exception as exc:
        logger.warning(
            "kill confirm-poll failed for %s in %s (delete in-flight, counting as kill): %s",
            vm_name,
            zone,
            exc,
        )
    return True


# ── Terminated-VM reaper ──────────────────────────────────────────────────────
# The heartbeat/shard watchdog above only evaluates RUNNING VMs. A VM that
# STOPPED (TERMINATED) — because it self-halted via ``shutdown -h``, was drained,
# was manually stopped, or hung-then-stopped — is never re-examined and orphans
# its boot disk as a recurring storage cost forever. (2026-06-30 cleanup: 127
# orphaned TERMINATED VMs, ~$330/mo in idle disk.) A stopped VM does zero work,
# so reaping it needs NO stall detection — only proof it is an abandoned
# ephemeral job:
#   1. status == TERMINATED                            — already stopped; no live work at risk
#   2. known prefix tagged EPHEMERAL_BATCH/_EXPERIMENT — never a paused live/cron VM
#   3. stopped longer than the grace period            — the operator's restart window
#   4. no ``keep=true`` label and not a daemon         — explicit retention opt-out
# run.log + EXIT_STATUS are uploaded to GCS before shutdown, so the only thing
# lost on delete is the disposable boot disk (``--delete-disks=all`` via _kill_vm).
REAPABLE_LIFECYCLE_CLASSES: frozenset[LifecycleClass] = frozenset(
    {LifecycleClass.EPHEMERAL_BATCH, LifecycleClass.EPHEMERAL_EXPERIMENT}
)


@dataclass
class ReapVerdict:
    """Per-VM result of the TERMINATED-reaper evaluation."""

    vm_name: str
    zone: str
    stopped_age_min: float | None  # None when the verdict is decided before age-gating
    lifecycle_class: LifecycleClass | None  # None = unknown / untagged prefix
    verdict: str
    # one of:
    #   'reap'               — abandoned ephemeral VM, delete it + boot disk
    #   'keep_retained'      — keep=true label or daemon opt-out; retained on purpose
    #   'keep_not_ephemeral' — LONG_LIVED_LIVE / SCHEDULED_RECURRING / unknown prefix
    #   'keep_within_grace'  — stopped, but still inside the restart grace window
    #   'keep_no_timestamp'  — no parseable lastStop/creation timestamp; cannot age-gate

    def should_reap(self) -> bool:
        return self.verdict == "reap"


def _resolve_lifecycle_class(vm_name: str) -> LifecycleClass | None:
    """Longest-prefix LifecycleClass for ``vm_name``, or None if no known prefix.

    Mirrors the longest-match lookup used by VM_PREFIX_TO_BUCKET /
    PREFIX_IDLE_THRESHOLDS so the most specific prefix wins. A ``None`` spec
    (heartbeat-only, no lifecycle tag) is treated as unknown.
    """
    best: LifecycleClass | None = None
    best_len = -1
    for prefix, spec in VM_PREFIX_TO_BUCKET.items():
        if spec is not None and vm_name.startswith(prefix) and len(prefix) > best_len:
            best = spec.lifecycle_class
            best_len = len(prefix)
    return best


def _list_terminated_vms(
    compute_client: compute_v1.InstancesClient,
) -> list[tuple[str, str, str, object]]:
    """List every TERMINATED (stopped) VM. Returns ``[(name, zone, stopped_ts, labels), ...]``.

    ``stopped_ts`` is the RFC3339 lastStopTimestamp, falling back to
    creationTimestamp when the VM was created but never started, or ``""``
    when neither is populated.
    """
    request = compute_v1.AggregatedListInstancesRequest(project=PROJECT_ID)
    out: list[tuple[str, str, str, object]] = []
    for zone, scoped in compute_client.aggregated_list(request=request):
        if not scoped.instances:
            continue
        for inst in scoped.instances:
            if inst.status != "TERMINATED":
                continue
            stopped_ts = inst.last_stop_timestamp or inst.creation_timestamp or ""
            z = zone.replace("zones/", "")
            out.append((inst.name, z, stopped_ts, inst.labels))
    return out


def _evaluate_terminated_vm(
    vm_name: str,
    zone: str,
    stopped_ts: str,
    labels: object,
    reap_after_min: float,
) -> ReapVerdict:
    """Decide whether an abandoned TERMINATED VM should be reaped (see gates above)."""
    lifecycle = _resolve_lifecycle_class(vm_name)
    # (4) Explicit retention: a keep=true label, or a daemon opt-out (incl. the
    #     watchdog itself), is never auto-deleted.
    if (labels and labels.get("keep") == "true") or _is_daemon(labels):
        return ReapVerdict(vm_name, zone, None, lifecycle, "keep_retained")
    # (2) Only ephemeral batch/experiment jobs are reapable. A paused
    #     LONG_LIVED_LIVE / SCHEDULED_RECURRING VM, or an unknown prefix, is
    #     left for the operator (same register-your-prefix convention as the
    #     RUNNING-VM watcher's unknown-prefix logging).
    if lifecycle not in REAPABLE_LIFECYCLE_CLASSES:
        return ReapVerdict(vm_name, zone, None, lifecycle, "keep_not_ephemeral")
    if not stopped_ts:
        return ReapVerdict(vm_name, zone, None, lifecycle, "keep_no_timestamp")
    try:
        stopped = datetime.fromisoformat(stopped_ts).astimezone(UTC)
    except ValueError:
        return ReapVerdict(vm_name, zone, None, lifecycle, "keep_no_timestamp")
    stopped_age_min = (datetime.now(UTC) - stopped).total_seconds() / 60.0
    # (3) Grace period: a recently-stopped VM may be deliberately paused; wait.
    if stopped_age_min < reap_after_min:
        return ReapVerdict(vm_name, zone, stopped_age_min, lifecycle, "keep_within_grace")
    return ReapVerdict(vm_name, zone, stopped_age_min, lifecycle, "reap")


def _reap_terminated_vms(
    compute_client: compute_v1.InstancesClient,
    reap_after_min: float,
    *,
    dry_run: bool,
    workers: int,
) -> int:
    """Delete abandoned ephemeral TERMINATED VMs (and their boot disks).

    Returns the number of VMs reaped (0 in dry-run). Re-uses :func:`_kill_vm`
    so each delete is preceded by the best-effort pre-kill log backup.
    """
    terminated = _list_terminated_vms(compute_client)
    verdicts = [
        _evaluate_terminated_vm(name, zone, stopped_ts, labels, reap_after_min)
        for (name, zone, stopped_ts, labels) in terminated
    ]
    reapable = [v for v in verdicts if v.should_reap()]
    logger.info(
        "Terminated-reaper: %d TERMINATED VMs — %d reapable (EPHEMERAL_*, stopped > %.0fmin), %d kept",
        len(terminated),
        len(reapable),
        reap_after_min,
        len(verdicts) - len(reapable),
    )
    for v in reapable:
        logger.warning(
            "REAP-CANDIDATE %s (%s) stopped=%.0fmin lifecycle=%s",
            v.vm_name,
            v.zone,
            v.stopped_age_min if v.stopped_age_min is not None else -1.0,
            v.lifecycle_class.value if v.lifecycle_class is not None else "UNKNOWN",
        )
    if dry_run:
        logger.info("DRY RUN — no terminated VMs reaped")
        return 0
    reaped = 0
    if reapable:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = {pool.submit(_kill_vm, compute_client, v.vm_name, v.zone): v for v in reapable}
            for fut in as_completed(results):
                v = results[fut]
                if fut.result():
                    reaped += 1
                    logger.warning("REAPED %s (%s) lifecycle=%s", v.vm_name, v.zone, v.lifecycle_class)
    logger.info("terminated-reaper complete: reaped %d/%d", reaped, len(reapable))
    return reaped


def _write_census_snapshot(
    storage_client: StorageClient,
    running: list[WatchdogVerdict],
    zombies: list[WatchdogVerdict],
) -> None:
    """Persist a small census JSON so deployment-api can surface zombie/OOM counts.

    Best-effort — a GCS failure logs a warning but never blocks the watchdog.
    Payload:
      ts      - ISO-8601 UTC write time
      running - sorted VM names currently alive (alive-verdict evaluated VMs)
      zombies - sorted VM names the watchdog classified as zombie this cycle
      oom     - always [] (no OOM signal today - honest absence)
    """
    payload = {
        "ts": datetime.now(UTC).isoformat(),
        "running": sorted(v.vm_name for v in running),
        "zombies": sorted(v.vm_name for v in zombies),
        "oom": [],
    }
    try:
        upload_to_storage(
            HEARTBEAT_BUCKET,
            CENSUS_BLOB_PATH,
            json.dumps(payload).encode(),
        )
        logger.info(
            "census snapshot written to %s/%s: %d running / %d zombie",
            HEARTBEAT_BUCKET,
            CENSUS_BLOB_PATH,
            len(running),
            len(zombies),
        )
    except Exception as exc:
        logger.warning("census snapshot write failed (non-blocking): %s", exc)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Report zombies but do not delete.")
    parser.add_argument("--min-age", type=float, default=15.0, help="Minimum VM age (min) before considered.")
    parser.add_argument("--heartbeat-stale", type=float, default=15.0, help="Heartbeat staleness threshold (min).")
    parser.add_argument("--shard-stale", type=float, default=120.0, help="Manifest shard staleness fallback (min).")
    parser.add_argument(
        "--finished-grace",
        type=float,
        default=10.0,
        help=(
            "Grace period (min) after EXIT_STATUS file appears before the VM is killed. "
            "Catches workloads that finished (cleanly or with rc!=0) but failed to "
            "self-delete because the launcher omitted VM_SHUTDOWN_ON_COMPLETION=true "
            "or the self-delete subshell failed. Default 10 min — shorter than other "
            "thresholds because a finished workload should not linger."
        ),
    )
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument(
        "--reap-terminated-after",
        type=float,
        default=1440.0,
        help=(
            "Grace period (min) after a VM STOPPED before an abandoned ephemeral "
            "(EPHEMERAL_BATCH / EPHEMERAL_EXPERIMENT) TERMINATED VM is deleted along "
            "with its boot disk. Default 1440 (24h) — gives an operator time to restart "
            "a deliberately-stopped VM. LONG_LIVED_LIVE / SCHEDULED_RECURRING / "
            "unknown-prefix VMs and any VM labelled keep=true are never reaped."
        ),
    )
    parser.add_argument(
        "--no-reap-terminated",
        action="store_true",
        help="Disable the TERMINATED-VM reaper (run only the RUNNING-VM zombie watchdog).",
    )
    parser.add_argument(
        "--notify-url",
        default="",
        help="Webhook URL for zombie notifications (Slack-compatible). Empty = skip.",
    )
    args = parser.parse_args(argv)

    compute_client = compute_v1.InstancesClient()
    storage_client = get_storage_client()
    _bump_pool_size(compute_client.transport._session)
    _bump_pool_size(storage_client._client._http)

    t0 = time.monotonic()
    watchable = _list_watchable_vms(compute_client)
    known = [(n, z) for (n, z, has_prefix) in watchable if has_prefix]
    unknown = [(n, z) for (n, z, has_prefix) in watchable if not has_prefix]
    logger.info(
        "found %d watchable VMs in %.1fs (%d known-prefix + shard signal, %d unknown-prefix → heartbeat-only)",
        len(watchable),
        time.monotonic() - t0,
        len(known),
        len(unknown),
    )
    if unknown:
        # Catch-all has them covered, but log so an operator can decide whether
        # the new prefix deserves a richer shard-write signal in
        # ``VM_PREFIX_TO_BUCKET``. Truncate the printed list to keep the log
        # quiet when many launchers grow without dict updates.
        sample = ", ".join(name for name, _ in unknown[:8])
        more = "" if len(unknown) <= 8 else f" (+ {len(unknown) - 8} more)"
        logger.info("unknown-prefix VMs (heartbeat-only watch): %s%s", sample, more)

    verdicts: list[WatchdogVerdict] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(
                _evaluate_vm,
                compute_client,
                storage_client,
                n,
                z,
                args.min_age,
                args.heartbeat_stale,
                args.shard_stale,
                args.finished_grace,
            ): (n, z)
            for (n, z, _has_prefix) in watchable
        }
        for fut in as_completed(futures):
            verdicts.append(fut.result())

    zombies = [v for v in verdicts if v.is_zombie()]
    alive = [v for v in verdicts if v.verdict == "alive"]
    young = [v for v in verdicts if v.verdict == "too_young"]
    _write_census_snapshot(storage_client, alive, zombies)

    logger.info("=" * 60)
    logger.info(
        "Watchdog summary: %d alive / %d zombie / %d too_young",
        len(alive),
        len(zombies),
        len(young),
    )
    for v in zombies:
        logger.warning(
            "ZOMBIE %s (%s) age=%.0fmin hb=%s shard=%s reason=%s",
            v.vm_name,
            v.zone,
            v.age_minutes,
            f"{v.heartbeat_age_min:.0f}min" if v.heartbeat_age_min is not None else "MISSING",
            f"{v.shard_age_min:.0f}min" if v.shard_age_min is not None else "MISSING",
            v.verdict,
        )

    notify_url: str = args.notify_url
    if zombies and notify_url:
        _send_zombie_notification(notify_url, zombies)

    if args.dry_run:
        logger.info("DRY RUN — no VMs killed")
    else:
        killed = 0
        for v in zombies:
            if _kill_vm(compute_client, v.vm_name, v.zone):
                killed += 1
                logger.warning("KILLED %s (%s) reason=%s", v.vm_name, v.zone, v.verdict)
        logger.info("watchdog complete: killed %d/%d zombies", killed, len(zombies))

    # Second pass: reap abandoned ephemeral TERMINATED VMs (the RUNNING-VM passes
    # above never see them). Honours --dry-run for reporting-only.
    if not args.no_reap_terminated:
        _reap_terminated_vms(
            compute_client,
            args.reap_terminated_after,
            dry_run=args.dry_run,
            workers=args.workers,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
