"""DP-VM-008: post-mortem heartbeat-sidecar reliability check.

Closes the P2 follow-up in
``zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md`` § "Incident 2":
the ``vm_zombie_watchdog.py`` sweep kills a VM once its
``vm-heartbeat/{vm}.txt`` blob (written every 60s by the
``vm_heartbeat_sidecar.sh`` nohup wired in ``setup-data-pipeline-vm.sh`` lines
816-833) reads stale past the VM-name-prefix threshold
(``PREFIX_IDLE_THRESHOLDS``). The 2026-07-18 recurrence found the heartbeat blob
for one of the killed VMs (``af-backfill-20260717-151237``) showing a write only
16s before the delete call — a gap far SHORTER than the watchdog's own 10min
threshold for that prefix. That mismatch has two possible explanations this
module exists to distinguish:

  (a) A last-second RACE — the watchdog's sweep read a genuinely-stale blob, but
      between that read and the actual ``compute.instances.delete`` call (the
      sweep evaluates every VM in a thread pool, then kills sequentially) the
      sidecar caught up and wrote a fresh heartbeat. The watchdog's OWN read at
      eval time was stale; the blob's CURRENT (post-mortem) state is fresh. This
      is a scheduling artifact, not a sidecar defect.
  (b) A sidecar GAP — the sidecar itself intermittently fails to start or write
      (a transient ``gsutil`` hiccup, a nohup that died and silently respawned,
      a boot-time race before the sidecar script downloads). The fresh write we
      see post-mortem is real, but it does not prove the sidecar was reliably
      ticking every 60s in the minutes before — only that this ONE write landed
      close to the kill.

The blob (``vm-heartbeat/{vm}.txt``) has no delete lifecycle, so it is still
readable long after the VM is gone — this check reads it AS-IS and compares its
last-write instant against the VM's kill time (supplied by the caller, sourced
from the GCP audit log / the watchdog's own kill log line / the log-archive
snapshot timestamp — none of which this module reads directly, keeping it
credential-free + block-network in tests).

Pure classification core (:func:`classify_heartbeat_reliability`) takes an
already-computed age so it is unit-tested with synthetic ages, mirroring
``heartbeat_stall_watcher.classify_vm_liveness``. The GCS read
(:func:`heartbeat_age_at_kill`) is a thin, separately-testable wrapper.

This module never emits a ``DP_*`` finding / never auto-recovers — it is a
diagnostic report for a human (or the follow-up P1 threshold-widening todo) to
read, not a live monitor. Mirrors ``check_vm.py``'s "always dry" posture.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from unified_trading_library import StorageClient

from deployment_service.data_pipeline_monitors import _gcs

logger = logging.getLogger(__name__)


class HeartbeatReliabilityVerdict(StrEnum):
    """Verdict for one post-mortem (killed VM, kill_time) pair."""

    GENUINELY_STALE = "genuinely_stale"  # blob age at kill time >= threshold — the kill was justified
    RELIABILITY_GAP_SUSPECTED = (
        "reliability_gap_suspected"  # blob younger than threshold at kill time — race or sidecar gap
    )
    UNKNOWN_NO_BLOB = "unknown_no_blob"  # no heartbeat blob at all — can't assess (VM never ran the sidecar)


@dataclass(frozen=True)
class HeartbeatReliabilityResult:
    vm_name: str
    verdict: HeartbeatReliabilityVerdict
    heartbeat_age_at_kill_min: float | None
    stall_threshold_min: float
    kill_time: str


def classify_heartbeat_reliability(
    vm_name: str,
    *,
    heartbeat_age_at_kill_min: float | None,
    stall_threshold_min: float,
    kill_time: str,
) -> HeartbeatReliabilityResult:
    """Pure classification. No I/O.

    ``heartbeat_age_at_kill_min`` is the heartbeat blob's last-write age relative
    to the VM's kill time (NOT relative to now) — see :func:`heartbeat_age_at_kill`.

    Precedence:
      - ``None`` (no blob / unreadable)                 → UNKNOWN_NO_BLOB
      - age >= ``stall_threshold_min``                   → GENUINELY_STALE (the
        watchdog's own prefix threshold — e.g. 10.0min for ``af-backfill-`` — is
        corroborated by the blob's own last-write instant: the sidecar really
        had gone silent that long)
      - age < ``stall_threshold_min``                    → RELIABILITY_GAP_SUSPECTED
        (the blob was written CLOSER to the kill than the very threshold that
        would justify a kill — consistent with either a last-second race between
        the watchdog's sweep-read and its delete call, or the sidecar
        intermittently failing to start/write and only catching up right before
        the kill; worth a closer look, not a proven watchdog bug)
    """
    if heartbeat_age_at_kill_min is None:
        verdict = HeartbeatReliabilityVerdict.UNKNOWN_NO_BLOB
    elif heartbeat_age_at_kill_min >= stall_threshold_min:
        verdict = HeartbeatReliabilityVerdict.GENUINELY_STALE
    else:
        verdict = HeartbeatReliabilityVerdict.RELIABILITY_GAP_SUSPECTED
    return HeartbeatReliabilityResult(
        vm_name=vm_name,
        verdict=verdict,
        heartbeat_age_at_kill_min=heartbeat_age_at_kill_min,
        stall_threshold_min=stall_threshold_min,
        kill_time=kill_time,
    )


def heartbeat_age_at_kill(
    storage_client: StorageClient,
    log_bucket: str,
    vm_name: str,
    kill_time_iso: str,
) -> float | None:
    """Age (min) of ``vm_name``'s heartbeat blob's last write, relative to ``kill_time_iso``.

    ``kill_time_iso`` is an ISO-8601 timestamp (naive timestamps are treated as
    UTC). Returns ``None`` when the blob is missing/unreadable or
    ``kill_time_iso`` doesn't parse — the caller (:func:`classify_heartbeat_reliability`)
    treats ``None`` as UNKNOWN_NO_BLOB, never as a fabricated age.
    """
    try:
        kill_dt = datetime.fromisoformat(kill_time_iso)
    except ValueError:
        logger.warning("heartbeat_age_at_kill: unparseable kill_time %r for %s", kill_time_iso, vm_name)
        return None
    if kill_dt.tzinfo is None:
        kill_dt = kill_dt.replace(tzinfo=UTC)
    write_epoch = _gcs.heartbeat_blob_write_epoch(storage_client, log_bucket, vm_name)
    if write_epoch is None:
        return None
    return (kill_dt.astimezone(UTC).timestamp() - write_epoch) / 60.0


def audit_killed_vm(
    storage_client: StorageClient,
    log_bucket: str,
    vm_name: str,
    kill_time_iso: str,
    *,
    stall_threshold_min: float,
) -> HeartbeatReliabilityResult:
    """Post-mortem reliability verdict for ONE killed VM."""
    age = heartbeat_age_at_kill(storage_client, log_bucket, vm_name, kill_time_iso)
    return classify_heartbeat_reliability(
        vm_name,
        heartbeat_age_at_kill_min=age,
        stall_threshold_min=stall_threshold_min,
        kill_time=kill_time_iso,
    )


def audit_killed_vms(
    storage_client: StorageClient,
    log_bucket: str,
    killed_vms: list[tuple[str, str]],
    *,
    stall_threshold_min_for_vm: Callable[[str], float],
) -> list[HeartbeatReliabilityResult]:
    """Post-mortem reliability sweep over a batch of ``(vm_name, kill_time_iso)`` pairs.

    ``stall_threshold_min_for_vm`` lets the caller supply the SAME per-prefix
    threshold the watchdog itself used (``vm_zombie_watchdog.PREFIX_IDLE_THRESHOLDS``
    / ``_resolve_idle_thresholds``) so GENUINELY_STALE is judged against the exact
    bar the watchdog applied, not a generic default.
    """
    return [
        audit_killed_vm(
            storage_client,
            log_bucket,
            vm_name,
            kill_time_iso,
            stall_threshold_min=stall_threshold_min_for_vm(vm_name),
        )
        for vm_name, kill_time_iso in killed_vms
    ]
