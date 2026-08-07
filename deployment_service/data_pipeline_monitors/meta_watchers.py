"""Meta-watchers — "is the checker itself alive" (DP-CATALOG-001 / DP-WATCHER-001/002).

The blind spot the other Wave-4 watchers don't cover: a watcher / cron / catalogue
generator that ITSELF stops running emits nothing, so its absence is invisible.
These are lean freshness probes — one per failure mode — that read the durable
GCS artifact each producer leaves behind and alert when it goes stale:

  - DP-CATALOG-001 (``DP_CATALOG_NOT_RUNNING``, CRITICAL): per AG, the instrument
    catalogue artifact has not been refreshed within ``max_age`` (default 24h) →
    the enumerator/catalogue regen stopped.
  - DP-WATCHER-001 (``DP_ZOMBIE_WATCHDOG_DOWN``, CRITICAL): the zombie-VM
    watchdog's own census blob (``vm-census/watchdog-census.json``) is stale →
    the watchdog daemon is down (the meta-watcher of the watcher).
  - DP-WATCHER-002 (``DP_CRON_DID_NOT_FIRE``, CRITICAL): a scheduled audit /
    consolidator / digest cron's heartbeat/output artifact missed its window.
  - DP-WATCHER-004 (``DP_CONSOLIDATOR_SCHEDULER_PAUSED``, CRITICAL): a non-
    ``-legacy-`` manifest-consolidator Cloud Scheduler job is PAUSED — the inverse
    of DP-WATCHER-002's pause-awareness (that one suppresses on PAUSED; this one
    exists specifically for an ACCIDENTAL pause on a job nobody meant to stop).
    Lives in ``consolidator_scheduler_watcher.py`` (this file is at its 900-line
    cap) — see ``check_consolidator_scheduler_paused`` there. (Was misassigned
    DP-WATCHER-003 pre-2026-07-31, colliding with DP_FLEET_MONITOR_RUN_FAILED's
    id — renumbered.)
  - DP-FETCH-009 (``DP_RUN_MOSTLY_EMPTY``, CRITICAL — event REUSED): per
    (asset_group, data_type), a HIGH ``attempted_failed`` count/ratio in the
    consolidated manifest ``_index``. Closes the gap where a backfill exits 0 /
    captured climbs yet fails thousands of cells invisibly (the exit-code monitor
    reads that as CLEAN). See ``check_high_attempted_failed``.

Each freshness probe is the same shape: read the artifact's age, compare to a
threshold, emit on stale/missing. The GCS read is injected for credential-free
testing. DP-FETCH-009 instead reads the index CONTENTS (counts), not freshness.
"""

from __future__ import annotations

import io
import json
import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import cast

import pandas as pd
from unified_trading_library import StorageClient
from unified_trading_library.events import (  # noqa: qg-deep-import
    DP_CATALOG_NOT_RUNNING,
    DP_CRON_DID_NOT_FIRE,
    DP_RUN_MOSTLY_EMPTY,
    DP_ZOMBIE_WATCHDOG_DOWN,
    log_event,
)

from deployment_service.data_pipeline_monitors import _gcs
from deployment_service.data_pipeline_monitors._miss_tracker import (
    DEFAULT_MIN_CONSECUTIVE_MISSES,
    MissTracker,
)
from deployment_service.data_pipeline_monitors.attempted_failed_staleness import (
    ATTEMPTED_FAILED_TRAILING_WINDOW_DAYS,
    recent_activity_mask,
    stale_backlog_annotation,
    stale_days_since,
    trailing_window_mask,
)
from deployment_service.data_pipeline_monitors.escalation import (
    EscalationTier,
    PipelineFinding,
    route_finding,
)
from deployment_service.data_pipeline_monitors.known_dead_cells_registry import is_known_dead
from deployment_service.data_pipeline_monitors.renag_tracker import (
    DEFAULT_RENAG_COOLDOWN_SECONDS,
    RenagTracker,
    apply_cooldown,
)

logger = logging.getLogger(__name__)

# The zombie watchdog persists its census here (SSOT: vm_zombie_watchdog.py).
WATCHDOG_CENSUS_BLOB = "vm-census/watchdog-census.json"
DEFAULT_CATALOGUE_MAX_AGE_MIN = 24 * 60.0
DEFAULT_WATCHDOG_MAX_AGE_MIN = 30.0  # the watchdog ticks every 5 min

# Active-alert tracking for the RESOLVED bookend (alert-lifecycle hardening,
# operator 2026-06-23). The meta sweep records every EMITTED finding's key in
# ``_EMITTED_THIS_SWEEP`` (via the ``_emit`` choke-point — so SUPPRESSED alerts,
# which never reach ``_emit``, are correctly NOT tracked). ``reconcile_resolved``
# then emits a ``✅ RESOLVED`` INFO bookend for any key that fired on a PRIOR sweep
# but did NOT re-fire this sweep (the condition cleared). Without it a transient
# stale-then-fresh artifact leaves a permanent RED in #data-pipeline-alerts (the
# channel never reflects closure). Mirrors
# ``scripts/repo-management/ci_failure_watcher.detect_resolved_prs``.
ACTIVE_DP_ALERTS_BLOB = "vm-census/active-dp-alerts.json"
_EMITTED_THIS_SWEEP: dict[str, str] = {}  # alert_key -> event_name (this sweep's fired set)


def reset_emitted_tracker() -> None:
    """Clear the per-sweep emitted-alert accumulator. Call at the START of a meta
    sweep (and in tests) so the active set reflects only THIS sweep's emissions."""
    _EMITTED_THIS_SWEEP.clear()


def _alert_key(finding: PipelineFinding) -> str:
    """Stable identity for an alert across sweeps: event + the most specific label
    in its details, so a re-fire of the SAME condition maps to the SAME key and a
    clear is detected by that key's ABSENCE this sweep."""
    d = finding.details
    label = d.get("asset_group") or d.get("label") or d.get("vm_name") or d.get("blob_path") or "default"
    return f"{finding.event}::{label}"


def alert_key(finding: PipelineFinding) -> str:
    """Public alias of :func:`_alert_key` — the cross-sweep alert identity. Lets the
    heartbeat / exit-code sweeps record their emitted findings for the same RESOLVED
    bookend (alert-lifecycle hardening, extended to all 3 sweeps 2026-06-24)."""
    return _alert_key(finding)


def reconcile_resolved(
    *,
    storage_client: StorageClient,
    log_bucket: str,
    active_blob: str = ACTIVE_DP_ALERTS_BLOB,
    emitted: dict[str, str] | None = None,
    dry_run: bool = False,
    renag_tracker: RenagTracker | None = None,
) -> list[str]:
    """Emit a ``✅ RESOLVED`` INFO bookend for each alert that fired on a PRIOR sweep
    but did NOT re-fire this sweep (the condition cleared), so #data-pipeline-alerts
    reflects closure instead of a permanent RED on a transient.

    Generic across the 3 fleet sweeps (2026-06-24): ``active_blob`` is the per-mode
    active-alert state blob (meta / heartbeat / exit-code each own one so they never
    clobber each other's set — the sweeps cover DISJOINT event types), and ``emitted``
    is THIS sweep's fired set (``alert_key -> event``). When ``emitted`` is ``None`` it
    falls back to the meta sweep's ``_EMITTED_THIS_SWEEP`` accumulator (populated via
    :func:`_emit`). Returns the resolved keys. Fail toward NO false-resolve: a
    read/parse miss treats the prior set as empty, and the bookend is INFO (never pages).

    ``renag_tracker``: every resolved key also has its re-nag state cleared (no-op if
    never held) so a LATER re-onset pages immediately.
    """
    prior: dict[str, str] = {}
    raw = _gcs.read_text(storage_client, log_bucket, active_blob)
    if raw:
        try:
            loaded = cast("object", json.loads(raw))
            if isinstance(loaded, dict):
                parsed = cast("dict[str, object]", loaded)
                prior = {str(k): str(v) for k, v in parsed.items()}
        except (ValueError, TypeError):
            prior = {}
    current = dict(_EMITTED_THIS_SWEEP if emitted is None else emitted)
    resolved = [k for k in prior if k not in current]
    for key in resolved:
        event = prior[key]
        label = key.split("::", 1)[1] if "::" in key else key
        logger.info("meta_watchers: RESOLVED %s (%s)", key, event)
        if renag_tracker is not None:
            renag_tracker.clear(key)
        if not dry_run:
            log_event(
                event,
                severity="INFO",
                details={
                    "resolved": True,
                    "label": label,
                    "registry_id": "DP-RESOLVED",
                    "message": f":white_check_mark: RESOLVED — {label} recovered ({event} cleared)",
                    "cloud": "GCP",
                },
            )
    if not dry_run:
        try:
            storage_client.upload_bytes(
                log_bucket,
                active_blob,
                json.dumps(current, sort_keys=True).encode("utf-8"),
                content_type="application/json",
            )
        except Exception as exc:
            logger.warning("reconcile_resolved: persist active set failed: %s", exc)
    return resolved


# CONSECUTIVE-MISS GATE (2026-06-24, Fix 2): a CRITICAL page on a SINGLE stale sweep
# is flappy — one transient GCS-read blip during a heavy backfill paged "artifact
# ABSENT" / "cron did not fire" and self-resolved on the next sweep (the DP-CATALOG-001
# / DP-WATCHER-002 false-positive class; incident 2026-06-24, 17:50-18:30 window under
# the 728k-object delete + 5 backfill VMs saturating the GCS API). So a probe must be
# stale for >= ``min_consecutive`` CONSECUTIVE sweeps before its alert fires. The count
# is GCS-persisted (each Cloud Run sweep is a fresh process — no in-memory carry-over),
# keyed IDENTICALLY to ``_alert_key`` so the counter, the emitted-set and the RESOLVED
# bookend all agree on one alert identity. A fresh (or suppressed-by-design) probe resets
# its key to 0, so only a SUSTAINED stale condition pages — a self-resolving blip never does.


def _catalogue_miss_key(target: FreshnessTarget) -> str:
    """Consecutive-miss key for a catalogue probe — matches ``_alert_key`` (which
    selects ``asset_group`` = ``target.label`` from the DP_CATALOG finding details)."""
    return f"{DP_CATALOG_NOT_RUNNING}::{target.label}"


def _cron_miss_key(target: FreshnessTarget) -> str:
    """Consecutive-miss key for a cron probe — matches ``_alert_key`` (the DP_CRON
    finding has no asset_group/label/vm_name, so it falls to ``blob_path``)."""
    return f"{DP_CRON_DID_NOT_FIRE}::{target.blob_path}"


# ── DP-FETCH-009: high attempted_failed manifest cells (the monitoring gap) ──
#
# The blind spot: a backfill VM can exit 0 with ``captured`` climbing, so the
# exit-code fleet monitor classifies it CLEAN — even when the same run wrote
# THOUSANDS of ``attempted_failed`` manifest cells, and the per-shard
# ``ADAPTER_FETCH_FAILED`` events never aggregate into a DP alert. So a backfill
# that fails thousands of cells is invisible (incident: sports golden-window had
# 5,265 ``trades`` failures with no alert). This meta-watcher reads the
# consolidated per-AG manifest ``_index`` and pages when a ``(asset_group,
# data_type)`` cell has a HIGH ``attempted_failed`` count AND/OR ratio.
#
# Thresholds are NAMED CONSTANTS — page when EITHER an absolute floor (a large
# batch of failures, even in a big corpus where the ratio stays small) OR a
# ratio floor (a small corpus where most cells failed) is crossed, gated on
# ``MIN_ATTEMPTED_FAILED_FOR_RATIO`` so a 1-of-2-failed micro-cell never pages on
# ratio alone. Gated behind the same MissTracker as the catalogue/cron probes
# (>= ``min_consecutive`` sweeps) so a transient consolidator blip never pages.
ATTEMPTED_FAILED_ABS_THRESHOLD = 500  # absolute attempted_failed count per (ag, data_type) → page
ATTEMPTED_FAILED_RATIO_THRESHOLD = 0.10  # attempted_failed / (captured + attempted_failed) → page
MIN_ATTEMPTED_FAILED_FOR_RATIO = 50  # ratio path ignored below this count (avoid micro-cell noise)
# STATIC_BACKLOG_STALE_DAYS_THRESHOLD: labeling-only constant, see attempted_failed_staleness.py.
# Consolidated availability-index blob (SSOT: manifest_writer ManifestWriter._INDEX_PATH).
AVAILABILITY_INDEX_BLOB = "_index/availability_index.parquet"


def _high_attempted_failed_cell_label(asset_group: str, data_type: str) -> str:
    """The per-cell identity label ``<asset_group>/<data_type>``. The finding stamps
    this as ``details["asset_group"]`` so ``_alert_key`` (which reads ``asset_group``
    first) yields ONE alert/RESOLVED-bookend identity PER (ag, data_type) cell — not a
    per-AG collapse — keeping the miss-counter, the emitted-set and the RESOLVED bookend
    all in agreement on a single identity (the cross-sweep alert-identity invariant)."""
    return f"{asset_group}/{data_type}"


def _high_attempted_failed_miss_key(asset_group: str, data_type: str) -> str:
    """Consecutive-miss key for a high-attempted_failed cell — matches ``_alert_key``
    EXACTLY (``event::<asset_group>/<data_type>``), so the counter, the emitted-set and
    the RESOLVED bookend all agree on one identity per (ag, data_type) cell."""
    return f"{DP_RUN_MOSTLY_EMPTY}::{_high_attempted_failed_cell_label(asset_group, data_type)}"


# The fleet-monitor / meta-watcher sweeps + their cadence (minutes). Each writes a
# ``vm-census/<mode>-last-run.json`` sentinel at end-of-sweep; the budget is 2x the
# cadence (a single missed tick is benign jitter; two missed = the cron stopped).
# The meta sweep CANNOT detect its OWN death in-band (it would have to be running to
# probe its own sentinel) — that is Layer-2's (the out-of-band deadman) job; it is
# still listed so the deadman reads it from the same builder.
MONITOR_CRON_CADENCE_MIN: dict[str, float] = {
    "exit-code": 5.0,  # */5 (data_pipeline_fleet_monitor_scheduler.tf)
    "heartbeat": 5.0,  # */5
    "meta": 15.0,  # */15
}

# monitor mode → Cloud Run **Job** name STEM (env-prefix-agnostic; the real job
# carries a ``${env_prefix}-`` prefix). The execution-history cross-check (KEY #4)
# queries this job's last SUCCEEDED execution to suppress a false stale-sentinel
# DP_CRON_DID_NOT_FIRE. SSOT for the stems: cloud_run_job_registry.CLOUD_RUN_JOBS.
MONITOR_CRON_CLOUD_RUN_JOB: dict[str, str] = {
    "exit-code": "dp-exit-code-monitor",
    "heartbeat": "dp-heartbeat-watcher",
    "meta": "dp-meta-watchers",
}


@dataclass(frozen=True)
class FreshnessTarget:
    """A single freshness probe target: a durable artifact + its staleness budget."""

    bucket: str
    blob_path: str
    max_age_min: float
    label: str  # human label (asset_group / cron name) for the alert
    # Cloud Scheduler job name backing this cron (e.g.
    # ``manifest-consolidator-market-data-sports``). When set + the injected
    # ``scheduler_state_reader`` reports it ``PAUSED``, ``check_cron_fired`` SKIPS
    # the stale-artifact alert (paused-by-design during the manual-backfill
    # campaign — KEY #2). "" → no pause check (always alert on stale, as before).
    scheduler_job: str = ""
    # Cloud Run **Job** name backing this cron (e.g. ``dp-exit-code-monitor``).
    # When set + the injected ``execution_history_reader`` reports the job's last
    # SUCCESSFUL execution within the budget, ``check_cron_fired`` SUPPRESSES the
    # stale-SENTINEL alert — the job DID fire (its run.log sentinel write is a
    # lagging/secondary signal that can silently miss while the execution itself
    # Completes), so a stale sentinel + a recent successful execution is a
    # FALSE-POSITIVE, not a dead cron (KEY #4, operator 2026-06-23:
    # dp-exit-code-monitor false-fired DP_CRON_DID_NOT_FIRE while its executions
    # Completed every 5 min). "" → no execution-history cross-check (sentinel is the
    # sole signal, the prior behaviour).
    cloud_run_job: str = ""


# A scheduler-state reader: ``job_name -> "ENABLED" | "PAUSED" | "DISABLED" | None``.
# ``None`` ⇒ the job state is UNKNOWN (job not found / API error) → the caller does
# NOT suppress (fail toward alerting on an unknown state — a missing job is itself
# worth an alert, never a silent skip). Injected so the watcher stays pure +
# credential-free; the cli wires the real Cloud Scheduler query.
SchedulerStateReader = Callable[[str], str | None]

# A Cloud Run **Job** last-success-age reader: ``job_name -> minutes since the
# job's most recent SUCCEEDED execution`` (``None`` ⇒ no successful execution found
# / job not found / API error → the caller does NOT suppress, fail toward
# alerting). Lets ``check_cron_fired`` cross-check the REAL execution history
# (``gcloud run jobs executions list``) against a stale SENTINEL: a job firing on
# schedule but whose run.log sentinel write lagged/missed is a false positive, so a
# recent successful execution suppresses the stale-sentinel alert (KEY #4). Injected
# so the watcher stays pure + credential-free; the cli wires the real query.
ExecutionHistoryReader = Callable[[str], float | None]


@dataclass(frozen=True)
class FreshnessResult:
    target: FreshnessTarget
    age_min: float | None  # None = artifact missing entirely
    stale: bool


# RETRY-BEFORE-FIRE (2026-06-24): blob_age_minutes returns None on BOTH a genuine
# absence AND a transient GCS read error (it swallows exceptions by design). Firing
# a CRITICAL page on a single None makes the probe FLAPPY — a momentary read blip
# pages "artifact ABSENT" and self-resolves on the next sweep (the DP-CATALOG-001 /
# DP-WATCHER-002 transient-read false-positive class; incident 2026-06-24: the defi
# catalogue paged ABSENT at 6:17 then RESOLVED at 6:30 with a stable 10:17Z mtime —
# the artifact was present the whole time). So on None we re-read a few times before
# concluding genuine absence; a present artifact returns its age on retry and never fires.
# EXPONENTIAL BACKOFF (2026-06-24, Fix 1): retry a few times before concluding ABSENT,
# with growing intervals so a sub-second GCS blip is caught on the FIRST quick retry
# while a slightly slower one still gets a longer wait. Kept DELIBERATELY SHORT (~1.75s
# total) — the in-sweep retry only rides out a momentary read blip; a SUSTAINED
# heavy-load throttle window is covered by the cross-sweep consecutive-miss gate
# (MissTracker below), NOT by sleeping longer in one sweep (a long per-probe sleep x many
# probes blows past the 60s per-test budget — incident 2026-06-24: a 15s backoff timed
# out the all-missing-storage meta-sweep test). Short retry + cross-sweep gate is the
# right split: fast probe, sustained-throttle tolerance handled across sweeps. (~0.6s
# total keeps the all-missing meta-sweep test well under the 60s per-test budget even
# with ~19 probes, while still re-reading 3x with growing intervals for a momentary blip.)
_PROBE_RETRY_BACKOFF_S: tuple[float, ...] = (0.1, 0.2, 0.3)


def probe_freshness(
    storage_client: StorageClient,
    target: FreshnessTarget,
) -> FreshnessResult:
    """Probe one artifact's freshness. A missing artifact counts as stale.

    Retries on a ``None`` read (transient GCS error vs genuine absence are
    indistinguishable at the ``blob_age_minutes`` layer) with exponential backoff
    before concluding stale — see the module comment above ``_PROBE_RETRY_BACKOFF_S``.
    """
    age = _gcs.blob_age_minutes(storage_client, target.bucket, target.blob_path)
    if age is None:
        for sleep_s in _PROBE_RETRY_BACKOFF_S:
            time.sleep(sleep_s)
            age = _gcs.blob_age_minutes(storage_client, target.bucket, target.blob_path)
            if age is not None:
                break
    stale = age is None or age > target.max_age_min
    return FreshnessResult(target=target, age_min=age, stale=stale)


def _emit(
    finding: PipelineFinding,
    *,
    pm_repo_path: str | None,
    dry_run: bool,
) -> None:
    # Record into the per-sweep emitted set FIRST (the active-alert tracker for the
    # RESOLVED bookend). Suppressed alerts never reach _emit, so this captures
    # exactly the fired set. Tracked even on dry_run (reconcile_resolved is dry too).
    _EMITTED_THIS_SWEEP[_alert_key(finding)] = finding.event
    if not dry_run:
        route_finding(finding, pm_repo_path=pm_repo_path)
    logger.warning("meta_watchers: %s %s", finding.event, finding.summary)


def emit_finding(
    finding: PipelineFinding,
    *,
    pm_repo_path: str | None,
    dry_run: bool,
) -> None:
    """Public alias of :func:`_emit` — for sibling modules (e.g. stale_image_watcher)
    that share the same alert-lifecycle bookend infrastructure."""
    _emit(finding, pm_repo_path=pm_repo_path, dry_run=dry_run)


def check_catalogue_freshness(
    *,
    storage_client: StorageClient,
    targets: Iterable[FreshnessTarget],
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> list[FreshnessResult]:
    """DP-CATALOG-001 — per-AG instrument-catalogue freshness.

    When ``miss_tracker`` is provided, the CRITICAL alert fires only after the probe
    has been stale for ``min_consecutive`` consecutive sweeps (transient-blip
    suppression — Fix 2). ``None`` ⇒ fire on the first stale probe (the prior
    behaviour; keeps existing call sites / tests unchanged)."""
    results: list[FreshnessResult] = []
    for target in targets:
        result = probe_freshness(storage_client, target)
        results.append(result)
        if miss_tracker is not None:
            misses = miss_tracker.register(_catalogue_miss_key(target), stale=result.stale)
            if result.stale and misses < min_consecutive:
                logger.info(
                    "meta_watchers: DP_CATALOG_NOT_RUNNING for '%s' below consecutive-miss "
                    "threshold (%d/%d) — not paging yet (transient-blip suppression)",
                    target.label,
                    misses,
                    min_consecutive,
                )
                continue
        if result.stale:
            # KEY #3 (operator 2026-06-23): the alert MUST SHOW WHAT IT PROBED —
            # the exact gs://bucket/path, the age it read (or "artifact ABSENT"),
            # and the freshness budget — so "(missing)" becomes a diagnosable
            # "probed gs://<bucket>/<path>, artifact ABSENT, budget=<N>h".
            budget_h = target.max_age_min / 60.0
            probed = f"gs://{target.bucket}/{target.blob_path}"  # noqa: gs-uri — human alert label (the probed path the operator clicks), not a storage lookup
            if result.age_min is None:
                state = "artifact ABSENT"
            else:
                state = f"age {result.age_min:.0f}m (={result.age_min / 60.0:.1f}h) > budget"
            summary = (
                f"instrument catalogue for {target.label} is stale — probed {probed}, {state}, budget={budget_h:.0f}h"
            )
            _emit(
                PipelineFinding(
                    event=DP_CATALOG_NOT_RUNNING,
                    severity="CRITICAL",
                    tier=EscalationTier.PAGE_OPERATOR,
                    summary=summary,
                    details={
                        "asset_group": target.label,
                        "bucket": target.bucket,
                        "blob_path": target.blob_path,
                        "probed_path": probed,
                        "artifact_present": result.age_min is not None,
                        "age_min": result.age_min,
                        "max_age_min": target.max_age_min,
                        "budget_hours": budget_h,
                    },
                    registry_id="DP-CATALOG-001",
                ),
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
    return results


@dataclass(frozen=True)
class AttemptedFailedCell:
    """One ``(asset_group, data_type)`` cell's capture-status counts read from the
    consolidated manifest ``_index``, plus the high-failure verdict for it."""

    asset_group: str
    data_type: str
    captured: int
    attempted_failed: int
    ratio: float  # attempted_failed / (captured + attempted_failed), 0.0 when denom is 0
    high: bool  # crossed the abs OR ratio threshold (a real failure batch)
    known_dead: bool = False  # registered dead cell, no activity since narrowing (known_dead_cells_registry.py)
    max_attempted_at: str = ""  # newest attempted_failed row's attempted_at (ISO-8601); "" = none/unknown
    stale_days: int | None = None  # days since max_attempted_at; None when unparseable/empty (never asserted)
    recent_attempted_failed: int = 0  # attempted_failed rows within STATIC_BACKLOG_STALE_DAYS_THRESHOLD of now
    trailing_attempted_failed: int = 0  # attempted_failed rows within ATTEMPTED_FAILED_TRAILING_WINDOW_DAYS of now


def _read_attempted_failed_cells(
    storage_client: StorageClient,
    *,
    asset_group: str,
    bucket: str,
    blob: str = AVAILABILITY_INDEX_BLOB,
) -> list[AttemptedFailedCell]:
    """Read the consolidated ``_index`` parquet for one AG bucket and return per
    ``data_type`` captured / attempted_failed counts + the high-failure verdict.

    The read is injected (``storage_client.download_bytes``) so the checker stays
    pure + credential-free; the cli wires the real GCS-backed client. A
    missing/unreadable/empty/schema-less index reads as zero cells (never raises) —
    an absent index is the catalogue/cron probes' job, not this one's.
    """
    try:
        if not storage_client.blob_exists(bucket, blob):
            return []
        raw = storage_client.download_bytes(bucket, blob)
    except Exception:
        return []
    try:
        # Read ONLY the columns this checker uses (avoids the full-index-materialise OOM
        # that stalled dp-meta-monitor). `attempted_at` feeds the is_known_dead check.
        index = pd.read_parquet(io.BytesIO(raw), columns=["capture_status", "data_type", "attempted_at"])
    except Exception:
        return []
    if index.empty or "capture_status" not in index.columns or "data_type" not in index.columns:
        return []

    status = index["capture_status"].astype(str)
    data_type_col = index["data_type"].astype(str)
    attempted_at_col = index["attempted_at"].astype(str)
    captured_mask = status == "captured"
    failed_mask = status == "attempted_failed"

    # Trailing-window mask applied to all rows; old failures beyond the window age out
    # of the HIGH verdict once the root cause is fixed.
    trailing_window = trailing_window_mask(attempted_at_col)
    cells: list[AttemptedFailedCell] = []
    # Stable order so the alert set + tests are deterministic (no set iteration).
    for data_type in sorted(data_type_col.unique()):
        dt_mask = data_type_col == data_type
        captured = int((dt_mask & captured_mask).sum())
        attempted_failed = int((dt_mask & failed_mask).sum())
        denom = captured + attempted_failed
        ratio = (attempted_failed / denom) if denom > 0 else 0.0
        # HIGH uses only the trailing window so a fixed historical incident ages out.
        trailing_af = int((dt_mask & failed_mask & trailing_window).sum())
        trailing_cap = int((dt_mask & captured_mask & trailing_window).sum())
        trailing_denom = trailing_cap + trailing_af
        trailing_ratio = (trailing_af / trailing_denom) if trailing_denom > 0 else 0.0
        high = trailing_af >= ATTEMPTED_FAILED_ABS_THRESHOLD or (
            trailing_af >= MIN_ATTEMPTED_FAILED_FOR_RATIO and trailing_ratio >= ATTEMPTED_FAILED_RATIO_THRESHOLD
        )
        failed_attempted_at = attempted_at_col[dt_mask & failed_mask]
        max_attempted_at = max(failed_attempted_at, default="")
        recent_attempted_failed = int(recent_activity_mask(failed_attempted_at).sum())
        cells.append(
            AttemptedFailedCell(
                asset_group=asset_group,
                data_type=data_type,
                captured=captured,
                attempted_failed=attempted_failed,
                ratio=ratio,
                high=high,
                known_dead=is_known_dead(asset_group, data_type, max_attempted_at=max_attempted_at),
                max_attempted_at=max_attempted_at,
                stale_days=stale_days_since(max_attempted_at),
                recent_attempted_failed=recent_attempted_failed,
                trailing_attempted_failed=trailing_af,
            )
        )
    return cells


def check_high_attempted_failed(
    *,
    storage_client: StorageClient,
    targets: Iterable[FreshnessTarget],
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
    renag_tracker: RenagTracker | None = None,
    renag_cooldown_seconds: float = DEFAULT_RENAG_COOLDOWN_SECONDS,
) -> list[AttemptedFailedCell]:
    """DP-FETCH-009 — page when a ``(asset_group, data_type)`` cell has a HIGH
    ``attempted_failed`` count/ratio in the consolidated manifest ``_index``.

    Closes the confirmed gap: nothing fires a Slack alert when a backfill exits 0
    with ``captured`` climbing yet writes thousands of ``attempted_failed`` cells
    (the exit-code monitor reads that as CLEAN; per-shard ``ADAPTER_FETCH_FAILED``
    events never aggregate to a DP alert). Each ``FreshnessTarget`` names an AG's
    market-data ``_index`` bucket (``label`` = the asset_group); the read is the
    same blob the consolidator writes.

    A cell is HIGH when its ``attempted_failed`` count crosses
    ``ATTEMPTED_FAILED_ABS_THRESHOLD`` OR (count >= ``MIN_ATTEMPTED_FAILED_FOR_RATIO``
    AND ratio >= ``ATTEMPTED_FAILED_RATIO_THRESHOLD``). When ``miss_tracker`` is
    provided the CRITICAL alert fires only after the SAME cell has been HIGH for
    ``min_consecutive`` consecutive sweeps (so a transient consolidator blip during
    a heavy backfill never false-pages); ``None`` ⇒ fire on the first HIGH cell
    (the prior fire-on-first behaviour; keeps back-compat for existing call sites).

    **Re-nag cooldown** (defense-in-depth, see the ``renag_tracker`` module docstring):
    with ``renag_tracker``, a cell past onset only re-emits once ``renag_cooldown_seconds``
    has elapsed since its last actual emission. ``None`` ⇒ no cooldown (back-compat).

    Reuses the registered ``DP_RUN_MOSTLY_EMPTY`` event (DP-FETCH-007, CRITICAL,
    PAGE_OPERATOR — the closest "a run failed to produce expected data" signal,
    already routed to #data-pipeline-alerts) rather than declaring a new UTL event,
    which would be cross-repo (UTL events + UAC alert rules). The ``registry_id``
    ``DP-FETCH-009`` keeps the high-attempted_failed case traceable in the alert
    details + issue doc.
    """
    all_cells: list[AttemptedFailedCell] = []
    for target in targets:
        cells = _read_attempted_failed_cells(
            storage_client,
            asset_group=target.label,
            bucket=target.bucket,
            blob=target.blob_path or AVAILABILITY_INDEX_BLOB,
        )
        all_cells.extend(cells)
        for cell in cells:
            if cell.known_dead:  # no miss/renag for a dead cell; new activity re-enables paging
                logger.info("DP_RUN_MOSTLY_EMPTY suppressed (known-dead) for '%s/%s'", cell.asset_group, cell.data_type)
                continue
            if miss_tracker is not None:
                misses = miss_tracker.register(
                    _high_attempted_failed_miss_key(cell.asset_group, cell.data_type),
                    stale=cell.high,
                )
                if cell.high and misses < min_consecutive:
                    logger.info(
                        "meta_watchers: DP_RUN_MOSTLY_EMPTY (high attempted_failed) for '%s/%s' "
                        "below consecutive-miss threshold (%d/%d) — not paging yet "
                        "(transient-blip suppression)",
                        cell.asset_group,
                        cell.data_type,
                        misses,
                        min_consecutive,
                    )
                    continue
            if not cell.high:
                continue
            renag_key = _high_attempted_failed_miss_key(cell.asset_group, cell.data_type)
            if apply_cooldown(
                renag_tracker,
                renag_key,
                cooldown_seconds=renag_cooldown_seconds,
                active_sweep=_EMITTED_THIS_SWEEP,
                event=DP_RUN_MOSTLY_EMPTY,
            ):
                continue
            # Reuse ATTEMPTED_FAILED_ABS_THRESHOLD (the SAME bar the alert itself uses to
            # decide "high") as the recent-window materiality floor — a cell only reads
            # genuinely "Fresh" when its OWN last-24h volume would independently justify a
            # CRITICAL page; MIN_ATTEMPTED_FAILED_FOR_RATIO (a much smaller micro-cell-noise
            # guard for the ratio path) would under-suppress a real-but-smaller daily trickle.
            is_static_backlog, staleness_note = stale_backlog_annotation(
                cell.stale_days,
                recent_attempted_failed=cell.recent_attempted_failed,
                materiality_floor=ATTEMPTED_FAILED_ABS_THRESHOLD,
            )
            summary = (
                f"high attempted_failed batch — asset_group={cell.asset_group} "
                f"data_type={cell.data_type}: {cell.trailing_attempted_failed} attempted_failed cells "
                f"in last {ATTEMPTED_FAILED_TRAILING_WINDOW_DAYS}d "
                f"(lifetime {cell.attempted_failed} of {cell.captured + cell.attempted_failed} attempted, "
                f"ratio {cell.ratio:.1%}; trailing abs>={ATTEMPTED_FAILED_ABS_THRESHOLD} "
                f"or ratio>={ATTEMPTED_FAILED_RATIO_THRESHOLD:.0%}). A backfill exited "
                f"0 / captured climbed but failed this batch invisibly."
            ) + staleness_note
            _emit(
                PipelineFinding(
                    event=DP_RUN_MOSTLY_EMPTY,
                    severity="CRITICAL",
                    tier=EscalationTier.PAGE_OPERATOR,
                    summary=summary,
                    details={
                        # ``asset_group`` carries the COMBINED <ag>/<data_type> so
                        # ``_alert_key`` yields one identity per cell (matches the
                        # miss-counter key); clean fields kept for diagnosability.
                        "asset_group": _high_attempted_failed_cell_label(cell.asset_group, cell.data_type),
                        "asset_group_name": cell.asset_group,
                        "data_type": cell.data_type,
                        "captured": cell.captured,
                        "attempted_failed": cell.attempted_failed,
                        "trailing_attempted_failed": cell.trailing_attempted_failed,
                        "trailing_window_days": ATTEMPTED_FAILED_TRAILING_WINDOW_DAYS,
                        "ratio": round(cell.ratio, 4),
                        "abs_threshold": ATTEMPTED_FAILED_ABS_THRESHOLD,
                        "ratio_threshold": ATTEMPTED_FAILED_RATIO_THRESHOLD,
                        "bucket": target.bucket,
                        "blob_path": target.blob_path or AVAILABILITY_INDEX_BLOB,
                        "max_attempted_at": cell.max_attempted_at,
                        "stale_days": cell.stale_days,
                        "recent_attempted_failed": cell.recent_attempted_failed,
                        "is_static_backlog": is_static_backlog,
                    },
                    registry_id="DP-FETCH-009",
                ),
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
            if renag_tracker is not None:
                renag_tracker.record(renag_key)
    return all_cells


def check_zombie_watchdog_alive(
    *,
    storage_client: StorageClient,
    log_bucket: str,
    max_age_min: float = DEFAULT_WATCHDOG_MAX_AGE_MIN,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
) -> FreshnessResult:
    """DP-WATCHER-001 — the zombie-VM watchdog's own census freshness."""
    target = FreshnessTarget(
        bucket=log_bucket,
        blob_path=WATCHDOG_CENSUS_BLOB,
        max_age_min=max_age_min,
        label="vm-zombie-watchdog",
    )
    result = probe_freshness(storage_client, target)
    if result.stale:
        _emit(
            PipelineFinding(
                event=DP_ZOMBIE_WATCHDOG_DOWN,
                severity="CRITICAL",
                tier=EscalationTier.PAGE_OPERATOR,
                summary=(
                    "vm-zombie-watchdog census stale "
                    + (f"({result.age_min:.0f}m)" if result.age_min is not None else "(missing) — watchdog down")
                ),
                details={
                    "bucket": log_bucket,
                    "blob_path": WATCHDOG_CENSUS_BLOB,
                    "age_min": result.age_min,
                    "max_age_min": max_age_min,
                },
                registry_id="DP-WATCHER-001",
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
    return result


def monitor_cron_targets(log_bucket: str) -> list[FreshnessTarget]:
    """Build a FreshnessTarget per fleet-monitor / meta-watcher sweep sentinel.

    Each sweep writes ``vm-census/<mode>-last-run.json`` (see ``_gcs`` /
    ``write_monitor_last_run``); a stale/absent sentinel = that monitor cron
    stopped firing → ``check_cron_fired`` emits ``DP_CRON_DID_NOT_FIRE``
    (DP-WATCHER-002, CRITICAL/page). The budget is 2x the cron's cadence
    (``MONITOR_CRON_CADENCE_MIN``) — one missed tick is benign jitter, two is the
    cron being down. This is the cron-watches-cron (in-band) layer; the meta sweep
    cannot detect its OWN death this way (Layer-2 / the out-of-band deadman owns
    that), but its sentinel is still listed so the deadman reads it from here too.
    """
    targets: list[FreshnessTarget] = []
    for mode, cadence_min in sorted(MONITOR_CRON_CADENCE_MIN.items()):
        targets.append(
            FreshnessTarget(
                bucket=log_bucket,
                blob_path=_gcs.MONITOR_LAST_RUN_BLOB.format(mode=mode),
                max_age_min=2.0 * cadence_min,
                label=f"dp-{mode}-monitor",
                # KEY #4 cross-check: a stale sentinel is suppressed when this Cloud
                # Run job's real execution history shows a recent SUCCEEDED run.
                cloud_run_job=MONITOR_CRON_CLOUD_RUN_JOB.get(mode, ""),
            )
        )
    return targets


def check_monitor_crons_fired(
    *,
    storage_client: StorageClient,
    log_bucket: str,
    scheduler_state_reader: SchedulerStateReader | None = None,
    execution_history_reader: ExecutionHistoryReader | None = None,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
    renag_tracker: RenagTracker | None = None,
    renag_cooldown_seconds: float = DEFAULT_RENAG_COOLDOWN_SECONDS,
) -> list[FreshnessResult]:
    """DP-WATCHER-002 — the fleet-monitor / meta-watcher crons fired on schedule.

    Convenience wrapper over :func:`check_cron_fired` for the monitor-sweep
    sentinels (built by :func:`monitor_cron_targets`). A stopped monitor cron
    leaves its ``vm-census/<mode>-last-run.json`` sentinel stale → emits
    ``DP_CRON_DID_NOT_FIRE`` (cron-did-not-fire, CRITICAL/page). NOTE: the meta
    sweep cannot catch its OWN death in-band — Layer-2's out-of-band deadman does.

    Pause-aware via ``scheduler_state_reader`` (KEY #2) — though the monitor-sweep
    schedulers (``dp-exit-code-monitor`` / ``dp-heartbeat-monitor`` /
    ``dp-meta-monitor``) are NOT paused during the campaign, so this is wired for
    parity; a paused monitor cron would (correctly) suppress.

    ``renag_tracker``/``renag_cooldown_seconds`` forward to :func:`check_cron_fired`
    (2026-07-23 — the exact path ``dp-exit-code-monitor``'s stale sentinel re-fired through).
    """
    return check_cron_fired(
        storage_client=storage_client,
        targets=monitor_cron_targets(log_bucket),
        scheduler_state_reader=scheduler_state_reader,
        execution_history_reader=execution_history_reader,
        pm_repo_path=pm_repo_path,
        dry_run=dry_run,
        miss_tracker=miss_tracker,
        min_consecutive=min_consecutive,
        renag_tracker=renag_tracker,
        renag_cooldown_seconds=renag_cooldown_seconds,
    )


def check_cron_fired(
    *,
    storage_client: StorageClient,
    targets: Iterable[FreshnessTarget],
    scheduler_state_reader: SchedulerStateReader | None = None,
    execution_history_reader: ExecutionHistoryReader | None = None,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
    renag_tracker: RenagTracker | None = None,
    renag_cooldown_seconds: float = DEFAULT_RENAG_COOLDOWN_SECONDS,
) -> list[FreshnessResult]:
    """DP-WATCHER-002 — scheduled audit/consolidator/digest crons fired on schedule.

    Each ``FreshnessTarget`` names a cron's durable output/heartbeat artifact +
    its expected cadence budget (e.g. a daily digest's report blob with a 25h
    budget). A stale artifact = the cron did not fire on schedule.

    **PAUSE-AWARE (KEY #2, operator 2026-06-23).** When a target names a
    ``scheduler_job`` AND the injected ``scheduler_state_reader`` reports it
    ``PAUSED``, the stale-artifact alert is SUPPRESSED — the scheduler is paused
    BY DESIGN during the manual-backfill campaign, so its stale output is expected,
    not a failure. An ``ENABLED`` job that missed its window (or any UNKNOWN/None
    state — fail toward alerting) STILL fires the CRITICAL alert. No
    ``scheduler_job`` / no reader ⇒ no pause check (always alert on stale, the
    prior behaviour).

    **EXECUTION-HISTORY CROSS-CHECK (KEY #4, operator 2026-06-23).** The SENTINEL
    artifact is a SECONDARY signal — a Cloud Run **Job** can fire on schedule + its
    execution ``Completes`` while its run.log sentinel write lags or silently misses
    (the ``dp-exit-code-monitor`` false-positive: executions Completed every 5 min
    yet DP_CRON_DID_NOT_FIRE fired CRITICAL on the stale sentinel). So when a target
    names a ``cloud_run_job`` AND the injected ``execution_history_reader`` reports
    the job's last SUCCESSFUL execution within the freshness budget, the
    stale-sentinel alert is SUPPRESSED — the cron DID fire. The execution history is
    the AUTHORITATIVE signal; the sentinel only catches a job that genuinely stopped
    executing. A ``None`` last-success age (no successful execution / job absent /
    API error) does NOT suppress (fail toward alerting). No ``cloud_run_job`` / no
    reader ⇒ no cross-check (sentinel is the sole signal, the prior behaviour).

    **Re-nag cooldown** (2026-07-23, extends the 2026-07-15 ``DP_RUN_MOSTLY_EMPTY``
    fix — see the ``renag_tracker`` module docstring — to this sibling detector, which
    had the identical no-ongoing-suppression gap). With ``renag_tracker``, a cron past
    onset only re-emits once ``renag_cooldown_seconds`` has elapsed since its last
    ACTUAL emission — mirrors ``check_high_attempted_failed``'s usage exactly. ``None``
    ⇒ no cooldown (back-compat: fire every sweep past onset, the prior behaviour).
    """
    results: list[FreshnessResult] = []
    for target in targets:
        result = probe_freshness(storage_client, target)
        results.append(result)
        miss_key = _cron_miss_key(target)
        if not result.stale:
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)  # fresh → reset
            continue
        # Pause-aware skip: a PAUSED-by-design scheduler's stale artifact is
        # EXPECTED — suppress (no actuator, no page). Only an explicit PAUSED
        # suppresses; ENABLED / DISABLED / None (unknown) all fall through to alert.
        if target.scheduler_job and scheduler_state_reader is not None:
            state = scheduler_state_reader(target.scheduler_job)
            if state == "PAUSED":
                logger.info(
                    "meta_watchers: DP_CRON_DID_NOT_FIRE suppressed for '%s' — scheduler job "
                    "'%s' is PAUSED (paused-by-design during the manual-backfill campaign)",
                    target.label,
                    target.scheduler_job,
                )
                if miss_tracker is not None:
                    miss_tracker.register(miss_key, stale=False)  # suppressed-by-design → not a real miss
                continue
        # Execution-history cross-check (KEY #4): the AUTHORITATIVE signal is the
        # Cloud Run Job's real execution history, NOT the lagging sentinel. A recent
        # SUCCEEDED execution within budget ⇒ the cron fired ⇒ suppress the false
        # stale-sentinel alert. None (no success / job absent / API error) → alert.
        if target.cloud_run_job and execution_history_reader is not None:
            last_success_age = execution_history_reader(target.cloud_run_job)
            if last_success_age is not None and last_success_age <= target.max_age_min:
                logger.info(
                    "meta_watchers: DP_CRON_DID_NOT_FIRE suppressed for '%s' — Cloud Run job "
                    "'%s' last SUCCEEDED execution %.0fm ago (<= budget %.0fm); the sentinel is "
                    "stale but the job IS firing (false positive)",
                    target.label,
                    target.cloud_run_job,
                    last_success_age,
                    target.max_age_min,
                )
                if miss_tracker is not None:
                    miss_tracker.register(miss_key, stale=False)  # job IS firing → not a real miss
                continue
        # Genuine stale + unsuppressed = a real miss. Consecutive-miss gate (Fix 2):
        # page only once it has been a real miss for ``min_consecutive`` sweeps.
        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=True)
            if misses < min_consecutive:
                logger.info(
                    "meta_watchers: DP_CRON_DID_NOT_FIRE for '%s' below consecutive-miss "
                    "threshold (%d/%d) — not paging yet (transient-blip suppression)",
                    target.label,
                    misses,
                    min_consecutive,
                )
                continue
        # Re-nag cooldown (2026-07-23): miss_key doubles as the renag identity (matches
        # _alert_key's selection for this event — see _cron_miss_key).
        if apply_cooldown(
            renag_tracker,
            miss_key,
            cooldown_seconds=renag_cooldown_seconds,
            active_sweep=_EMITTED_THIS_SWEEP,
            event=DP_CRON_DID_NOT_FIRE,
        ):
            continue
        if result.stale:
            _emit(
                PipelineFinding(
                    event=DP_CRON_DID_NOT_FIRE,
                    severity="CRITICAL",
                    tier=EscalationTier.PAGE_OPERATOR,
                    summary=(
                        f"cron '{target.label}' did not fire on schedule "
                        + (f"(last output {result.age_min:.0f}m ago)" if result.age_min is not None else "(no output)")
                    ),
                    details={
                        "cron": target.label,
                        "bucket": target.bucket,
                        "blob_path": target.blob_path,
                        "age_min": result.age_min,
                        "max_age_min": target.max_age_min,
                    },
                    registry_id="DP-WATCHER-002",
                ),
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
            if renag_tracker is not None:
                renag_tracker.record(miss_key)
    return results
