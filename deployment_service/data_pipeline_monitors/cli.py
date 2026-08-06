"""CLI entry point for the data-pipeline fleet monitors (Cloud Run Job / VM).

Wires the real compute + storage clients to the watcher sweeps:

  python -m deployment_service.data_pipeline_monitors.cli --mode exit-code
  python -m deployment_service.data_pipeline_monitors.cli --mode heartbeat
  python -m deployment_service.data_pipeline_monitors.cli --mode meta

``exit-code`` and ``heartbeat`` list the RUNNING VM census (compute API), read
each VM's per-VM manifest shard captured count, and run their respective sweeps.
``meta`` runs the freshness probes (catalogue / zombie-watchdog / crons).

The compute/storage clients are resolved here (not in the sweep modules) so the
sweeps stay pure + unit-testable; this entry point is the only credential-bound
surface. Exit code 0 always — alerting is event-driven, never exit-status-driven,
so a transient cloud hiccup never silently fails the monitor (mirrors the zombie
watchdog + ci-failure-watcher convention).
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import io
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import UTC, datetime
from types import ModuleType
from typing import cast

import pandas as pd
from unified_trading_library import (
    DeploymentsRegistry,
    PubSubEventSink,
    StorageClient,
    UnifiedCloudConfig,
    get_storage_client,
    resolve_bucket_name,
    run_lifecycle,
    setup_events,
)
from unified_trading_library.cloud_interface import get_compute_engine_client  # noqa: qg-deep-import

from deployment_service.data_pipeline_monitors import (
    _compute_ops,
    _gcs,
    consolidator_oom_watcher,
    consolidator_scheduler_watcher,
    exit_code_fleet_monitor,
    heartbeat_stall_watcher,
    live_stream_watcher,
    meta_watchers,
    stale_image_watcher,
)
from deployment_service.data_pipeline_monitors.escalation import PipelineFinding
from deployment_service.data_pipeline_monitors.launcher_registry import resolve_launcher_for_vm
from deployment_service.data_pipeline_monitors.meta_targets import ASSET_GROUPS
from deployment_service.data_pipeline_monitors.meta_targets import catalogue_targets as _catalogue_targets
from deployment_service.data_pipeline_monitors.meta_targets import (
    consolidator_cloud_run_job as _consolidator_cloud_run_job,
)
from deployment_service.data_pipeline_monitors.meta_targets import (
    consolidator_scheduler_job as _consolidator_scheduler_job,
)
from deployment_service.data_pipeline_monitors.meta_targets import (
    high_attempted_failed_targets as _high_attempted_failed_targets,
)
from deployment_service.data_pipeline_monitors.meta_targets import market_data_bucket as _market_data_bucket
from deployment_service.data_pipeline_monitors.meta_targets import scheduler_env_prefix as _scheduler_env_prefix
from deployment_service.data_pipeline_monitors.renag_tracker import RenagTracker
from deployment_service.data_pipeline_monitors.vm_classification import asset_group_for_vm as _asset_group_for_vm
from deployment_service.data_pipeline_monitors.vm_classification import is_data_vm as _is_data_vm
from deployment_service.deployment_classification import (
    UnclassifiedDeploymentError,
    umbrella_for_vm_name,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Per-VM manifest shard path (per-VM-shard isolation SSOT).
_PER_VM_SHARD = "_index/per_vm/{vm}.parquet"
# Hard ceiling on the synchronous GCP aggregated_list gRPC call.  60 s leaves
# ample time for the sweep + sentinel write within the Cloud Run Job timeout.
_LIST_VMS_TIMEOUT_SECS = 60.0
# Bounds the Cloud Scheduler get_job/list_jobs RPCs (KEY #2/DP-WATCHER-003). The
# GAPIC client's default retry policy treats a transient-looking auth/network
# failure as retryable and can block far longer than this module's documented
# fail-safe intent (return None/[] promptly) — a real outage should not stall
# the whole meta sweep. 10s is generous for a single-job describe/list call.
_SCHEDULER_RPC_TIMEOUT_SECS = 10.0
# First-seen census for the heartbeat watcher's grace window (vm_name -> ISO ts).
_FIRST_SEEN_BLOB = "vm-census/heartbeat-first-seen.json"


def _project_id() -> str:
    """Resolve the GCP project id from UnifiedCloudConfig (never hardcoded)."""
    try:
        cfg = UnifiedCloudConfig()
    except (ValueError, RuntimeError, OSError):
        return ""
    return getattr(cfg, "gcp_project_id", "") or ""


def _log_bucket() -> str:
    """The durable VM log/heartbeat bucket: ``deployment-scripts-<project>``."""
    proj = _project_id()
    return f"deployment-scripts-{proj}" if proj else ""


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def _minutes_since(iso_ts: str | None) -> float:
    """Minutes since an ISO timestamp; 0.0 when missing (treat as just-booted)."""
    if not iso_ts:
        return 0.0
    try:
        seen = datetime.fromisoformat(iso_ts)
    except ValueError:
        return 0.0
    if seen.tzinfo is None:
        seen = seen.replace(tzinfo=UTC)
    return (datetime.now(UTC) - seen.astimezone(UTC)).total_seconds() / 60.0


def _load_first_seen(storage_client: StorageClient, log_bucket: str) -> dict[str, str]:
    raw = _gcs.read_text(storage_client, log_bucket, _FIRST_SEEN_BLOB)
    if not raw:
        return {}
    try:
        loaded = cast("object", json.loads(raw))
    except (json.JSONDecodeError, ValueError):
        return {}
    if not isinstance(loaded, dict):
        return {}
    data = cast("dict[str, object]", loaded)
    out: dict[str, str] = {}
    for vm_name, ts in data.items():
        if isinstance(ts, str):
            out[str(vm_name)] = ts
    return out


def _write_first_seen(storage_client: StorageClient, log_bucket: str, census: dict[str, str]) -> None:
    payload = json.dumps(census, sort_keys=True).encode("utf-8")
    try:
        storage_client.upload_bytes(log_bucket, _FIRST_SEEN_BLOB, payload, content_type="application/json")
    except Exception as exc:
        logger.warning("heartbeat first-seen census persist failed: %s", exc)


def _list_running_vms() -> list[tuple[str, str]]:
    """Return ``(vm_name, zone)`` for every RUNNING VM.

    Routes through the UTL Compute Engine client (cloud-SDK confined to the
    unified-cloud-interface — the same path ``gcp_instance_lister`` uses) rather
    than touching ``google.cloud.compute_v1`` directly.  Read-only + failure
    isolated: an API error or timeout returns an empty list (the sweep then
    no-ops, the safe default).

    The underlying ``aggregated_list_instances`` call is a synchronous blocking
    paginated gRPC stream with no built-in timeout.  A slow or unresponsive GCP
    Compute API stalls the entire Cloud Run Job, preventing ``write_monitor_last_run``
    from being called and producing a spurious DP_CRON_DID_NOT_FIRE alert (DP-WATCHER-002).
    The ``ThreadPoolExecutor`` + ``future.result(timeout=...)`` pattern bounds the
    blocking call to ``_LIST_VMS_TIMEOUT_SECS``; on expiry the stuck gRPC thread is
    abandoned (Python 3.13 ``cancel_futures=True``) and an empty list is returned so
    the sweep + sentinel write proceed normally.
    """
    project_id = _project_id()
    try:
        client = get_compute_engine_client(provider="gcp", project_id=project_id)

        def _fetch() -> list[tuple[str, str]]:
            running: list[tuple[str, str]] = []
            for inst in client.aggregated_list_instances(project_id, ""):
                status = str(inst.get("status", ""))
                name = str(inst.get("name", ""))
                zone = str(inst.get("zone", "") or "")
                if status == "RUNNING" and name:
                    running.append((name, zone))
            return running

        pool = ThreadPoolExecutor(max_workers=1)
        fut = pool.submit(_fetch)
        try:
            return fut.result(timeout=_LIST_VMS_TIMEOUT_SECS)
        except FuturesTimeoutError:
            logger.warning(
                "_list_running_vms: aggregated_list stalled for >%.0fs — "
                "returning [] so sentinel write still fires; stuck gRPC thread abandoned",
                _LIST_VMS_TIMEOUT_SECS,
            )
            pool.shutdown(wait=False, cancel_futures=True)
            return []
    except Exception as exc:
        logger.warning("_list_running_vms failed: %s", exc)
        return []


def _make_shard_backed_ag_fn(storage_client: StorageClient):
    def _probe(ag: str, blob: str) -> bool:
        try:
            return storage_client.blob_exists(
                resolve_bucket_name(cloud="gcp", kind="market-data", asset_group=ag), blob
            )
        except Exception:
            return False

    def _fn(vm_name: str) -> str:
        if (ag := _asset_group_for_vm(vm_name)) != "unknown":
            return ag
        found = [c for c in ASSET_GROUPS if _probe(c, _PER_VM_SHARD.format(vm=vm_name))]
        return found[0] if len(found) == 1 else ("multi" if found else "unknown")

    return _fn


def _shard_bucket_for_vm(vm_name: str) -> str | None:
    ag = _asset_group_for_vm(vm_name)
    if ag == "unknown":
        return None
    try:
        return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group=ag)
    except Exception:
        return None


def _make_captured_reader(storage_client: StorageClient):
    """Return ``vm_name -> captured_cum`` reading the per-VM manifest shard.

    Reads ``_index/per_vm/{vm}.parquet`` from the VM's asset_group market-data
    bucket and counts ``capture_status == "captured"`` rows. Returns 0 on any
    read miss (a VM with no shard yet / heartbeat-only VM) — the cross-check then
    treats it as "no captured progress", the fail-safe direction.

    When ``_shard_bucket_for_vm`` can't resolve the bucket from the VM name
    (e.g. ``fs-backfill-*`` for sports), probes every asset-group market-data
    bucket to find the per-VM shard — closes the consolidation-lag false-positive
    where a VM that wrote real rows alerts CRITICAL DP_VM_GONE_NO_CAPTURE.
    """

    _shard_buckets_cache: list[str] | None = None

    def _shard_buckets() -> list[str]:
        nonlocal _shard_buckets_cache
        if _shard_buckets_cache is None:
            buckets: list[str] = []
            for ag in ASSET_GROUPS:
                try:
                    buckets.append(resolve_bucket_name(cloud="gcp", kind="market-data", asset_group=ag))
                except Exception:
                    continue
            _shard_buckets_cache = buckets
        return _shard_buckets_cache

    def _probe_all(vm_name: str) -> int:
        blob_path = _PER_VM_SHARD.format(vm=vm_name)
        for bucket in _shard_buckets():
            try:
                if not storage_client.blob_exists(bucket, blob_path):
                    continue
                raw = storage_client.download_bytes(bucket, blob_path)
                frame = pd.read_parquet(io.BytesIO(raw))
                if "capture_status" not in frame.columns:
                    return len(frame)
                return int((frame["capture_status"] == "captured").sum())
            except Exception:
                continue
        return 0

    def _read(vm_name: str) -> int:
        bucket = _shard_bucket_for_vm(vm_name)
        if not bucket:
            return _probe_all(vm_name)
        blob_path = _PER_VM_SHARD.format(vm=vm_name)
        try:
            if not storage_client.blob_exists(bucket, blob_path):
                return 0
            raw = storage_client.download_bytes(bucket, blob_path)
            frame = pd.read_parquet(io.BytesIO(raw))
            if "capture_status" not in frame.columns:
                return len(frame)
            return int((frame["capture_status"] == "captured").sum())
        except Exception:
            return 0

    return _read


def _make_shard_mtime_reader(storage_client: StorageClient):
    """Return ``vm_name -> per-VM manifest-shard mtime AGE (min)`` (None on miss).

    Age of ``_index/per_vm/{vm}.parquet`` in the VM's asset_group market-data
    bucket — the AUTHORITATIVE low-lag liveness signal: the worker writes that
    shard DIRECTLY to GCS as it captures (~60s cadence), so a fresh mtime PROVES
    the worker is alive AND capturing right now. The heartbeat-stall watcher reads
    the ``PIPELINE_HEARTBEAT`` marker from the GCS-TEE'd run.log, which can lag the
    on-VM log by tens of minutes (incident 2026-06-23: a tradfi-bf VM capturing
    114k rows + heartbeating on-box every 60s false-STALLed on a 42m-behind tee'd
    run.log). ``None`` ⇒ no shard / read miss ⇒ no override (fail-safe).
    """

    def _read(vm_name: str) -> float | None:
        bucket = _shard_bucket_for_vm(vm_name)
        if not bucket:
            return None
        blob_path = _PER_VM_SHARD.format(vm=vm_name)
        return _gcs.blob_age_minutes(storage_client, bucket, blob_path)

    return _read


def _make_sidecar_age_reader(storage_client: StorageClient):
    """Return ``vm_name -> infra sidecar blob AGE (min)`` (None ⇒ no blob).

    Age of ``vm-heartbeat/{vm}.txt`` in the log bucket — the FRESH HOST-liveness
    signal (the ``vm_heartbeat_sidecar.sh`` nohup writes it 60s via a direct GCS
    channel). The heartbeat-stall watcher reads this as the authoritative
    ``heartbeat_age_min`` (REVISED 2026-06-24): a fresh blob ⇒ the VM host/network
    is alive ⇒ never STALL/auto-kill on the laggy run.log; a STALE blob ⇒ the
    host/network wedged ⇒ the reapable hang. SSOT: heartbeat_stall_watcher module
    docstring + dp_alert_flood_triage_and_monitor_fixes_2026_06_23.md.
    """
    log_bucket = _log_bucket()

    def _read(vm_name: str) -> float | None:
        return _gcs.heartbeat_blob_age_minutes(storage_client, log_bucket, vm_name)

    return _read


def _launcher_for_vm(vm_name: str) -> str:
    """``vm_name -> launcher-script-name`` for the relaunch actuators ("" when none).

    Wraps ``launcher_registry.resolve_launcher_for_vm`` (which returns ``str | None``)
    to the sweep's ``Callable[[str], str]`` contract: a non-relaunchable prefix maps
    to ``""``, which the sweep treats as "no launcher binding" → the finding falls
    through to file_issue (never a wrong relaunch).
    """
    return resolve_launcher_for_vm(vm_name) or ""


def _zombie_watchdog() -> ModuleType | None:
    """Import the ``vm_zombie_watchdog`` module — or ``None`` if unavailable.

    Tries the IMAGE module path ``scripts.vm.vm_zombie_watchdog`` FIRST (the monitor
    jobs run in the deployment-api image where WORKDIR=/app + ``scripts/`` is a package,
    so the module resolves as ``scripts.vm.…``), then the bare ``vm_zombie_watchdog``
    (the VM-side script's own cwd). The prior bare-only import silently failed in the
    image (``find_spec`` → None) → the auto-kill returned False on EVERY stalled VM since
    FIX 1b shipped AND ``_umbrella_for_vm`` defaulted to "" (2026-06-24 incident:
    genuinely-hung VMs were never reaped → a persistent DP_VM_STALL flood). The module
    imports ``google.cloud.compute_v1`` + does ``bucket_naming`` at load (needs
    GCP_PROJECT_ID, set in the job env), so the import stays deferred here.
    """
    for name in ("scripts.vm.vm_zombie_watchdog", "vm_zombie_watchdog"):
        try:
            spec = importlib.util.find_spec(name)
        except ModuleNotFoundError:
            # find_spec() imports the PARENT package to resolve a dotted name, so when
            # `scripts` is importable in the image but `scripts.vm` is absent from the
            # wheel, find_spec RAISES `No module named 'scripts.vm'` instead of
            # returning None. Treat that as unavailable and try the next candidate.
            # (2026-07-13 monitoring-deadman regression: this unguarded raise crashed
            # EVERY fleet sweep the instant it processed a terminated/stalled VM →
            # no sentinel written → the out-of-band deadman paged. Same packaging
            # class as the 2026-06-23 scripts.recovery incident.)
            continue
        if spec is None:
            continue
        try:
            return importlib.import_module(name)
        except Exception as exc:
            logger.warning("vm_zombie_watchdog import via %s failed: %s", name, exc)
    return None


def _umbrella_for_vm(vm_name: str) -> str:
    """``vm_name -> deployment umbrella`` (LIVE/BATCH/PAPER/EXPERIMENT; "" when unresolved).

    Resolves via the packaged ``VM_PREFIX_TO_BUCKET`` registry
    (``deployment_service.vm_prefix_registry``) through the classification SSOT
    ``umbrella_for_vm_name`` so DP_VM_* findings carry the umbrella the
    alerting-service router splits on (LIVE → #uts-live-alerts, BATCH →
    #data-pipeline-alerts). Returns "" on an unregistered prefix → the alert routes to
    the batch default (never a sweep crash).

    The registry moved into the deployment_service PACKAGE (2026-07-13); it used to
    live in the unpackaged ``scripts/vm/vm_zombie_watchdog.py``, which is absent from
    the deployment-api image this monitor runs in — so the umbrella lookup crashed
    every sweep with ``ModuleNotFoundError: No module named 'scripts.vm'``
    (monitoring-deadman alert). Imported lazily so cli module-load stays
    credential-free: the registry resolves bucket names via ``resolve_bucket_name``
    on first use, inside the sweep where bucket resolution already works.
    """
    from deployment_service.vm_prefix_registry import VM_PREFIX_TO_BUCKET

    try:
        return umbrella_for_vm_name(vm_name, VM_PREFIX_TO_BUCKET).value
    except UnclassifiedDeploymentError:
        return ""


def _kill_stalled_vm(vm_name: str, zone: str) -> bool:
    """``(vm_name, zone) -> killed`` deleter for the heartbeat sweep's auto-kill.

    Reuses the zombie watchdog's ``_kill_vm`` (which archives run.log + serial
    console for forensics before deleting via the compute API), so the kill path is
    identical to the watchdog's. ``vm_zombie_watchdog`` is a VM-side launcher script
    that imports ``google.cloud.compute_v1`` at module load, so it is pulled in HERE
    (deferred) rather than at this package's module top — the sweep itself never
    touches a cloud SDK (it receives this resolver injected, keeping the pure sweep
    credential-free + block-network in tests). Returns False if the watchdog module
    is unavailable in the runtime (the sweep then only alerts, never crashes).
    """
    watchdog = _zombie_watchdog()
    if watchdog is None:
        logger.warning("auto-kill: vm_zombie_watchdog unavailable in runtime — cannot kill %s", vm_name)
        return False
    try:
        compute_client = watchdog.compute_v1.InstancesClient()  # pyright: ignore[reportAny] — dynamic cloud-SDK boundary
        return bool(watchdog._kill_vm(compute_client, vm_name, zone))  # pyright: ignore[reportAny] — dynamic VM-side helper
    except Exception as exc:
        logger.warning("auto-kill: failed to delete stalled VM %s in %s: %s", vm_name, zone, exc)
        return False


def _make_scheduler_state_reader() -> meta_watchers.SchedulerStateReader:
    """Return ``job_name -> "ENABLED"|"PAUSED"|... | None`` via Cloud Scheduler.

    PAUSE-AWARE meta-watcher input (KEY #2): a scheduler PAUSED-by-design during
    the manual-backfill campaign should NOT fire DP_CRON_DID_NOT_FIRE. The
    google.cloud.scheduler_v1 client is deferred-imported here (the credential-bound
    surface) — NOT at module top — mirroring the ``vm_zombie_watchdog`` deferral, so
    the watcher modules stay import-safe + credential-free. A lookup error / missing
    client returns ``None`` (UNKNOWN → the watcher does NOT suppress, the fail-safe
    direction). The job's short name is resolved to the fully-qualified
    ``projects/{p}/locations/{loc}/jobs/{name}`` path.
    """
    project = _project_id()
    location = "asia-northeast1"  # the fleet's canonical region (all schedulers + GCS)
    try:
        scheduler_mod = importlib.import_module("google.cloud.scheduler_v1")  # noqa: imports-inside-functions — credential-bound SDK, deferred
        client = scheduler_mod.CloudSchedulerClient()
    except Exception as exc:
        logger.info("scheduler-state reader unavailable (pause-awareness off, alerts fail-safe-on): %s", exc)
        return lambda _job: None

    def _read(job_name: str) -> str | None:
        if not project or not job_name:
            return None
        qualified = f"projects/{project}/locations/{location}/jobs/{job_name}"
        try:
            job = client.get_job(name=qualified, timeout=_SCHEDULER_RPC_TIMEOUT_SECS)
            # Job.state is an enum; .name gives "ENABLED"/"PAUSED"/"DISABLED"/...
            return str(getattr(job.state, "name", "") or "") or None
        except Exception as exc:
            # NotFound / permission / transient → UNKNOWN; the watcher does not
            # suppress on None (a genuinely-missing scheduler still alerts).
            logger.info("scheduler-state lookup for %s → unknown: %s", job_name, exc)
            return None

    return _read


def _make_execution_history_reader() -> meta_watchers.ExecutionHistoryReader:
    """Return ``job_stem -> minutes since last SUCCEEDED Cloud Run execution | None``.

    KEY #4: suppresses DP_CRON_DID_NOT_FIRE when execution history shows a recent
    success (vs lagging GCS sentinel). Deferred-import; errors return ``None``.
    """
    project = _project_id()
    location = "asia-northeast1"  # the fleet's canonical region (all Cloud Run jobs)
    env_prefix = _scheduler_env_prefix()
    try:
        run_mod = importlib.import_module("google.cloud.run_v2")  # noqa: imports-inside-functions — credential-bound SDK, deferred
        client = run_mod.ExecutionsClient()  # pyright: ignore[reportAny] — dynamic cloud-SDK boundary
    except Exception as exc:
        logger.info("execution-history reader unavailable (KEY#4 cross-check off, alerts fail-safe-on): %s", exc)
        return lambda _job: None

    def _read(job_stem: str) -> float | None:
        if not project or not job_stem:
            return None
        job_name = f"{env_prefix}-{job_stem}" if env_prefix else job_stem
        parent = f"projects/{project}/locations/{location}/jobs/{job_name}"
        try:
            youngest_age: float | None = None
            for execution in client.list_executions(parent=parent):  # pyright: ignore[reportAny] — dynamic SDK page iterator
                # A SUCCEEDED execution has a non-zero succeeded_count + a
                # completion_time. Take the most RECENT completion across successes.
                if int(getattr(execution, "succeeded_count", 0) or 0) <= 0:  # pyright: ignore[reportAny] — dynamic SDK attr
                    continue
                completion = getattr(execution, "completion_time", None)  # pyright: ignore[reportAny] — dynamic SDK attr
                if completion is None:
                    continue
                completed_dt = completion if isinstance(completion, datetime) else completion.ToDatetime(tzinfo=UTC)  # pyright: ignore[reportAny] — protobuf Timestamp
                age_min = (datetime.now(UTC) - completed_dt).total_seconds() / 60.0
                if youngest_age is None or age_min < youngest_age:
                    youngest_age = age_min
            return youngest_age
        except Exception as exc:
            # NotFound / permission / transient → UNKNOWN; the watcher does not
            # suppress on None (a genuinely-dead job still alerts).
            logger.info("execution-history lookup for %s → unknown: %s", job_name, exc)
            return None

    return _read


# Storm-mode re-sweep (asia_northeast1_c_spot_preemption_storm_2026_08_04.md
# todo 3): a VM whose whole lifetime fits inside one ~5-min
# dp_exit_code_monitor_cron tick never enters sweep()'s `prior` census before
# it's gone, so the terminated-diff can never see it (confirmed live:
# af-backfill-20260804-004955, ~1.5min). Fix: on a pass with >= threshold
# PREEMPTED verdicts (storm evidence), re-sweep at a short in-process interval
# — bounded so the worst case stays under the 5-min gap to the next scheduled
# invocation and inside the Job's own 900s timeout. Never fires in --dry-run.
_STORM_PREEMPTION_THRESHOLD = 2
_STORM_RESWEEP_INTERVAL_SECS = 60.0
_STORM_MAX_RESWEEPS = 4


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Data-pipeline fleet monitors")
    parser.add_argument("--mode", required=True, choices=["exit-code", "heartbeat", "meta"])
    parser.add_argument("--pm-repo-path", default=None, help="PM clone path for the file_issue tier")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--stall-minutes", type=float, default=heartbeat_stall_watcher.DEFAULT_STALL_MINUTES)
    parser.add_argument(
        "--kill-minutes",
        type=float,
        default=heartbeat_stall_watcher.DEFAULT_KILL_MINUTES,
        help="auto-DELETE a stalled backfill VM once its stall is this old (reclaims its wave-launcher slot)",
    )
    parser.add_argument(
        "--no-auto-kill",
        action="store_true",
        help="disable the heartbeat-stall auto-kill (alert-only) — the kill defaults ON for the heartbeat mode",
    )
    args = parser.parse_args(argv)
    mode: str = str(cast("object", args.mode))
    _pm_raw = cast("object", args.pm_repo_path)
    pm_repo_path: str | None = str(_pm_raw) if _pm_raw else None
    dry_run: bool = bool(cast("object", args.dry_run))
    kill_minutes: float = float(cast("object", args.kill_minutes))
    auto_kill: bool = not bool(cast("object", args.no_auto_kill))
    stall_minutes: float = float(cast("str | float", args.stall_minutes))

    # Wire log_event to PubSub so DP_* findings reach #data-pipeline-alerts.
    # Best-effort — a setup failure must not crash the monitor.
    try:
        project_id = getattr(UnifiedCloudConfig(), "gcp_project_id", "") or ""
        if project_id:
            sink = PubSubEventSink(project_id=project_id, topic="lifecycle-events", service_name="dp-fleet-monitor")
            setup_events(service_name="dp-fleet-monitor", mode="live", sink=sink)
    except Exception as exc:
        logger.warning("dp_fleet_monitor live-events setup failed (best-effort): %s", exc)

    with run_lifecycle(service_name="dp-fleet-monitor"):
        storage_client = get_storage_client()
        captured_reader = _make_captured_reader(storage_client)
        log_bucket = _log_bucket()

        if mode == "exit-code":
            sweep_pass = 0
            while True:
                sweep_pass += 1
                all_running_vms = _list_running_vms()
                running = [vm for vm in all_running_vms if _is_data_vm(vm[0])]
                ec_findings: list[PipelineFinding] = []
                results = exit_code_fleet_monitor.sweep(
                    storage_client=storage_client,
                    log_bucket=log_bucket,
                    running_vms=running,
                    captured_reader=captured_reader,
                    asset_group_for_vm=_make_shard_backed_ag_fn(storage_client),
                    launcher_for_vm=_launcher_for_vm,
                    umbrella_for_vm=_umbrella_for_vm,
                    preemption_op_checker=_compute_ops.make_preemption_op_checker(_project_id()),
                    scheduling_model_checker=_compute_ops.make_scheduling_model_checker(_project_id()),
                    finding_sink=ec_findings,
                    pm_repo_path=pm_repo_path,
                    dry_run=dry_run,
                )
                # "clean" + "expected_no_capture" are both benign (the latter = rows
                # written but consolidated lags / honest-absence / shard already
                # complete — the 2026-06-23 false-positive killer). "rate_limited" is a
                # real-transient finding, so it counts as non_clean (it alerts WARN).
                non_clean = [r for r in results if r.verdict.value not in ("clean", "expected_no_capture")]
                preempted_count = sum(
                    1 for r in results if r.verdict is exit_code_fleet_monitor.TerminationVerdict.PREEMPTED
                )
                logger.info(
                    "exit-code sweep (pass %d): %d terminated, %d non-clean, %d preempted (%s)",
                    sweep_pass,
                    len(results),
                    len(non_clean),
                    preempted_count,
                    ", ".join(f"{r.vm_name}:{r.verdict}" for r in non_clean) or "none",
                )
                if not dry_run:
                    _gcs.write_monitor_last_run(
                        storage_client,
                        log_bucket,
                        "exit-code",
                        ok=True,
                        counts={"terminated": len(results), "non_clean": len(non_clean)},
                    )
                    # DeploymentsRegistry.reap_stale() was already implemented + unit-tested
                    # but had zero callers — a deployments/active/*.json registration whose
                    # GCE instance is confirmed gone stayed status="running" forever (live-
                    # verified: one record stayed "running" 4 days after its instance was
                    # deleted). Wire it here using the SAME running-VM census this sweep
                    # already fetched (unfiltered by _is_data_vm — registry entries are not
                    # limited to data VMs). Best-effort: a failure here must never abort the
                    # exit-code sweep itself.
                    try:
                        reaped = DeploymentsRegistry().reap_stale(
                            running_vm_names={vm_name for vm_name, _zone in all_running_vms}
                        )
                        if reaped:
                            logger.info(
                                "exit-code sweep: reap_stale archived %d stale deployment registration(s): %s",
                                len(reaped),
                                ", ".join(e.deployment_id for e in reaped),
                            )
                    except Exception as exc:
                        logger.warning("exit-code sweep: reap_stale failed (best-effort): %s", exc)
                # RESOLVED bookend (alert-lifecycle, extended to exit-code 2026-06-24): a
                # DP_VM_GONE/DP_VM_EXIT that fired last sweep but not this one (the cell got
                # captured / relaunched) posts a ✅ RESOLVED. Own active blob (disjoint events).
                meta_watchers.reconcile_resolved(
                    storage_client=storage_client,
                    log_bucket=log_bucket,
                    active_blob="vm-census/active-dp-alerts-exit-code.json",
                    emitted={meta_watchers.alert_key(f): f.event for f in ec_findings},
                    dry_run=dry_run,
                )
                if dry_run or preempted_count < _STORM_PREEMPTION_THRESHOLD or sweep_pass >= _STORM_MAX_RESWEEPS:
                    break
                logger.info(
                    "exit-code sweep: storm mode — %d PREEMPTED this pass (>=%d) — re-sweeping in %.0fs "
                    "(pass %d/%d) to catch VMs whose entire lifetime falls inside one tick",
                    preempted_count,
                    _STORM_PREEMPTION_THRESHOLD,
                    _STORM_RESWEEP_INTERVAL_SECS,
                    sweep_pass,
                    _STORM_MAX_RESWEEPS,
                )
                time.sleep(_STORM_RESWEEP_INTERVAL_SECS)
        elif mode == "heartbeat":
            running = [vm for vm in _list_running_vms() if _is_data_vm(vm[0])]
            prior = exit_code_fleet_monitor.load_census(storage_client, log_bucket)
            # VM age is derived from a first-seen census the monitor maintains (no
            # per-instance compute describe — keeps the cloud SDK fully confined to
            # the UTL list call above). A VM in the prior census has been seen ≥1
            # tick → past grace; a brand-new VM is treated as just-booted (defer).
            first_seen = _load_first_seen(storage_client, log_bucket)
            now_iso = _utcnow_iso()
            updated_first_seen = dict(first_seen)
            for vm_name, _zone in running:
                updated_first_seen.setdefault(vm_name, now_iso)

            def _age_reader(vm_name: str, _zone: str) -> float:
                return _minutes_since(updated_first_seen.get(vm_name))

            hb_findings: list[PipelineFinding] = []
            results = heartbeat_stall_watcher.sweep(
                storage_client=storage_client,
                log_bucket=log_bucket,
                running_vms=running,
                vm_age_reader=_age_reader,
                captured_reader=captured_reader,
                shard_mtime_reader=_make_shard_mtime_reader(storage_client),
                sidecar_age_reader=_make_sidecar_age_reader(storage_client),
                asset_group_for_vm=_make_shard_backed_ag_fn(storage_client),
                launcher_for_vm=_launcher_for_vm,
                umbrella_for_vm=_umbrella_for_vm,
                finding_sink=hb_findings,
                vm_killer=_kill_stalled_vm if auto_kill else None,
                prior_captured=prior,
                stall_minutes=stall_minutes,
                kill_minutes=kill_minutes,
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
            stalled = [r for r in results if r.verdict.value in ("stall", "event_loop_starved")]
            logger.info("heartbeat sweep: %d running, %d stalled", len(results), len(stalled))
            # RESOLVED bookend (alert-lifecycle, extended to heartbeat 2026-06-24): a
            # DP_VM_STALL that fired last sweep but not this one (the VM recovered / was
            # reaped + relaunched) posts a ✅ RESOLVED. Own active blob (disjoint events).
            meta_watchers.reconcile_resolved(
                storage_client=storage_client,
                log_bucket=log_bucket,
                active_blob="vm-census/active-dp-alerts-heartbeat.json",
                emitted={meta_watchers.alert_key(f): f.event for f in hb_findings},
                dry_run=dry_run,
            )
            if not dry_run:
                # Prune first-seen entries for VMs that have gone (terminated), then persist.
                running_names = {name for name, _ in running}
                pruned = {vm: ts for vm, ts in updated_first_seen.items() if vm in running_names}
                _write_first_seen(storage_client, log_bucket, pruned)
                _gcs.write_monitor_last_run(
                    storage_client,
                    log_bucket,
                    "heartbeat",
                    ok=True,
                    counts={"running": len(results), "stalled": len(stalled)},
                )
        else:  # meta
            meta_watchers.reset_emitted_tracker()
            # Consecutive-miss gate (Fix 2, 2026-06-24): page the catalogue + cron
            # alerts only after a probe is stale for N consecutive sweeps, so a single
            # transient GCS-read blip during a heavy backfill never false-pages. The
            # counters persist across Cloud Run sweeps in a GCS blob (each sweep is a
            # fresh process). Loaded once, shared by all three checkers, persisted at
            # end-of-sweep so the next sweep continues the count.
            miss_tracker = meta_watchers.MissTracker.load(storage_client=storage_client, log_bucket=log_bucket)
            # Re-nag cooldown (2026-07-15, defense-in-depth for the DP_RUN_MOSTLY_EMPTY
            # duplicate-alert spam): once check_high_attempted_failed's onset gate
            # (above) has fired for a cell, this tracker suppresses an identical re-page
            # every */15 sweep until renag_cooldown_seconds elapses since the last
            # ACTUAL alert. Loaded/persisted the same way as miss_tracker.
            renag_tracker = RenagTracker.load(storage_client=storage_client, log_bucket=log_bucket)
            meta_watchers.check_catalogue_freshness(
                storage_client=storage_client,
                targets=_catalogue_targets(),
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
                miss_tracker=miss_tracker,
            )
            # DP-FETCH-009: page when a (asset_group, data_type) cell has a HIGH
            # attempted_failed batch in the consolidated manifest _index — closes the
            # gap where a backfill exits 0 / captured climbs yet fails thousands of
            # cells invisibly (the exit-code monitor reads that as CLEAN; per-shard
            # ADAPTER_FETCH_FAILED never aggregates to a DP alert). Shares the
            # miss_tracker so a transient consolidator blip only pages when sustained.
            meta_watchers.check_high_attempted_failed(
                storage_client=storage_client,
                targets=_high_attempted_failed_targets(),
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
                miss_tracker=miss_tracker,
                renag_tracker=renag_tracker,
            )
            meta_watchers.check_zombie_watchdog_alive(
                storage_client=storage_client,
                log_bucket=log_bucket,
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
            # Pause-aware scheduler-state reader (KEY #2): a consolidator scheduler
            # PAUSED-by-design during the manual-backfill campaign suppresses its
            # stale-_index DP_CRON_DID_NOT_FIRE; an ENABLED-but-stale one still alerts.
            scheduler_state_reader = _make_scheduler_state_reader()
            # Cron freshness: the orphan-ping / consolidator / digest crons leave a
            # durable artifact. Probe the consolidator heartbeat (per-AG market-data
            # _index) + the data-pipeline daily digest output (when present). Each
            # target names its backing Cloud Scheduler job so a PAUSED scheduler skips.
            cron_targets: list[meta_watchers.FreshnessTarget] = []
            for ag in ASSET_GROUPS:
                try:
                    # honours the flat prediction bucket (see market_data_bucket docstring)
                    bucket = _market_data_bucket(ag)
                except Exception:
                    continue
                cron_targets.append(
                    meta_watchers.FreshnessTarget(
                        bucket=bucket,
                        blob_path="_index/availability_index.parquet",
                        max_age_min=180.0,  # consolidator should touch every cycle
                        label=f"manifest-consolidator-{ag}",
                        scheduler_job=_consolidator_scheduler_job(ag),
                        # KEY #4: suppress a transiently-stale _index read when the
                        # consolidator Cloud Run job shows a recent SUCCEEDED execution
                        # (needs execution_history_reader passed to check_cron_fired below).
                        cloud_run_job=_consolidator_cloud_run_job(ag),
                    )
                )
            # Host-cron freshness (FIX 2, 2026-06-24): the TradFi wave-launcher runs as
            # a HOST cron (0 */3 on the monitor host) — invisible to Cloud Run execution
            # history, so it carries NO cloud_run_job/scheduler_job cross-check. It writes
            # vm-census/wave-launcher-last-run.json each tick (wave_launcher.py
            # _write_last_run_sentinel); probe THAT freshness directly as the
            # host-cron-fired signal (budget 2x the 3h cadence = 360m). A genuinely-dead
            # wave-launcher still pages DP_CRON_DID_NOT_FIRE; a healthy host cron whose
            # tick is invisible to Cloud Run no longer false-fires. Blob path mirrors
            # scripts/wave_launcher.WAVE_LAUNCHER_LAST_RUN_BLOB (scripts/ is not importable).
            cron_targets.append(
                meta_watchers.FreshnessTarget(
                    bucket=log_bucket,
                    blob_path="vm-census/wave-launcher-last-run.json",
                    max_age_min=360.0,
                    label="tradfi-wave-launcher",
                )
            )
            # KEY #4 reader shared by the consolidator + monitor-cron sweeps: cross-checks
            # the backing Cloud Run Job's REAL execution history so a transiently-stale
            # artifact + a recent SUCCEEDED run is suppressed, not paged.
            execution_history_reader = _make_execution_history_reader()
            meta_watchers.check_cron_fired(
                storage_client=storage_client,
                targets=cron_targets,
                scheduler_state_reader=scheduler_state_reader,
                execution_history_reader=execution_history_reader,
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
                miss_tracker=miss_tracker,
                renag_tracker=renag_tracker,  # 2026-07-23 re-nag cooldown
            )
            # DP-WATCHER-003: the INVERSE of check_cron_fired's pause-awareness —
            # page when a non-`-legacy-` manifest-consolidator scheduler is PAUSED
            # (an accidental pause silently starves an entire asset_group/bucket
            # pair; root-caused 2026-07-13 on the 2 sports consolidator schedulers).
            # maintenance_window_reader (2026-07-29): a live, plan-tracked pause
            # downgrades to INFO instead of paging — see its factory's docstring.
            consolidator_scheduler_watcher.check_consolidator_scheduler_paused(
                scheduler_job_lister=consolidator_scheduler_watcher.make_consolidator_scheduler_lister(
                    project_id=_project_id(), scheduler_rpc_timeout_secs=_SCHEDULER_RPC_TIMEOUT_SECS
                ),
                scheduler_state_reader=scheduler_state_reader,
                maintenance_window_reader=consolidator_scheduler_watcher.make_consolidator_maintenance_window_reader(),
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
                miss_tracker=miss_tracker,
            )
            # DP-WATCHER-005: OOM-killed consolidator → CONSOLIDATOR_DOWN with oom=True (cefi_hl_aster P3)
            consolidator_oom_watcher.check_consolidator_oom(
                asset_groups=ASSET_GROUPS,
                execution_oom_reader=consolidator_oom_watcher.make_consolidator_execution_oom_reader(
                    project_id=_project_id(), env_prefix=_scheduler_env_prefix()
                ),
                index_age_reader=consolidator_oom_watcher.make_consolidator_index_age_reader(
                    storage_client, _market_data_bucket
                ),
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
                miss_tracker=miss_tracker,
            )
            # Cron-watches-cron (in-band): probe the fleet-monitor / meta-watcher
            # sweep sentinels themselves. A stopped exit-code/heartbeat/meta cron
            # leaves its vm-census/<mode>-last-run.json stale → DP_CRON_DID_NOT_FIRE
            # (DP-WATCHER-002, page). The meta sweep can't catch its OWN death this
            # way — Layer-2's out-of-band deadman owns that. KEY #4: the
            # execution-history reader cross-checks each monitor's REAL Cloud Run
            # Job execution history so a stale sentinel + recent SUCCEEDED execution
            # (the dp-exit-code-monitor false positive) is suppressed, not paged.
            meta_watchers.check_monitor_crons_fired(
                storage_client=storage_client,
                log_bucket=log_bucket,
                scheduler_state_reader=scheduler_state_reader,
                execution_history_reader=execution_history_reader,
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
                miss_tracker=miss_tracker,
                renag_tracker=renag_tracker,  # 2026-07-23: dp-exit-code-monitor's own spam path
            )
            _live_shards = live_stream_watcher.build_prediction_live_shards(
                storage_client
            )  # DP-LIVE-001/002 shared shard read
            live_stream_watcher.check_live_stream_stale(
                storage_client=storage_client,
                shards=_live_shards,
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
                miss_tracker=miss_tracker,
            )
            live_stream_watcher.check_live_stream_gcs_write_mismatch(
                storage_client=storage_client,
                shards=_live_shards,
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
                miss_tracker=miss_tracker,
            )
            # DP-VM-007: stale-image check (stale_cloud_run_image_alert_gap_2026_06_26)
            _svc_map = stale_image_watcher.job_to_service_map()
            stale_image_watcher.check_cloud_run_image_freshness(
                job_names=list(_svc_map.keys()),
                job_to_service=_svc_map.get,
                image_digest_reader=stale_image_watcher.make_image_digest_reader(_scheduler_env_prefix()),
                latest_digest_reader=stale_image_watcher.make_latest_digest_reader(),
                project_id=_project_id(),
                location=stale_image_watcher.ARTIFACT_REGISTRY_LOCATION,
                artifact_repository=stale_image_watcher.ARTIFACT_REGISTRY_REPO,
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
                miss_tracker=miss_tracker,
            )
            # Persist the consecutive-miss counters so the NEXT sweep continues the
            # count (a stale condition that has now been a miss for N sweeps pages;
            # a fresh/suppressed probe reset its key above). Before reconcile_resolved
            # so a partial-sweep failure still records the misses.
            miss_tracker.persist(dry_run=dry_run)
            # RESOLVED bookend (alert-lifecycle hardening): emit ✅ for any meta-watcher
            # alert that cleared since the last sweep, so #data-pipeline-alerts reflects
            # closure instead of a permanent RED on a transient stale-then-fresh artifact.
            # renag_tracker: a cell that TRULY clears here also has its re-nag state
            # cleared (in-memory), so a LATER re-onset of the SAME cell pages
            # immediately — MUST persist renag_tracker AFTER this call (below), or the
            # clear() would be silently lost (each Cloud Run sweep is a fresh process).
            meta_watchers.reconcile_resolved(
                storage_client=storage_client,
                log_bucket=log_bucket,
                dry_run=dry_run,
                renag_tracker=renag_tracker,
            )
            # Persist the re-nag timestamps so the NEXT sweep continues the cooldown
            # clock (a cell whose cooldown hasn't elapsed stays suppressed; one that
            # reconcile_resolved just cleared above is now dropped from the persisted
            # state too, so a later re-onset is NOT rate-limited by the prior incident).
            renag_tracker.persist(dry_run=dry_run)
            logger.info("meta sweep complete")
            if not dry_run:
                _gcs.write_monitor_last_run(storage_client, log_bucket, "meta", ok=True)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
