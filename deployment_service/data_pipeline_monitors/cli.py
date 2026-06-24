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
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import UTC, datetime
from types import ModuleType
from typing import cast

import pandas as pd
from unified_api_contracts import VmPrefixSpec
from unified_trading_library import (
    PubSubEventSink,
    StorageClient,
    UnifiedCloudConfig,
    get_storage_client,
    resolve_bucket_name,
    run_lifecycle,
    setup_events,
)
from unified_trading_library.cloud_interface import get_compute_engine_client  # noqa: qg-deep-import
from unified_trading_library.cloud_interface.constants import get_environment  # noqa: qg-deep-import

from deployment_service.data_pipeline_monitors import (
    _gcs,
    exit_code_fleet_monitor,
    heartbeat_stall_watcher,
    meta_watchers,
)
from deployment_service.data_pipeline_monitors.escalation import PipelineFinding
from deployment_service.data_pipeline_monitors.launcher_registry import resolve_launcher_for_vm
from deployment_service.deployment_classification import (
    UnclassifiedDeploymentError,
    umbrella_for_vm_name,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ASSET_GROUPS = ("cefi", "defi", "tradfi", "sports", "prediction")
# Per-VM manifest shard path (per-VM-shard isolation SSOT).
_PER_VM_SHARD = "_index/per_vm/{vm}.parquet"
# Hard ceiling on the synchronous GCP aggregated_list gRPC call.  60 s leaves
# ample time for the sweep + sentinel write within the Cloud Run Job timeout.
_LIST_VMS_TIMEOUT_SECS = 60.0
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


def _asset_group_for_vm(vm_name: str) -> str:
    """Best-effort asset_group from the VM-name segment (cefi/defi/tradfi/...)."""
    lowered = vm_name.lower()
    for ag in ASSET_GROUPS:
        if ag in lowered:
            return ag
    return "unknown"


# VM-name prefixes that ARE data-pipeline backfill / live-capture VMs (the only
# ones that emit a PIPELINE_HEARTBEAT + write a per-VM manifest shard). The
# heartbeat / exit-code sweeps must SKIP infra VMs (zombie-watchdog, orchestrator,
# consolidator, qg-snapshot, …) — they never heartbeat, so sweeping them produced
# a flood of false EVENT_LOOP_STARVED verdicts (2026-06-22 BUG2).
_DATA_VM_PREFIXES = (
    "mtds-",
    "tm-backfill",
    "fs-backfill",
    "instruments-",
    "tradfi-bf",
    "tradfi-fwd",
    "cefi-",
    "defi-",
    "sports-",
    "prediction-",
    "weather-backfill",
    "solana-",
)


def _is_data_vm(vm_name: str) -> bool:
    """True when ``vm_name`` is a data-pipeline VM (heartbeats + per-VM shard).

    Filters the RUNNING census down to the data VMs the heartbeat/exit-code
    sweeps apply to. An AG segment in the name (cefi/defi/tradfi/sports/
    prediction) OR a known data-VM prefix qualifies; everything else (infra /
    orchestrator / watchdog VMs) is skipped so they never false-alert.
    """
    lowered = vm_name.lower()
    if _asset_group_for_vm(vm_name) != "unknown":
        return True
    return any(lowered.startswith(p) for p in _DATA_VM_PREFIXES)


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
    """

    def _read(vm_name: str) -> int:
        bucket = _shard_bucket_for_vm(vm_name)
        if not bucket:
            return 0
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


# The catalogue regen (build_instrument_catalogue.py) writes the canonical
# artifact to ``gs://{instruments-store-{ag}-{env-short}-{pid}}/{DEPLOYMENT_ENV}/catalog.parquet``
# — bucket is env-SHORT (``-prd-``), the blob PREFIX is the LONG env name (default
# ``prod``). The monitor MUST mirror BOTH or it probes a non-existent object →
# age=None → false "missing" (KEY #3, the documented env-less-vs-env-short reader
# bug class). SSOT: instruments-service/scripts/build_instrument_catalogue.py
# (``_catalogue_object_paths`` + ``_instruments_store_bucket_for``).
_CATALOGUE_FILENAME = "catalog.parquet"
# prediction uses a dedicated FLAT bucket key (no PREDICTION entry in the per-AG
# ``instruments-store`` dict), mirroring build_instrument_catalogue's resolver.
_INSTRUMENTS_STORE_KIND_OVERRIDE: dict[str, str] = {"prediction": "instruments-store-prediction"}


def _deployment_env_long() -> str:
    """The LONG env name used as the catalogue blob prefix (default ``prod``).

    Mirrors build_instrument_catalogue.py ``get_config("DEPLOYMENT_ENV", "prod")``
    — this is the path PREFIX (``prod/`` / ``staging/`` / ``dev/``), NOT the
    env-SHORT (``-prd-``) the bucket NAME carries. Resolved via the UTL
    ``get_environment()`` config-bootstrap function (same DEPLOYMENT_ENV →
    ENVIRONMENT → "prod" probe order the writer uses).
    """
    return get_environment().strip().lower() or "prod"


def _catalogue_targets() -> list[meta_watchers.FreshnessTarget]:
    """Per-AG instrument-catalogue freshness targets (24h budget).

    The catalogue regen (build_instrument_catalogue.py) writes the per-AG
    artifact to ``{env}/catalog.parquet`` in the env-SHORT instruments-store
    bucket. Both the bucket (env-SHORT ``-prd-`` via ``resolve_bucket_name``) AND
    the blob prefix (LONG ``DEPLOYMENT_ENV``, default ``prod``) must match the
    writer or the probe reads age=None → a false DP-CATALOG-001 (KEY #3). A
    genuinely missing/stale blob still fires (with the probed path in the alert).
    """
    env_long = _deployment_env_long()
    blob_path = f"{env_long}/{_CATALOGUE_FILENAME}"
    targets: list[meta_watchers.FreshnessTarget] = []
    for ag in ASSET_GROUPS:
        kind = _INSTRUMENTS_STORE_KIND_OVERRIDE.get(ag, "instruments-store")
        try:
            # prediction's flat key takes no asset_group arg (matches the writer).
            if ag in _INSTRUMENTS_STORE_KIND_OVERRIDE:
                bucket = resolve_bucket_name(cloud="gcp", kind=kind)
            else:
                bucket = resolve_bucket_name(cloud="gcp", kind=kind, asset_group=ag)
        except Exception:
            continue
        targets.append(
            meta_watchers.FreshnessTarget(
                bucket=bucket,
                blob_path=blob_path,
                max_age_min=meta_watchers.DEFAULT_CATALOGUE_MAX_AGE_MIN,
                label=ag,
            )
        )
    return targets


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
        if importlib.util.find_spec(name) is None:
            continue
        try:
            return importlib.import_module(name)
        except Exception as exc:
            logger.warning("vm_zombie_watchdog import via %s failed: %s", name, exc)
    return None


def _umbrella_for_vm(vm_name: str) -> str:
    """``vm_name -> deployment umbrella`` (LIVE/BATCH/PAPER/EXPERIMENT; "" when unresolved).

    Resolves via ``vm_zombie_watchdog.VM_PREFIX_TO_BUCKET`` through the classification
    SSOT ``umbrella_for_vm_name`` so DP_VM_* findings carry the umbrella the
    alerting-service router splits on (LIVE → #uts-live-alerts, BATCH →
    #data-pipeline-alerts). Returns "" on an unregistered prefix → the alert routes to
    the batch default (never a sweep crash).
    """
    watchdog = _zombie_watchdog()
    if watchdog is None:
        return ""
    registry = cast("dict[str, VmPrefixSpec | None]", watchdog.VM_PREFIX_TO_BUCKET)
    try:
        return umbrella_for_vm_name(vm_name, registry).value
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


# Cloud Scheduler state strings (mirror the google.cloud.scheduler_v1 Job.State enum).
_SCHEDULER_PAUSED = "PAUSED"
_SCHEDULER_ENABLED = "ENABLED"
# Long → 3-char env-short (mirrors UTL bucket_naming._DEPLOYMENT_ENV_SHORT_FORM, the
# same map ``resolve_bucket_name`` uses for the ``-prd-`` segment). Kept inline (a
# tiny constant) rather than importing the UTL private to avoid an in-function /
# deep private import; the public ``get_environment()`` (imported at top) gives the
# long name. Default → prd (the fleet default).
_ENV_SHORT_FORM: dict[str, str] = {
    "dev": "dev",
    "development": "dev",
    "staging": "stg",
    "stg": "stg",
    "prod": "prd",
    "prd": "prd",
    "production": "prd",
}


def _scheduler_env_prefix() -> str:
    """The TF ``env_prefix`` segment in scheduler/job names: ``uts-{env-short}``.

    The consolidator scheduler jobs are ``{env_prefix}-manifest-consolidator-{key}-cron``
    (manifest_consolidator_scheduler.tf). ``env_prefix`` = ``uts-{deployment_env_short}``
    in the fleet TF — the env-short derived from ``get_environment()`` the same way
    ``resolve_bucket_name`` derives the bucket's ``-prd-`` segment.
    """
    short = _ENV_SHORT_FORM.get(_deployment_env_long(), "prd")
    return f"uts-{short}"


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
            job = client.get_job(name=qualified)
            # Job.state is an enum; .name gives "ENABLED"/"PAUSED"/"DISABLED"/...
            return str(getattr(job.state, "name", "") or "") or None
        except Exception as exc:
            # NotFound / permission / transient → UNKNOWN; the watcher does not
            # suppress on None (a genuinely-missing scheduler still alerts).
            logger.info("scheduler-state lookup for %s → unknown: %s", job_name, exc)
            return None

    return _read


def _make_execution_history_reader() -> meta_watchers.ExecutionHistoryReader:
    """Return ``job_stem -> minutes since the job's last SUCCEEDED execution | None``.

    KEY #4 (operator 2026-06-23): the authoritative "did the cron fire" signal is the
    Cloud Run **Job**'s real execution history, NOT the lagging GCS sentinel. The
    ``dp-exit-code-monitor`` false-fired DP_CRON_DID_NOT_FIRE on a stale sentinel
    while its executions Completed every 5 min. This reader queries the Run v2
    executions API for the most recent SUCCEEDED execution and returns its age in
    minutes; ``check_cron_fired`` suppresses the stale-sentinel alert when that age
    is within budget.

    The ``google.cloud.run_v2`` client is deferred-imported here (credential-bound),
    mirroring the scheduler reader — the watcher modules stay import-safe +
    credential-free. Any error / no-success returns ``None`` (UNKNOWN → the watcher
    does NOT suppress; a genuinely-dead job still alerts). The job-name STEM is
    resolved to ``{env_prefix}-{stem}`` then to the fully-qualified
    ``projects/{p}/locations/{loc}/jobs/{name}`` path.
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


def _consolidator_scheduler_job(ag: str) -> str:
    """The Cloud Scheduler job name backing the per-AG market-data consolidator.

    Matches manifest_consolidator_scheduler.tf:
    ``{env_prefix}-manifest-consolidator-market-data-{ag}-cron``. Used so a
    PAUSED consolidator scheduler suppresses its stale-_index DP_CRON_DID_NOT_FIRE
    (KEY #2) while an ENABLED-but-stale one still alerts.
    """
    return f"{_scheduler_env_prefix()}-manifest-consolidator-market-data-{ag}-cron"


def _consolidator_cloud_run_job(ag: str) -> str:
    """The Cloud Run **Job** backing the per-AG market-data consolidator.

    The ``-cron`` scheduler triggers this job. KEY #4 cross-check: a transiently
    stale ``_index`` read is suppressed when this job shows a recent SUCCEEDED
    execution (the consolidator fires every minute, so a stale-then-fresh artifact
    must not page when the job is demonstrably healthy). Matches
    ``manifest_consolidator_scheduler.tf`` (the job is the scheduler name minus
    ``-cron``). A genuinely-down consolidator (no recent success) still pages.
    """
    return f"{_scheduler_env_prefix()}-manifest-consolidator-market-data-{ag}"


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
            running = [vm for vm in _list_running_vms() if _is_data_vm(vm[0])]
            ec_findings: list[PipelineFinding] = []
            results = exit_code_fleet_monitor.sweep(
                storage_client=storage_client,
                log_bucket=log_bucket,
                running_vms=running,
                captured_reader=captured_reader,
                asset_group_for_vm=_asset_group_for_vm,
                launcher_for_vm=_launcher_for_vm,
                umbrella_for_vm=_umbrella_for_vm,
                finding_sink=ec_findings,
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
            # "clean" + "expected_no_capture" are both benign (the latter = rows
            # written but consolidated lags / honest-absence / shard already
            # complete — the 2026-06-23 false-positive killer). "rate_limited" is a
            # real-transient finding, so it counts as non_clean (it alerts WARN).
            non_clean = [r for r in results if r.verdict.value not in ("clean", "expected_no_capture")]
            logger.info(
                "exit-code sweep: %d terminated, %d non-clean (%s)",
                len(results),
                len(non_clean),
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
                asset_group_for_vm=_asset_group_for_vm,
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
            meta_watchers.check_catalogue_freshness(
                storage_client=storage_client,
                targets=_catalogue_targets(),
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
                miss_tracker=miss_tracker,
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
                    bucket = resolve_bucket_name(cloud="gcp", kind="market-data", asset_group=ag)
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
            )
            # Persist the consecutive-miss counters so the NEXT sweep continues the
            # count (a stale condition that has now been a miss for N sweeps pages;
            # a fresh/suppressed probe reset its key above). Before reconcile_resolved
            # so a partial-sweep failure still records the misses.
            miss_tracker.persist(dry_run=dry_run)
            # RESOLVED bookend (alert-lifecycle hardening): emit ✅ for any meta-watcher
            # alert that cleared since the last sweep, so #data-pipeline-alerts reflects
            # closure instead of a permanent RED on a transient stale-then-fresh artifact.
            meta_watchers.reconcile_resolved(storage_client=storage_client, log_bucket=log_bucket, dry_run=dry_run)
            logger.info("meta sweep complete")
            if not dry_run:
                _gcs.write_monitor_last_run(storage_client, log_bucket, "meta", ok=True)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
