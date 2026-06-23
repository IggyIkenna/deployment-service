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
from datetime import UTC, datetime
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

from deployment_service.data_pipeline_monitors import (
    _gcs,
    exit_code_fleet_monitor,
    heartbeat_stall_watcher,
    meta_watchers,
)
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
    isolated: an API error returns an empty list (the sweep then no-ops, the
    safe default).
    """
    project_id = _project_id()
    try:
        client = get_compute_engine_client(provider="gcp", project_id=project_id)
        running: list[tuple[str, str]] = []
        for inst in client.aggregated_list_instances(project_id, ""):
            status = str(inst.get("status", ""))
            name = str(inst.get("name", ""))
            zone = str(inst.get("zone", "") or "")
            if status == "RUNNING" and name:
                running.append((name, zone))
        return running
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


def _catalogue_targets() -> list[meta_watchers.FreshnessTarget]:
    """Per-AG instrument-catalogue freshness targets (24h budget).

    The catalogue regen (instrument_catalogue_scheduler.tf) writes the matrix to
    the strategy-store bucket; per AG we probe the per-AG catalogue artifact.
    Path is the canonical catalogue output prefix; a missing/stale blob fires
    DP-CATALOG-001.
    """
    targets: list[meta_watchers.FreshnessTarget] = []
    for ag in ASSET_GROUPS:
        try:
            bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group=ag)
        except Exception:
            continue
        targets.append(
            meta_watchers.FreshnessTarget(
                bucket=bucket,
                blob_path="_catalogue/instrument_catalogue.parquet",
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


def _umbrella_for_vm(vm_name: str) -> str:
    """``vm_name -> deployment umbrella`` (LIVE/BATCH/PAPER/EXPERIMENT; "" when unresolved).

    Resolves via ``vm_zombie_watchdog.VM_PREFIX_TO_BUCKET`` through the classification
    SSOT ``umbrella_for_vm_name`` so DP_VM_* findings carry the umbrella the
    alerting-service router splits on (LIVE → #uts-live-alerts, BATCH →
    #data-pipeline-alerts). Returns "" on an unregistered prefix → the alert routes to
    the batch default (never a sweep crash).
    """
    if importlib.util.find_spec("vm_zombie_watchdog") is None:
        return ""
    # vm_zombie_watchdog is a VM-side launcher script (imports google.cloud.compute_v1
    # at module load) — NOT import-safe at this package's module top; deferred here so
    # only the sweep path that needs the prefix registry pulls it in.
    watchdog = importlib.import_module("vm_zombie_watchdog")  # noqa: imports-inside-functions — VM-side script, cloud-SDK-laden
    registry = cast("dict[str, VmPrefixSpec | None]", watchdog.VM_PREFIX_TO_BUCKET)
    try:
        return umbrella_for_vm_name(vm_name, registry).value
    except UnclassifiedDeploymentError:
        return ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Data-pipeline fleet monitors")
    parser.add_argument("--mode", required=True, choices=["exit-code", "heartbeat", "meta"])
    parser.add_argument("--pm-repo-path", default=None, help="PM clone path for the file_issue tier")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--stall-minutes", type=float, default=heartbeat_stall_watcher.DEFAULT_STALL_MINUTES)
    args = parser.parse_args(argv)
    mode: str = str(cast("object", args.mode))
    _pm_raw = cast("object", args.pm_repo_path)
    pm_repo_path: str | None = str(_pm_raw) if _pm_raw else None
    dry_run: bool = bool(cast("object", args.dry_run))
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
            results = exit_code_fleet_monitor.sweep(
                storage_client=storage_client,
                log_bucket=log_bucket,
                running_vms=running,
                captured_reader=captured_reader,
                asset_group_for_vm=_asset_group_for_vm,
                launcher_for_vm=_launcher_for_vm,
                umbrella_for_vm=_umbrella_for_vm,
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
            non_clean = [r for r in results if r.verdict.value not in ("clean",)]
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

            results = heartbeat_stall_watcher.sweep(
                storage_client=storage_client,
                log_bucket=log_bucket,
                running_vms=running,
                vm_age_reader=_age_reader,
                captured_reader=captured_reader,
                asset_group_for_vm=_asset_group_for_vm,
                launcher_for_vm=_launcher_for_vm,
                umbrella_for_vm=_umbrella_for_vm,
                prior_captured=prior,
                stall_minutes=stall_minutes,
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
            stalled = [r for r in results if r.verdict.value in ("stall", "event_loop_starved")]
            logger.info("heartbeat sweep: %d running, %d stalled", len(results), len(stalled))
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
            meta_watchers.check_catalogue_freshness(
                storage_client=storage_client,
                targets=_catalogue_targets(),
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
            meta_watchers.check_zombie_watchdog_alive(
                storage_client=storage_client,
                log_bucket=log_bucket,
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
            # Cron freshness: the orphan-ping / consolidator / digest crons leave a
            # durable artifact. Probe the consolidator heartbeat (per-AG market-data
            # _index) + the data-pipeline daily digest output (when present).
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
                    )
                )
            meta_watchers.check_cron_fired(
                storage_client=storage_client,
                targets=cron_targets,
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
            # Cron-watches-cron (in-band): probe the fleet-monitor / meta-watcher
            # sweep sentinels themselves. A stopped exit-code/heartbeat/meta cron
            # leaves its vm-census/<mode>-last-run.json stale → DP_CRON_DID_NOT_FIRE
            # (DP-WATCHER-002, page). The meta sweep can't catch its OWN death this
            # way — Layer-2's out-of-band deadman owns that.
            meta_watchers.check_monitor_crons_fired(
                storage_client=storage_client,
                log_bucket=log_bucket,
                pm_repo_path=pm_repo_path,
                dry_run=dry_run,
            )
            logger.info("meta sweep complete")
            if not dry_run:
                _gcs.write_monitor_last_run(storage_client, log_bucket, "meta", ok=True)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
