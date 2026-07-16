"""CLI entry point for the ad-hoc single-VM liveness check (DP-VM-006).

    python -m deployment_service.data_pipeline_monitors.check_vm_cli --vm-name <VM_NAME>

A SEPARATE entry point from ``cli.py`` on purpose: ``cli.py``'s ``--mode
exit-code/heartbeat/meta`` are the scheduled Cloud Run Job fleet sweeps; this
is an interactive, on-demand check for the manual "VM roster + run.log tail"
check-in convention used across long-running backfill-plan dispatches —
closing the zombie-VM blind spot where a RUNNING instance with a dead worker
process went undetected for ~4h44m
(drift_v2_sig_index_program_wide_helius_oom_2026_07_15).

Self-contained: wires the SAME public GCS/compute primitives (``_gcs``,
``resolve_bucket_name``, ``get_compute_engine_client``) that ``cli.py``'s own
factories use (never module-private cross-imports) — scoped down to what a
single on-demand check needs, not a scheduled-sweep's timeout/kill machinery.
The verdict logic itself (``heartbeat_stall_watcher.classify_vm_liveness``)
IS fully reused via ``check_vm.py``, never re-derived.
"""

from __future__ import annotations

import argparse
import sys
from typing import cast

from unified_trading_library import StorageClient, UnifiedCloudConfig, get_storage_client, resolve_bucket_name
from unified_trading_library.cloud_interface import get_compute_engine_client  # noqa: qg-deep-import

from deployment_service.data_pipeline_monitors import _gcs, check_vm
from deployment_service.data_pipeline_monitors.heartbeat_stall_watcher import DEFAULT_STALL_MINUTES
from deployment_service.data_pipeline_monitors.meta_targets import ASSET_GROUPS

_PER_VM_SHARD = "_index/per_vm/{vm}.parquet"


def _project_id() -> str:
    return getattr(UnifiedCloudConfig(), "gcp_project_id", "") or ""


def _log_bucket() -> str:
    proj = _project_id()
    return f"deployment-scripts-{proj}" if proj else ""


def _find_running_vm(vm_name: str) -> tuple[str, str] | None:
    """``(vm_name, zone)`` if RUNNING, else ``None``.

    No fleet-wide timeout guard (cli.py's ``_list_running_vms`` bounds the
    aggregated-list call because a Cloud Run Job has a hard sweep budget) —
    this is a single on-demand interactive call, not a scheduled sweep.
    """
    project_id = _project_id()
    client = get_compute_engine_client(provider="gcp", project_id=project_id)
    for inst in client.aggregated_list_instances(project_id, ""):
        if str(inst.get("name", "")) == vm_name and str(inst.get("status", "")) == "RUNNING":
            return vm_name, str(inst.get("zone", "") or "")
    return None


def _asset_group_for_vm(vm_name: str) -> str:
    lowered = vm_name.lower()
    for ag in ASSET_GROUPS:
        if ag in lowered:
            return ag
    return "unknown"


def _shard_bucket_for_vm(vm_name: str) -> str | None:
    ag = _asset_group_for_vm(vm_name)
    if ag == "unknown":
        return None
    try:
        return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group=ag)
    except Exception:
        return None


def _make_shard_mtime_reader(storage_client: StorageClient):
    def _read(vm_name: str) -> float | None:
        bucket = _shard_bucket_for_vm(vm_name)
        if not bucket:
            return None
        return _gcs.blob_age_minutes(storage_client, bucket, _PER_VM_SHARD.format(vm=vm_name))

    return _read


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ad-hoc single-VM liveness check (never alerts/kills)")
    parser.add_argument("--vm-name", required=True)
    parser.add_argument("--stall-minutes", type=float, default=DEFAULT_STALL_MINUTES)
    args = parser.parse_args(argv)
    vm_name = str(cast("object", args.vm_name))
    stall_minutes = float(cast("object", args.stall_minutes))

    storage_client = get_storage_client()
    log_bucket = _log_bucket()
    match = _find_running_vm(vm_name)
    return check_vm.run_check_vm_mode(
        vm_name,
        storage_client=storage_client,
        log_bucket=log_bucket,
        running_vms=[match] if match else [],
        # No persisted prior-tick baseline for a one-shot check — captured_flat
        # always resolves False regardless of this reader's return value (see
        # check_vm.py), so a trivial stub is correct, not a shortcut.
        captured_reader=lambda _vm: 0,
        asset_group_for_vm=_asset_group_for_vm,
        stall_minutes=stall_minutes,
        pm_repo_path=None,
        shard_mtime_reader=_make_shard_mtime_reader(storage_client),
        sidecar_age_reader=lambda vm: _gcs.heartbeat_blob_age_minutes(storage_client, log_bucket, vm),
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
