"""CLI entry point for the post-mortem heartbeat-sidecar reliability check (DP-VM-008).

    python -m deployment_service.data_pipeline_monitors.heartbeat_sidecar_reliability_cli \\
        --vm-name af-backfill-20260717-151237 --kill-time 2026-07-18T09:18:00+00:00

    python -m deployment_service.data_pipeline_monitors.heartbeat_sidecar_reliability_cli \\
        --killed-vms-json /path/to/killed_vms.json

A SEPARATE entry point from ``cli.py`` on purpose — mirrors ``check_vm_cli.py``:
this is an on-demand, after-the-fact audit ("was this kill justified, or does the
sidecar have a reliability gap?"), never a scheduled sweep. Self-contained: wires
its own storage client + the zombie watchdog's ``PREFIX_IDLE_THRESHOLDS`` (the
SAME per-prefix threshold the watchdog itself applied) rather than re-deriving
it, deferred-imported the same way ``cli.py._zombie_watchdog`` is (the module
pulls in ``google.cloud.compute_v1`` at load, so it stays out of this CLI's
module-load path).

``--killed-vms-json`` accepts a JSON array of
``{"vm_name": str, "kill_time": <ISO-8601>, "stall_threshold_min": float (optional)}``
objects — the natural shape for the victim list a watchdog kill-storm incident
doc already enumerates. Exit code 1 iff any VM classifies
RELIABILITY_GAP_SUSPECTED (worth a closer look), 0 otherwise — mirrors
``check_vm_cli``'s branchable-exit-code convention.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import logging
import sys
from collections.abc import Callable
from pathlib import Path
from typing import cast

from unified_trading_library import UnifiedCloudConfig, get_storage_client

from deployment_service.data_pipeline_monitors import heartbeat_sidecar_reliability as reliability
from deployment_service.data_pipeline_monitors.heartbeat_stall_watcher import DEFAULT_STALL_MINUTES

logger = logging.getLogger(__name__)


def _project_id() -> str:
    return getattr(UnifiedCloudConfig(), "gcp_project_id", "") or ""


def _log_bucket() -> str:
    proj = _project_id()
    return f"deployment-scripts-{proj}" if proj else ""


def _default_threshold_resolver() -> Callable[[str], float]:
    """``vm_name -> heartbeat-stale threshold (min)`` via the watchdog's OWN registry.

    Deferred-imports ``vm_zombie_watchdog`` (mirrors ``cli.py._zombie_watchdog`` —
    the module loads ``google.cloud.compute_v1`` at import time) so a genuinely
    stale-vs-not verdict is judged against the EXACT ``PREFIX_IDLE_THRESHOLDS``
    entry the watchdog applied to this VM name, not a generic default. Falls back
    to ``DEFAULT_STALL_MINUTES`` when the watchdog module is unavailable in this
    environment (never crashes the CLI — the caller can still override via
    ``--stall-minutes``).
    """
    watchdog = None
    for name in ("scripts.vm.vm_zombie_watchdog", "vm_zombie_watchdog"):
        try:
            spec = importlib.util.find_spec(name)
        except ModuleNotFoundError:
            # find_spec() imports the PARENT package to resolve a dotted name —
            # when `scripts` is importable but `scripts.vm` isn't, this raises
            # instead of returning None. Mirrors cli.py._zombie_watchdog.
            continue
        if spec is None:
            continue
        try:
            watchdog = importlib.import_module(name)
            break
        except Exception as exc:
            logger.info("vm_zombie_watchdog import via %s failed: %s", name, exc)
    if watchdog is None:
        logger.info("vm_zombie_watchdog unavailable — falling back to DEFAULT_STALL_MINUTES for every VM")
        return lambda _vm_name: DEFAULT_STALL_MINUTES

    def _resolve(vm_name: str) -> float:
        hb_stale, _shard_stale = cast(
            "tuple[float, float]",
            watchdog._resolve_idle_thresholds(vm_name, DEFAULT_STALL_MINUTES, 120.0),  # pyright: ignore[reportAny] — dynamic VM-side module attr
        )
        return hb_stale

    return _resolve


def _report(results: list[reliability.HeartbeatReliabilityResult]) -> int:
    gaps = 0
    for r in results:
        age = f"{r.heartbeat_age_at_kill_min:.1f}min" if r.heartbeat_age_at_kill_min is not None else "NO_BLOB"
        logger.info(
            "heartbeat-reliability %s: verdict=%s heartbeat_age_at_kill=%s threshold=%.1fmin kill_time=%s",
            r.vm_name,
            r.verdict.value,
            age,
            r.stall_threshold_min,
            r.kill_time,
        )
        if r.verdict is reliability.HeartbeatReliabilityVerdict.RELIABILITY_GAP_SUSPECTED:
            gaps += 1
            logger.warning(
                "heartbeat-reliability %s: RELIABILITY_GAP_SUSPECTED — blob written %s before kill, "
                "under the %.1fmin threshold that justified the kill (last-second race OR sidecar gap)",
                r.vm_name,
                age,
                r.stall_threshold_min,
            )
    logger.info(
        "heartbeat-reliability summary: %d checked, %d genuinely_stale, %d reliability_gap_suspected, %d unknown_no_blob",
        len(results),
        sum(1 for r in results if r.verdict is reliability.HeartbeatReliabilityVerdict.GENUINELY_STALE),
        gaps,
        sum(1 for r in results if r.verdict is reliability.HeartbeatReliabilityVerdict.UNKNOWN_NO_BLOB),
    )
    return 1 if gaps else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Post-mortem heartbeat-sidecar reliability check (never alerts/kills)")
    parser.add_argument("--vm-name", default=None, help="Single killed VM name (paired with --kill-time)")
    parser.add_argument("--kill-time", default=None, help="ISO-8601 kill/delete timestamp for --vm-name")
    parser.add_argument(
        "--killed-vms-json",
        default=None,
        help='Path to a JSON array of {"vm_name","kill_time","stall_threshold_min"(optional)} objects (batch mode)',
    )
    parser.add_argument(
        "--stall-minutes",
        type=float,
        default=None,
        help="Override the per-VM threshold for --vm-name/--kill-time mode (default: the watchdog's own per-prefix threshold)",
    )
    args = parser.parse_args(argv)
    vm_name = cast("str | None", args.vm_name)
    kill_time = cast("str | None", args.kill_time)
    killed_vms_json = cast("str | None", args.killed_vms_json)
    stall_minutes_override = cast("float | None", args.stall_minutes)

    if not killed_vms_json and not (vm_name and kill_time):
        parser.error("provide either --killed-vms-json, or both --vm-name and --kill-time")

    storage_client = get_storage_client()
    log_bucket = _log_bucket()
    resolve_threshold = _default_threshold_resolver()

    if killed_vms_json:
        raw = cast("list[dict[str, object]]", json.loads(Path(killed_vms_json).read_text(encoding="utf-8")))
        results: list[reliability.HeartbeatReliabilityResult] = []
        for entry in raw:
            entry_vm_name = str(entry["vm_name"])
            entry_threshold = entry.get("stall_threshold_min")
            results.append(
                reliability.audit_killed_vm(
                    storage_client,
                    log_bucket,
                    entry_vm_name,
                    str(entry["kill_time"]),
                    stall_threshold_min=float(entry_threshold)
                    if entry_threshold is not None
                    else resolve_threshold(entry_vm_name),
                )
            )
    elif vm_name and kill_time:
        threshold = stall_minutes_override if stall_minutes_override is not None else resolve_threshold(vm_name)
        results = [
            reliability.audit_killed_vm(
                storage_client,
                log_bucket,
                vm_name,
                kill_time,
                stall_threshold_min=threshold,
            )
        ]
    else:
        # Unreachable: the parser.error() gate above exits before this branch when
        # neither --killed-vms-json nor the --vm-name/--kill-time pair is set.
        results = []

    return _report(results)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
