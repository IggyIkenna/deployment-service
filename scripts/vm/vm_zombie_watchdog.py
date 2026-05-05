#!/usr/bin/env python3
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
time so the watchdog VM bootstraps from the repo copy. To extend coverage,
add a prefix to VM_PREFIX_TO_BUCKET below, then relaunch the watchdog VM.

Prefix table is the source of truth for what counts as a "backfill-class VM."
Every new VM-launching script must use a prefix that appears here, OR add
its prefix here in the same change. Naming convention: see
unified-trading-pm/cursor-configs/CLAUDE.md § "VM Naming Convention".

Heartbeat blob path:
  gs://deployment-scripts-{project_id}/vm-heartbeat/{vm_name}.txt

The blob format is `<unix_epoch>\n<python_pid>\n<status>` and is rewritten
every 60s by a side-loop in setup-data-pipeline-vm.sh. VMs whose launchers
bypass setup-data-pipeline-vm.sh (e.g. the perp-funding one-off) will not
have a heartbeat — they fall through to the SHARD_STALE check.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime

from google.cloud import compute_v1, storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
HEARTBEAT_BUCKET = f"deployment-scripts-{PROJECT_ID}"

# VM prefix → manifest-shard bucket. None = no shard check (heartbeat-only).
#
# Grouped by asset_group / pipeline so it's obvious where a new launcher slots
# in. To extend coverage, add a row in the matching block then relaunch the
# watchdog VM (`gcloud compute instances delete vm-zombie-watchdog-* --zone=asia-northeast1-c --quiet`
# followed by `bash deployment-service/scripts/vm/launch-vm-zombie-watchdog.sh`).
#
# Bucket selection rule: set the asset_group's market-data bucket if the VM
# uses ManifestWriter to write `_index/per_vm/{vm_name}.parquet`; otherwise
# `None` so the watchdog falls back to the heartbeat sidecar (which the
# `setup-data-pipeline-vm.sh` bootstrap writes every 60s).
VM_PREFIX_TO_BUCKET: dict[str, str | None] = {
    # ------------------------------------------------------------------
    # CeFi market-data backfill / forward-poll (per-vm shard writers)
    # ------------------------------------------------------------------
    "cefi-mr-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-fwd-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-binance-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-bybit-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-deribit-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-coinbase-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-okx-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-upbit-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-hyperliquid-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-bitfinex-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-bitget-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-kraken-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-aster-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-cme-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-extended-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-lighter-": f"market-data-tick-cefi-{PROJECT_ID}",
    "cefi-pacifica-": f"market-data-tick-cefi-{PROJECT_ID}",
    "aster-fwd-": f"market-data-tick-cefi-{PROJECT_ID}",  # launch-aster-forward-poll.sh
    # ------------------------------------------------------------------
    # CeFi instrument discovery + one-offs (heartbeat-only)
    # ------------------------------------------------------------------
    "cefi-instr-": None,  # cefi-instr-{venue}-{ts} from instruments-service launchers
    "cefi-rogue-": None,  # cefi-rogue-rekey one-off cleanup
    # ------------------------------------------------------------------
    # TradFi market-data backfill / forward-poll / incremental
    # ------------------------------------------------------------------
    "tradfi-bf-": f"market-data-tick-tradfi-{PROJECT_ID}",
    "tradfi-fwd-": f"market-data-tick-tradfi-{PROJECT_ID}",
    "tradfi-recent-": f"market-data-tick-tradfi-{PROJECT_ID}",
    # ------------------------------------------------------------------
    # TradFi instrument discovery + audits (heartbeat-only)
    # ------------------------------------------------------------------
    "tradfi-instr-": None,  # tradfi-instr-{venue}-{year} from instruments-service
    "tradfi-phantom-audit": None,
    # ------------------------------------------------------------------
    # MDPS sharded backfill (per asset_group)
    # ------------------------------------------------------------------
    "mdps-cefi-": f"market-data-tick-cefi-{PROJECT_ID}",
    "mdps-tradfi-": f"market-data-tick-tradfi-{PROJECT_ID}",
    "mdps-prediction-": f"market-data-tick-prediction-{PROJECT_ID}",
    # ------------------------------------------------------------------
    # MTDS asset-group-scoped backfills (DeFi onchain feeds + prediction)
    # ------------------------------------------------------------------
    "mtds-prediction-": f"market-data-tick-prediction-{PROJECT_ID}",
    "mtds-perp-funding-": f"market-data-tick-defi-{PROJECT_ID}",
    "mtds-gas-fees-": f"market-data-tick-defi-{PROJECT_ID}",
    "mtds-lst-rates-": f"market-data-tick-defi-{PROJECT_ID}",
    "mtds-vault-": f"market-data-tick-defi-{PROJECT_ID}",
    # ------------------------------------------------------------------
    # Options-chain backfills (per-venue bucket)
    # ------------------------------------------------------------------
    "opt-deribit-": f"market-data-tick-cefi-{PROJECT_ID}",
    "opt-cboe-": f"market-data-tick-tradfi-{PROJECT_ID}",
    "opt-cme-": f"market-data-tick-tradfi-{PROJECT_ID}",
    # ------------------------------------------------------------------
    # CME event-contract backfill (TradFi)
    # ------------------------------------------------------------------
    "cme-events-": f"market-data-tick-tradfi-{PROJECT_ID}",
    # ------------------------------------------------------------------
    # Sports reference-data backfill (per-source launcher prefixes)
    # ------------------------------------------------------------------
    "fs-backfill-": f"instruments-store-sports-{PROJECT_ID}",
    "af-backfill-": f"instruments-store-sports-{PROJECT_ID}",
    "tm-backfill-": f"instruments-store-sports-{PROJECT_ID}",
    "sfi-backfill-": f"instruments-store-sports-{PROJECT_ID}",
    "us-backfill-": f"instruments-store-sports-{PROJECT_ID}",
    "weather-backfill-": f"instruments-store-sports-{PROJECT_ID}",  # open_meteo (launcher emits weather-* not openmeteo-*)
    # ------------------------------------------------------------------
    # Features pipeline VMs (heartbeat-only — bucket varies by features-{group}-{asset_group}-)
    # ------------------------------------------------------------------
    "features-": None,  # features-{calendar,commodity,cross-instrument,delta-one,multi-timeframe,onchain,volatility}-{asset_group}-
    # ------------------------------------------------------------------
    # Long-lived daemons + audits + one-offs (heartbeat-only)
    # ------------------------------------------------------------------
    "manifest-consolidator-": None,  # long-lived consolidator daemon
    "tier3-audit-": None,
    "reconcile-phantom-": None,  # cefi/defi/sports phantom audits
    "instr-": None,  # tier3-cefi instr-{venue}-{ts} + e2e-testing instr-backfill-defi-targeted
    "instruments-smoke-": None,
    "combo-migration-": None,
}


@dataclass
class WatchdogVerdict:
    vm_name: str
    zone: str
    age_minutes: float
    heartbeat_age_min: float | None  # None if missing
    shard_age_min: float | None  # None if no bucket / no shard
    verdict: str  # 'alive' | 'zombie_no_heartbeat' | 'zombie_stale_heartbeat' | 'zombie_stale_shard' | 'too_young'

    def is_zombie(self) -> bool:
        return self.verdict.startswith("zombie")


def _list_backfill_vms(compute_client: compute_v1.InstancesClient) -> list[tuple[str, str]]:
    """List all RUNNING VMs that match a backfill prefix. Returns [(name, zone), ...]."""
    request = compute_v1.AggregatedListInstancesRequest(project=PROJECT_ID)
    out: list[tuple[str, str]] = []
    for zone, scoped in compute_client.aggregated_list(request=request):
        if not scoped.instances:
            continue
        for inst in scoped.instances:
            if inst.status != "RUNNING":
                continue
            for prefix in VM_PREFIX_TO_BUCKET:
                if inst.name.startswith(prefix):
                    z = zone.replace("zones/", "")
                    out.append((inst.name, z))
                    break
    return out


def _blob_age_minutes(bucket: storage.Bucket, blob_name: str) -> float | None:
    """Return age in minutes of a blob, or None if missing."""
    blob = bucket.blob(blob_name)
    try:
        if not blob.exists():
            return None
        blob.reload()
        if blob.updated is None:
            return None
        return (datetime.now(UTC) - blob.updated).total_seconds() / 60.0
    except Exception:  # noqa: BLE001
        return None


def _vm_age_minutes(compute_client: compute_v1.InstancesClient, vm_name: str, zone: str) -> float:
    inst = compute_client.get(project=PROJECT_ID, zone=zone, instance=vm_name)
    created = datetime.fromisoformat(inst.creation_timestamp)
    return (datetime.now(UTC) - created.astimezone(UTC)).total_seconds() / 60.0


def _evaluate_vm(
    compute_client: compute_v1.InstancesClient,
    storage_client: storage.Client,
    vm_name: str,
    zone: str,
    min_age: float,
    heartbeat_stale: float,
    shard_stale: float,
) -> WatchdogVerdict:
    age = _vm_age_minutes(compute_client, vm_name, zone)
    if age < min_age:
        return WatchdogVerdict(vm_name, zone, age, None, None, "too_young")

    hb_bucket = storage_client.bucket(HEARTBEAT_BUCKET)
    hb_age = _blob_age_minutes(hb_bucket, f"vm-heartbeat/{vm_name}.txt")

    shard_age: float | None = None
    for prefix, data_bucket in VM_PREFIX_TO_BUCKET.items():
        if vm_name.startswith(prefix) and data_bucket:
            shard_age = _blob_age_minutes(
                storage_client.bucket(data_bucket),
                f"_index/per_vm/{vm_name}.parquet",
            )
            break

    # Verdict logic:
    #   Heartbeat is the primary signal — if missing or stale → zombie.
    #   If heartbeat is unimplemented (None for both heartbeat AND shard older
    #   than 2× shard_stale, fall back to shard-only).
    if hb_age is None and shard_age is None and age > 30:
        return WatchdogVerdict(vm_name, zone, age, None, None, "zombie_no_heartbeat")
    if hb_age is not None and hb_age > heartbeat_stale:
        return WatchdogVerdict(vm_name, zone, age, hb_age, shard_age, "zombie_stale_heartbeat")
    if hb_age is None and shard_age is not None and shard_age > shard_stale:
        return WatchdogVerdict(vm_name, zone, age, None, shard_age, "zombie_stale_shard")
    return WatchdogVerdict(vm_name, zone, age, hb_age, shard_age, "alive")


def _kill_vm(compute_client: compute_v1.InstancesClient, vm_name: str, zone: str) -> bool:
    try:
        op = compute_client.delete(project=PROJECT_ID, zone=zone, instance=vm_name)
        op.result(timeout=120)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("kill failed for %s in %s: %s", vm_name, zone, exc)
        return False


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Report zombies but do not delete.")
    parser.add_argument(
        "--min-age", type=float, default=15.0, help="Minimum VM age (min) before considered."
    )
    parser.add_argument(
        "--heartbeat-stale", type=float, default=15.0, help="Heartbeat staleness threshold (min)."
    )
    parser.add_argument(
        "--shard-stale", type=float, default=120.0, help="Manifest shard staleness fallback (min)."
    )
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args(argv)

    compute_client = compute_v1.InstancesClient()
    storage_client = storage.Client(project=PROJECT_ID)

    t0 = time.monotonic()
    vms = _list_backfill_vms(compute_client)
    logger.info("found %d backfill VMs (RUNNING) in %.1fs", len(vms), time.monotonic() - t0)

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
            ): (n, z)
            for n, z in vms
        }
        for fut in as_completed(futures):
            verdicts.append(fut.result())

    zombies = [v for v in verdicts if v.is_zombie()]
    alive = [v for v in verdicts if v.verdict == "alive"]
    young = [v for v in verdicts if v.verdict == "too_young"]

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

    if args.dry_run:
        logger.info("DRY RUN — no VMs killed")
        return 0

    killed = 0
    for v in zombies:
        if _kill_vm(compute_client, v.vm_name, v.zone):
            killed += 1
            logger.warning("KILLED %s (%s) reason=%s", v.vm_name, v.zone, v.verdict)

    logger.info("watchdog complete: killed %d/%d zombies", killed, len(zombies))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
