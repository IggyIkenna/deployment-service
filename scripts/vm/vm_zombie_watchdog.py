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
+ heartbeating but no useful work happening" failures (network
partitions, hung adapters, etc.). New launchers don't NEED a dict
entry to be watched — only when the richer signal is desired.

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
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime

from google.cloud import compute_v1, storage
from requests.adapters import HTTPAdapter
from unified_api_contracts import VmPrefixSpec
from unified_api_contracts.canonical.crosscutting import LifecycleClass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
HEARTBEAT_BUCKET = f"deployment-scripts-{PROJECT_ID}"

# urllib3 pool size for the AuthorizedSession on each Google client. The
# default of 10 overflows under our 16-thread × 50-prefix workload —
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


# VM prefix → VmPrefixSpec: manifest-shard bucket + lifecycle_class.
# VmPrefixSpec(bucket=None) = no shard check (heartbeat-only).
#
# Grouped by asset_group / pipeline so it's obvious where a new launcher slots
# in. To extend coverage, add a row in the matching block then relaunch the
# watchdog VM (`gcloud compute instances delete vm-zombie-watchdog-* --zone=asia-northeast1-c --quiet`
# followed by `bash deployment-service/scripts/vm/launch-vm-zombie-watchdog.sh`).
#
# Bucket selection rule: set the asset_group's market-data bucket if the VM
# uses ManifestWriter to write `_index/per_vm/{vm_name}.parquet`; otherwise
# bucket=None so the watchdog falls back to the heartbeat sidecar (which the
# `setup-data-pipeline-vm.sh` bootstrap writes every 60s).
# lifecycle_class field tags the LifecycleClass of VMs with this prefix (per
# deployment-ui-architecture.md § VM Naming Convention).
VM_PREFIX_TO_BUCKET: dict[str, VmPrefixSpec | None] = {
    # ------------------------------------------------------------------
    # CeFi market-data backfill / forward-poll (per-vm shard writers)
    # ------------------------------------------------------------------
    "cefi-mr-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-fwd-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-binance-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-bybit-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-deribit-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-coinbase-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-okx-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-upbit-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-hyperliquid-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-bitfinex-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-bitget-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-kraken-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-aster-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-cme-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-extended-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-lighter-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "cefi-pacifica-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "aster-fwd-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),  # launch-aster-forward-poll.sh
    # ------------------------------------------------------------------
    # CeFi instrument discovery + one-offs (heartbeat-only)
    # ------------------------------------------------------------------
    "cefi-instr-": None,  # cefi-instr-{venue}-{ts} from instruments-service launchers
    "cefi-rogue-": None,  # cefi-rogue-rekey one-off cleanup
    # ------------------------------------------------------------------
    # Instruments-service per-AG parallel backfill (launch-instruments-
    # backfill-vm.sh emits 5 VMs: instr-backfill-cefi-{1,2,3} +
    # instr-backfill-{defi,tradfi,sports}). Migrated 2026-05-08 (Tab 11)
    # from `e2e-testing/scripts/common/launch_instruments_backfill_vms.sh`.
    # Bucket = `instruments-store-{ag}-{pid}` (where the launcher writes
    # _vm_staging + the per-AG instruments parquet).
    # ------------------------------------------------------------------
    "instr-backfill-cefi-": VmPrefixSpec(
        bucket=f"instruments-store-cefi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "instr-backfill-defi": VmPrefixSpec(
        bucket=f"instruments-store-defi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "instr-backfill-tradfi": VmPrefixSpec(
        bucket=f"instruments-store-tradfi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "instr-backfill-sports": VmPrefixSpec(
        bucket=f"instruments-store-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # features-sports-service parallel backfill (launch-features-sports-
    # parallel-backfill-vm.sh emits N VMs `fss-backfill-vm-{i}` for a
    # date range chunk-split). Bucket = features-sports-{pid}. Migrated
    # 2026-05-08 (Tab 11) from
    # `features-sports-service/scripts/launch_parallel_backfill.sh`.
    # ------------------------------------------------------------------
    "fss-backfill-vm-": VmPrefixSpec(
        bucket=f"features-sports-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    # ------------------------------------------------------------------
    # Sports instruments-reference v3 backfill (launch-sports-instruments-
    # reference-vm.sh emits 3 VMs `sports-ref-v3-{1,2,3}` for chunked
    # date-range coverage of api_football reference entities). Bucket =
    # instruments-store-sports-{pid}. Migrated 2026-05-08 (Tab 11) from
    # `e2e-testing/scripts/sports/launch_instruments_reference_v3.sh`.
    # ------------------------------------------------------------------
    "sports-ref-v3-": VmPrefixSpec(
        bucket=f"instruments-store-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # TradFi market-data backfill / forward-poll / incremental
    # ------------------------------------------------------------------
    "tradfi-bf-": VmPrefixSpec(
        bucket=f"market-data-tick-tradfi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "tradfi-fwd-": VmPrefixSpec(
        bucket=f"market-data-tick-tradfi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "tradfi-recent-": VmPrefixSpec(
        bucket=f"market-data-tick-tradfi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # TradFi instrument discovery + audits (heartbeat-only)
    # ------------------------------------------------------------------
    "tradfi-instr-": None,  # tradfi-instr-{venue}-{year} from instruments-service
    "tradfi-phantom-audit": None,
    # ------------------------------------------------------------------
    # MDPS sharded backfill (per asset_group)
    # ------------------------------------------------------------------
    "mdps-cefi-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "mdps-tradfi-": VmPrefixSpec(
        bucket=f"market-data-tick-tradfi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "mdps-defi-": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "mdps-prediction-": VmPrefixSpec(
        bucket=f"market-data-tick-prediction-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # MDPS per-AG backfill (launch-mdps-backfill-vm.sh emits
    # mdps-backfill-{ag}-{ts}, distinct from the sharded prefix above).
    # 2026-05-06 incident: mdps-backfill-cefi-... ran rc=0 then sat
    # RUNNING for 12h because (a) prefix was not in this dict so the
    # watchdog never inspected it, (b) the launcher omitted
    # VM_SHUTDOWN_ON_COMPLETION=true.
    # ------------------------------------------------------------------
    "mdps-backfill-cefi-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "mdps-backfill-tradfi-": VmPrefixSpec(
        bucket=f"market-data-tick-tradfi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "mdps-backfill-defi-": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "mdps-backfill-prediction-": VmPrefixSpec(
        bucket=f"market-data-tick-prediction-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "mdps-backfill-sports-": VmPrefixSpec(
        bucket=f"market-data-tick-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # MTDS asset-group-scoped backfills (DeFi onchain feeds + prediction)
    # ------------------------------------------------------------------
    "mtds-prediction-": VmPrefixSpec(
        bucket=f"market-data-tick-prediction-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "mtds-perp-funding-": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "mtds-gas-fees-": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "mtds-lst-rates-": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "mtds-vault-": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "mtds-lending-indices-": VmPrefixSpec(
        bucket=f"lending-indices-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    # Pyth Hermes archive backfill (Solana SOL/USD pre-2023-10 gap window).
    # Hermes archive starts ~2023-10-01 per UAC ORACLE_COVERAGE_START SSOT
    # (UAC@3adee82 2026-05-08). Pre-2023-10 SOL/USD oracle valuation needed
    # for carry_staked_basis Solana-leg backtest. Sources cascade: Pythnet
    # historical RPC (free, slow) → CoinGecko historical daily (free, daily
    # granularity). Operator-decision pending on Birdeye paid-tier add.
    "mtds-pyth-archive-": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    # Pyth Hermes LST oracle_prices backfill (2023-10-01 → today).
    # Covers JitoSOL/USD, mSOL/USD, bSOL/USD, INF/USD feeds for
    # carry_staked_basis Solana leg. Singleton-locked per launcher.
    # Launcher: launch-mtds-pyth-lst-backfill-vm.sh (MTDS@0636dd4 2026-05-14).
    # Awaiting operator [ack] in pings/slot_2.md before launch.
    "pyth-lst-backfill-": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    # ------------------------------------------------------------------
    # Strategy-service 2-yr config-grid backtest VMs (2026-05-10).
    # VM name pattern: `strategy-backtest-grid-{archetype-slug}-{ts}`.
    # Two archetypes lead the May-23 live-DeFi cutover:
    #   - carry_staked_basis        → strategy-backtest-grid-carry-staked-basis-...
    #   - ARBITRAGE_PRICE_DISPERSION → strategy-backtest-grid-arbitrage-price-...
    # Heartbeat-only — strategy-service does NOT write per-VM manifest
    # shards under _index/per_vm/. Output goes to
    # gs://strategy-store-{pid}/backtests/config_grid_2yr/{archetype}/{run_id}/
    # so watchdog falls back to the GCS heartbeat sidecar at
    # gs://deployment-scripts-{pid}/vm-heartbeat/{vm-name}.txt.
    # ------------------------------------------------------------------
    "strategy-backtest-grid-": None,
    # Strategy paper-trade VMs (launch-strategy-paper-vm.sh; plan:
    # promote_workflow_may23_cli_path_2026_05_10.md Phase 1). VM name pattern:
    # `strategy-paper-{archetype-slug}-{ts}`. Heartbeat-only — paper VMs write
    # to event-archive only (no per-VM manifest shards).
    "strategy-paper-": None,
    # Strategy live-trade VMs (launch-strategy-live-vm.sh; plan:
    # promote_workflow_may23_cli_path_2026_05_10.md Phase 1). VM name pattern:
    # `strategy-live-{archetype-slug}-{ts}`. Heartbeat-only — live VMs write
    # to event-archive only (no per-VM manifest shards).
    "strategy-live-": None,
    # Deployment dashboard VM (single instance, hardcoded name "deployment-dashboard-vm")
    # Migrated 2026-05-08 from intra-repo deployment-service/scripts/deploy-dashboard-gce-vm.sh
    # to scripts/vm/launch-dashboard-vm.sh per CLAUDE.md "VM launcher script SSOT".
    # Heartbeat-only (no shard bucket — UI service VM, not data-pipeline writer).
    "deployment-dashboard-vm": None,
    # alerting-service Phase 7 quietness baseline VM (2026-05-10) — runs the
    # alerting-service in live mode against staging-noise Telegram channel for
    # 48h continuous so the operator can measure per-AlertCode false-positive
    # rate before tuning ALERT_THRESHOLDS pre-cutover. Heartbeat-only — the
    # service emits to events stream + AlertStorageStore, not to a per-VM
    # manifest shard. Launcher: launch-alerting-quietness-baseline.sh; plan:
    # alerting_service_live_rules_2026_05_07.md Phase 7.
    "alerting-quietness-": None,
    # Synthetic-data pipeline benchmark VMs (launch-synthetic-benchmark-vm.sh; plan:
    # mock_data_pipeline_benchmarking_2026_05_10.md Phase 5). One VM per (archetype, machine-type);
    # name `synbench-{archetype-short}-{shape-short}-{ts}`. Heartbeat-only — the VM writes
    # stage_profile.parquet + synthetic_run_manifest.json to the benchmark-reports prefix, not a
    # per-vm manifest shard. Auto-shutdown via VM_SHUTDOWN_ON_COMPLETION at run completion.
    "synbench-": None,
    # Phase 2 lift-and-shift 2026-05-08 — launchers migrated from
    # e2e-testing/scripts/defi/ + e2e-testing/scripts/prediction/ per
    # vm_launcher_consolidation_audit_2026_05_08.md.
    # mtds-liquidations-backfill: hardcoded VM name (singleton) — DEX/CEX
    # liquidation events feed for risk + carry archetypes.
    "mtds-liquidations-backfill": f"market-data-tick-defi-{PROJECT_ID}",
    # prediction-features-: feature-engineering VMs for prediction asset_group.
    # VM_NAME pattern is `prediction-features-{N}` (numbered shards).
    "prediction-features-": f"market-data-tick-prediction-{PROJECT_ID}",
    # mtds-gas-fees-solana: distinct from generic mtds-gas-fees- (EVM chains)
    # — Solana-specific gas/priority-fee feed. Same target bucket as defi.
    "mtds-gas-fees-solana": f"market-data-tick-defi-{PROJECT_ID}",
    # Phase 3 orchestrator-emitted VM name patterns (2026-05-08).
    # Each orchestrator launcher emits N child VMs with these prefixes.
    # sports-full-sweep-{year}: full_api_football_sweep year-chunk fan-out (8 VMs).
    "sports-full-sweep-": f"market-data-tick-sports-{PROJECT_ID}",
    # sports-entity-{type}: full_sports_entity_sweep per-entity fan-out (17 VMs).
    "sports-entity-": f"market-data-tick-sports-{PROJECT_ID}",
    # prediction-pipeline-{N}: prediction multi-stage pipeline VMs (MDPS + features-cross-instrument + features-delta-one).
    "prediction-pipeline-": f"market-data-tick-prediction-{PROJECT_ID}",
    # Singleton DeFi backfills (no -{ts}) migrated 2026-05-08 (Tab 11)
    # from e2e-testing/scripts/defi/. Each launcher emits a single VM
    # with a fixed name; bucket = market-data-tick-defi-{pid}.
    "mtds-dex-pools-backfill": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "mtds-eigenlayer-rewards-backfill": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "mtds-solana-drift-backfill": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    # CeFi instrument_type partition migrations (one-off cleanup VMs).
    # Heartbeat-only — VM rewrites in-place under the cefi tick bucket
    # but doesn't write per-VM manifest shards. Migrated 2026-05-08
    # (Tab 11) from e2e-testing/scripts/common/launch_cefi_migration_vm.sh.
    "mtds-migrate-": None,
    # ------------------------------------------------------------------
    # MTDS per-AG generic backfill (launch-mtds-backfill-vm.sh emits
    # mtds-backfill-{ag}-{ts}, the canonical entry-point for the
    # Deploy-Missing UI button on the market-tick-data-service service).
    # Migrated 2026-05-08 (Tab 11) from
    # `e2e-testing/scripts/common/launch_mtds_category_backfill_vm.sh`.
    # ------------------------------------------------------------------
    "mtds-backfill-cefi-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "mtds-backfill-tradfi-": VmPrefixSpec(
        bucket=f"market-data-tick-tradfi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "mtds-backfill-defi-": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "mtds-backfill-prediction-": VmPrefixSpec(
        bucket=f"market-data-tick-prediction-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "mtds-backfill-sports-": VmPrefixSpec(
        bucket=f"market-data-tick-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # mtds-backfill-odds-{N} from launch-mtds-sports-odds-backfill-vm.sh
    # (sports-Odds-API specific). Migrated 2026-05-08 (Tab 11) from
    # e2e-testing/scripts/sports/launch_mtds_backfill_vm.sh.
    "mtds-backfill-odds-": VmPrefixSpec(
        bucket=f"market-data-tick-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # Phantom-row reconciliation (read-only audit, 2026-05-07).
    # Read-only on the asset_group's manifest bucket — heartbeat-only
    # signal (None) since the script doesn't write per-VM shards. The
    # watchdog still tracks STARTED/STOPPED + heartbeat staleness.
    # ------------------------------------------------------------------
    "defi-phantom-recon-": None,
    # ------------------------------------------------------------------
    # Combined manifest reconciliation audit (2026-05-13) — read-only
    # all-3-reconciler dry-run per asset_group: phantom rows (--dry-run) +
    # expected-absence null-reason scan + legacy-blank reclassification scan.
    # Singleton-locked per asset_group. Launcher:
    # launch-manifest-recon-all-vm.sh.
    # ------------------------------------------------------------------
    "manifest-recon-": None,
    # ------------------------------------------------------------------
    # GCS migration bundle Phase 0 calibration VM (2026-05-10) — read-only
    # all-asset-group reconciler dry-run that feeds §§(c)(d)(e) of the
    # pre-audit doc (drift-axis histogram + manifest shape + phantom
    # baseline) for the gcs_migration_bundle_pipeline_mode_2026_05_08
    # plan. Single VM iterates cefi/defi/tradfi/sports/prediction
    # sequentially. Heartbeat-only — same as defi-phantom-recon-,
    # script doesn't write per-VM shards. Launcher:
    # launch-gcs-migration-phase0-calibration.sh.
    # ------------------------------------------------------------------
    "gcs-migration-phase0-": None,
    # ------------------------------------------------------------------
    # Batch-live reconciliation cron (GAP-18, topology_qgroup_gap_closure,
    # 2026-05-15). Nightly T+1 run of batch-live-reconciliation-service.
    # Reads from live/ + t1-recon/ GCS paths; outputs JSON report to
    # gs://recon-store-{pid}/reports/{date}/. Heartbeat-only (None) since
    # the script writes a single JSON report, not per-VM manifest shards.
    # Singleton-locked to prevent concurrent nightly runs.
    # Launcher: launch-batch-live-recon-cron-vm.sh.
    # ------------------------------------------------------------------
    "batch-live-recon-": None,
    # ------------------------------------------------------------------
    # Expected-universe enumerator (Phase 3.D.4 writegate, 2026-05-07).
    # In --apply-write mode it writes per-VM manifest shards under
    # gs://market-data-tick-{asset_group}-{pid}/_index/per_vm/{vm_name}.parquet
    # which the consolidator daemon merges into the canonical manifest.
    # Per-VM shard write means the watchdog needs no per-asset-group
    # bucket signal here — heartbeat-only (None) is correct.
    # ------------------------------------------------------------------
    "expected-universe-enum-": None,
    # ------------------------------------------------------------------
    # Per-instrument v2 expected-universe enumerator (Gate G3
    # manifest_evolution_master_2026_05_08 / Phase 2.B, 2026-05-13).
    # Writes per-VM manifest shards only; no canonical data bucket to poll.
    # Heartbeat-only (None) is correct — same rationale as v1 above.
    "expected-universe-v2-": None,
    # ------------------------------------------------------------------
    # Blank-reason reconciler (writegate Phase 3.D.5 Wave 2.M, 2026-05-07).
    # Walks an asset_group manifest, reclassifies blank-reason
    # empty_confirmed rows per the asset-group-specific legitimacy rule.
    # Heartbeat-only — writes to per-VM manifest shards.
    # ------------------------------------------------------------------
    "blank-reason-recon-": None,
    # ------------------------------------------------------------------
    # Options-chain backfills (per-venue bucket)
    # ------------------------------------------------------------------
    "opt-deribit-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "opt-cboe-": VmPrefixSpec(
        bucket=f"market-data-tick-tradfi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "opt-cme-": VmPrefixSpec(
        bucket=f"market-data-tick-tradfi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # CME event-contract backfill (TradFi)
    # ------------------------------------------------------------------
    "cme-events-": VmPrefixSpec(
        bucket=f"market-data-tick-tradfi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # Sports reference-data backfill (per-source launcher prefixes)
    # ------------------------------------------------------------------
    "fs-backfill-": VmPrefixSpec(
        bucket=f"instruments-store-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "af-backfill-": VmPrefixSpec(
        bucket=f"instruments-store-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # api_football fixtures truth-set audit (launch-fixtures-truthset-audit-vm.sh,
    # 2026-05-06 — Phase 1 of sports_fixtures_truthset_recovery_2026_05_06.md).
    # Writes to gs://instruments-store-sports-{pid}/_audits/, so the watchdog
    # checks the same shard bucket — its progress signal is the truth-set
    # parquet checkpoint mtime under _audits/.
    "af-audit-": VmPrefixSpec(
        bucket=f"instruments-store-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # Phase 2 fixtures recovery from the truth-set
    # (launch-fixtures-recovery-vm.sh). Writes per-league sub-partition
    # fixtures parquets + per-VM manifest shards.
    "af-recover-": VmPrefixSpec(
        bucket=f"instruments-store-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "tm-backfill-": VmPrefixSpec(
        bucket=f"instruments-store-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "sfi-backfill-": VmPrefixSpec(
        bucket=f"instruments-store-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "us-backfill-": VmPrefixSpec(
        bucket=f"instruments-store-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "weather-backfill-": VmPrefixSpec(
        bucket=f"instruments-store-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),  # open_meteo (launcher emits weather-* not openmeteo-*)
    # Targeted (date, league_id) gap-fill — reads canonical manifest, fires
    # only at the missing-shard set. First use: PLAYER_STATS via
    # launch-fill-missing-player-stats-vm.sh (2026-05-06), replacing the
    # slow chronological af-backfill iteration.
    "fill-missing-player-stats-": VmPrefixSpec(
        bucket=f"instruments-store-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # Features pipeline VMs (heartbeat-only — bucket varies by
    # features-{family}-{asset_group}-).
    # Family ∈ {calendar, commodity, cross-instrument, delta-one,
    # multi-timeframe, onchain, sports, volatility} — 8 families served by
    # the consolidated `launch-features-vm.sh --feature-family <name>` per
    # Phase 8A of features_repo_consolidation_2026_05_08. Specialty
    # halftime-backfill VMs (sfi-progressive-, e2-standard-4) also match
    # the catch-all features-* prefix.
    # ------------------------------------------------------------------
    "features-": None,  # features-{family-dashed}-{asset_group}-{ts} per launch-features-vm.sh
    # ------------------------------------------------------------------
    # Long-lived daemons + audits + one-offs (heartbeat-only)
    # ------------------------------------------------------------------
    "manifest-consolidator-": None,  # long-lived consolidator daemon
    "data-status-rollup-": None,  # */5 min Cloud Run Job — offline rollup of /api/data-status/manifest
    "tier3-audit-": None,
    "reconcile-phantom-": None,  # cefi/defi/sports phantom audits
    "cross-asset-rescan-": None,  # manifest_cross_asset_rescan_design_2026_05_08 Phase 3.A; manifest_schema_final_gate_2026_05_09 Phase 3.A — class-A auto-flips + class-C triage routing across all 5 asset_groups; singleton-locked launcher.
    "measure-honest-coverage-": None,  # cross_asset_group_catalogue_audit_2026_05_10 Phase 2B — daily cross-AG coverage % measurement; output → gs://{pid}-honest-coverage/{date}/coverage.json
    "tradfi-audit-aggregate-": None,  # tradfi phantom audit + ES_OPT legacy aggregation one-off
    "instr-": None,  # tier3-cefi instr-{venue}-{ts} + e2e-testing instr-backfill-defi-targeted
    "instruments-smoke-": None,
    "combo-migration-": None,
    # ------------------------------------------------------------------
    # canonical-migration VMs — launch-canonical-migration-vm.sh
    # naming: canonical-migration-{cefi|tradfi|defi|prediction|sports}-{ts}
    # writes per-VM manifest shards under each asset_group's tick bucket.
    # ------------------------------------------------------------------
    "canonical-migration-cefi-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "canonical-migration-tradfi-": VmPrefixSpec(
        bucket=f"market-data-tick-tradfi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "canonical-migration-defi-": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "canonical-migration-prediction-": VmPrefixSpec(
        bucket=f"market-data-tick-prediction-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "canonical-migration-sports-": VmPrefixSpec(
        bucket=f"market-data-tick-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # MDPS sports bucket-pass VMs — launch-mdps-sports-bucket-vm.sh
    # naming: mdps-sports-bucket-{ts}
    # writes per-(league_id, horizon) bucketed parquets to the same sports
    # tick bucket that the canonical-migration step writes raw data to.
    # ------------------------------------------------------------------
    "mdps-sports-bucket-": VmPrefixSpec(
        bucket=f"market-data-tick-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # Live-pipeline VMs (Phase 13 of live_pipeline_mtds_mdps_features_2026_05_08.md;
    # shipped 2026-05-11 by Ikenna slot 4). Five VM types: per-asset_group MTDS-
    # live producer + per-asset_group MDPS+features-live consumer + singleton
    # features-cross-cutting + singleton replay-cascade. Alerting-service uses
    # an existing launcher (no new prefix needed).
    #
    # VM names embed asset_group + RUN_TS (per CLAUDE.md "VM Naming Convention"):
    #   mtds-live-{ag}-{ts}                 — one per asset_group
    #   mdps-features-live-{ag}-{ts}        — one per asset_group
    #   features-xc-{ts}                    — singleton
    #   replay-{ag}-{ts}                    — singleton (one window at a time)
    #
    # All four write to per-VM manifest shards under
    # `_index/per_vm/{vm_name}.parquet` per workspace per-VM-shard-isolation
    # rule. Bucket NAMES use the (b+) env-aware shape resolved at runtime via
    # `unified_trading_library.cloud_interface.bucket_naming.resolve_bucket_name`;
    # the watchdog uses prod-tier flat naming below since flat-bucket data
    # still lives there until Phase 2 of bucket-name-ssot ships (window
    # 2026-05-15→05-19). When env-tiered buckets land, replace the flat names
    # below with `f"market-data-tick-{ag}-prod-{PROJECT_ID}"` etc.
    # ------------------------------------------------------------------
    "mtds-live-cefi-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.LONG_LIVED_LIVE
    ),
    "mtds-live-defi-": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.LONG_LIVED_LIVE
    ),
    "mtds-live-tradfi-": VmPrefixSpec(
        bucket=f"market-data-tick-tradfi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
    ),
    "mtds-live-sports-": VmPrefixSpec(
        bucket=f"market-data-tick-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
    ),
    "mtds-live-prediction-": VmPrefixSpec(
        bucket=f"market-data-tick-prediction-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
    ),
    "mdps-features-live-cefi-": VmPrefixSpec(
        bucket=f"market-data-tick-cefi-{PROJECT_ID}", lifecycle_class=LifecycleClass.LONG_LIVED_LIVE
    ),
    "mdps-features-live-defi-": VmPrefixSpec(
        bucket=f"market-data-tick-defi-{PROJECT_ID}", lifecycle_class=LifecycleClass.LONG_LIVED_LIVE
    ),
    "mdps-features-live-tradfi-": VmPrefixSpec(
        bucket=f"market-data-tick-tradfi-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
    ),
    "mdps-features-live-sports-": VmPrefixSpec(
        bucket=f"market-data-tick-sports-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
    ),
    "mdps-features-live-prediction-": VmPrefixSpec(
        bucket=f"market-data-tick-prediction-{PROJECT_ID}",
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
    ),
    # Singletons — cross-cutting features bucket-resolves at runtime; replay
    # writes to the same per-asset_group market-data buckets the live producer
    # uses. Heartbeat-only (None) for replay since its actual writes are routed
    # through whichever asset_group's bucket the --asset-group flag selects;
    # watchdog can't statically know which.
    "features-xc-": None,
    "replay-": None,
    # ------------------------------------------------------------------
    # DR (disaster-recovery) drill VMs — disaster_recovery_circuit_breakers_2026_05_10.md
    # Phase 6.A + 9.A.
    #   disaster-drill-cron-{ts}: nightly chaos-drill cron VM (run_chaos_drill.py).
    #   dr-drill-cutover-{ts}: per-archetype cutover evidence VM (run_dr_drill_cutover.py).
    # Both write drill reports/evidence to the events bucket — heartbeat-only (None).
    # ------------------------------------------------------------------
    "disaster-drill-cron-": None,
    "dr-drill-cutover-": None,
    # ------------------------------------------------------------------
    # Reserved live- prefixes (LONG_LIVED_LIVE, heartbeat-only allocation)
    # ------------------------------------------------------------------
    "live-strategy-": None,
    "live-execution-": None,
    "live-mtds-": None,
    "live-pbm-": None,
    "live-risk-": None,
    "live-alerting-": None,
    # ------------------------------------------------------------------
    # Reserved exp- prefixes (EPHEMERAL_EXPERIMENT, heartbeat-only allocation)
    # ------------------------------------------------------------------
    "exp-ml-": None,
    "exp-strategy-": None,
    "exp-execution-": None,
    # ------------------------------------------------------------------
    # DeFi execution validation VMs (Phase 2 + Phase 3C, 2026-05-13).
    # Heartbeat-only (None) — these VMs do NOT write per-VM manifest
    # shards; output is results.json to the defi-validation bucket.
    # Launchers: launch-aave-lending-rate-validation-vm.sh (singleton,
    # Alchemy shared key) + launch-amm-golden-fixture-validation-vm.sh
    # (per-shape singleton). After updating this dict, operator MUST
    # relaunch the watchdog VM.
    # ------------------------------------------------------------------
    "aave-lending-rate-val-": None,  # Phase 3C lending rate validation
    "amm-golden-": None,             # Phase 2 AMM golden-swap validation (per-shape)
    # ------------------------------------------------------------------
    # Wallet/treasury cutover dry-run VM (Phase 9.A of
    # wallet_treasury_client_flow_2026_05_10.md). One singleton VM named
    # `wallet-treasury-cutover-{ts}` runs a 24h demo-client lifecycle:
    # onboarding → treasury ping → allocation → paper-trade → settle →
    # fee accrual + HWM-ledger → statement → withdrawal → crystallization.
    # Heartbeat-only (None) — writes to event-archive + client-statements
    # but NOT to a per-VM manifest shard. Singleton-locked.
    # Launcher: deployment-service/scripts/vm/launch-wallet-treasury-cutover-vm.sh
    # Registered 2026-05-13 per CLAUDE.md "VM Naming Convention" HARD RULE.
    # After updating this dict, relaunch the watchdog VM.
    # ------------------------------------------------------------------
    "wallet-treasury-cutover-": None,
    # QG snapshot cron VM (B-018 Phase 4.A). Heartbeat-only; no manifest shard writes.
    # Launcher: deployment-service/scripts/vm/launch-qg-snapshot-vm.sh
    # Registered 2026-05-14 per CLAUDE.md "VM Naming Convention" HARD RULE.
    # After updating this dict, relaunch the watchdog VM.
    "qg-snapshot-": None,
}


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
    except Exception:
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
    finished_grace: float,
) -> WatchdogVerdict:
    age = _vm_age_minutes(compute_client, vm_name, zone)
    if age < min_age:
        return WatchdogVerdict(vm_name, zone, age, None, None, "too_young")

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
    """Issue a delete and treat best-effort.

    The delete is counted as a kill iff the API call itself returned (the
    Compute API has accepted the delete and the VM is going away). The
    follow-up `op.result()` poll is best-effort — if it raises (transient
    connection-pool issue, network blip, eventual-consistency lag), the
    delete is still in-flight, so the kill counts. Previously we returned
    False on poll-raise, which under-counted real kills (observed
    2026-05-05: watchdog reported `killed 0/4` while all 4 VMs deleted).
    """
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
    args = parser.parse_args(argv)

    compute_client = compute_v1.InstancesClient()
    storage_client = storage.Client(project=PROJECT_ID)
    _bump_pool_size(compute_client.transport._session)
    _bump_pool_size(storage_client._http)

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
