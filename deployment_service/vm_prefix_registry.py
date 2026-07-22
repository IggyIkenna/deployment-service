"""VM-name-prefix → :class:`VmPrefixSpec` registry (packaged SSOT).

Moved here from ``scripts/vm/vm_zombie_watchdog.py`` (2026-07-13). The registry
must live in the ``deployment_service`` package — not the unpackaged
``scripts/`` tree — because the data-pipeline fleet monitors run inside the
``deployment-api`` image, whose wheel install of ``deployment_service`` EXCLUDES
the top-level ``scripts/`` directory. While ``VM_PREFIX_TO_BUCKET`` lived in the
watchdog script it was structurally absent from that image, so the monitor CLI's
``_umbrella_for_vm`` raised ``ModuleNotFoundError: No module named 'scripts.vm'``
and every fleet-monitor sweep crashed (monitoring-deadman alert, 2026-07-12).

Consumers:

* ``deployment_service.data_pipeline_monitors.cli`` — umbrella routing (in-image).
* ``scripts/vm/vm_zombie_watchdog.py`` — re-imports ``VM_PREFIX_TO_BUCKET`` from
  here for back-compat; still deployed standalone to the watchdog VM (which
  pip-installs ``deployment_service``).
* Guard tests (``test_launcher_registry.py``, ``test_vm_zombie_watchdog.py``,
  ``test_validate_vm_prefix_mapping.py``) assert launcher/registry parity against
  this dict.

Bucket names resolve via :func:`resolve_bucket_name` (reads the UAC-packaged
``cloud-providers.yaml`` — no network) at import time, exactly as they did in the
watchdog script.
"""

from __future__ import annotations

from unified_api_contracts import DeploymentUmbrella, LifecycleClass, VmPrefixSpec
from unified_trading_library import UnifiedCloudConfig, resolve_bucket_name


def _project_id() -> str:
    """GCP project id from UnifiedCloudConfig — never hardcoded (mirrors cli._project_id)."""
    try:
        cfg = UnifiedCloudConfig()
    except (ValueError, RuntimeError, OSError):
        return ""
    return getattr(cfg, "gcp_project_id", "") or ""


# ``lending-indices`` and ``scenario-reports`` (below) are not yet in the
# cloud-providers.yaml SSOT, so they are composed from the config-resolved project
# id until they are. Resolved at import: if config were unavailable the _b() calls
# below would already raise, so this yields the real project whenever the module loads.
_PROJECT_ID: str = _project_id()


def _b(kind: str, asset_group: str | None = None) -> str:
    """Resolve a shard-check bucket name from the yaml SSOT at process start.

    Reads DEPLOYMENT_ENV from env (defaults to "prod" if unset) and returns
    the env-tiered bucket name matching the yaml canonical SSOT in
    deployment-service/configs/cloud-providers.yaml.  Called once at module
    load; all VmPrefixSpec.bucket values below use the result.
    """
    return resolve_bucket_name(cloud="gcp", kind=kind, asset_group=asset_group)


# Pre-computed shard-bucket names (yaml SSOT via resolve_bucket_name at process start).
# Replaces the former hardcoded _TICK_CEFI pattern.
# Phase 0c-watchdog: bucket_name_ssot_canonicalisation_2026_05_10.md
_TICK_CEFI: str = _b("market-data", "cefi")
_TICK_DEFI: str = _b("market-data", "defi")
_TICK_TRADFI: str = _b("market-data", "tradfi")
_TICK_SPORTS: str = _b("market-data", "sports")
_TICK_PRED: str = _b("market-data-tick-prediction")
_INSTR_CEFI: str = _b("instruments-store", "cefi")
_INSTR_DEFI: str = _b("instruments-store", "defi")
_INSTR_TRADFI: str = _b("instruments-store", "tradfi")
_INSTR_SPORTS: str = _b("instruments-store", "sports")
_INSTR_PRED: str = _b("instruments-store-prediction")
_FEAT_SPORTS: str = _b("features-sports")
# Tier-2 per-datapoint id+schema validation RESULTS bucket. FLAT kind (no
# asset_group axis, no env tier — mirrors defi-validation; audit results are
# environment-neutral), so all 5 datapoint-validation-{ag}- prefixes resolve to
# the same bucket. SSOT: codex/02-data/reconciliation-census-and-compute-tiers.md § 3.3.
_DATAPOINT_VALIDATION: str = _b("datapoint-validation")
# scenario-reports is NOT in cloud-providers.yaml SSOT yet; kept as a hardcoded
# string until it is. lending-indices USED to be a separate hardcoded flat-bucket
# string here too, but that bucket kind was retired 2026-07-14
# (bucket_estate_consolidation_to_sub100_2026_07_13.md item C) — the
# mtds-lending-indices-* launcher's manifest shard has always actually landed in
# the shared market-data-tick-defi-prd bucket (confirmed live via VM run.log:
# `_index/per_vm/{vm_name}.parquet` written there, not the flat bucket this
# constant used to point at), so it now just reuses `_TICK_DEFI` below.
_SCENARIO_REPORTS: str = f"scenario-reports-{_PROJECT_ID}"


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
    "cefi-mr-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-fwd-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-binance-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-bybit-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-deribit-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-coinbase-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-okx-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-upbit-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-hyperliquid-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-bitfinex-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-bitget-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-kraken-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-aster-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-cme-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-extended-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-ext-bfill-": VmPrefixSpec(
        bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),  # launch-mtds-extended-ohlcv-backfill.sh — EXTENDED-STARKNET 2024-07-26..2025-07-31
    "cefi-lighter-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-queue-": VmPrefixSpec(
        bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),  # launch-cefi-sharded-backfill.sh SINGLE_VM_QUEUE=1 mode — cefi-queue-{group}-{ts},
    # one combined multi-venue VM per (group,data_types) bucket instead of per-shard.
    # tardis_concurrent_ip_lockout_2026_07_12 course-correction 2026-07-13.
    "aster-fwd-": VmPrefixSpec(
        bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),  # launch-aster-forward-poll.sh
    # ------------------------------------------------------------------
    # DeFi forward-poll (launch-defi-forward-poll.sh). Implemented 2026-06-21
    # (was a stub). Runs collect-lst-rates --mode live with
    # MANIFEST_PER_VM_SHARDS=true → writes _index/per_vm/{vm_name}.parquet to
    # market-data-tick-defi-* bucket. Updated from None to VmPrefixSpec so the
    # watchdog checks the per-VM shard (not just the heartbeat).
    # ------------------------------------------------------------------
    "defi-fwd-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # ------------------------------------------------------------------
    # Prediction forward-poll (launch-prediction-forward-poll.sh).
    # VM_SHUTDOWN_ON_COMPLETION=true; no MANIFEST_PER_VM_SHARDS.
    # Registered 2026-05-15 (slot-2 B-011 blindspot audit).
    # ------------------------------------------------------------------
    "prediction-fwd-": None,  # launch-prediction-forward-poll.sh — Polymarket/Kalshi forward poll
    # ------------------------------------------------------------------
    # Prediction live WS producer (launch-prediction-live.sh).
    # MANIFEST_PER_VM_SHARDS=true; LONG_LIVED_LIVE — runs continuously
    # as a live websocket stream for the full IS-enumerated universe.
    # One VM per (venue x data_type) shard; singleton lock per shard.
    # Registered 2026-06-22.
    # ------------------------------------------------------------------
    "prediction-live-": VmPrefixSpec(bucket=_TICK_PRED, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    # ------------------------------------------------------------------
    # Cross-venue (Kalshi ↔ Polymarket) prediction arb DETECTOR
    # (launch-prediction-arb-detector.sh). LONG_LIVED_LIVE paper-mode loop:
    # reads both venues' live book_snapshot_5, flags PURE_ARB / QUOTABLE_ARB,
    # streams opportunities to the features-cross-instrument arb store. It does
    # NOT write per-VM tick-manifest shards (its output is the arb store, not the
    # manifest), so bucket=None = heartbeat-only liveness (no shard staleness
    # check). Classified LIVE via LONG_LIVED_LIVE. Singleton-locked per prefix.
    # Registered 2026-06-24.
    # ------------------------------------------------------------------
    "prediction-arb-detector-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
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
        bucket=_INSTR_CEFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "instr-backfill-defi": VmPrefixSpec(
        bucket=_INSTR_DEFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "instr-backfill-tradfi": VmPrefixSpec(
        bucket=_INSTR_TRADFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "instr-backfill-sports": VmPrefixSpec(
        bucket=_INSTR_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # CeFi by_date/catalog durability force-converge (2026-07-10, one-off, see
    # instrument_id_format_canonicalization_2026_07_08.md). launch-cefi-
    # durability-force-converge-vm.sh — bucket = instruments-store-cefi.
    "cefi-durability-force-converge-": VmPrefixSpec(
        bucket=_INSTR_CEFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "instr-backfill-pred": VmPrefixSpec(
        bucket=_INSTR_PRED,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # features-sports-service parallel backfill (launch-features-sports-
    # parallel-backfill-vm.sh emits N VMs `fss-backfill-vm-{i}` for a
    # date range chunk-split). Bucket = features-sports-{pid}. Migrated
    # 2026-05-08 (Tab 11) from
    # `features-sports-service/scripts/launch_parallel_backfill.sh`.
    # ------------------------------------------------------------------
    "fss-backfill-vm-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # ------------------------------------------------------------------
    # Sports instruments-reference v3 backfill (launch-sports-instruments-
    # reference-vm.sh emits 3 VMs `sports-ref-v3-{1,2,3}` for chunked
    # date-range coverage of api_football reference entities). Bucket =
    # instruments-store-sports-{pid}. Migrated 2026-05-08 (Tab 11) from
    # `e2e-testing/scripts/sports/launch_instruments_reference_v3.sh`.
    # ------------------------------------------------------------------
    "sports-ref-v3-": VmPrefixSpec(
        bucket=_INSTR_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # Sports instruments forward-polls (heartbeat-only — no MANIFEST_PER_VM_SHARDS).
    # Both write to instruments-store-sports-* but via non-shard paths.
    # VM_SHUTDOWN_ON_COMPLETION=true; EPHEMERAL_BATCH lifecycle.
    # Registered 2026-05-15 (slot-2 B-011 blindspot audit).
    # ------------------------------------------------------------------
    "footystats-fwd-": None,  # launch-footystats-forward-poll.sh — FootyStats entity poll
    "sfi-fwd-": None,  # launch-sfi-forward-poll.sh — SFI (SoccerFootballInfo) entity poll
    # ------------------------------------------------------------------
    # Sports manifest rescan VMs (launch-sports-manifest-rescan-vm.sh).
    # Three launch shapes: singleton coordinator + N chunk VMs.
    # VM_SHUTDOWN_ON_COMPLETION=true; writes _index/partial/<run-id>/ NOT
    # _index/per_vm/ (non-standard shard path → heartbeat-only).
    # Registered 2026-05-15 (slot-2 B-011 blindspot audit).
    # ------------------------------------------------------------------
    "sports-manifest-rescan-": None,  # coordinator + chunk VMs (all shapes share prefix)
    # ------------------------------------------------------------------
    # TradFi market-data backfill / forward-poll / incremental
    # ------------------------------------------------------------------
    "tradfi-bf-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "tradfi-fwd-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "tradfi-recent-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # TradFi CME event-contract catalog backfill (instruments-service)
    # ------------------------------------------------------------------
    "tradfi-event-contract-backfill-": VmPrefixSpec(
        bucket=_INSTR_TRADFI,
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
    "mdps-cefi-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mdps-tradfi-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "mdps-defi-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mdps-prediction-": VmPrefixSpec(
        bucket=_TICK_PRED,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # launch-mdps-sharded-backfill.sh's default asset-group set INCLUDES sports
    # (SPORTS_YEARS 2020-2026, SKIP_DEPENDENCY_CHECK=true) and it emits
    # mdps-sports-{year}-{ts} — but this prefix was MISSING until 2026-07-20, so a
    # sports MDPS shard got only the watchdog's heartbeat-only fallback (no per-VM
    # manifest-shard progress signal) and — because launcher_registry keys are a
    # superset of this dict — NO relaunch binding at all, so a preempted sports
    # shard fell through to file_issue instead of being auto-recovered.
    # NOT covered by "mdps-sports-bucket-" below — that is a different launcher
    # (launch-mdps-sports-bucket-vm.sh) and, being LONGER, still wins the
    # longest-prefix match for its own VMs. Sports is genuinely in scope for the
    # sharded launcher (it carries sports-specific STALL_TIMEOUT_SEC=7200 +
    # STALL_PROGRESS_REGEX verified against a live mdps-sports run.log), so the fix
    # is to register it, not to drop sports from the launcher defaults.
    "mdps-sports-": VmPrefixSpec(
        bucket=_TICK_SPORTS,
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
    "mdps-backfill-cefi-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mdps-backfill-tradfi-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "mdps-backfill-defi-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mdps-backfill-prediction-": VmPrefixSpec(
        bucket=_TICK_PRED,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "mdps-backfill-sports-": VmPrefixSpec(
        bucket=_TICK_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # MTDS asset-group-scoped backfills (DeFi onchain feeds + prediction)
    # ------------------------------------------------------------------
    "mtds-prediction-": VmPrefixSpec(
        bucket=_TICK_PRED,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "mtds-perp-funding-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-gas-fees-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-lst-rates-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-vault-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-lending-indices-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # Pyth Hermes archive backfill (Solana SOL/USD pre-2023-10 gap window).
    # Hermes archive starts ~2023-10-01 per UAC ORACLE_COVERAGE_START SSOT
    # (UAC@3adee82 2026-05-08). Pre-2023-10 SOL/USD oracle valuation needed
    # for carry_staked_basis Solana-leg backtest. Sources cascade: Pythnet
    # historical RPC (free, slow) → CoinGecko historical daily (free, daily
    # granularity). Operator-decision pending on Birdeye paid-tier add.
    "mtds-pyth-archive-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # Governance proposals backfill (launch-governance-backfill-vm.sh).
    # Writes governance_proposals data_type to market-data-tick-defi-* for
    # Aave V3 / Compound V3 / Spark / Lido (Phase 4A defi_simulation_realism).
    # Registered 2026-05-17 (slot-7 Phase 4D).
    "governance-backfill-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # Pyth Hermes LST oracle_prices backfill (2023-10-01 → today).
    # Covers JitoSOL/USD, mSOL/USD, bSOL/USD, INF/USD feeds for
    # carry_staked_basis Solana leg. Singleton-locked per launcher.
    # Launcher: launch-mtds-pyth-lst-backfill-vm.sh (MTDS@0636dd4 2026-05-14).
    # Awaiting operator [ack] in pings/slot_2.md before launch.
    "pyth-lst-backfill-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # JITO-SOLANA lst_rates historical backfill (2022-08-01 → today).
    # jitoSOL staking APY for carry_staked_basis Solana leg.
    # Launcher: launch-jito-solana-backfill-vm.sh (deployment-service 2026-05-18).
    # Awaiting operator [ack] in harsh_orchestrator/pings/slot_2.md before launch.
    "jito-solana-backfill-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # MARINADE-SOLANA lst_rates historical backfill (2021-02-01 → today).
    # mSOL staking APY for carry_staked_basis Solana leg.
    # Launcher: launch-marinade-solana-backfill-vm.sh (deployment-service 2026-05-18).
    # Awaiting operator [ack] in harsh_orchestrator/pings/slot_2.md before launch.
    "marinade-backfill-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
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
    # Strategy test VMs (launch-strategy-test-vm.sh). Heartbeat-only — runs
    # backtests for CI/validation; writes to deployment-scripts bucket (logs),
    # no per-VM manifest shards. EPHEMERAL_BATCH lifecycle.
    # Registered 2026-05-15 (slot-2 B-011 blindspot audit).
    "strategy-test-": None,  # launch-strategy-test-vm.sh — CI strategy validation
    # ML VMs (launch-ml-vm.sh). Heartbeat-only — writes model artefacts to
    # model_registry bucket, no per-VM manifest shards. Covers training,
    # inference, and evaluation operations on the consolidated ml-service.
    # VM_SHUTDOWN_ON_COMPLETION=true; EPHEMERAL_BATCH lifecycle.
    # Registered 2026-05-15 (slot-2 B-011 blindspot audit); prefix updated
    # 2026-05-20 (ml_repo_consolidation: ml-training-service + ml-inference-service → ml-service).
    "ml-": None,  # launch-ml-vm.sh — ml-service training + inference
    # Execution-alpha parallel measurement VMs (launch-execution-alpha-vm.sh; plan:
    # compute_optimization_mock_data_2026_05_13.md Phase 3). VM name pattern:
    # `exec-alpha-{ts}`. Heartbeat-only — the VM writes per-chunk JSON results to
    # gs://strategy-store-{pid}/backtests/execution_alpha/, not a per-VM manifest shard.
    "exec-alpha-": None,
    # Strategy paper-trade VMs (launch-strategy-paper-vm.sh; plan:
    # promote_workflow_may23_cli_path_2026_05_10.md Phase 1). VM name pattern:
    # `strategy-paper-{archetype-slug}-shard{N}-{ts}` (per-client-isolation Phase 8;
    # pre-Phase-8 pattern was `strategy-paper-{archetype-slug}-{ts}`). Prefix
    # match covers both patterns. Heartbeat-only — paper VMs write to event-archive
    # only (no per-VM manifest shards).
    "strategy-paper-": VmPrefixSpec(
        bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE, umbrella=DeploymentUmbrella.PAPER
    ),
    # Greeks-service compute VMs (greeks-service repo; plan:
    # plans/active/pricing_ledger_carry_rates_mtds_2026_06_01.md Phase 3).
    # Two prefixes for the two runtime modes:
    #   greeks-compute-live-{ts}  → LONG_LIVED_LIVE streaming greeks-service
    #     subscribed to MTDS mark_update; writes greek+carry columns back to
    #     PricingLedger MARK_UPDATE rows.
    #   greeks-compute-batch-{archetype-slug}-{ts} → EPHEMERAL_BATCH backfill
    #     cron that recomputes greeks over PricingLedger history (per-row
    #     option_delta/gamma/theta/vega/rho + funding_rate/lending_rate/
    #     borrow_rate/staking_apy/dividend_yield/rebase_rate).
    # Heartbeat-only — writes go through MTDS PricingLedger sink bucket via
    # _resolve_policy_output_data_type, NOT per-VM manifest shards.
    # Registered 2026-05-23 (operator-ACK'd Phase 5a of global_ledger discovery).
    "greeks-compute-live-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "greeks-compute-batch-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # GCS migration bundle VMs (launch-gcs-migration-bundle-vm.sh; plan:
    # gcs_migration_bundle_pipeline_mode_2026_05_08.md Phase 3). VM name pattern:
    # `gcs-migration-bundle-{asset_group}-{year}-{ts}`. Heartbeat-only — migration
    # VMs write to per-asset-group market-data-tick buckets but the mapping is
    # dynamic (asset_group is a runtime arg), so watchdog falls back to heartbeat.
    # Registered 2026-05-19 (slot-8 fix pre-existing blindspot in remote fast-forward).
    "gcs-migration-bundle-": None,
    # DeFi batch backtest VMs (launch-defi-backtest-vm.sh; plan:
    # batch_live_symmetry_2026_05_10.md Tab 8). VM name pattern:
    # `defi-backtest-{archetype-slug}-{ts}`. Heartbeat-only — writes backtest
    # scores to gs://strategy-store-{pid}/backtests/defi/, no per-VM manifest shards.
    # EPHEMERAL_BATCH lifecycle (completes and self-deletes).
    # Registered 2026-05-19 (slot-8 Tab 8 greenfield ship).
    "defi-backtest-": None,
    # DeFi paper-trading VMs (launch-defi-paper-trading-vm.sh; plan:
    # batch_live_symmetry_2026_05_10.md Tab 8 Step 3). VM name pattern:
    # `defi-paper-{archetype-slug}-{ts}`. Heartbeat-only — paper VMs write
    # to event-archive only (no per-VM manifest shards). LONG_LIVED_LIVE
    # lifecycle (runs 7+ days during paper-soak).
    # Registered 2026-05-19 (slot-8 Tab 8 greenfield ship).
    "defi-paper-": VmPrefixSpec(
        bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE, umbrella=DeploymentUmbrella.PAPER
    ),
    # Funding/basis ensemble paper VM (launch-funding-ensemble-paper-cron-vm.sh; plan:
    # carry_staked_basis_funding_scan_experiment_2026_06_16.md). VM name pattern:
    # `funding-ensemble-paper-{ts}`. Heartbeat-only — runs funding_ensemble_engine.py
    # ONCE (VM_BACKFILL_CMD via _launch_with_tee → DEPLOYMENT_STARTED/COMPLETED), uploads
    # the desired-state book to GCS, then self-deletes (VM_SHUTDOWN_ON_COMPLETION). Daily
    # recurrence = an external scheduler re-launching it. EPHEMERAL_EXPERIMENT lifecycle.
    # Registered 2026-06-19 (P2 funding ensemble paper path).
    "funding-ensemble-paper-": VmPrefixSpec(
        bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_EXPERIMENT, umbrella=DeploymentUmbrella.PAPER
    ),
    # Strategy live-trade VMs (launch-strategy-live-vm.sh; plan:
    # promote_workflow_may23_cli_path_2026_05_10.md Phase 1). VM name pattern:
    # `strategy-live-{archetype-slug}-shard{N}-{ts}` (per-client-isolation Phase 8;
    # pre-Phase-8 pattern was `strategy-live-{archetype-slug}-{ts}`). Prefix
    # match covers both patterns. Heartbeat-only — live VMs write to event-archive
    # only (no per-VM manifest shards).
    "strategy-live-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    # DeFi recursive-borrow live-trading VMs (launch-defi-recursive-borrow-vm.sh;
    # plan: defi_recursive_borrow_archetypes_2026_05_10.md Phase 13). VM name
    # pattern: `defi-recursive-{variant-slug}-{ts}`. Heartbeat-only — live VMs
    # write to event-archive only (no per-VM manifest shards). Singleton-locked
    # per variant (refuses launch if same-variant VM RUNNING). Registered
    # 2026-05-17 (slot-5 Phase 13 launcher).
    "defi-recursive-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
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
    "mtds-liquidations-backfill": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # prediction-features-: feature-engineering VMs for prediction asset_group.
    # VM_NAME pattern is `prediction-features-{N}` (numbered shards).
    "prediction-features-": VmPrefixSpec(bucket=_TICK_PRED, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # mtds-gas-fees-solana: distinct from generic mtds-gas-fees- (EVM chains)
    # — Solana-specific gas/priority-fee feed. Same target bucket as defi.
    "mtds-gas-fees-solana": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # Phase 3 orchestrator-emitted VM name patterns (2026-05-08).
    # Each orchestrator launcher emits N child VMs with these prefixes.
    # sports-full-sweep-{year}: full_api_football_sweep year-chunk fan-out (8 VMs).
    "sports-full-sweep-": VmPrefixSpec(bucket=_TICK_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # sports-entity-{type}: full_sports_entity_sweep per-entity fan-out (17 VMs).
    "sports-entity-": VmPrefixSpec(bucket=_TICK_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # prediction-pipeline-{N}: prediction multi-stage pipeline VMs (MDPS + features-cross-instrument + features-delta-one).
    "prediction-pipeline-": VmPrefixSpec(bucket=_TICK_PRED, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # Singleton DeFi backfills (no -{ts}) migrated 2026-05-08 (Tab 11)
    # from e2e-testing/scripts/defi/. Each launcher emits a single VM
    # with a fixed name; bucket = market-data-tick-defi-{pid}.
    "mtds-dex-pools-backfill": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-dex-swaps-backfill": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # Prefix variants for the catalogue-capture EU backfill (windowed/highmem VM names like
    # mtds-dex-pools-eu-hm, mtds-liquidations-eu-…) + the 3 new per-data_type launchers
    # (position_data / liquidation_events / flash_loan_events — handlers existed, launchers added
    # 2026-06-24 to fill their expected_unattempted). All write the defi tick bucket.
    "mtds-dex-pools-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-dex-swaps-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-liquidations-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-position-data-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-liquidation-events-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-flash-loan-events-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-risk-params-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-eigenlayer-rewards-backfill": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # Multi-protocol Solana DeFi backfill (marginfi, solend, kamino, orca,
    # raydium, phoenix, jito, kamino_lending). Marinade has its own dedicated
    # launcher. (DRIFT removed 2026-07-16 — Solana perp DEX cull, operator ruling.)
    # DRIFT-SOLANA + PACIFICA-SOLANA VM prefixes REMOVED 2026-07-16 (operator ruling:
    # kill Drift + every other Solana perp DEX; Jupiter is the only keep and is not
    # integrated). Removed in lockstep with their launcher_registry entries — this
    # registry and launcher_registry are invariant-coupled (test_every_watchdog_prefix
    # _has_a_registry_entry). SSOT: unified-trading-pm/plans/active/issues/
    # solana_perp_dex_cull_drift_pacifica_2026_07_16.md
    "mtds-solana-defi-backfill": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
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
    "mtds-backfill-cefi-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-backfill-tradfi-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "mtds-backfill-defi-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-backfill-prediction-": VmPrefixSpec(
        bucket=_TICK_PRED,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "mtds-backfill-sports-": VmPrefixSpec(
        bucket=_TICK_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # mtds-backfill-odds-{N} from launch-mtds-sports-odds-backfill-vm.sh
    # (sports-Odds-API specific). Migrated 2026-05-08 (Tab 11) from
    # e2e-testing/scripts/sports/launch_mtds_backfill_vm.sh.
    "mtds-backfill-odds-": VmPrefixSpec(
        bucket=_TICK_SPORTS,
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
    # Tier-2 per-datapoint id+schema validation (launch-datapoint-validation-vm.sh).
    # ONE sanctioned single-walk per (asset_group, campaign) — reads the corpus,
    # writes a RESULTS manifest (never data) to the flat datapoint-validation
    # results bucket via MANIFEST_PER_VM_SHARDS=true (_index/per_vm/{vm}.parquet).
    # Real VmPrefixSpec (not heartbeat-None) so the fleet monitor keys on
    # results-row write-progress (the 2026-07-18 entity-agnostic blind spot).
    # SPOT + presence-skip idempotent → standard preemption relaunch.
    # Singleton-locked per AG. Launcher + launcher_registry entries land in todo 32.
    # SSOT: codex/02-data/reconciliation-census-and-compute-tiers.md § 3.
    # ------------------------------------------------------------------
    "datapoint-validation-cefi-": VmPrefixSpec(
        bucket=_DATAPOINT_VALIDATION, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "datapoint-validation-defi-": VmPrefixSpec(
        bucket=_DATAPOINT_VALIDATION, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "datapoint-validation-tradfi-": VmPrefixSpec(
        bucket=_DATAPOINT_VALIDATION, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "datapoint-validation-sports-": VmPrefixSpec(
        bucket=_DATAPOINT_VALIDATION, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    "datapoint-validation-prediction-": VmPrefixSpec(
        bucket=_DATAPOINT_VALIDATION, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH
    ),
    # ------------------------------------------------------------------
    # GCS→manifest orphan sweep (launch-orphan-sweep-vm.sh) — runs
    # instruments-service/scripts/migration_orphan_sweep.py --asset-group <ag> for
    # REAL (not --dry-run; the tool is read-only regardless — it classifies every
    # object A/B/C/C2/D/E and writes ONE audit parquet, never deletes/patches GCS).
    # ONE single full-corpus walk per asset_group, UNLIKE the Tier-2 datapoint-
    # validator above: it writes a FIXED per-AG report path
    # (_index/audit/orphan_sweep_{ag}.parquet, inside the AG's OWN market-data tick
    # bucket — not a separate flat results bucket) rather than a per-VM shard, so
    # bucket=None here is correct (heartbeat-only) — pointing bucket at the tick
    # bucket would make the fleet monitor look for a `_index/per_vm/{vm}.parquet`
    # this tool never writes, false-STALL/zombie-classifying a healthy VM.
    # EPHEMERAL_BATCH matches the datapoint-validation spec's lifecycle_class
    # convention for deployment-ui Monitor-tab grouping. Singleton-locked per
    # asset_group. Sports is EXCLUDED (its own migration_orphan_sweep_sports.py,
    # run separately, not via this launcher).
    # SSOT: codex/02-data/reconciliation-census-and-compute-tiers.md § 3;
    # unified-trading-pm/plans/active/issues/estate_orphan_assessment_2026_07_21.md
    # todo 3.
    # ------------------------------------------------------------------
    "orphan-sweep-cefi-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "orphan-sweep-defi-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "orphan-sweep-tradfi-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "orphan-sweep-prediction-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
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
    # Per-instrument v2 expected-universe enumerator (Gate G3
    # manifest_evolution_SUPERSEDED_2026_05_21 / Phase 2.B, 2026-05-13).
    # Writes per-VM manifest shards only; no canonical data bucket to poll.
    # Heartbeat-only (None) is correct.
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
    "opt-deribit-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # Deribit BTC/ETH options_chain DAILY forward-snapshot via the live/replay
    # `deribit-options-chain` MTDS operation — feeds cefi tick bucket under
    # pipeline_mode=live_deribit. Distinct from opt-deribit- (historical Tardis
    # batch). Wired by launch-deribit-options-chain-daily.sh under Plan 6
    # infra_capture_and_devops_leftovers_2026_07_06 task 002.
    "deribit-opts-fwd-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # OKX BTC/ETH options_chain historical Tardis batch backfill — added
    # 2026-07-12 (cefi_deribit_combo_and_okx_bare_venue_gaps_2026_07_12.md
    # todo 2) once the options_chain/futures_chain exchange resolution
    # became instrument-type-aware for OKX. Same launcher + bucket shape
    # as opt-deribit-. Also covers opt-deribit-combo- (a prefix of
    # opt-deribit- above, no separate entry needed).
    "opt-okx-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "opt-cboe-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "opt-cme-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # CME event-contract backfill (TradFi)
    # ------------------------------------------------------------------
    "cme-events-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # Sports reference-data backfill (per-source launcher prefixes)
    # ------------------------------------------------------------------
    "fs-backfill-": VmPrefixSpec(
        bucket=_INSTR_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # features-sports singleton backfill (launch-features-sports-backfill-vm.sh,
    # features-service). Previously collided with fs-backfill- above (FootyStats,
    # instruments-service) — both launchers emitted the same VM_NAME prefix, so
    # name-based fleet inspection couldn't tell them apart without reading VM
    # metadata. Split 2026-07-18, see
    # api_football_backfill_chronological_scan_never_reaches_pending_tail_2026_07_18.md
    # todo P3. Distinct from fss-backfill-vm- below (the parallel-fanout variant
    # of the same features-sports backfill).
    "fts-backfill-": VmPrefixSpec(
        bucket=_FEAT_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "af-backfill-": VmPrefixSpec(
        bucket=_INSTR_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # api_football fixtures truth-set audit (launch-fixtures-truthset-audit-vm.sh,
    # 2026-05-06 — Phase 1 of sports_fixtures_truthset_recovery_2026_05_06.md).
    # Writes to gs://instruments-store-sports-{pid}/_audits/, so the watchdog
    # checks the same shard bucket — its progress signal is the truth-set
    # parquet checkpoint mtime under _audits/.
    "af-audit-": VmPrefixSpec(
        bucket=_INSTR_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # Phase 2 fixtures recovery from the truth-set
    # (launch-fixtures-recovery-vm.sh). Writes per-league sub-partition
    # fixtures parquets + per-VM manifest shards.
    "af-recover-": VmPrefixSpec(
        bucket=_INSTR_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "tm-backfill-": VmPrefixSpec(
        bucket=_INSTR_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "tm-forward-poll-": VmPrefixSpec(
        bucket=_INSTR_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "sfi-backfill-": VmPrefixSpec(
        bucket=_INSTR_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "us-backfill-": VmPrefixSpec(
        bucket=_INSTR_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "us-forward-poll-": VmPrefixSpec(
        bucket=_INSTR_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "weather-backfill-": VmPrefixSpec(
        bucket=_INSTR_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),  # open_meteo (launcher emits weather-* not openmeteo-*)
    # Targeted (date, league_id) gap-fill — reads canonical manifest, fires
    # only at the missing-shard set. First use: PLAYER_STATS via
    # launch-fill-missing-player-stats-vm.sh (2026-05-06), replacing the
    # slow chronological af-backfill iteration.
    "fill-missing-player-stats-": VmPrefixSpec(
        bucket=_INSTR_SPORTS,
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
    # Sports scheduler daemon (launch-sports-scheduler-vm.sh). Long-lived
    # fixture-aware poll loop (300s cadence). No VM_SHUTDOWN_ON_COMPLETION.
    # Has tier=scheduler label → _is_daemon() exempts from heartbeat-staleness
    # alerts. Registered here for prefix recognition (not zombie alerts).
    # Registered 2026-05-15 (slot-2 B-011 blindspot audit).
    "sports-scheduler-": None,  # launch-sports-scheduler-vm.sh — fixture trigger daemon
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
    "canonical-migration-cefi-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "canonical-migration-tradfi-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "canonical-migration-defi-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "canonical-migration-prediction-": VmPrefixSpec(
        bucket=_TICK_PRED,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "canonical-migration-sports-": VmPrefixSpec(
        bucket=_TICK_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # legacy→canonical sharded migration VMs — launch-legacy-bucket-migration-sharded.sh
    # naming: canonical-migration-legacy-{cefi|tradfi|defi|prediction|sports}-{shard}-{ts}
    # (bucket_name_ssot_legacy_dual_write_remediation Phase 5; per-shard data-only copy).
    "canonical-migration-legacy-cefi-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "canonical-migration-legacy-tradfi-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "canonical-migration-legacy-defi-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "canonical-migration-legacy-prediction-": VmPrefixSpec(
        bucket=_TICK_PRED,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "canonical-migration-legacy-sports-": VmPrefixSpec(
        bucket=_TICK_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # sports v9 migration VMs — launch-sports-v9-migration-vm.sh (E4)
    # naming: sports-v9-migration-{mdps|instruments}-{year}-{ts}
    # Two-phase: migrate_sports_canonical_v9 + rebuild_sports_manifest_v9
    # MANIFEST_PER_VM_SHARDS=true; one VM per (surface, year).
    # Registered 2026-06-28 (plan: sports_manifest_canonicalisation_2026_06_01.md § E4).
    "sports-v9-migration-": VmPrefixSpec(
        bucket=_TICK_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # GCS migration bundle Phase 3 VMs — launch-gcs-migration-bundle-vm.sh
    # naming: gcs-migration-bundle-{ag}-{year}-{ts}
    # Runs gcs_migration_bundle_2026_05_08.py --apply for one year-slice
    # per VM. Writes per-VM manifest shards under each ag's tick bucket.
    # MANIFEST_PER_VM_SHARDS=true + unique VM_NAME ensure no collisions.
    # Registered 2026-05-19 (slot 1 Phase 3 fleet launch).
    # ------------------------------------------------------------------
    "gcs-migration-bundle-cefi-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "gcs-migration-bundle-defi-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "gcs-migration-bundle-tradfi-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "gcs-migration-bundle-sports-": VmPrefixSpec(
        bucket=_TICK_SPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    "gcs-migration-bundle-prediction-": VmPrefixSpec(
        bucket=_TICK_PRED,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # MDPS sports bucket-pass VMs — launch-mdps-sports-bucket-vm.sh
    # naming: mdps-sports-bucket-{ts}
    # writes per-(league_id, horizon) bucketed parquets to the same sports
    # tick bucket that the canonical-migration step writes raw data to.
    # ------------------------------------------------------------------
    "mdps-sports-bucket-": VmPrefixSpec(
        bucket=_TICK_SPORTS,
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
    # rule. Bucket names resolved via resolve_bucket_name() (yaml SSOT) at
    # process start — same env-tiered names as all other entries in this dict.
    # ------------------------------------------------------------------
    # Consolidated MVP CeFi launcher (2026-06-27): one VM, all shards.
    # Must appear BEFORE "mtds-live-cefi-" (longest-prefix wins in watchdog + registry).
    "mtds-live-cefi-consolidated-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "mtds-live-cefi-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "mtds-live-defi-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "mtds-live-tradfi-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
    ),
    "mtds-live-sports-": VmPrefixSpec(
        bucket=_TICK_SPORTS,
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
    ),
    "mtds-live-prediction-": VmPrefixSpec(
        bucket=_TICK_PRED,
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
    ),
    # launch-mtds-live.sh --test-run: bounded, test-bucket-routed, auto-shutdown live smoke
    # check (plan todo 16, data_pipeline_e2e_check_2026_07_10.md) — distinct prefix root, so
    # it never collides with the real per-ag `mtds-live-{ag}-` singleton lock above. EPHEMERAL
    # (self-shuts-down via --max-duration-seconds), not LONG_LIVED_LIVE like a real producer;
    # bucket untracked (None) since the target varies by embedded asset_group in the shard slug
    # and it's always a `-test-` bucket regardless — same simplification as `instruments-smoke-`.
    "mtds-live-smoke-": None,
    "mdps-features-live-cefi-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "mdps-features-live-defi-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "mdps-features-live-tradfi-": VmPrefixSpec(
        bucket=_TICK_TRADFI,
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
    ),
    "mdps-features-live-sports-": VmPrefixSpec(
        bucket=_TICK_SPORTS,
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
    ),
    "mdps-features-live-prediction-": VmPrefixSpec(
        bucket=_TICK_PRED,
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
    "amm-golden-": None,  # Phase 2 AMM golden-swap validation (per-shape)
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
    # ------------------------------------------------------------------
    # Client-reporting PnL attribution cutover VM (Phase 8.A of
    # client_reporting_pnl_attribution_mvp_2026_05_10.md). One singleton VM named
    # `client-reporting-cutover-{ts}` runs a 24h paper-trade attribution loop:
    # carry_staked_basis + arbitrage_price_dispersion for demo_client_001.
    # Emits per-archetype attribution parquets + hourly invariant checks.
    # Heartbeat-only (None) — writes to event-archive + client-reports bucket,
    # NOT to a per-VM manifest shard. Singleton-locked.
    # Launcher: deployment-service/scripts/vm/launch-client-reporting-cutover-vm.sh
    # Registered 2026-05-15 per CLAUDE.md "VM Naming Convention" HARD RULE.
    # After updating this dict, relaunch the watchdog VM.
    # ------------------------------------------------------------------
    "client-reporting-cutover-": None,
    # QG snapshot cron VM (B-018 Phase 4.A). Heartbeat-only; no manifest shard writes.
    # Launcher: deployment-service/scripts/vm/launch-qg-snapshot-vm.sh
    # Registered 2026-05-14 per CLAUDE.md "VM Naming Convention" HARD RULE.
    # After updating this dict, relaunch the watchdog VM.
    "qg-snapshot-": None,
    # Batch+live smoke matrix cron VM (batch_live_smoke_matrix_2026_06_19.md P2).
    # Heartbeat-only; runs validate_batch_live_smoke_matrix.py --live-window 8 daily.
    # Launcher: deployment-service/terraform/gcp/batch_live_smoke_matrix_scheduler.tf.
    # Registered 2026-06-22 per CLAUDE.md "VM Naming Convention" HARD RULE.
    "batch-live-smoke-matrix-": None,
    # Honest-coverage cron VM (B-018 Phase 8.A). Heartbeat-only; output is project-level JSON,
    # not per-VM manifest shards. Launcher: launch-honest-coverage-vm.sh (Cloud Scheduler daily).
    # Registered 2026-05-15 per CLAUDE.md "VM Naming Convention" HARD RULE.
    "honest-coverage-": None,
    # ------------------------------------------------------------------
    # Forward-poll daily cron hosts — SCHEDULED_RECURRING long-lived VMs that
    # install a crontab firing the matching `launch-{tradfi,cefi}-forward-poll.sh`
    # launcher once a day. Replaces the previously-broken Cloud Scheduler →
    # Cloud Run trigger pattern (HTTP 403 + zero executions for 4+ months on
    # `trigger-market-tick-cefi-job`; absent entirely on TradFi). Operator
    # authorised 2026-05-20 (Option B in tradfi_forward_poll_cron_missing_2026_05_17.md).
    # Heartbeat-only (None) — the cron host itself writes no manifest shards;
    # the worker VMs it spawns (`tradfi-fwd-*` / `cefi-fwd-*`) carry the data
    # output and have their own VmPrefixSpec entries above.
    # Launchers:
    #   launch-tradfi-fwd-daily-cron-vm.sh (fires 06:00 UTC)
    #   launch-cefi-fwd-daily-cron-vm.sh   (fires 09:00 UTC)
    # Singleton-locked; --force bypasses. After updating this dict, relaunch
    # the watchdog VM. Registered 2026-05-20 per CLAUDE.md "VM Naming
    # Convention" HARD RULE + Phase A.2 lifecycle_class requirement.
    # ------------------------------------------------------------------
    "tradfi-fwd-daily-cron-": None,
    "cefi-fwd-daily-cron-": None,
    # Phase 2.6 bucket-rsync VMs (gap-2.6.A; flat→env-tiered cutover Wave 2-5 workers).
    # Heartbeat-only; output is the dest-bucket itself (not per-VM manifest shards).
    # Launcher: launch-bucket-rsync-vm.sh; singleton-locked per source-bucket-hash.
    # Registered 2026-05-16 per CLAUDE.md "VM Naming Convention" HARD RULE.
    "bucket-rsync-": None,
    # Watchdog VM itself — registered for self-documentation per
    # service_registry_drift_audit_2026_05_15.md P3 recommendation.
    # The watchdog does NOT reap itself: see EXEMPT_LABELS at line ~861
    # which checks `purpose=vm-zombie-watchdog` GCE label — that's the
    # actual exemption mechanism, not this dict entry. This None entry
    # exists so `VM_PREFIX_TO_BUCKET` is a complete + self-documenting
    # registry of every prefix the watchdog knows about.
    "vm-zombie-watchdog-": None,
    # Deploy-missing auto-launch VMs (deploy_missing_auto_launch_2026_05_07.md Phase 2).
    # VM naming: dm-{shard_key_hash16}-{YYYYMMDD-HHMMSS}. Launched by deployment-api
    # POST /api/data-status/deploy-missing-launch for surgical per-shard backfill.
    # bucket=None (heartbeat-only); each VM writes to its own service-specific data bucket.
    # Registered 2026-05-17 per CLAUDE.md "VM Naming Convention" HARD RULE.
    "dm-": None,
    # ------------------------------------------------------------------
    # Scenario regression matrix VMs (launch-scenario-runner-vm.sh).
    # VM naming: scenario-matrix-{archetype}-{YYYYMMDD-HHMMSS}.
    # Launched by deployment-service/scripts/vm/launch-scenario-runner-vm.sh;
    # writes ScenarioReport parquets to scenario-reports-{pid}.
    # Registered 2026-05-19 per simulation_scenarios_topology §Phase 9 prereq.
    # ------------------------------------------------------------------
    "scenario-matrix-": VmPrefixSpec(
        bucket=_SCENARIO_REPORTS,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
    ),
    # ------------------------------------------------------------------
    # Agent-orchestrator VMs — planning VM + per-epic VMs.
    # All LONG_LIVED_LIVE (run until operator tears them down).
    # bucket=None: orchestrator VMs write to the orchestrator GCS state
    # bucket, not to per-VM manifest shards. Heartbeat-only.
    # Launchers:
    #   launch-planning-vm.sh  → agent-orch-planning-vm-{YYYYMMDD}
    #   launch-epic-vm.sh      → agent-orch-{vm-id}-{YYYYMMDD}
    # Registered 2026-05-21 (orchestrator Phase 7/11 fleet launch).
    # ------------------------------------------------------------------
    "agent-orch-planning-vm-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-defi-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-cefi-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-tradfi-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-sports-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-prediction-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-ml-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-trading-core-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-operator-ops-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-cross-cutting-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-orchestrator-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
}
