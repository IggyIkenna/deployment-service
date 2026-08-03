# Epic: observability_master
# Lifecycle: permanent
"""``vm_name`` → relaunch-launcher registry for the self-heal actuators.

The data-pipeline self-heal actuators (``relaunch_stalled_vm`` /
``relaunch_backfill_vm``) re-run a watchdog-killed / OOM'd backfill VM by invoking
its ``deployment-service/scripts/vm/launch-*.sh`` launcher. They accept the launcher
script name as a binding on the finding (``relaunch_launcher``). The fleet-monitor
sweeps (``heartbeat_stall_watcher.sweep`` / ``exit_code_fleet_monitor.sweep``)
resolve that binding from a ``launcher_for_vm(vm_name) -> str`` callable.

This module is that callable's data: ``LAUNCHER_FOR_VM_PREFIX`` maps each VM-name
prefix the zombie watchdog knows about (``vm_zombie_watchdog.VM_PREFIX_TO_BUCKET``)
to the ``scripts/vm/launch-*.sh`` filename that produces that prefix. ``None`` =
deliberately not auto-relaunchable (a fan-out wrapper that emits N children, a
singleton/one-off with no deterministic re-run, or a non-backfill infra VM); the
finding then falls through to ``file_issue`` (escalation guarantees no auto_recover
finding is lost).

Why a registry, not a name heuristic: a launcher's VM_NAME prefix is NOT always the
launcher's filename stem (``launch-openmeteo-backfill-vm.sh`` emits ``weather-backfill-``;
``launch-cefi-massive-rollout.sh`` emits ``cefi-mr-``). Each mapping below was derived
by reading the actual launcher's ``VM_NAME=`` / ``VM_PREFIX=`` assignment.

``resolve_launcher_for_vm(vm_name)`` does LONGEST-prefix match (same precedence as
``vm_zombie_watchdog`` bucket resolution) so a specific prefix (``cefi-fwd-daily-cron-``)
wins over its parent (``cefi-fwd-``).

Guard: ``tests/unit/test_launcher_registry.py`` asserts every ``VM_PREFIX_TO_BUCKET``
prefix is present here AND every non-None launcher resolves to an existing
``scripts/vm/launch-*.sh`` file — so "added a launcher prefix, forgot the registry"
fails CI (mirrors ``test_validate_vm_prefix_mapping`` / the cloud-run-job registry guard).
"""

from __future__ import annotations

from pathlib import Path

# scripts/vm dir — the launcher home (matches the actuators' _LAUNCHER_DIR resolution:
# scripts/recovery/relaunch_*.py → ``parent.parent / "vm"``).
_LAUNCHER_DIR = Path(__file__).resolve().parents[2] / "scripts" / "vm"


# VM-name prefix → launcher filename under scripts/vm/ (or None when not
# deterministically relaunchable). Keys MUST be a superset of
# vm_zombie_watchdog.VM_PREFIX_TO_BUCKET (the guard test enforces parity).
#
# Each non-None value was derived by reading the launcher's VM_NAME=/VM_PREFIX=
# assignment. None values carry a one-line reason.
LAUNCHER_FOR_VM_PREFIX: dict[str, str | None] = {
    # ── CeFi market-data backfill / forward-poll ──────────────────────────
    "cefi-mr-": "launch-cefi-massive-rollout.sh",  # VM_NAME=cefi-mr-${RUN_TS}-${idx}
    "cefi-fwd-": "launch-cefi-forward-poll.sh",
    # Per-venue historical OHLCV backfill — launch-cefi-sharded-backfill.sh emits
    # cefi-${venue}-${year}-${group}-${ts} for every CeFi venue.
    "cefi-binance-": "launch-cefi-sharded-backfill.sh",
    "cefi-bybit-": "launch-cefi-sharded-backfill.sh",
    "cefi-deribit-": "launch-cefi-sharded-backfill.sh",
    "cefi-coinbase-": "launch-cefi-sharded-backfill.sh",
    "cefi-okx-": "launch-cefi-sharded-backfill.sh",
    "cefi-upbit-": "launch-cefi-sharded-backfill.sh",
    "cefi-hyperliquid-": "launch-cefi-hl-aster-historical-backfill.sh",
    "cefi-bitfinex-": "launch-cefi-sharded-backfill.sh",
    "cefi-bitget-": "launch-cefi-sharded-backfill.sh",
    "cefi-kraken-": "launch-cefi-sharded-backfill.sh",
    "cefi-aster-": "launch-cefi-hl-aster-historical-backfill.sh",
    "cefi-cme-": "launch-cefi-sharded-backfill.sh",
    "cefi-extended-": "launch-cefi-sharded-backfill.sh",
    "cefi-ext-bfill-": "launch-mtds-extended-ohlcv-backfill.sh",  # VM_PREFIX=cefi-ext-bfill-
    "cefi-lighter-": "launch-cefi-sharded-backfill.sh",
    # SINGLE_VM_QUEUE=1 mode — cefi-queue-{group}-{ts}, one combined multi-venue VM per
    # (group,data_types) bucket instead of per-shard (tardis_concurrent_ip_lockout_2026_07_12
    # course-correction 2026-07-13).
    "cefi-queue-": "launch-cefi-sharded-backfill.sh",
    "cefi-durability-force-converge-": "launch-cefi-durability-force-converge-vm.sh",
    "aster-fwd-": "launch-aster-forward-poll.sh",
    # ── DeFi / Prediction forward-poll + live ─────────────────────────────
    "defi-fwd-": "launch-defi-forward-poll.sh",
    "prediction-fwd-": "launch-prediction-forward-poll.sh",
    "prediction-live-": "launch-prediction-live.sh",
    "prediction-arb-detector-": "launch-prediction-arb-detector.sh",
    # ── CeFi instrument discovery + one-offs ──────────────────────────────
    "cefi-instr-": "launch-cefi-instruments-backfill.sh",
    "cefi-rogue-": None,  # one-off rekey cleanup — no recurring launcher (manual)
    # ── Instruments-service per-AG parallel backfill ──────────────────────
    # launch-instruments-backfill-vm.sh fans out instr-backfill-{ag}; --asset-group
    # scopes it, so re-running it re-launches the matching AG VM.
    "instr-backfill-cefi-": "launch-instruments-backfill-vm.sh",
    "instr-backfill-defi": "launch-instruments-backfill-vm.sh",
    "instr-backfill-tradfi": "launch-instruments-backfill-vm.sh",
    "instr-backfill-sports": "launch-instruments-backfill-vm.sh",
    "instr-backfill-pred": "launch-instruments-backfill-vm.sh",
    # ── features-sports parallel backfill ─────────────────────────────────
    "fss-backfill-vm-": "launch-features-sports-parallel-backfill-vm.sh",
    # ── Sports instruments-reference v3 backfill ──────────────────────────
    "sports-ref-v3-": "launch-sports-instruments-reference-vm.sh",
    # ── Sports forward-polls ──────────────────────────────────────────────
    "footystats-fwd-": "launch-footystats-forward-poll.sh",
    "sfi-fwd-": "launch-sfi-forward-poll.sh",
    # ── Sports manifest rescan ────────────────────────────────────────────
    "sports-manifest-rescan-": None,  # coordinator+chunk fan-out (multi-shape) — file_issue
    # ── TradFi market-data backfill / forward-poll / incremental ──────────
    # tradfi-bf-* fans out across per-(venue,schedule) launchers; re-run via the
    # canonical generic backfill launcher (asset-group/source scoped).
    "tradfi-bf-": "launch-tradfi-backfill-vm.sh",
    # FRED macro backfill is its OWN single-VM launcher (VM_NAME=tradfi-bf-fred-{full,YYYY}-{ts},
    # see launch-tradfi-bf-fred.sh) — NOT a shard of the generic CME/BTC/ETH launcher above.
    # Longest-prefix match makes this win for tradfi-bf-fred-* names. Without this entry a
    # FRED VM's relaunch finding resolved to the WRONG launcher (the generic one defaults
    # --root-symbol=ES and has no FRED root at all), which would have launched an unrelated ES
    # backfill instead of resuming FRED — confirmed live 2026-07-30 (DP-VM-001 escalation
    # agt-f421bc): tradfi-bf-fred-full-20260730-064542's relaunch finding carried
    # relaunch_launcher=launch-tradfi-backfill-vm.sh via this exact gap.
    "tradfi-bf-fred-": "launch-tradfi-bf-fred.sh",
    # Per-venue OHLCV-1m/-24h launchers — SAME reasoning as the FRED entry above (a more
    # specific prefix so longest-prefix match wins over the generic `tradfi-bf-` ->
    # CME/BTC/ETH quarterly launcher, which is a DIFFERENT sharding scheme). Confirmed live
    # 2026-07-31 (DP-VM-002 escalation agt-d6540c): `tradfi-bf-cme-ohlcv-1m-g01-es-es-2020-*`
    # preempted with no dedicated entry here, so a PREEMPTED relaunch would have resolved to
    # `launch-tradfi-backfill-vm.sh` (wrong shape — root-group year-shard OHLCV vs
    # quarterly/monthly ES-BTC-ETH tiers) instead of the launcher that actually produced it.
    "tradfi-bf-cme-ohlcv-1m-": "launch-tradfi-bf-cme-ohlcv-1m.sh",
    "tradfi-bf-ice-ohlcv-1m-": "launch-tradfi-bf-ice-ohlcv-1m.sh",
    "tradfi-bf-nasdaq-ohlcv-1m-": "launch-tradfi-bf-nasdaq-ohlcv-1m.sh",
    "tradfi-bf-nyse-ohlcv-1m-": "launch-tradfi-bf-nyse-ohlcv-1m.sh",
    "tradfi-bf-cboe-ohlcv-1m-": "launch-tradfi-bf-cboe-ohlcv-1m.sh",
    "tradfi-bf-cfe-ohlcv-1m-": "launch-tradfi-bf-cfe-ohlcv-1m.sh",
    "tradfi-bf-fx-ohlcv-24h-": "launch-tradfi-bf-fx-ohlcv-24h.sh",
    "tradfi-bf-krx-eq-ohlcv-24h-": "launch-tradfi-bf-krx-equities-ohlcv-24h.sh",
    "tradfi-bf-cboe-idx-ohlcv-24h-": "launch-tradfi-bf-cboe-indices-ohlcv-24h.sh",
    "tradfi-fwd-": "launch-tradfi-forward-poll.sh",
    "tradfi-recent-": "launch-tradfi-forward-poll.sh",
    "tradfi-event-contract-backfill-": "launch-tradfi-event-contract-backfill.sh",
    "tradfi-instr-": None,  # tradfi-instr-{venue}-{year} discovery — fan-out, no single launcher
    "tradfi-phantom-audit": None,  # read-only audit — not a relaunch target
    # ── MDPS sharded backfill (per asset_group) ───────────────────────────
    "mdps-cefi-": "launch-mdps-sharded-backfill.sh",
    "mdps-tradfi-": "launch-mdps-sharded-backfill.sh",
    "mdps-defi-": "launch-mdps-sharded-backfill.sh",
    "mdps-prediction-": "launch-mdps-sharded-backfill.sh",
    # Registered 2026-07-20 alongside the matching vm_prefix_registry row. The
    # sharded launcher's DEFAULT asset-group set includes sports, so it emits
    # mdps-sports-{year}-{ts}; without this binding a preempted/OOM'd sports shard
    # fell through to file_issue with no relaunch. Distinct from the longer
    # "mdps-sports-bucket-" prefix (launch-mdps-sports-bucket-vm.sh), which still
    # wins longest-prefix match for its own VMs.
    "mdps-sports-": "launch-mdps-sharded-backfill.sh",
    # ── MDPS per-AG backfill (mdps-backfill-{ag}) ─────────────────────────
    "mdps-backfill-cefi-": "launch-mdps-backfill-vm.sh",
    "mdps-backfill-tradfi-": "launch-mdps-backfill-vm.sh",
    "mdps-backfill-defi-": "launch-mdps-backfill-vm.sh",
    "mdps-backfill-prediction-": "launch-mdps-backfill-vm.sh",
    "mdps-backfill-sports-": "launch-mdps-backfill-vm.sh",
    # ── MTDS asset-group-scoped backfills ─────────────────────────────────
    "mtds-prediction-": "launch-mtds-prediction-backfill-vm.sh",
    "mtds-perp-funding-": "launch-mtds-perp-funding-backfill-vm.sh",
    "mtds-gas-fees-": "launch-mtds-gas-fees-backfill-vm.sh",
    "mtds-lst-rates-": "launch-mtds-lst-rates-backfill-vm.sh",
    "mtds-vault-": "launch-mtds-vault-share-price-backfill-vm.sh",
    "mtds-lending-indices-": "launch-mtds-lending-indices-backfill-vm.sh",
    "mtds-pyth-archive-": "launch-mtds-pyth-archive-backfill-vm.sh",
    "governance-backfill-": "launch-governance-backfill-vm.sh",
    "pyth-lst-backfill-": "launch-mtds-pyth-lst-backfill-vm.sh",
    "jito-solana-backfill-": "launch-jito-solana-backfill-vm.sh",
    "marinade-backfill-": "launch-marinade-solana-backfill-vm.sh",
    # ── Strategy / ML / execution compute (heartbeat-only, not data backfill) ──
    "strategy-backtest-grid-": None,  # backtest grid — re-run owned by promote/CLI, not auto-relaunch
    "strategy-test-": None,  # CI strategy validation — not a data backfill
    "ml-": None,  # ml-service train/infer — model artefacts, not a relaunchable backfill
    "exec-alpha-": None,  # execution-alpha measurement — owned by its plan harness
    "strategy-paper-": None,  # paper VM (LONG_LIVED_LIVE) — promote-workflow owns lifecycle
    "greeks-compute-live-": None,  # streaming greeks — live service, not a backfill
    "greeks-compute-batch-": None,  # greeks recompute cron — scheduler-owned, not auto-relaunch
    "gcs-migration-bundle-": None,  # generic prefix; the per-(ag,year) keys below carry the launcher
    "defi-backtest-": None,  # defi backtest — owned by its plan harness
    "defi-paper-": None,  # paper VM (LONG_LIVED_LIVE) — promote-workflow owns lifecycle
    "funding-ensemble-paper-": None,  # paper experiment — external scheduler re-launches it
    "strategy-live-": None,  # live trade VM — promote-workflow owns lifecycle (never auto-relaunch a live trader)
    "defi-recursive-": None,  # live recursive-borrow trader — promote-workflow owns lifecycle
    # ── UI / infra service VMs (heartbeat-only, never a data backfill) ────
    "deployment-dashboard-vm": None,  # UI service VM — not a data-pipeline backfill
    "alerting-quietness-": None,  # alerting baseline soak — one-off measurement VM
    "synbench-": None,  # synthetic benchmark — per-shape one-off, no auto-relaunch
    # ── Singleton DeFi backfills + misc MTDS ──────────────────────────────
    "mtds-liquidations-backfill": "launch-mtds-liquidations-backfill-vm.sh",
    "prediction-features-": "launch-prediction-features-vm.sh",
    "mtds-gas-fees-solana": "launch-mtds-solana-gas-backfill-vm.sh",
    "sports-full-sweep-": "launch-sports-full-sweep-vm.sh",
    "sports-entity-": "launch-sports-entity-sweep-vm.sh",
    "prediction-pipeline-": "launch-prediction-pipeline-vm.sh",
    "mtds-dex-pools-backfill": "launch-mtds-dex-pools-backfill-vm.sh",
    "mtds-dex-swaps-backfill": "launch-mtds-dex-swaps-backfill-vm.sh",
    # Prefix variants for windowed/highmem EU-backfill VM names (mtds-dex-pools-eu-hm,
    # mtds-liquidations-eu-20260624, …) — the singleton names above are exact; these
    # cover the per-window/per-machine suffixed launches the catalogue-capture backfill uses.
    "mtds-dex-pools-": "launch-mtds-dex-pools-backfill-vm.sh",
    "mtds-dex-swaps-": "launch-mtds-dex-swaps-backfill-vm.sh",
    "mtds-liquidations-": "launch-mtds-liquidations-backfill-vm.sh",
    "mtds-position-data-": "launch-mtds-position-data-backfill-vm.sh",
    "mtds-liquidation-events-": "launch-mtds-liquidation-events-backfill-vm.sh",
    "mtds-flash-loan-events-": "launch-mtds-flash-loan-events-backfill-vm.sh",
    "mtds-risk-params-": "launch-mtds-risk-params-backfill-vm.sh",
    "mtds-eigenlayer-rewards-backfill": "launch-mtds-eigenlayer-rewards-backfill-vm.sh",
    # G1.6 (mvp_backfill_defi_onchain_v10_2026_06_27.md): dedicated ORCA/RAYDIUM/
    # KAMINO dex_pool_state backfill (VM_SOLANA_PROTOCOLS scopes the fan-out).
    # DRIFT-SOLANA + PACIFICA-SOLANA launchers REMOVED 2026-07-16 (operator ruling:
    # kill Drift entirely + every other Solana perp DEX; Jupiter is the only one we
    # keep and it is not integrated). Drift was hacked 2026-04-01 (~$280M) and
    # relaunched as Velocity DEX 2026-07-01 with ~$0 TVL. Leaving these mapped let the
    # self-heal watchdog relaunch a stopped VM and resurrect purged data. SSOT:
    # unified-trading-pm/plans/active/issues/solana_perp_dex_cull_drift_pacifica_2026_07_16.md
    "mtds-solana-defi-backfill": "launch-mtds-solana-defi-backfill-vm.sh",
    "mtds-migrate-": "launch-cefi-migration-vm.sh",
    # ── MTDS per-AG generic backfill (the Deploy-Missing entry point) ─────
    "mtds-backfill-cefi-": "launch-mtds-backfill-vm.sh",
    "mtds-backfill-tradfi-": "launch-mtds-backfill-vm.sh",
    "mtds-backfill-defi-": "launch-mtds-backfill-vm.sh",
    "mtds-backfill-prediction-": "launch-mtds-backfill-vm.sh",
    "mtds-backfill-sports-": "launch-mtds-backfill-vm.sh",
    "mtds-backfill-odds-": "launch-mtds-sports-odds-backfill-vm.sh",
    # ── Read-only reconciliation / audit VMs (never a relaunch target) ────
    "defi-phantom-recon-": None,  # read-only phantom audit
    "manifest-recon-": None,  # read-only all-reconciler dry-run
    # Tier-2 per-datapoint id+schema validation (VM_PREFIX_TO_BUCKET parity, todo 31/32).
    # SPOT + presence-skip idempotent → auto-relaunchable: a preempted VM resumes the
    # SAME (asset_group, campaign) from measured progress (LAUNCH_PARAMS.json), the
    # presence-skip loop re-covers the frontier. The registry entry MUST have a
    # launcher_registry entry (test_every_watchdog_prefix_has_a_registry_entry).
    "datapoint-validation-cefi-": "launch-datapoint-validation-vm.sh",
    "datapoint-validation-defi-": "launch-datapoint-validation-vm.sh",
    "datapoint-validation-tradfi-": "launch-datapoint-validation-vm.sh",
    "datapoint-validation-sports-": "launch-datapoint-validation-vm.sh",
    "datapoint-validation-prediction-": "launch-datapoint-validation-vm.sh",
    # GCS→manifest orphan sweep (migration_orphan_sweep.py) — read-only + idempotent
    # (re-running just re-classifies + overwrites the same fixed report path), so a
    # SPOT preemption relaunch is a SAFE restart-from-scratch. Distinct from
    # datapoint-validation's presence-skip frontier resume: this tool has no
    # per-shard/day checkpoint, so relaunch re-walks the whole corpus, not a resume.
    "orphan-sweep-cefi-": "launch-orphan-sweep-vm.sh",
    "orphan-sweep-defi-": "launch-orphan-sweep-vm.sh",
    "orphan-sweep-tradfi-": "launch-orphan-sweep-vm.sh",
    "orphan-sweep-prediction-": "launch-orphan-sweep-vm.sh",
    # Sports derived_features post-floor residue census — read-only + idempotent
    # (re-running just re-scans and overwrites the same fixed report path), so a
    # SPOT preemption relaunch is a safe restart-from-scratch.
    "sports-derived-features-census-": "launch-sports-derived-features-census-vm.sh",
    "backfill-orphan-e-cefi-": "launch-backfill-orphan-e-vm.sh",
    "backfill-orphan-e-defi-": "launch-backfill-orphan-e-vm.sh",
    "backfill-orphan-e-tradfi-": "launch-backfill-orphan-e-vm.sh",
    "backfill-orphan-e-prediction-": "launch-backfill-orphan-e-vm.sh",
    # Candle-corpus class-E/F record_captured backfill (backfill_candle_manifest.py)
    # — RECORD-ONLY (never rewrites/deletes the source object), so a SPOT preemption
    # relaunch is a safe restart-from-scratch of the whole report.
    "backfill-candle-manifest-cefi-": "launch-backfill-candle-manifest-vm.sh",
    "backfill-candle-manifest-defi-": "launch-backfill-candle-manifest-vm.sh",
    "backfill-candle-manifest-tradfi-": "launch-backfill-candle-manifest-vm.sh",
    "backfill-candle-manifest-prediction-": "launch-backfill-candle-manifest-vm.sh",
    "gcs-migration-phase0-": None,  # read-only calibration audit
    "batch-live-recon-": None,  # nightly recon cron — scheduler-owned
    "expected-universe-v2-": "launch-expected-universe-v2-vm.sh",
    "blank-reason-recon-": "launch-blank-reason-recon-vm.sh",
    # ── Options-chain / CME-events backfills ──────────────────────────────
    "opt-deribit-": "launch-targeted-options-chain-backfill.sh",
    # Deribit BTC/ETH options_chain daily forward-snapshot (live/replay handler);
    # distinct from opt-deribit- (historical Tardis batch). Wired under
    # infra_capture_and_devops_leftovers_2026_07_06 Plan 6 task 002.
    "deribit-opts-fwd-": "launch-deribit-options-chain-daily.sh",
    # Deribit DVOL (BTC/ETH implied-vol index) FULL 2021-03-24->now historical
    # batch backfill. One-off; wired under vol_dvol_backtestable_engines_2026_07_13.md
    # Todo 3.
    "dvol-deribit-": "launch-deribit-dvol-backfill-vm.sh",
    "opt-okx-": "launch-targeted-options-chain-backfill.sh",
    "opt-cboe-": "launch-targeted-options-chain-backfill.sh",
    "opt-cme-": "launch-targeted-options-chain-backfill.sh",
    "cme-events-": None,  # CME event-contract — covered by tradfi-event-contract-backfill prefix
    # ── Sports reference-data backfill (per-source launcher prefixes) ─────
    "fs-backfill-": "launch-footystats-backfill-vm.sh",  # VM_NAME=fs-backfill-${RUN_TS}
    # features-sports singleton backfill — split off the shared fs-backfill-
    # prefix above 2026-07-18 (was colliding with FootyStats's identical
    # VM_NAME shape). VM_NAME=fts-backfill-${RUN_TS}.
    "fts-backfill-": "launch-features-sports-backfill-vm.sh",
    "af-backfill-": "launch-api-football-backfill-vm.sh",
    "af-audit-": "launch-fixtures-truthset-audit-vm.sh",
    "af-recover-": "launch-fixtures-recovery-vm.sh",
    "tm-backfill-": "launch-transfermarkt-backfill-vm.sh",
    "tm-forward-poll-": "launch-transfermarkt-forward-poll.sh",
    "sfi-backfill-": "launch-sfi-backfill-vm.sh",
    "us-backfill-": "launch-understat-backfill-vm.sh",
    "us-forward-poll-": "launch-understat-forward-poll.sh",
    "weather-backfill-": "launch-openmeteo-backfill-vm.sh",  # VM_NAME=weather-backfill-${RUN_TS}
    "fill-missing-player-stats-": "launch-fill-missing-player-stats-vm.sh",
    # ── Features pipeline VMs (family-dashed, bucket varies) ──────────────
    "features-": "launch-features-vm.sh",  # features-{family}-{ag}-{ts}; --feature-family scopes
    # ── Long-lived daemons + audits + one-offs (never a relaunch target) ──
    "manifest-consolidator-": None,  # consolidator is Cloud Run / Batch-Fargate, not a relaunchable VM
    "data-status-rollup-": None,  # Cloud Run Job — scheduler-owned
    "sports-scheduler-": None,  # scheduler daemon (tier=scheduler) — not a backfill
    "tier3-audit-": None,  # read-only audit
    "reconcile-phantom-": None,  # read-only phantom audit
    "cross-asset-rescan-": None,  # singleton rescan coordinator — fan-out, file_issue
    "measure-honest-coverage-": None,  # daily coverage measurement — scheduler-owned
    "tradfi-audit-aggregate-": None,  # one-off audit/aggregation
    "instr-": None,  # generic instr-* discovery prefix — fan-out, no single launcher
    "instruments-smoke-": None,  # CI smoke — not a data backfill
    "combo-migration-": None,  # one-off migration — no recurring launcher
    # ── canonical-migration VMs (per-AG) ──────────────────────────────────
    "canonical-migration-cefi-": "launch-canonical-migration-vm.sh",
    "canonical-migration-tradfi-": "launch-canonical-migration-vm.sh",
    "canonical-migration-defi-": "launch-canonical-migration-vm.sh",
    "canonical-migration-prediction-": "launch-canonical-migration-vm.sh",
    "canonical-migration-sports-": "launch-canonical-migration-vm.sh",
    "canonical-migration-sports-features-": "launch-canonical-migration-vm.sh",
    # CeFi perp derivative_ticker funding_timestamp fix VMs — dedicated launchers (NOT
    # launch-canonical-migration-vm.sh, which expects a different category/dry-full CLI
    # shape). Longest-prefix-match wins over the generic "canonical-migration-cefi-"
    # entry above, so a SPOT-preemption relaunch invokes the correct launcher with the
    # correct VENUE/PIPELINE_MODE/START_DATE/END_DATE env — see
    # perp_funding_data_semantics_and_cadence_2026_06_16.md.
    "canonical-migration-cefi-fts-": "launch-cefi-funding-timestamp-fix-vm.sh",
    "canonical-migration-cefi-fts-ext-": "launch-cefi-extended-starknet-funding-timestamp-vm.sh",
    # legacy→canonical sharded migration VMs
    "canonical-migration-legacy-cefi-": "launch-legacy-bucket-migration-sharded.sh",
    "canonical-migration-legacy-tradfi-": "launch-legacy-bucket-migration-sharded.sh",
    "canonical-migration-legacy-defi-": "launch-legacy-bucket-migration-sharded.sh",
    "canonical-migration-legacy-prediction-": "launch-legacy-bucket-migration-sharded.sh",
    "canonical-migration-legacy-sports-": "launch-legacy-bucket-migration-sharded.sh",
    # ── Sports v9 migration VMs (E4 — year-sharded fleet, both surfaces) ──
    "sports-v9-migration-": "launch-sports-v9-migration-vm.sh",
    # ── GCS migration bundle Phase 3 VMs (per-(ag, year)) ─────────────────
    "gcs-migration-bundle-cefi-": "launch-gcs-migration-bundle-vm.sh",
    "gcs-migration-bundle-defi-": "launch-gcs-migration-bundle-vm.sh",
    "gcs-migration-bundle-tradfi-": "launch-gcs-migration-bundle-vm.sh",
    "gcs-migration-bundle-sports-": "launch-gcs-migration-bundle-vm.sh",
    "gcs-migration-bundle-prediction-": "launch-gcs-migration-bundle-vm.sh",
    # ── MDPS sports bucket-pass ───────────────────────────────────────────
    "mdps-sports-bucket-": "launch-mdps-sports-bucket-vm.sh",
    # ── Live-pipeline VMs (per-AG live producer/consumer + singletons) ────
    # Consolidated launcher (2026-06-27): one VM runs all MVP CeFi shards.
    # Must appear BEFORE the generic "mtds-live-cefi-" entry (longest-prefix wins).
    "mtds-live-cefi-consolidated-": "launch-mtds-live-cefi-consolidated.sh",
    "mtds-live-cefi-": "launch-mtds-live.sh",
    "mtds-live-defi-": "launch-mtds-live.sh",
    "mtds-live-tradfi-": "launch-mtds-live.sh",
    "mtds-live-sports-": "launch-mtds-live.sh",
    "mtds-live-prediction-": "launch-mtds-live.sh",
    # launch-mtds-live.sh --test-run: bounded, test-bucket-routed live smoke check (plan
    # todo 16) — same launcher script, distinct VM-name prefix root (see vm_zombie_watchdog.py).
    "mtds-live-smoke-": "launch-mtds-live.sh",
    "mdps-features-live-cefi-": "launch-mdps-features-live.sh",
    "mdps-features-live-defi-": "launch-mdps-features-live.sh",
    "mdps-features-live-tradfi-": "launch-mdps-features-live.sh",
    "mdps-features-live-sports-": "launch-mdps-features-live.sh",
    "mdps-features-live-prediction-": "launch-mdps-features-live.sh",
    "features-xc-": "launch-features-cross-cutting.sh",
    "replay-": "launch-replay-cascade.sh",
    # ── DR drill VMs (scheduler/evidence — not a relaunch target) ─────────
    "disaster-drill-cron-": None,  # nightly chaos drill cron — scheduler-owned
    "dr-drill-cutover-": None,  # per-archetype evidence VM — one-off
    # ── Reserved live-/exp- allocation prefixes (no concrete launcher) ────
    "live-strategy-": None,  # reserved allocation prefix — no concrete launcher
    "live-execution-": None,  # reserved allocation prefix — no concrete launcher
    "live-mtds-": None,  # reserved allocation prefix — no concrete launcher
    "live-pbm-": None,  # reserved allocation prefix — no concrete launcher
    "live-risk-": None,  # reserved allocation prefix — no concrete launcher
    "live-alerting-": None,  # reserved allocation prefix — no concrete launcher
    "exp-ml-": None,  # reserved experiment prefix — run_id-embedded, no static launcher
    "exp-strategy-": None,  # reserved experiment prefix — run_id-embedded, no static launcher
    "exp-execution-": None,  # reserved experiment prefix — run_id-embedded, no static launcher
    # ── DeFi execution validation VMs (results.json, not a data backfill) ──
    "aave-lending-rate-val-": None,  # lending-rate validation — one-off, results.json
    "amm-golden-": None,  # AMM golden-fixture validation — per-shape one-off
    # ── Cutover dry-run / cron / one-off VMs (never a data backfill) ──────
    "wallet-treasury-cutover-": None,  # 24h cutover dry-run — one-off lifecycle demo
    "client-reporting-cutover-": None,  # 24h attribution cutover — one-off
    "qg-snapshot-": None,  # QG snapshot cron — scheduler-owned
    "batch-live-smoke-matrix-": None,  # smoke-matrix cron — scheduler-owned
    "tradfi-fwd-daily-cron-": None,  # cron HOST (spawns tradfi-fwd-* workers) — not itself a backfill
    "cefi-fwd-daily-cron-": None,  # cron HOST (spawns cefi-fwd-* workers) — not itself a backfill
    "bucket-rsync-": None,  # one-off bucket rsync — campaign cutover, no recurring launcher
    "vm-zombie-watchdog-": None,  # the watchdog itself — never a relaunch target
    "dm-": None,  # deploy-missing surgical VM — launched by deployment-api, not a recurring launcher
    "scenario-matrix-": "launch-scenario-runner-vm.sh",
    # ── Agent-orchestrator VMs (planning; per-epic REMOVED 2026-07-24) ────
    "agent-orch-planning-vm-": None,  # orchestrator VM — operator-owned lifecycle, never auto-relaunch
}


def resolve_launcher_for_vm(vm_name: str) -> str | None:
    """Resolve the relaunch launcher for ``vm_name`` by longest-prefix match.

    Returns the ``scripts/vm/launch-*.sh`` filename for the most-specific matching
    prefix in ``LAUNCHER_FOR_VM_PREFIX``, or ``None`` when the matching prefix is
    explicitly non-relaunchable OR no prefix matches (the fail-safe direction: the
    finding falls through to file_issue, never a wrong relaunch).

    Longest-prefix match (sorted by key length descending) mirrors
    ``vm_zombie_watchdog`` bucket resolution: ``cefi-fwd-daily-cron-`` wins over
    ``cefi-fwd-``.
    """
    for prefix in sorted(LAUNCHER_FOR_VM_PREFIX, key=len, reverse=True):
        if vm_name.startswith(prefix):
            return LAUNCHER_FOR_VM_PREFIX[prefix]
    return None


def launcher_path(launcher: str) -> Path:
    """Absolute path to a launcher filename under ``scripts/vm/`` (for the guard test)."""
    return _LAUNCHER_DIR / launcher
