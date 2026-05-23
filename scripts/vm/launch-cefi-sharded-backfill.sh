#!/usr/bin/env bash
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch sharded CeFi + TradFi Tardis backfill VMs.
#
# Operator instrument filter (authoritative, 2026-04-19):
#   CeFi:
#     BTC + ETH  → spot + futures + perps + options (options DERIBIT only)
#     Selected x-coins (SOL XRP BNB DOGE ADA AVAX LINK) → spot + perps + futures
#     (NO options on x-coins — saves Tardis cost)
#     Options ONLY on DERIBIT.
#   TradFi:
#     CME ES (S&P 500 E-mini) futures + options
#     CBOE VIX futures + options
#     Combos (calendar spreads / iron condors / butterflies on ES/VIX)
#     Nothing else — no equities, no rates, no commodities.
#
# Why this matters: without --instrument-ids, MTDS downloads the FULL symbol
# universe per venue from Tardis. Binance-Futures alone has ~1000+ instruments;
# Binance-Spot >2000. One unfiltered week-year shard ≈ hundreds of $$ in Tardis
# API cost. Per-venue symbol lists below are hand-curated from adapter symbol
# conventions + operator's selected coin list.
#
# Symbol conventions per venue (verified from:
#   unified-api-contracts/unified_api_contracts/registry/venue_mapping.py
#   market-tick-data-service/market_tick_data_service/market_interface/adapters/
# ):
#   BINANCE-SPOT      → lowercase concatenated (btcusdt, ethusdt, solusdt)
#   BINANCE-FUTURES   → uppercase concatenated USDT-M perps (BTCUSDT, ETHUSDT)
#   BYBIT             → uppercase linear perps (BTCUSDT, ETHUSDT)
#   DERIBIT           → base currency prefix; Tardis expands chain globs
#                       server-side for options (BTC, ETH, BTC-PERPETUAL,
#                       ETH-PERPETUAL)
#   COINBASE-SPOT     → dash-separated (BTC-USD, ETH-USD)
#   OKX-SPOT          → dash-separated (BTC-USDT, ETH-USDT)
#   OKX-SWAP          → SWAP suffix (BTC-USDT-SWAP, ETH-USDT-SWAP)
#   OKX-FUTURES       → date-suffixed quarterlies — we skip (too many permutations,
#                       perps are the liquid product)
#   HYPERLIQUID       → perp-only, base symbol (BTC, ETH)
#   UPBIT             → KRW-prefixed (KRW-BTC, KRW-ETH)
#
# TradFi via Tardis (CBOE + CME carried by Tardis):
#   cme-futures       → ESM26, ESU26 quarterly contracts (we keep 2 quarters/yr)
#   cme-options       → Tardis expands — pass base ES with prefix glob
#   cboe-vix-futures  → VXM26, VXU26
#   cboe-vix-options  → VX base
#
# Observability (MUST match):
#   - startup-script-url=gs://deployment-scripts.../vm/setup-data-pipeline-vm.sh
#   - VM_TASK=cefi-backfill → routes through setup-data-pipeline-vm.sh line 424
#     elif branch which assembles --operation/--mode/--asset-group/--venues/... CLI
#   - VM_INSTRUMENT_IDS comma-separated (setup script expands to space-sep
#     --instrument-ids nargs='+' argparse)
#   - heartbeat + watchdog are in the GCS tee wrapper; nothing to add here
#
# Dry-run:  DRY_RUN=1 bash scripts/vm/launch-cefi-sharded-backfill.sh
set -e

PROJECT=central-element-323112
# Zone: asia-northeast1-c (matches launch-mdps-backfill-vm.sh,
# launch-canonical-migration-vm.sh, launch-features-backfill-vm.sh).
# Previous -b was a typo; Tokyo -b has fewer e2-standard-4 instances available.
ZONE=asia-northeast1-c
STARTUP=gs://deployment-scripts-central-element-323112/vm/setup-data-pipeline-vm.sh

DRY_RUN="${DRY_RUN:-0}"
FORCE="${FORCE:-0}"
MAX_CONCURRENT="${MAX_CONCURRENT:-15}"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# Parse --env (Phase 0f env-tier targeting per bucket-naming SSOT). The legacy
# behavior (no CLI args, env-var overrides only) is preserved — only --env is
# accepted on the command line. Other config still flows via env vars
# (DRY_RUN / FORCE / MACHINE_TYPE_HEAVY / etc.) per the comment block above.
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "ERROR: unknown flag '$1' (only --env is supported; other config via env vars)" >&2; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# ─── Singleton lock: shared Tardis account + project egress NAT ──────────────
# This launcher fans out across CeFi venues (Tardis) AND TradFi (Tardis-carried
# CME/CBOE futures+options). Tardis enforces per-account rate limits that bite
# under concurrent-VM load — and the Databento path (NASDAQ/NYSE ETFs via
# launch-tradfi-backfill-vm.sh) is the same shape: the 2026-05-05 silent-drop
# incident was 7+ MTDS VMs hitting a shared provider account and exhausting
# the adapter retry budget, which the per-schema loop in download_batch_df
# masked as "captured" with zero rows.
#
# VMs created by this launcher follow the unique shape
#   {cefi|tradfi}-{venue|instrument}-{year}-{heavy|light}-{RUN_TS}
# so we lock on the trailing `-{heavy|light}-` segment, which scopes the check
# to PRIOR sharded-backfill invocations (and never collides with single-shot
# launchers like `tradfi-bf-*` or `tradfi-instr-*`).
#
# Bypass: FORCE=1 bash launch-cefi-sharded-backfill.sh
if [[ "$FORCE" != "1" && "$DRY_RUN" != "1" ]]; then
  EXISTING="$(gcloud compute instances list \
      --filter='name~"^(cefi|tradfi)-.*-(heavy|light)-" AND status=RUNNING' \
      --zones="$ZONE" \
      --project="$PROJECT" \
      --format='value(name)' 2>/dev/null | head -3)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: cefi-sharded-backfill VMs already running in $ZONE:
$EXISTING

Refusing to launch a duplicate sharded backfill — Tardis rate-limits per-account
and the project egress NAT is shared. Concurrent runs collide on 429 backoffs;
the adapter retry budget can exhaust silently (see 2026-05-05 silent-drop
incident: 1004 MES/BTC/ETH/ES (root, date) pairs lost to this exact pattern
on the bundled Databento ohlcv_1m;trades CME parent symbology).

Options:
  Inspect:   gcloud compute instances list --filter='name~"^(cefi|tradfi)-.*-(heavy|light)-"' --zones=$ZONE
  Tail logs: gsutil cat gs://deployment-scripts-${PROJECT}/vm-logs/<vm-name>/run.log
  Stop all:  gcloud compute instances list --filter='name~"^(cefi|tradfi)-.*-(heavy|light)-"' --zones=$ZONE --format='value(name)' | xargs -I{} gcloud compute instances delete {} --zone=$ZONE --quiet
  Force:     FORCE=1 bash $0
EOF
    exit 1
  fi
fi

# ─── Per-venue symbol universes ──────────────────────────────────────────────
# Keep comma-separated (VM metadata format). Order: majors first, x-coins after.
#
# Operator's "selected x-coins": SOL XRP BNB DOGE ADA AVAX LINK
#   (BTC + ETH already counted as majors)

# BINANCE-SPOT: lowercase concatenated USDT pairs
SYMBOLS_BINANCE_SPOT="btcusdt;ethusdt;solusdt;xrpusdt;bnbusdt;dogeusdt;adausdt;avaxusdt;linkusdt"

# BINANCE-FUTURES: uppercase USDT-M perps (same base set)
SYMBOLS_BINANCE_FUTURES="BTCUSDT;ETHUSDT;SOLUSDT;XRPUSDT;BNBUSDT;DOGEUSDT;ADAUSDT;AVAXUSDT;LINKUSDT"

# BYBIT: uppercase linear perps
SYMBOLS_BYBIT="BTCUSDT;ETHUSDT;SOLUSDT;XRPUSDT;BNBUSDT;DOGEUSDT;ADAUSDT;AVAXUSDT;LINKUSDT"

# DERIBIT: options + perps. Majors only — BTC/ETH are all Deribit really trades.
# Tardis supports chain globs server-side when passed the base currency.
SYMBOLS_DERIBIT="BTC;ETH;BTC-PERPETUAL;ETH-PERPETUAL"

# COINBASE-SPOT: dash USD pairs. BNB not listed on Coinbase → omitted.
SYMBOLS_COINBASE_SPOT="BTC-USD;ETH-USD;SOL-USD;XRP-USD;DOGE-USD;ADA-USD;AVAX-USD;LINK-USD"

# OKX-SPOT: dash USDT pairs
SYMBOLS_OKX_SPOT="BTC-USDT;ETH-USDT;SOL-USDT;XRP-USDT;BNB-USDT;DOGE-USDT;ADA-USDT;AVAX-USDT;LINK-USDT"

# OKX-SWAP: perps
SYMBOLS_OKX_SWAP="BTC-USDT-SWAP;ETH-USDT-SWAP;SOL-USDT-SWAP;XRP-USDT-SWAP;BNB-USDT-SWAP;DOGE-USDT-SWAP;ADA-USDT-SWAP;AVAX-USDT-SWAP;LINK-USDT-SWAP"

# HYPERLIQUID: perp-only
SYMBOLS_HYPERLIQUID="BTC;ETH;SOL;XRP;BNB;DOGE;ADA;AVAX;LINK"

# UPBIT: KRW-prefixed. BNB not on Upbit.
SYMBOLS_UPBIT="KRW-BTC;KRW-ETH;KRW-SOL;KRW-XRP;KRW-DOGE;KRW-ADA;KRW-AVAX;KRW-LINK"

# ─── Per-venue data-type splits ──────────────────────────────────────────────
# Heavy = order flow (trades + book_snapshot_5). Memory/CPU bound.
# Light = periodic/derived (derivative_ticker, liquidations, chains).
# Options venue (DERIBIT) adds options_chain to light.
DATA_HEAVY="trades;book_snapshot_5"
DATA_LIGHT_PERPS="derivative_ticker;liquidations;futures_chain"
DATA_LIGHT_DERIBIT="derivative_ticker;options_chain;futures_chain"

# TradFi symbols. ES = S&P 500 E-mini (CME). VIX futures ticker = VX (CFE).
# Two quarterly contracts per year is plenty for backtest; skipping serials.
SYMBOLS_CME_ES_2026="ESM26;ESU26;ESZ26"
SYMBOLS_CME_ES_2025="ESM25;ESU25;ESZ25"
SYMBOLS_CME_ES_2024="ESM24;ESU24;ESZ24"
SYMBOLS_CBOE_VIX_2026="VXM26;VXU26;VXZ26;VX"
SYMBOLS_CBOE_VIX_2025="VXM25;VXU25;VXZ25;VX"
SYMBOLS_CBOE_VIX_2024="VXM24;VXU24;VXZ24;VX"

# ─── Launcher ────────────────────────────────────────────────────────────────
_launched=0
_concurrent=0

_stagger() {
  # Stagger VM creation by 3s so RUN_TS timestamps don't collide across shards
  # (canonical-migration had this bug 2026-04-18 — several VMs shared RUN_TS
  # and clobbered each other's GCS progress files).
  sleep 3
}

_batch_guard() {
  _concurrent=$((_concurrent + 1))
  if (( _concurrent >= MAX_CONCURRENT )); then
    echo "  ... reached MAX_CONCURRENT=$MAX_CONCURRENT, waiting 10s before next batch"
    sleep 10
    _concurrent=0
  fi
}

launch_cefi_shard() {
  local venue="$1"         # canonical (BINANCE-FUTURES, DERIBIT, etc.)
  local year="$2"
  local group="$3"         # heavy | light
  local data_types="$4"
  local symbols="$5"

  local start_date end_date machine
  if [[ "$year" == "2026" ]]; then
    start_date="${year}-01-01"
    end_date="2026-05-22"
  else
    start_date="${year}-01-01"
    end_date="${year}-12-31"
  fi

  # Machine types — heavy groups (trades + book_snapshot_5 for 9 symbols).
  # 2026-04-22 P2.B + P2.B follow-up: after killing ``small_frames`` accumulation
  # in TardisAdapter.download_batch (MTDS 5ec195f) + removing the legacy
  # PartitionedTickWriter dual-write path (same commit) + pre-flight
  # granularity fix (MTDS ab6338b) that filters to only missing data_types
  # per venue/date, heavy-profile peak RSS dropped from >8 GB (OS-OOM on
  # e2-standard-2) to ~500 MB on the same smoke (cefi-smoke-p2b-20260422,
  # BINANCE-FUTURES 2026-04-18 BTCUSDT+ETHUSDT trades+book_snapshot_5 =
  # 8.66M records, ran to completion rc=0). Downgraded heavy profile from
  # e2-highmem-4 (32 GB, $0.27/hr) to e2-standard-2 (8 GB, $0.07/hr) — a
  # 4x cost reduction on 95 VMs × expected 24-48 h runtime.
  #
  # 2026-05-06: bumped heavy default from e2-standard-2 (8 GB) to
  # e2-highmem-2 (16 GB) post-streaming-finalize ship (MTDS f07f3f9 default-on).
  # AVAX-USD smoke peaked at 1.04 GB; BTC-USD heavy days scale ~10-20×.
  # 2026-05-22: bumped heavy from e2-highmem-2 (16 GB) to e2-highmem-4 (32 GB)
  # after systematic OOM failures across BINANCE-FUTURES/SPOT/BYBIT 2021+;
  # 2021 bull-market days scale ~20-30× from AVAX baseline → 20-30 GB peaks.
  # Light bumped from e2-standard-2 (8 GB) to e2-highmem-2 (16 GB) after
  # BYBIT-2021-light OOM on 8 GB. DERIBIT light (options_chain) uses e2-highmem-4
  # (32 GB) — options_chain 2026 data is multi-GB per day.
  # 2026-05-23: bumped heavy default from e2-highmem-4 (32 GB) to e2-highmem-8
  # (64 GB). shard_memory_profile.py recommends e2-highmem-8 for any DERIBIT /
  # BINANCE-FUTURES / BYBIT / OKX-SWAP heavy book_snapshot_5 shards (38 GB peak
  # observed on DERIBIT BTC-PERPETUAL + BTC-options_chain expiry days). 2024
  # bull-market BINANCE-SPOT data is also 5-10× larger than the 2022-calibrated
  # 18 MB profile, causing constant 30s memory-pressure pauses on 32 GB that
  # double backfill runtime. e2-highmem-8 ($0.54/hr) at 64 GB eliminates pauses;
  # the 2× cost is dominated by VM count × runtime reduction.
  # Override: MACHINE_TYPE_HEAVY / MACHINE_TYPE_LIGHT.
  if [[ "$group" == "heavy" ]]; then
    machine="${MACHINE_TYPE_HEAVY:-e2-highmem-8}"
  elif [[ "$venue" == "DERIBIT" ]]; then
    machine="${MACHINE_TYPE_LIGHT:-e2-highmem-4}"
  else
    machine="${MACHINE_TYPE_LIGHT:-e2-highmem-2}"
  fi

  # ONLY="venue1:year1:group1 venue2:year2:group2 ..." filters to specific
  # combos. Used by relaunch-OOM workflow to retry only the dead heavy VMs
  # at higher memory, e.g.:
  #   ONLY="BINANCE-SPOT:2026:heavy DERIBIT:2026:heavy" \
  #     MACHINE_TYPE_HEAVY=e2-highmem-8 FORCE=1 \
  #     bash launch-cefi-sharded-backfill.sh
  if [[ -n "${ONLY:-}" ]]; then
    local key="${venue}:${year}:${group}"
    local matched=0
    for tok in $ONLY; do
      [[ "$tok" == "$key" ]] && matched=1 && break
    done
    [[ $matched -eq 0 ]] && return 0
  fi

  local venue_lower
  venue_lower=$(echo "$venue" | tr '[:upper:]' '[:lower:]')
  # RUN_TS suffix per workspace VM naming convention — sortable, unique across
  # re-launches so the orchestrator can write to its own per-VM manifest shard.
  local vm_name="cefi-${venue_lower}-${year}-${group}-${RUN_TS}"

  local meta="startup-script-url=$STARTUP"
  meta+=",VM_TASK=cefi-backfill"
  meta+=",VM_SERVICE=market_tick_data_service"
  meta+=",VM_OPERATION=download"
  meta+=",VM_ASSET_GROUP=CEFI"
  meta+=",VM_VENUE=$venue"
  meta+=",VM_START_DATE=$start_date"
  meta+=",VM_END_DATE=$end_date"
  meta+=",VM_DATA_TYPES=$data_types"
  meta+=",VM_INSTRUMENT_IDS=$symbols"
  meta+=",VM_FORCE=${VM_FORCE:-false}"
  meta+=",DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  # 2026-05-01: opt-in auto-delete after task completion (read by
  # vm-exec-with-gcs-tee.sh:253). Without this, one-shot backfill VMs sat
  # RUNNING idle after rc!=0 (or even rc==0) until manually killed — cost leak.
  meta+=",VM_SHUTDOWN_ON_COMPLETION=true"

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[DRY-RUN] $vm_name  venue=$venue year=$year group=$group"
    echo "          data_types=$data_types"
    echo "          symbols=$symbols"
    echo "          metadata=$meta"
  else
    echo "Launching $vm_name ($venue $year $group)"
    # Metadata passed as single --metadata arg to avoid shell interpretation
    # of ';' inside VM_DATA_TYPES / VM_INSTRUMENT_IDS values.
    gcloud compute instances create "$vm_name" \
      --zone="$ZONE" --machine-type="$machine" \
      --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
      --scopes=cloud-platform --metadata="$meta" \
      --labels=env="${DEPLOYMENT_ENV}" \
      --project="$PROJECT" --async 2>&1 | tail -1 &
    _stagger
    _batch_guard
  fi
  _launched=$((_launched + 1))
}

launch_tradfi_shard() {
  local instrument="$1"    # es | vix
  local venue="$2"         # CME-FUTURES | CBOE-VIX-FUTURES (metadata tag)
  local year="$3"
  local group="$4"
  local data_types="$5"
  local symbols="$6"

  local start_date end_date machine
  if [[ "$year" == "2026" ]]; then
    start_date="${year}-01-01"
    end_date="2026-05-22"
  else
    start_date="${year}-01-01"
    end_date="${year}-12-31"
  fi
  machine="${MACHINE_TYPE_TRADFI:-e2-standard-2}"  # TradFi is lighter than CeFi book flow

  # ONLY filter — same shape as launch_cefi_shard (venue:year:group).
  if [[ -n "${ONLY:-}" ]]; then
    local key="${instrument}:${year}:${group}"
    local matched=0
    for tok in $ONLY; do
      [[ "$tok" == "$key" ]] && matched=1 && break
    done
    [[ $matched -eq 0 ]] && return 0
  fi

  local vm_name="tradfi-${instrument}-${year}-${group}-${RUN_TS}"

  local meta="startup-script-url=$STARTUP"
  meta+=",VM_TASK=cefi-backfill"   # same routing branch — triggers MTDS CLI
  meta+=",VM_SERVICE=market_tick_data_service"
  meta+=",VM_OPERATION=download"
  meta+=",VM_ASSET_GROUP=TRADFI"
  meta+=",VM_VENUE=$venue"
  meta+=",VM_START_DATE=$start_date"
  meta+=",VM_END_DATE=$end_date"
  meta+=",VM_DATA_TYPES=$data_types"
  meta+=",VM_INSTRUMENT_IDS=$symbols"
  meta+=",VM_FORCE=${VM_FORCE:-false}"
  meta+=",DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  # 2026-05-01: opt-in auto-delete after task completion (read by
  # vm-exec-with-gcs-tee.sh:253).
  meta+=",VM_SHUTDOWN_ON_COMPLETION=true"

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[DRY-RUN] $vm_name  venue=$venue year=$year group=$group"
    echo "          data_types=$data_types"
    echo "          symbols=$symbols"
    echo "          metadata=$meta"
  else
    echo "Launching $vm_name ($venue $year $group)"
    gcloud compute instances create "$vm_name" \
      --zone="$ZONE" --machine-type="$machine" \
      --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
      --scopes=cloud-platform --metadata="$meta" \
      --labels=env="${DEPLOYMENT_ENV}" \
      --project="$PROJECT" --async 2>&1 | tail -1 &
    _stagger
    _batch_guard
  fi
  _launched=$((_launched + 1))
}

# ─── CeFi sharding — year-per-shard × heavy/light group ──────────────────────
# Binance (spot + futures) has the longest history; Deribit from 2020; others
# varying. Years tuned to venue data availability (per UAC venue_start_dates).

# BINANCE-FUTURES: 2020-2026 heavy/light
for year in 2020 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "BINANCE-FUTURES" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_BINANCE_FUTURES"
  launch_cefi_shard "BINANCE-FUTURES" "$year" "light" "$DATA_LIGHT_PERPS" "$SYMBOLS_BINANCE_FUTURES"
done

# BINANCE-SPOT: 2020-2026 heavy only (spot has no derivative_ticker/liquidations)
for year in 2020 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "BINANCE-SPOT" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_BINANCE_SPOT"
done

# BYBIT: 2021-2026 (older data thin on Tardis)
for year in 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "BYBIT" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_BYBIT"
  launch_cefi_shard "BYBIT" "$year" "light" "$DATA_LIGHT_PERPS" "$SYMBOLS_BYBIT"
done

# DERIBIT: 2020-2026 — ONLY venue for options. futures+options+perps chain.
for year in 2020 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "DERIBIT" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_DERIBIT"
  launch_cefi_shard "DERIBIT" "$year" "light" "$DATA_LIGHT_DERIBIT" "$SYMBOLS_DERIBIT"
done

# COINBASE-SPOT: 2020-2026 spot only
for year in 2020 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "COINBASE-SPOT" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_COINBASE_SPOT"
done

# OKX-SPOT: 2021-2026
for year in 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "OKX-SPOT" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_OKX_SPOT"
done

# OKX-SWAP: 2021-2026 — perps (OKX-FUTURES dated quarterly contracts skipped,
# too many permutations for the operator's current scope)
for year in 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "OKX-SWAP" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_OKX_SWAP"
  launch_cefi_shard "OKX-SWAP" "$year" "light" "$DATA_LIGHT_PERPS" "$SYMBOLS_OKX_SWAP"
done

# HYPERLIQUID: 2024-2026 (launched 2023, meaningful data from 2024)
for year in 2024 2025 2026; do
  launch_cefi_shard "HYPERLIQUID" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_HYPERLIQUID"
  launch_cefi_shard "HYPERLIQUID" "$year" "light" "$DATA_LIGHT_PERPS" "$SYMBOLS_HYPERLIQUID"
done

# UPBIT: 2022-2026 (KRW market, spot only)
for year in 2022 2023 2024 2025 2026; do
  launch_cefi_shard "UPBIT" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_UPBIT"
done

# ─── TradFi — CME ES + CBOE VIX only ─────────────────────────────────────────
# ES futures list goes back decades on Tardis but operator's signal window is
# recent vol regimes → 2024-2026 is the meaningful window.
# VIX futures same window.
for year in 2024 2025 2026; do
  case "$year" in
    2026) es_syms="$SYMBOLS_CME_ES_2026"; vix_syms="$SYMBOLS_CBOE_VIX_2026" ;;
    2025) es_syms="$SYMBOLS_CME_ES_2025"; vix_syms="$SYMBOLS_CBOE_VIX_2025" ;;
    2024) es_syms="$SYMBOLS_CME_ES_2024"; vix_syms="$SYMBOLS_CBOE_VIX_2024" ;;
  esac

  # ES futures + options (combos inferred from same contracts — calendar
  # spreads / butterflies reconstructed by features-delta-one, not fetched).
  launch_tradfi_shard "es" "CME-FUTURES" "$year" "futures" "trades;book_snapshot_5" "$es_syms"
  launch_tradfi_shard "es" "CME-OPTIONS" "$year" "options" "trades;options_chain" "$es_syms"

  # VIX futures + options.
  launch_tradfi_shard "vix" "CBOE-VIX-FUTURES" "$year" "futures" "trades;book_snapshot_5" "$vix_syms"
  launch_tradfi_shard "vix" "CBOE-VIX-OPTIONS" "$year" "options" "trades;options_chain" "$vix_syms"
done

wait
if [[ "$DRY_RUN" == "1" ]]; then
  echo ""
  echo "=========================================="
  echo "DRY-RUN: $_launched VMs would be launched"
  echo "=========================================="
else
  echo "All $_launched VMs launched"
fi
