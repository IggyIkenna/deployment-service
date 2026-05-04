#!/usr/bin/env bash
# Tier-3 CeFi backfill launcher — Bitfinex / Bitget / Kraken venues via Tardis.
#
# Phase 1 of cefi_venue_universe_expansion_2026_05_01.plan.md (initial 8-symbol
# fanout). 2026-05-04 expansion: top-25 perp universe per venue covers >90%
# of liquid volume per Tardis availableSymbols probe.
#
# Architecture isolation: each venue writes to dedicated GCS partitions
# (instruments-store-cefi/.../venue=BITFINEX-SPOT/, market-data-tick-cefi/.../
# venue=BITFINEX-SPOT/...). Existing venue paths untouched.
#
# Usage::
#
#   bash scripts/vm/launch-tier3-cefi-backfill.sh --instruments
#   bash scripts/vm/launch-tier3-cefi-backfill.sh --market-tick
#   bash scripts/vm/launch-tier3-cefi-backfill.sh --dry-run --market-tick

set -euo pipefail

DRY_RUN=false
DO_INSTRUMENTS=true
DO_MARKET_TICK=true
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)        DRY_RUN=true; shift ;;
        --instruments)    DO_MARKET_TICK=false; shift ;;
        --market-tick)    DO_INSTRUMENTS=false; shift ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
STARTUP="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"

# Tardis Machine symbol conventions (verified 2026-05-04 via api.tardis.dev/v1/exchanges/<id>):
#   bitfinex:              BTCUSD (NO `t` prefix; that's Bitfinex's own REST API convention)
#   bitfinex-derivatives:  BTCF0:USTF0 (F0 = perp marker, USTF0 = USDT-margined)
#   bitget (spot):         BTCUSDT
#   bitget-futures:        BTCUSDT (NO `_UMCBL` suffix; that's Bitget legacy V1 REST)
#   kraken:                XBT/USD (slashed; XBT not BTC)
#   cryptofacilities:      PI_XBTUSD (Kraken Futures = legacy Crypto Facilities, only ~10 PI_ symbols)
#
# Per-venue Tardis universe sizes (2026-05-04 probe):
#   bitfinex-derivatives: 82 perps    → top 24 listed below
#   bitget-futures: 806 perps         → top 24 listed (covers >90% volume)
#   cryptofacilities: 10 perps total  → all 10 listed
#   bitfinex (spot): 762 spots        → top 24 majors+stables
#   bitget (spot): 1329 spots         → top 24 majors
#   kraken (spot): 1854 spots         → top 24 majors

SYMBOLS_BITFINEX_SPOT="BTCUSD;ETHUSD;SOLUSD;XRPUSD;DOGEUSD;ADAUSD;AVAXUSD;LINKUSD;BNBUSD;LTCUSD;DOTUSD;UNIUSD;ATOUSD;FILUSD;XLMUSD;TRXUSD;ETCUSD;XTZUSD;COMPUSD;AAVEUSD;NEOUSD;ZECUSD;XAUTUSD;BCHUSD"
SYMBOLS_BITFINEX_FUTURES="BTCF0:USTF0;ETHF0:USTF0;BNBF0:USTF0;LTCF0:USTF0;SOLF0:USTF0;XRPF0:USTF0;AVAXF0:USTF0;TRXF0:USTF0;ETCF0:USTF0;DOTF0:USTF0;LINKF0:USTF0;ADAF0:USTF0;DOGEF0:USTF0;SHIBF0:USTF0;UNIF0:USTF0;XLMF0:USTF0;XTZF0:USTF0;XAUTF0:USTF0;COMPF0:USTF0;AAVEF0:USTF0;FILF0:USTF0;NEOF0:USTF0;ZECF0:USTF0;ATOF0:USTF0"
SYMBOLS_BITGET_SPOT="BTCUSDT;ETHUSDT;SOLUSDT;XRPUSDT;BNBUSDT;DOGEUSDT;ADAUSDT;AVAXUSDT;LINKUSDT;DOTUSDT;LTCUSDT;BCHUSDT;TRXUSDT;UNIUSDT;ATOMUSDT;FILUSDT;NEARUSDT;ARBUSDT;OPUSDT;APTUSDT;SUIUSDT;INJUSDT;TIAUSDT;ETCUSDT"
SYMBOLS_BITGET_FUTURES="BTCUSDT;ETHUSDT;SOLUSDT;XRPUSDT;BNBUSDT;DOGEUSDT;ADAUSDT;AVAXUSDT;LINKUSDT;DOTUSDT;LTCUSDT;BCHUSDT;TRXUSDT;UNIUSDT;ATOMUSDT;FILUSDT;NEARUSDT;ARBUSDT;OPUSDT;APTUSDT;SUIUSDT;INJUSDT;TIAUSDT;ETCUSDT"
SYMBOLS_KRAKEN_SPOT="XBT/USD;ETH/USD;SOL/USD;XRP/USD;XDG/USD;ADA/USD;AVAX/USD;LINK/USD;DOT/USD;LTC/USD;UNI/USD;ATOM/USD;FIL/USD;NEAR/USD;ARB/USD;OP/USD;APT/USD;SUI/USD;INJ/USD;TIA/USD;ETC/USD;TRX/USD;XLM/USD;BCH/USD"
# Kraken Futures (cryptofacilities) only has 10 PI_/PF_ perps total — list all.
SYMBOLS_KRAKEN_FUTURES="PI_XBTUSD;PI_ETHUSD;PI_LTCUSD;PI_XRPUSD;PF_USDTUSD;PI_DOGEUSD;PI_SOLUSD;PI_LINKUSD;PI_ADAUSD;PI_BCHUSD"

DATA_HEAVY="trades;book_snapshot_5"
DATA_LIGHT_PERPS="derivative_ticker;liquidations"

declare -A START_BY_VENUE=(
    [BITFINEX-SPOT]="2020-01-01"
    [BITFINEX-FUTURES]="2020-01-01"
    [BITGET-SPOT]="2024-11-08"
    [BITGET-FUTURES]="2024-11-08"
    [KRAKEN-SPOT]="2020-01-01"
    [KRAKEN-FUTURES]="2020-01-01"
)
declare -A SYMBOLS_BY_VENUE=(
    [BITFINEX-SPOT]="$SYMBOLS_BITFINEX_SPOT"
    [BITFINEX-FUTURES]="$SYMBOLS_BITFINEX_FUTURES"
    [BITGET-SPOT]="$SYMBOLS_BITGET_SPOT"
    [BITGET-FUTURES]="$SYMBOLS_BITGET_FUTURES"
    [KRAKEN-SPOT]="$SYMBOLS_KRAKEN_SPOT"
    [KRAKEN-FUTURES]="$SYMBOLS_KRAKEN_FUTURES"
)
declare -A MODE_BY_VENUE=(
    [BITFINEX-SPOT]="spot"
    [BITFINEX-FUTURES]="perps"
    [BITGET-SPOT]="spot"
    [BITGET-FUTURES]="perps"
    [KRAKEN-SPOT]="spot"
    [KRAKEN-FUTURES]="perps"
)

VENUES=(BITFINEX-SPOT BITFINEX-FUTURES BITGET-SPOT BITGET-FUTURES KRAKEN-SPOT KRAKEN-FUTURES)

create_vm() {
    local vm_name="$1" md="$2"
    if $DRY_RUN; then
        echo "[DRY-RUN] $vm_name"
        return
    fi
    echo "Launching $vm_name"
    gcloud compute instances create "$vm_name" \
        --project="$PROJECT" --zone="$ZONE" \
        --machine-type=e2-standard-4 \
        --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
        --boot-disk-size=50GB --scopes=cloud-platform \
        --metadata="startup-script-url=${STARTUP},${md}" \
        --labels=purpose=cefi-tardis-tier3 2>&1 | tail -1
    sleep 2
}

TODAY="$(date +%Y-%m-%d)"
RUN_TS="$(date +%Y%m%d-%H%M%S)"

if $DO_INSTRUMENTS; then
    echo ""
    echo "=== Phase 1: instruments-service for ${#VENUES[@]} new venues ==="
    for v in "${VENUES[@]}"; do
        vm_name="instr-${v,,}-${RUN_TS}"
        vm_name="${vm_name//_/-}"
        md="VM_TASK=instruments-smoke,VM_SERVICE=instruments_service,VM_OPERATION=download,VM_ASSET_GROUP=CEFI,VM_VENUE=${v},VM_START_DATE=${TODAY},VM_END_DATE=${TODAY},VM_SHUTDOWN_ON_COMPLETION=true"
        create_vm "$vm_name" "$md"
    done
fi

if $DO_MARKET_TICK; then
    echo ""
    echo "=== Phase 2: market-tick year-shards (heavy/light per venue) ==="
    for v in "${VENUES[@]}"; do
        start_year="${START_BY_VENUE[$v]:0:4}"
        symbols="${SYMBOLS_BY_VENUE[$v]}"
        mode="${MODE_BY_VENUE[$v]}"
        for year in $(seq "$start_year" "$(date +%Y)"); do
            local_start="${START_BY_VENUE[$v]}"
            year_start="${year}-01-01"
            if [[ "$year_start" < "$local_start" ]]; then year_start="$local_start"; fi
            year_end="${year}-12-31"
            if [[ "$year" == "$(date +%Y)" ]]; then year_end="$(date +%Y-%m-%d)"; fi

            run_ts2="$(date +%Y%m%d-%H%M%S)"
            vm_name="cefi-${v,,}-${year}-heavy-${run_ts2}"
            vm_name="${vm_name//_/-}"
            md="VM_TASK=cefi-backfill,VM_SERVICE=market_tick_data_service,VM_OPERATION=download,VM_ASSET_GROUP=CEFI,VM_VENUE=${v},VM_START_DATE=${year_start},VM_END_DATE=${year_end},VM_DATA_TYPES=${DATA_HEAVY},VM_INSTRUMENT_IDS=${symbols},VM_FORCE_WINDOW=true,VM_SHUTDOWN_ON_COMPLETION=true"
            create_vm "$vm_name" "$md"

            if [[ "$mode" == "perps" ]]; then
                run_ts3="$(date +%Y%m%d-%H%M%S)"
                vm_name="cefi-${v,,}-${year}-light-${run_ts3}"
                vm_name="${vm_name//_/-}"
                md="VM_TASK=cefi-backfill,VM_SERVICE=market_tick_data_service,VM_OPERATION=download,VM_ASSET_GROUP=CEFI,VM_VENUE=${v},VM_START_DATE=${year_start},VM_END_DATE=${year_end},VM_DATA_TYPES=${DATA_LIGHT_PERPS},VM_INSTRUMENT_IDS=${symbols},VM_FORCE_WINDOW=true,VM_SHUTDOWN_ON_COMPLETION=true"
                create_vm "$vm_name" "$md"
            fi
        done
    done
fi

echo ""
echo "Done."
