#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Tier-3 CeFi backfill launcher — Bitfinex / Bitget / Kraken venues via Tardis.
#
# Phase 1 of cefi_venue_universe_expansion_2026_05_01.md (initial 8-symbol
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
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)        DRY_RUN=true; shift ;;
        --instruments)    DO_MARKET_TICK=false; shift ;;
        --market-tick)    DO_INSTRUMENTS=false; shift ;;
        --env)            DEPLOYMENT_ENV="$2"; shift 2 ;;
        --on-demand)      ON_DEMAND=true; shift ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
STARTUP="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"

# Tardis concurrent-VM cap (HARD RULE, at most 1 — see the guard's header for the
# measured reason). Sourced here; enforced just before the Phase 2 launch loop, where
# the planned VM count is computable.
# shellcheck source=./tardis-concurrency-guard.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tardis-concurrency-guard.sh"

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
# bitfinex-derivatives: only 8 of the documented 24 perps return data on Tardis.
# 16 phantom symbols (BNBF0/LTCF0/TRXF0/ETCF0/DOTF0/SHIBF0/UNIF0/XLMF0/XTZF0/
# XAUTF0/COMPF0/AAVEF0/FILF0/NEOF0/ZECF0/ATOF0) verified via manifest probe
# 2026-05-04 — never any captured rows across 2020..today. Dropped to save quota.
SYMBOLS_BITFINEX_FUTURES="BTCF0:USTF0;ETHF0:USTF0;LINKF0:USTF0;ADAF0:USTF0;XRPF0:USTF0;AVAXF0:USTF0;DOGEF0:USTF0;SOLF0:USTF0"
SYMBOLS_BITGET_SPOT="BTCUSDT;ETHUSDT;SOLUSDT;XRPUSDT;BNBUSDT;DOGEUSDT;ADAUSDT;AVAXUSDT;LINKUSDT;DOTUSDT;LTCUSDT;BCHUSDT;TRXUSDT;UNIUSDT;ATOMUSDT;FILUSDT;NEARUSDT;ARBUSDT;OPUSDT;APTUSDT;SUIUSDT;INJUSDT;TIAUSDT;ETCUSDT"
SYMBOLS_BITGET_FUTURES="BTCUSDT;ETHUSDT;SOLUSDT;XRPUSDT;BNBUSDT;DOGEUSDT;ADAUSDT;AVAXUSDT;LINKUSDT;DOTUSDT;LTCUSDT;BCHUSDT;TRXUSDT;UNIUSDT;ATOMUSDT;FILUSDT;NEARUSDT;ARBUSDT;OPUSDT;APTUSDT;SUIUSDT;INJUSDT;TIAUSDT;ETCUSDT"
SYMBOLS_KRAKEN_SPOT="XBT/USD;ETH/USD;SOL/USD;XRP/USD;XDG/USD;ADA/USD;AVAX/USD;LINK/USD;DOT/USD;LTC/USD;UNI/USD;ATOM/USD;FIL/USD;NEAR/USD;ARB/USD;OP/USD;APT/USD;SUI/USD;INJ/USD;TIA/USD;ETC/USD;TRX/USD;XLM/USD;BCH/USD"
# cryptofacilities: only 3 of the documented PI_/PF_ perps return data on
# Tardis (BTC, ETH, XRP). 8 phantom (PI_LTCUSD, PI_BCHUSD, PF_USDTUSD,
# PI_DOGEUSD, PI_SOLUSD, PI_LINKUSD, PI_ADAUSD, PI_AVAXUSD) verified via
# manifest probe 2026-05-04 — never any captured rows. cryptofacilities is
# a 2018-era legacy feed Tardis never expanded; canonical Kraken Futures uses
# a different REST API not fully archived.
SYMBOLS_KRAKEN_FUTURES="PI_XBTUSD;PI_ETHUSD;PI_XRPUSD"

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

# Per-symbol listing dates (empirical first_captured_date from manifest probe
# 2026-05-04). Symbols absent from this map default to their venue START_BY_VENUE.
# Used by filter_symbols_for_year() below to drop symbols whose listing year is
# in the future relative to the shard year — avoids burning Tardis quota on
# pre-listing 404s that the audit reclassifier would just flip to empty_confirmed.
declare -A SYMBOL_LISTING_DATE=(
    # bitfinex-derivatives
    [BTCF0:USTF0]="2020-01-01"
    [ETHF0:USTF0]="2020-01-01"
    [LINKF0:USTF0]="2021-01-01"
    [ADAF0:USTF0]="2021-02-03"
    [XRPF0:USTF0]="2022-01-01"
    [AVAXF0:USTF0]="2022-01-01"
    [DOGEF0:USTF0]="2022-01-01"
    [SOLF0:USTF0]="2022-01-01"
    # cryptofacilities (real perps only — phantoms already removed from list)
    [PI_XBTUSD]="2020-01-01"
    [PI_ETHUSD]="2020-01-01"
    [PI_XRPUSD]="2020-01-01"
)

# Filter a semicolon-joined symbol list to those whose listing year <= target year.
# Symbols missing from SYMBOL_LISTING_DATE are passed through unchanged (Bitget,
# Kraken-Spot, Bitfinex-Spot have whole-venue START_BY_VENUE only).
filter_symbols_for_year() {
    local symbols_csv="$1" year="$2"
    local out=()
    local IFS=';'
    for s in $symbols_csv; do
        local listing="${SYMBOL_LISTING_DATE[$s]:-}"
        if [[ -z "$listing" ]]; then
            out+=("$s")
            continue
        fi
        local listing_year="${listing:0:4}"
        if (( year >= listing_year )); then
            out+=("$s")
        fi
    done
    local result=""
    local first=true
    for s in "${out[@]}"; do
        if $first; then result="$s"; first=false; else result="${result};${s}"; fi
    done
    echo "$result"
}

# Default = all 6 Tier-3 CeFi venues. Override via ONLY_VENUES env var to limit
# scope (e.g. resume after partial-failure / target a single venue):
#   ONLY_VENUES="BITGET-SPOT BITGET-FUTURES KRAKEN-SPOT KRAKEN-FUTURES" \
#     MACHINE_TYPE=e2-highmem-8 \
#     bash launch-tier3-cefi-backfill.sh --market-tick
#
# Default machine type: e2-highmem-2 (16 GB / $0.10/hr). Streaming-finalize
# (MTDS f07f3f9 default-on 2026-05-06) keeps Tardis peak RSS bounded by one
# pyarrow-CSV row-group (~100-300 MB on smoke). 16 GB is comfortable for
# anything except Coinbase BTC-USD book_snapshot_5 heavy days; bump to
# e2-highmem-8 (32 GB) only for those.
if [[ -n "${ONLY_VENUES:-}" ]]; then
    # shellcheck disable=SC2206
    VENUES=($ONLY_VENUES)
else
    VENUES=(BITFINEX-SPOT BITFINEX-FUTURES BITGET-SPOT BITGET-FUTURES KRAKEN-SPOT KRAKEN-FUTURES)
fi

create_vm() {
    local vm_name="$1" md="$2"
    if $DRY_RUN; then
        echo "[DRY-RUN] $vm_name"
        return
    fi
    # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
    PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
    if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi
    echo "Launching $vm_name (machine=${MACHINE_TYPE:-e2-highmem-8}) [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
    # shellcheck disable=SC2086
    gcloud compute instances create "$vm_name" \
        --project="$PROJECT" --zone="$ZONE" \
        --machine-type="${MACHINE_TYPE:-e2-highmem-8}" \
        ${PROVISIONING_FLAGS} \
        --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
        --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" --scopes=cloud-platform \
        --metadata="startup-script-url=${STARTUP},${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}" \
        --labels=purpose=cefi-tier3-fullbackfill,env="${DEPLOYMENT_ENV}" 2>&1 | tail -1
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

    # ─── Tardis concurrent-VM cap (HARD RULE: operator 2026-07-16, at most 1) ──────────
    # Phase 2 VMs run `market_tick_data_service --operation download --asset-group CEFI`
    # against Tardis venues (BITFINEX/BITGET/KRAKEN), so they are Tardis consumers and
    # MUST be counted. This launcher's venue x year fan-out asks for far more than 1 VM,
    # so the guard will normally REFUSE here — that is the intended outcome, not a bug:
    # N>1 on the single Tardis key measured ~94% 403s + 37,212 FALSE attempted_failed
    # rows (manifest CORRUPTION) + coverage going BACKWARD. To backfill many venues under
    # the cap, use `launch-cefi-sharded-backfill.sh` with SINGLE_VM_QUEUE=1, which bundles
    # the shards into ONE VM and scales via TARDIS_MAX_CONCURRENT_DOWNLOADS instead.
    # Only FORCE=1 (operator override) downgrades refusal to a warning.
    # Planned count mirrors the launch loop's own symbol filter so the guard sees truth.
    PLANNED_TARDIS_VMS=0
    for v in "${VENUES[@]}"; do
        _sy="${START_BY_VENUE[$v]:0:4}"
        for year in $(seq "$_sy" "$(date +%Y)"); do
            [[ -z "$(filter_symbols_for_year "${SYMBOLS_BY_VENUE[$v]}" "$year")" ]] && continue
            PLANNED_TARDIS_VMS=$((PLANNED_TARDIS_VMS + 1))
            [[ "${MODE_BY_VENUE[$v]}" == "perps" ]] && PLANNED_TARDIS_VMS=$((PLANNED_TARDIS_VMS + 1))
        done
    done
    tardis_concurrency_guard "$PLANNED_TARDIS_VMS" "$ZONE" "$PROJECT" || exit 1

    for v in "${VENUES[@]}"; do
        start_year="${START_BY_VENUE[$v]:0:4}"
        venue_symbols="${SYMBOLS_BY_VENUE[$v]}"
        mode="${MODE_BY_VENUE[$v]}"
        for year in $(seq "$start_year" "$(date +%Y)"); do
            local_start="${START_BY_VENUE[$v]}"
            year_start="${year}-01-01"
            if [[ "$year_start" < "$local_start" ]]; then year_start="$local_start"; fi
            year_end="${year}-12-31"
            if [[ "$year" == "$(date +%Y)" ]]; then year_end="$(date +%Y-%m-%d)"; fi

            # Drop pre-listing symbols for this year.
            symbols="$(filter_symbols_for_year "$venue_symbols" "$year")"
            if [[ -z "$symbols" ]]; then
                echo "  skip ${v} ${year} — no symbols listed by then"
                continue
            fi

            # No VM_FORCE_WINDOW: MTDS resumes from manifest, skipping
            # already-captured / empty_confirmed shards. Failed shards retry.
            run_ts2="$(date +%Y%m%d-%H%M%S)"
            vm_name="cefi-${v,,}-${year}-heavy-${run_ts2}"
            vm_name="${vm_name//_/-}"
            md="VM_TARDIS_CONSUMER=1,VM_TASK=cefi-backfill,VM_SERVICE=market_tick_data_service,VM_OPERATION=download,VM_ASSET_GROUP=CEFI,VM_VENUE=${v},VM_START_DATE=${year_start},VM_END_DATE=${year_end},VM_DATA_TYPES=${DATA_HEAVY},VM_INSTRUMENT_IDS=${symbols},VM_FORCE=${VM_FORCE:-false},VM_SHUTDOWN_ON_COMPLETION=true"
            create_vm "$vm_name" "$md"

            if [[ "$mode" == "perps" ]]; then
                run_ts3="$(date +%Y%m%d-%H%M%S)"
                vm_name="cefi-${v,,}-${year}-light-${run_ts3}"
                vm_name="${vm_name//_/-}"
                md="VM_TARDIS_CONSUMER=1,VM_TASK=cefi-backfill,VM_SERVICE=market_tick_data_service,VM_OPERATION=download,VM_ASSET_GROUP=CEFI,VM_VENUE=${v},VM_START_DATE=${year_start},VM_END_DATE=${year_end},VM_DATA_TYPES=${DATA_LIGHT_PERPS},VM_INSTRUMENT_IDS=${symbols},VM_FORCE=${VM_FORCE:-false},VM_SHUTDOWN_ON_COMPLETION=true"
                create_vm "$vm_name" "$md"
            fi
        done
    done
fi

echo ""
echo "Done."
