#!/usr/bin/env bash
# Epic: tradfi_master
# Lifecycle: permanent
# Launch FX spot-pair OHLCV-24h (daily) backfill VMs (one per year-shard).
#
# Universe: FX_SPOT_PAIRS from `unified_api_contracts.registry` — the MTDS
# download path routes `VM_VENUE=FX` to the Yahoo Finance daily adapter
# (`_fetch_yahoo_fx` in umi_tick_provider), which iterates EVERY FX spot pair
# itself, so no client-side instrument list / --source is needed (FX bypasses
# the databento/massive source branch; venue=FX → Yahoo daily ohlcv_24h).
#
# Source: yahoo_finance (daily OHLCV). Databento/Massive do NOT carry FX spot
# pairs (e.g. USD/KRW) — Massive's flat-files cover global_forex but the
# operator chose Yahoo daily for FX; this launcher uses the Yahoo venue-route,
# NOT --source (the CLI's --source choices are databento|massive only; FX is
# venue-routed). data_type = ohlcv_24h (daily bars are acceptable for FX).
#
# Window: 2019-01-01 → today by default. Override with `--start-floor`.
# Per-VM shard isolation: VM_NAME + MANIFEST_PER_VM_SHARDS=true.
# Singleton lock matches ^tradfi-bf-.
#
# Usage:
#   bash launch-tradfi-bf-fx-ohlcv-24h.sh --dry-run
#   bash launch-tradfi-bf-fx-ohlcv-24h.sh --year 2025
#
# SSOT: tradfi_multisource_backfill_2026_06_22.md (FX-via-yahoo tranche) +
#       codex/02-data/tradfi-databento-sourcing-ssot.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

# FX = Yahoo daily. The shared lib defaults TRADFI_OHLCV_DATA_TYPES to
# "ohlcv_1m;ohlcv_1s" (the Databento OHLCV path); FX is daily-only, so pin
# ohlcv_24h here. The Yahoo FX adapter ignores any other requested data_type.
export OHLCV_DATA_TYPES="${OHLCV_DATA_TYPES:-ohlcv_24h}"
# FX is venue-routed to Yahoo — NOT a --source=databento|massive run. Leaving
# OHLCV_SOURCE unset would default the lib to "databento" and mis-stamp the
# row provenance; the FX venue branch in umi_tick_provider never consults
# --source, but we must not pass a misleading databento source string. The
# MTDS CLI only accepts databento|massive for --source, so pass NEITHER: blank
# the lib's source so VM_SOURCE is empty and setup-data-pipeline-vm.sh omits
# the --source flag (FX run needs no source selector).
export OHLCV_SOURCE=""

ohlcv_parse_common_args "$@"

ohlcv_check_singleton_lock "$FORCE" "$DRY_RUN"

YEAR_SHARDS="$(ohlcv_year_shards "${START_FLOOR:0:4}" "$START_FLOOR")"
YEAR_SHARDS="$(ohlcv_apply_year_filter "$YEAR_SHARDS")"
IFS=';' read -ra _shards <<< "$YEAR_SHARDS"
if (( ${#_shards[@]} == 0 )); then
    echo "ERROR: no year-shards match --year=${ONLY_YEAR}" >&2; exit 1
fi

for shard in "${_shards[@]}"; do
    start="${shard%%:*}"
    end="${shard##*:}"
    year="${start:0:4}"
    run_ts="$(date +%Y%m%d-%H%M%S)"
    vm_name="tradfi-bf-fx-ohlcv-24h-${year}-${run_ts}"
    # instrument_ids="" — the Yahoo FX adapter fetches the WHOLE FX_SPOT_PAIRS
    # universe from UAC itself; no client-side list (UAC is SSOT).
    ohlcv_create_vm "$vm_name" "FX" "$start" "$end" "" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
done

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "=========================================="
    echo "DRY-RUN: FX OHLCV-24h (Yahoo daily) year-shards (${START_FLOOR}..today)"
    echo "=========================================="
else
    echo "FX OHLCV-24h (Yahoo daily) year-shards launched in ${TRADFI_OHLCV_ZONE}."
    echo "Manifest check (post-drain):"
    echo "  gsutil cp gs://market-data-tick-tradfi-${TRADFI_OHLCV_PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
    echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[(df.venue=='FX')&(df.data_type=='ohlcv_24h')].groupby('capture_status').size())\""
fi
