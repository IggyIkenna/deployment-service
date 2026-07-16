#!/usr/bin/env bash
# Launch CFE (CBOE Futures Exchange) OHLCV-1m backfill VMs (one per year-shard).
#
# Epic: tradfi_master
# Lifecycle: campaign
# Delete-when: CFE/XCBF.PITCH VX futures backfill 2018-11-04..today complete and
#   manifest shows captured rows for venue=CBOE/instrument_type=future/data_type=ohlcv_1m.
#
# Universe: VX.FUT (Cboe VX futures chain) via Databento XCBF.PITCH dataset.
# The operator calls this dataset "CFE"; the Databento code is XCBF.PITCH.
# A bare "CFE" dataset name 400-errors from Databento — always use XCBF.PITCH.
#
# Routing: VM_VENUE=CBOE → MTDS adapter → Databento XCBF.PITCH. The VX root
# symbol (VX.FUT parent) expands to the full front-month futures chain per date.
# This is DISTINCT from the VIX cash index (ohlcv_15m via Barchart/Yahoo) —
# Databento CFE gives VX FUTURES only, not the VIX cash index.
#
# Window: 2018-11-04 → today (XCBF.PITCH L0 coverage floor per UAC
# VENUE_DATA_TYPE_CAPABILITIES['CBOE']['ohlcv_1m']). Override with --start-floor.
# Per-VM shard isolation: VM_NAME + MANIFEST_PER_VM_SHARDS=true.
#
# Usage:
#   bash launch-tradfi-bf-cfe-ohlcv-1m.sh --dry-run
#   bash launch-tradfi-bf-cfe-ohlcv-1m.sh
#   bash launch-tradfi-bf-cfe-ohlcv-1m.sh --start-floor 2022-01-01
#
# SSOT: tradfi_ohlcv_only_mvp_backfill_2026_05_15.md Phase 6 +
#       codex/02-data/tradfi-databento-sourcing-ssot.md (XCBF.PITCH activated 2026-06-19).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

# XCBF.PITCH L0 coverage floor: 2018-11-04 (per UAC VENUE_DATA_TYPE_CAPABILITIES['CBOE']).
# Pre-2018-11-04 requests return 0 rows. Caller can override with --start-floor
# if Databento adds earlier coverage.
START_FLOOR_DEFAULT="2018-11-04"
if [[ "${1:-}" != *"--start-floor"* ]]; then
    set -- --start-floor "$START_FLOOR_DEFAULT" "$@"
fi
ohlcv_parse_common_args "$@"

# VX futures Databento parent symbology: VX.FUT pulls the full front-month chain
# for any date window via the XCBF.PITCH dataset.
CFE_INSTRUMENT_IDS="VX.FUT"

ohlcv_check_singleton_lock "$FORCE" "$DRY_RUN"

# CFE VX routes to the CBOE venue (XCBF.PITCH) — clamp to its UAC discovery floor.
START_FLOOR="$(ohlcv_clamp_floor_to_venue "CBOE" "$START_FLOOR")"
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
    vm_name="tradfi-bf-cfe-ohlcv-1m-${year}-${run_ts}"
    ohlcv_create_vm "$vm_name" "CBOE" "$start" "$end" "$CFE_INSTRUMENT_IDS" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
done

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "=========================================="
    echo "DRY-RUN: CFE (CBOE/XCBF.PITCH) OHLCV-1m year-shards (${START_FLOOR}..today)"
    echo "=========================================="
else
    echo "CFE OHLCV-1m year-shards launched in ${TRADFI_OHLCV_ZONE}."
    echo "Manifest check (post-drain):"
    echo "  gsutil cp gs://market-data-tick-tradfi-${TRADFI_OHLCV_PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
    echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[(df.venue=='CBOE')&(df.data_type=='ohlcv_1m')].groupby('capture_status').size())\""
fi
