#!/usr/bin/env bash
# Epic: mtds_mdps_master
# Lifecycle: campaign
# Delete-when: tradfi CBOE/XCBF ohlcv_1m backfill is complete + honest-cov maxed
#              (the 3-dataset Databento universe: GLBX + DBEQ + XCBF all captured).
# Launch CBOE / XCBF.PITCH (Cboe Futures Exchange) OHLCV-1m backfill VMs — VX (VIX)
# FUTURES, one per year-shard.
#
# Symbol-set: Databento parent symbology `VX.FUT` pulls all listed VX contract
# months for the window (UAC tradfi_instrument_universe:
# DatabentoInstrumentDef("VX.FUT","CBOE","FUTURE","XCBF.PITCH","parent",...)).
# The MTDS adapter routes VM_VENUE=CBOE → XCBF.PITCH (the operator's "CFE"
# subscription; a bare "CFE" 400-errors). This is the 3rd of the 3 subscribed
# Databento datasets (GLBX.MDP3 + DBEQ.BASIC + XCBF.PITCH).
#
# NOTE: XCBF.PITCH gives VIX *futures* (VX), NOT the VIX *cash index* (that gap
# is Barchart + Yahoo per CLAUDE.md). `--source databento` is the ONLY capable
# source for CBOE — UAC `_VENUE_SOURCE_EXCLUSIONS` excludes `massive` for CBOE.
#
# Window: 2019-01-01 → yesterday by default (futures, no equity clip — XCBF L0
# 16y included history covers the window). Override with `--start-floor`.
# Per-VM shard isolation: VM_NAME + MANIFEST_PER_VM_SHARDS=true. Singleton lock
# matches ^tradfi-bf-.
#
# Usage:
#   bash launch-tradfi-bf-cboe-ohlcv-1m.sh --dry-run
#   bash launch-tradfi-bf-cboe-ohlcv-1m.sh
#   bash launch-tradfi-bf-cboe-ohlcv-1m.sh --year 2026 --force
#
# SSOT: codex/02-data/tradfi-databento-sourcing-ssot.md +
#       plans/active/data_completion_to_100_all_ag_2026_06_21.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

ohlcv_parse_common_args "$@"

# CBOE root universe — VX (VIX) futures parent symbol (XCBF.PITCH dataset).
VX_PARENT="VX.FUT"

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
    vm_name="tradfi-bf-cboe-ohlcv-1m-vx-${year}-${run_ts}"
    ohlcv_create_vm "$vm_name" "CBOE" "$start" "$end" "$VX_PARENT" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
done

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "=========================================="
    echo "DRY-RUN: CBOE/XCBF VX-futures OHLCV-1m year-shards (${START_FLOOR}..yesterday)"
    echo "=========================================="
else
    echo "CBOE/XCBF VX-futures OHLCV-1m year-shards launched in ${TRADFI_OHLCV_ZONE}."
    echo "Manifest check (post-drain):"
    echo "  gsutil cp gs://market-data-tick-tradfi-${TRADFI_OHLCV_PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
    echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[(df.venue=='CBOE')&(df.data_type=='ohlcv_1m')].groupby('capture_status').size())\""
fi
