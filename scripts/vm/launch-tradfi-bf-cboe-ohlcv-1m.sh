#!/usr/bin/env bash
# Launch CBOE/XCBF OHLCV-1m backfill VMs (one per year-shard).
#
# Dataset: XCBF.PITCH (Cboe Futures Exchange — VX/VIX futures).
# IMPORTANT: Databento's dataset CODE is XCBF.PITCH; a bare "CFE" 400-errors.
# This launcher only covers VX futures (VX.FUT parent symbol); the VIX cash
# index is NOT available via Databento CFE — that gap stays Barchart+Yahoo.
#
# The `massive` source is excluded for CBOE/VX; only `databento` is capable
# (per _tradfi-ohlcv-launcher-lib.sh comment + operator 2026-06-19).
#
# Window: 2026-01-01 → today by default (CBOE/XCBF cells are 2026-YTD per
# audit data_completion_to_100_all_ag_2026_06_21.md ~212 unattempted cells).
# Override with --start-floor YYYY-MM-DD.
#
# Per-VM shard isolation: VM_NAME + MANIFEST_PER_VM_SHARDS=true (CLAUDE.md
# HARD RULE). Singleton lock matches ^tradfi-bf-.
#
# Usage:
#   bash launch-tradfi-bf-cboe-ohlcv-1m.sh --dry-run
#   bash launch-tradfi-bf-cboe-ohlcv-1m.sh --force   # (when other tradfi-bf-* VMs running)
#   bash launch-tradfi-bf-cboe-ohlcv-1m.sh --env staging --start-floor 2025-01-01
#
# Epic: tradfi_master
# Lifecycle: campaign
# Delete-when: CBOE/XCBF ohlcv_1m coverage reaches 100% confirmed in manifest
#
# SSOT: tradfi_ohlcv_only_mvp_backfill_2026_05_15.md Phase 6 +
#       data_completion_to_100_all_ag_2026_06_21.md tradfi fan-out.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

# CBOE only uses databento (massive is excluded for CBOE/VX per operator 2026-06-19).
OHLCV_SOURCE="databento"
export OHLCV_SOURCE

# Default floor: 2026-01-01 (CBOE unattempted cells are 2026-YTD).
START_FLOOR="${START_FLOOR:-2026-01-01}"

ohlcv_parse_common_args "$@"

# Override the floor to CBOE minimum if not user-set below our default.
# (ohlcv_parse_common_args resets START_FLOOR to 2019-01-01; re-apply 2026 floor.)
if [[ "${START_FLOOR}" == "2019-01-01" ]]; then
    START_FLOOR="2026-01-01"
fi

# CBOE/XCBF instrument universe (VX futures).
# VX.FUT is the Databento parent symbol for VX futures on XCBF.PITCH.
declare -a CBOE_ROOTS=(
    "VX|VX.FUT"
)

ohlcv_check_singleton_lock "$FORCE" "$DRY_RUN"

# Belt-and-suspenders: clamp to CBOE's UAC discovery floor (2020-06-01). The
# 2026-01-01 override above is a deliberate stricter scope and is preserved
# (the clamp only ever raises the floor); this also protects an explicit
# `--start-floor` set below the UAC floor.
START_FLOOR="$(ohlcv_clamp_floor_to_venue "CBOE" "$START_FLOOR")"
YEAR_SHARDS="$(ohlcv_year_shards "${START_FLOOR:0:4}" "$START_FLOOR")"
YEAR_SHARDS="$(ohlcv_apply_year_filter "$YEAR_SHARDS")"
IFS=';' read -ra _shards <<< "$YEAR_SHARDS"
if (( ${#_shards[@]} == 0 )); then
    echo "ERROR: no year-shards match --year=${ONLY_YEAR}" >&2; exit 1
fi

for spec in "${CBOE_ROOTS[@]}"; do
    root="${spec%%|*}"
    syms="${spec##*|}"
    for shard in "${_shards[@]}"; do
        start="${shard%%:*}"
        end="${shard##*:}"
        year="${start:0:4}"
        run_ts="$(date +%Y%m%d-%H%M%S)"
        root_lower="$(echo "$root" | tr '[:upper:]' '[:lower:]')"
        root_safe="${root_lower//./-}"
        vm_name="tradfi-bf-cboe-ohlcv-1m-${root_safe}-${year}-${run_ts}"
        # Venue MUST be CBOE (not XCBF): the MTDS adapter maps CBOE→XCBF.PITCH;
        # "XCBF" is unmapped → defaults to GLBX.MDP3 (wrong dataset), and existing
        # manifest rows use venue=CBOE. XCBF.PITCH is the Databento DATASET, CBOE is
        # the canonical VENUE (UAC DatabentoInstrumentDef VX.FUT → "CBOE").
        ohlcv_create_vm "$vm_name" "CBOE" "$start" "$end" "$syms" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
    done
done

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "=========================================="
    echo "DRY-RUN: CBOE/XCBF OHLCV-1m year-shards (${START_FLOOR}..today)"
    echo "=========================================="
else
    echo "CBOE/XCBF OHLCV-1m year-shards launched in ${TRADFI_OHLCV_ZONE}."
    echo "Manifest check (post-drain):"
    echo "  gsutil cp gs://market-data-tick-tradfi-${TRADFI_OHLCV_PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
    echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[(df.venue=='CBOE')&(df.data_type=='ohlcv_1m')].groupby(['symbol','capture_status']).size())\""
fi
