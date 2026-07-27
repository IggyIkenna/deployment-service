#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch NASDAQ OHLCV-1m backfill VMs (one per (ticker-group, year) shard).
#
# Universe: SP500_TICKERS + NASDAQ_TICKERS + ETF_TICKERS from
# `unified_api_contracts.registry.tradfi_ticker_universe`. MTDS adapter
# routes by VM_VENUE=NASDAQ → Databento XNAS.ITCH dataset; tickers that are
# NOT NASDAQ-listed return empty_confirmed manifest rows (harmless — the
# split between NASDAQ-listed vs NYSE-listed is intentionally not encoded
# client-side; let the venue dataset arbitrate).
#
# Window: 2019-01-01 → today by default (clamped to the 2023-04-15 XNAS
# discovery floor). Override with `--start-floor`.
#
# Sharding: DATE-RANGE (default) — `--date-slices N` (default 5) splits each
# year-shard's calendar days into N contiguous date-range slices, ALL
# tickers per VM, so the default fan-out is ~5 slices x ~4 years = ~20 VMs.
# Equity per-calendar-date cost is ~ticker-count-invariant (measured:
# tradfi_backfill_throughput_followups_2026_07_24.md tick-26), so this pays
# the fixed per-date overhead once per date instead of once per group.
# Legacy TICKER-GROUP sharding (`--shard-mode ticker-group`) stays reachable
# for the pathological single-VM-memory-ceiling case. Per-VM shard isolation:
# VM_NAME + MANIFEST_PER_VM_SHARDS=true.
#
# Usage:
#   bash launch-tradfi-bf-nasdaq-ohlcv-1m.sh --dry-run
#   bash launch-tradfi-bf-nasdaq-ohlcv-1m.sh
#   bash launch-tradfi-bf-nasdaq-ohlcv-1m.sh --date-slices 10                     # finer date shards
#   bash launch-tradfi-bf-nasdaq-ohlcv-1m.sh --shard-mode ticker-group --ticker-groups 10  # legacy path
#
# SSOT: tradfi_backfill_throughput_followups_2026_07_24.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

# Extract optional --only-group <N> (1-based ticker-group index) before the
# common parser — allows resuming a cap-stopped fan-out (e.g. g03-g05 after the
# fleet cap halted the initial run at g02) WITHOUT re-launching running groups.
ONLY_GROUP=""
_remaining_args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --only-group) ONLY_GROUP="$2"; shift 2 ;;
        *)            _remaining_args+=("$1"); shift ;;
    esac
done
set -- "${_remaining_args[@]+"${_remaining_args[@]}"}"

ohlcv_parse_common_args "$@"

# Clamp START_FLOOR to NASDAQ's UAC discovery floor (2023-04-15). Databento
# NASDAQ (XNAS.ITCH) and NYSE (XNYS.PILLAR) have no data before it, so a
# 2019-2022 year-shard captures zero AND wastes a VM: the orchestrator
# `is_venue_available()` gate filters NASDAQ out on every pre-floor date →
# "No active venues for date=YYYY-MM-DD asset_groups=['TRADFI']" warnings +
# immediate rc=0 self-delete + a false-CRITICAL DP_VM_GONE_NO_CAPTURE alert.
# Empirical: VM `tradfi-bf-nyse-ohlcv-1m-2019-20260517-101526` ran 2 min,
# emitted 365 "No active venues" warnings, wrote 0 parquets. This is now the
# shared UAC-driven clamp (`ohlcv_clamp_floor_to_venue`) — the same bug bit the
# 2019 CME shards (2026-07-16), so EVERY TradFi launcher clips, not just equity.
# It clamps ANY sub-floor `--start-floor`, not only the 2019-01-01 default.
START_FLOOR="$(ohlcv_clamp_floor_to_venue "NASDAQ" "$START_FLOOR")"

# Pull the universe from UAC at launch-time (never duplicate ticker lists
# client-side — UAC is SSOT per CLAUDE.md).
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
UAC_DIR="${WORKSPACE_ROOT}/unified-api-contracts"
if [[ ! -d "$UAC_DIR" ]]; then
    echo "ERROR: unified-api-contracts not found at $UAC_DIR" >&2
    echo "       Set WORKSPACE_ROOT or run from a slot worktree." >&2
    exit 1
fi
TICKER_LIST="$(cd "$UAC_DIR" && "${WORKSPACE_ROOT}/.venv-workspace/bin/python3" -c "
from unified_api_contracts.registry.tradfi_ticker_universe import (
    SP500_TICKERS, NASDAQ_TICKERS, ETF_TICKERS,
)
tickers = sorted(set(SP500_TICKERS) | set(NASDAQ_TICKERS) | set(ETF_TICKERS))
print(';'.join(tickers))
")"
ticker_count="$(awk -F';' '{print NF}' <<< "$TICKER_LIST")"
echo "Resolved $ticker_count NASDAQ candidate tickers from UAC."

ohlcv_check_singleton_lock "$FORCE" "$DRY_RUN"

YEAR_SHARDS="$(ohlcv_year_shards "${START_FLOOR:0:4}" "$START_FLOOR")"
YEAR_SHARDS="$(ohlcv_apply_year_filter "$YEAR_SHARDS")"
IFS=';' read -ra _shards <<< "$YEAR_SHARDS"
if (( ${#_shards[@]} == 0 )); then
    echo "ERROR: no year-shards match --year=${ONLY_YEAR}" >&2; exit 1
fi

# Shard axis: date-range (default) or ticker-group (legacy escape hatch).
# Ticker-group sharding was found to re-pay equity's ~1.46 min/date fixed
# per-date overhead once PER GROUP over the SAME calendar dates (5x compute
# for only ~1.0-1.2x wall-clock speedup); date-range sharding pays that
# overhead once per date TOTAL. See `ohlcv_split_date_slices` /
# `ohlcv_split_ticker_groups` in the lib for the full rationale.
if [[ "$OHLCV_SHARD_MODE" == "ticker-group" ]]; then
    TICKER_GROUPS_OUT="$(ohlcv_split_ticker_groups "$TICKER_LIST" "$OHLCV_TICKER_GROUPS")"
    if [[ -n "$ONLY_GROUP" ]]; then
        TICKER_GROUPS_OUT="$(printf '%s\n' "$TICKER_GROUPS_OUT" | awk -F'|' -v g="$ONLY_GROUP" '$1==g')"
        if [[ -z "$TICKER_GROUPS_OUT" ]]; then
            echo "ERROR: --only-group '$ONLY_GROUP' matched no group (valid 1..${OHLCV_TICKER_GROUPS})" >&2
            exit 1
        fi
        echo "Filtered to ticker-group ${ONLY_GROUP} (--only-group)."
    fi
    group_count="$(printf '%s\n' "$TICKER_GROUPS_OUT" | grep -c .)"
    echo "[ticker-group mode] Sharding $ticker_count tickers into $group_count group(s) x ${#_shards[@]} year(s) = $(( group_count * ${#_shards[@]} )) VM(s)."

    while IFS='|' read -r gidx gfirst glast gtickers; do
        [[ -z "$gidx" ]] && continue
        gtag="$(printf 'g%02d' "$gidx")"
        for shard in "${_shards[@]}"; do
            start="${shard%%:*}"
            end="${shard##*:}"
            year="${start:0:4}"
            run_ts="$(date +%Y%m%d-%H%M%S)"
            vm_name="tradfi-bf-nasdaq-ohlcv-1m-${gtag}-${year}-${run_ts}"
            echo "  ${gtag} (${gfirst}..${glast}) ${year}"
            ohlcv_create_vm "$vm_name" "NASDAQ" "$start" "$end" "$gtickers" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
        done
    done <<< "$TICKER_GROUPS_OUT"
else
    [[ -n "$ONLY_GROUP" ]] && { echo "ERROR: --only-group requires --shard-mode ticker-group" >&2; exit 1; }
    total_vms=0
    for shard in "${_shards[@]}"; do
        start="${shard%%:*}"
        end="${shard##*:}"
        year="${start:0:4}"
        DATE_SLICES_OUT="$(ohlcv_split_date_slices "$start" "$end" "$OHLCV_DATE_SLICES")"
        didx=0
        while IFS=':' read -r dstart dend; do
            [[ -z "$dstart" ]] && continue
            didx=$(( didx + 1 ))
            dtag="$(printf 'd%02d' "$didx")"
            run_ts="$(date +%Y%m%d-%H%M%S)"
            vm_name="tradfi-bf-nasdaq-ohlcv-1m-${year}-${dtag}-${run_ts}"
            echo "  ${year} ${dtag} (${dstart}..${dend})"
            ohlcv_create_vm "$vm_name" "NASDAQ" "$dstart" "$dend" "$TICKER_LIST" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
            total_vms=$(( total_vms + 1 ))
        done <<< "$DATE_SLICES_OUT"
    done
    echo "[date-range mode] Sharded into $total_vms date-range VM(s) across ${#_shards[@]} year(s), all $ticker_count tickers per VM."
fi

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "=========================================="
    echo "DRY-RUN: NASDAQ OHLCV-1m (${OHLCV_SHARD_MODE}) shards (${START_FLOOR}..today)"
    echo "=========================================="
else
    echo "NASDAQ OHLCV-1m (${OHLCV_SHARD_MODE}) shards launched in ${TRADFI_OHLCV_ZONE}."
    echo "Manifest check (post-drain):"
    echo "  gsutil cp gs://market-data-tick-tradfi-${TRADFI_OHLCV_PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
    echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[(df.venue=='NASDAQ')&(df.data_type=='ohlcv_1m')].groupby('capture_status').size())\""
fi
