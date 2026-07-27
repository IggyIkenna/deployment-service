#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch NYSE OHLCV-1m backfill VMs (one per year-shard).
#
# Universe: SP500_TICKERS + ETF_TICKERS from
# `unified_api_contracts.registry.tradfi_ticker_universe`. MTDS adapter routes
# by VM_VENUE=NYSE → Databento XNYS.PILLAR / XCHI.PITCH datasets; non-NYSE
# tickers return empty_confirmed (harmless — venue dataset arbitrates).
#
# Window: 2019-01-01 → today by default. Override with `--start-floor`.
# Per-VM shard isolation: VM_NAME + MANIFEST_PER_VM_SHARDS=true.
#
# Sharding: DATE-RANGE (default) — `--date-slices N` (default 5) splits each
# year-shard's calendar days into N contiguous date-range slices, ALL
# tickers per VM (equity per-calendar-date cost is ~ticker-count-invariant,
# measured: tradfi_backfill_throughput_followups_2026_07_24.md tick-26).
# Legacy TICKER-GROUP sharding (`--shard-mode ticker-group`) stays reachable
# for the pathological single-VM-memory-ceiling case.
#
# Usage:
#   bash launch-tradfi-bf-nyse-ohlcv-1m.sh --dry-run
#   bash launch-tradfi-bf-nyse-ohlcv-1m.sh
#   bash launch-tradfi-bf-nyse-ohlcv-1m.sh --date-slices 10                     # finer date shards
#   bash launch-tradfi-bf-nyse-ohlcv-1m.sh --shard-mode ticker-group --ticker-groups 10  # legacy path
#
# SSOT: tradfi_backfill_throughput_followups_2026_07_24.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

# NYSE ohlcv_1m starts 2023-04-15 per UAC VENUE_DATA_TYPE_CAPABILITIES
# (Phase 2 of tradfi_ohlcv_only_mvp_backfill_2026_05_15.md). Pre-2023-04-15
# requests return "No active venues" warnings + 0 rows. Caller can override
# with --start-floor for ad-hoc backfills if Databento adds earlier coverage.
START_FLOOR_DEFAULT="2023-04-15"
if [[ "${1:-}" != *"--start-floor"* ]]; then
    set -- --start-floor "$START_FLOOR_DEFAULT" "$@"
fi
ohlcv_parse_common_args "$@"

# Equity venue START_FLOOR overrides shared default. Databento XNYS.PILLAR /
# XCHI.PITCH coverage starts 2023-04-15 per UAC `VENUE_DATA_TYPE_CAPABILITIES['NYSE']`.
# Pre-2023-04-15 year-shards trigger orchestrator's `is_venue_available()`
# filter → "No active venues" warnings → rc=0 self-delete with 0 parquets.
# Empirical evidence at `tradfi-bf-nyse-ohlcv-1m-2019-20260517-101526` (2-min
# run, 365 warnings, 0 parquets). Override with `--start-floor` for vendor
# coverage expansion.
if [[ "$START_FLOOR" == "2019-01-01" ]]; then
    START_FLOOR="2023-04-15"
    echo "NYSE equity venue: START_FLOOR auto-clipped to Databento XNYS coverage floor $START_FLOOR"
fi

WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
UAC_DIR="${WORKSPACE_ROOT}/unified-api-contracts"
if [[ ! -d "$UAC_DIR" ]]; then
    echo "ERROR: unified-api-contracts not found at $UAC_DIR" >&2
    exit 1
fi
TICKER_LIST="$(cd "$UAC_DIR" && "${WORKSPACE_ROOT}/.venv-workspace/bin/python3" -c "
from unified_api_contracts.registry.tradfi_ticker_universe import (
    SP500_TICKERS, ETF_TICKERS,
)
tickers = sorted(set(SP500_TICKERS) | set(ETF_TICKERS))
print(';'.join(tickers))
")"
ticker_count="$(awk -F';' '{print NF}' <<< "$TICKER_LIST")"
echo "Resolved $ticker_count NYSE candidate tickers from UAC."

ohlcv_check_singleton_lock "$FORCE" "$DRY_RUN"

YEAR_SHARDS="$(ohlcv_year_shards "${START_FLOOR:0:4}" "$START_FLOOR")"
YEAR_SHARDS="$(ohlcv_apply_year_filter "$YEAR_SHARDS")"
IFS=';' read -ra _shards <<< "$YEAR_SHARDS"
if (( ${#_shards[@]} == 0 )); then
    echo "ERROR: no year-shards match --year=${ONLY_YEAR}" >&2; exit 1
fi

# Shard axis: date-range (default) or ticker-group (legacy escape hatch) —
# see `ohlcv_split_date_slices` / `ohlcv_split_ticker_groups` in the lib for
# the full rationale and for why more VMs is SAFE here (per-IP Databento
# budget, one ephemeral IP per VM).
if [[ "$OHLCV_SHARD_MODE" == "ticker-group" ]]; then
    TICKER_GROUPS_OUT="$(ohlcv_split_ticker_groups "$TICKER_LIST" "$OHLCV_TICKER_GROUPS")"
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
            vm_name="tradfi-bf-nyse-ohlcv-1m-${gtag}-${year}-${run_ts}"
            echo "  ${gtag} (${gfirst}..${glast}) ${year}"
            ohlcv_create_vm "$vm_name" "NYSE" "$start" "$end" "$gtickers" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
        done
    done <<< "$TICKER_GROUPS_OUT"
else
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
            vm_name="tradfi-bf-nyse-ohlcv-1m-${year}-${dtag}-${run_ts}"
            echo "  ${year} ${dtag} (${dstart}..${dend})"
            ohlcv_create_vm "$vm_name" "NYSE" "$dstart" "$dend" "$TICKER_LIST" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
            total_vms=$(( total_vms + 1 ))
        done <<< "$DATE_SLICES_OUT"
    done
    echo "[date-range mode] Sharded into $total_vms date-range VM(s) across ${#_shards[@]} year(s), all $ticker_count tickers per VM."
fi

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "=========================================="
    echo "DRY-RUN: NYSE OHLCV-1m (${OHLCV_SHARD_MODE}) shards (${START_FLOOR}..today)"
    echo "=========================================="
else
    echo "NYSE OHLCV-1m (${OHLCV_SHARD_MODE}) shards launched in ${TRADFI_OHLCV_ZONE}."
    echo "Manifest check (post-drain):"
    echo "  gsutil cp gs://market-data-tick-tradfi-${TRADFI_OHLCV_PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
    echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[(df.venue=='NYSE')&(df.data_type=='ohlcv_1m')].groupby('capture_status').size())\""
fi
