#!/usr/bin/env bash
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
# Usage:
#   bash launch-tradfi-bf-nyse-ohlcv-1m.sh --dry-run
#   bash launch-tradfi-bf-nyse-ohlcv-1m.sh
#
# SSOT: tradfi_ohlcv_only_mvp_backfill_2026_05_15.md Phase 6.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

ohlcv_parse_common_args "$@"

WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
UAC_DIR="${WORKSPACE_ROOT}/unified-api-contracts"
if [[ ! -d "$UAC_DIR" ]]; then
    echo "ERROR: unified-api-contracts not found at $UAC_DIR" >&2
    exit 1
fi
TICKER_LIST="$(cd "$UAC_DIR" && python3 -c "
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
IFS=';' read -ra _shards <<< "$YEAR_SHARDS"

for shard in "${_shards[@]}"; do
    start="${shard%%:*}"
    end="${shard##*:}"
    year="${start:0:4}"
    run_ts="$(date +%Y%m%d-%H%M%S)"
    vm_name="tradfi-bf-nyse-ohlcv-1m-${year}-${run_ts}"
    ohlcv_create_vm "$vm_name" "NYSE" "$start" "$end" "$TICKER_LIST" "$DRY_RUN" "$DEPLOYMENT_ENV" "true"
done

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "=========================================="
    echo "DRY-RUN: NYSE OHLCV-1m year-shards (${START_FLOOR}..today)"
    echo "=========================================="
else
    echo "NYSE OHLCV-1m year-shards launched in ${TRADFI_OHLCV_ZONE}."
    echo "Manifest check (post-drain):"
    echo "  gsutil cp gs://market-data-tick-tradfi-${TRADFI_OHLCV_PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
    echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[(df.venue=='NYSE')&(df.data_type=='ohlcv_1m')].groupby('capture_status').size())\""
fi
