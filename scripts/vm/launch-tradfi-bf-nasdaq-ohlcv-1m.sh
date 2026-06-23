#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch NASDAQ OHLCV-1m backfill VMs (one per year-shard).
#
# Universe: SP500_TICKERS + NASDAQ_TICKERS + ETF_TICKERS from
# `unified_api_contracts.registry.tradfi_ticker_universe`. MTDS adapter
# routes by VM_VENUE=NASDAQ → Databento XNAS.ITCH dataset; tickers that are
# NOT NASDAQ-listed return empty_confirmed manifest rows (harmless — the
# split between NASDAQ-listed vs NYSE-listed is intentionally not encoded
# client-side; let the venue dataset arbitrate).
#
# Window: 2019-01-01 → today by default. Override with `--start-floor`.
# Per-VM shard isolation: VM_NAME + MANIFEST_PER_VM_SHARDS=true.
#
# Usage:
#   bash launch-tradfi-bf-nasdaq-ohlcv-1m.sh --dry-run
#   bash launch-tradfi-bf-nasdaq-ohlcv-1m.sh
#
# SSOT: tradfi_ohlcv_only_mvp_backfill_2026_05_15.md Phase 6.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

# NASDAQ ohlcv_1m starts 2023-04-15 per UAC VENUE_DATA_TYPE_CAPABILITIES
# (Phase 2 of tradfi_ohlcv_only_mvp_backfill_2026_05_15.md). Pre-2023-04-15
# requests return "No active venues" warnings + 0 rows. Caller can override
# with --start-floor for ad-hoc backfills if Databento adds earlier coverage.
START_FLOOR_DEFAULT="2023-04-15"
if [[ "${1:-}" != *"--start-floor"* ]]; then
    # Inject default before parsing; user-passed --start-floor takes precedence.
    set -- --start-floor "$START_FLOOR_DEFAULT" "$@"
fi
ohlcv_parse_common_args "$@"

# Equity venue START_FLOOR overrides the shared default. Databento NASDAQ
# (XNAS.ITCH) and NYSE (XNYS.PILLAR) datasets only cover 2023-04-15 onwards
# per UAC `VENUE_DATA_TYPE_CAPABILITIES['NASDAQ']` / `['NYSE']`. Launching
# year-shards 2019-2022 produces zero rows AND wastes a VM (the orchestrator
# `is_venue_available()` gate filters NASDAQ out on every pre-2023-04-15 date,
# leading to "No active venues for date=YYYY-MM-DD asset_groups=['TRADFI']"
# warnings + immediate rc=0 self-delete). Empirical evidence: VM
# `tradfi-bf-nyse-ohlcv-1m-2019-20260517-101526` ran 2 min, emitted 365
# "No active venues" warnings, wrote 0 parquets. Equity launchers ALWAYS
# clip; futures launchers (CME / ICE) keep the 2019-01-01 default since
# Databento GLBX.MDP3 + ICE.IMPACT futures coverage spans the full window.
# Override by passing `--start-floor YYYY-MM-DD` explicitly (e.g. for
# Databento-side coverage expansion after a vendor backfill).
if [[ "$START_FLOOR" == "2019-01-01" ]]; then
    START_FLOOR="2023-04-15"
    echo "NASDAQ equity venue: START_FLOOR auto-clipped to Databento XNAS coverage floor $START_FLOOR"
fi

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

for shard in "${_shards[@]}"; do
    start="${shard%%:*}"
    end="${shard##*:}"
    year="${start:0:4}"
    run_ts="$(date +%Y%m%d-%H%M%S)"
    vm_name="tradfi-bf-nasdaq-ohlcv-1m-${year}-${run_ts}"
    ohlcv_create_vm "$vm_name" "NASDAQ" "$start" "$end" "$TICKER_LIST" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
done

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "=========================================="
    echo "DRY-RUN: NASDAQ OHLCV-1m year-shards (${START_FLOOR}..today)"
    echo "=========================================="
else
    echo "NASDAQ OHLCV-1m year-shards launched in ${TRADFI_OHLCV_ZONE}."
    echo "Manifest check (post-drain):"
    echo "  gsutil cp gs://market-data-tick-tradfi-${TRADFI_OHLCV_PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
    echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[(df.venue=='NASDAQ')&(df.data_type=='ohlcv_1m')].groupby('capture_status').size())\""
fi
