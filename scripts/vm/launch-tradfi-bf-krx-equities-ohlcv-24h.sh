#!/usr/bin/env bash
# Epic: tradfi_master
# Lifecycle: permanent
# Delete-when: NA
# Launch KRX (Korea Exchange) single-stock daily OHLCV-24h backfill VMs (one
# per year-shard).
#
# Universe: the 3 KRX single-stock underliers in the UAC ``KRX_EQUITIES``
# registry — HYUNDAI (005380.KS), SAMSUNG (005930.KS), SKHYNIX (000660.KS),
# the Korean equity-basis underliers of the corresponding Binance tradfi-perps
# (added 2026-06-24, KRX venue close-out). Canonical ids
# ``KRX:EQUITY:{code}-USD``, instrument_type=EQUITY, data_type=ohlcv_24h.
#
# Routing: VM_VENUE=KRX + VM_DATA_TYPES=ohlcv_24h + BLANK source → the MTDS
# download handler's ``route_yahoo_tradfi("KRX", ...)`` path
# (market_tick_data_service/adapters/_umi_yahoo.py::fetch_yahoo_equities) →
# Yahoo Finance daily .KS tickers. KRX is Yahoo-ONLY (2026-06-24) — neither
# Databento nor Massive ever carried KRX, so this is the ONLY viable source;
# a non-blank --source would fail-closed / mis-stamp provenance (mirrors the
# FX/CBOE-indices launchers' convention).
#
# Why this launcher exists: honest-coverage audit (2026-07-22) found KRX at
# 0.07% coverage (6 rows captured of ~8,629 total) — no KRX-specific backfill
# had ever been launched this closeout; the equity launchers (NASDAQ/NYSE)
# hard-assume Databento and never emit a VM_VENUE=KRX shard.
#
# Window: 2019-01-01 → today by default (Yahoo .KS daily history goes back
# further, but this matches the rest of this closeout's default floor).
# Override with `--start-floor YYYY-MM-DD`.
#
# Per-VM shard isolation: VM_NAME + MANIFEST_PER_VM_SHARDS=true. Singleton
# lock matches ^tradfi-bf-. VMs default to SPOT (idempotent backfill).
#
# Usage:
#   bash launch-tradfi-bf-krx-equities-ohlcv-24h.sh --dry-run
#   bash launch-tradfi-bf-krx-equities-ohlcv-24h.sh
#   bash launch-tradfi-bf-krx-equities-ohlcv-24h.sh --year 2025
#
# SSOT: codex/02-data/tradfi-databento-sourcing-ssot.md (Yahoo daily
#       equities) + unified-trading-pm/plans/active/tradfi_consolidated_closeout_2026_07_18.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

# KRX single-stock equities = Yahoo daily. The shared lib FREEZES
# TRADFI_OHLCV_DATA_TYPES / TRADFI_OHLCV_SOURCE at SOURCE time, so override the
# FROZEN lib vars DIRECTLY (an `export OHLCV_*` after the `source` line is a
# NO-OP — see the FX launcher's 2026-06-23 incident note). ohlcv_24h ONLY +
# BLANK source (venue-routed to Yahoo).
TRADFI_OHLCV_DATA_TYPES="${OHLCV_DATA_TYPES:-ohlcv_24h}"
TRADFI_OHLCV_SOURCE="${OHLCV_SOURCE:-}"

ohlcv_parse_common_args "$@"

ohlcv_check_singleton_lock "$FORCE" "$DRY_RUN"

# NO venue-discovery-floor clamp (KRX has no Databento genesis to clamp
# against — Yahoo-only). The default START_FLOOR (2019-01-01, or the
# caller's --start-floor) is used as-is.
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
    vm_name="tradfi-bf-krx-eq-ohlcv-24h-${year}-${run_ts}"
    # instrument_ids="" — fetch_yahoo_equities fetches the WHOLE venue=KRX
    # KRX_EQUITIES set from UAC itself; no client-side list.
    ohlcv_create_vm "$vm_name" "KRX" "$start" "$end" "" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
done

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "=========================================="
    echo "DRY-RUN: KRX equities OHLCV-24h (Yahoo daily) year-shards (${START_FLOOR}..today)"
    echo "=========================================="
else
    echo "KRX equities OHLCV-24h (Yahoo daily) year-shards launched in ${TRADFI_OHLCV_ZONE}."
    echo "Manifest check (post-drain):"
    echo "  gsutil cp gs://market-data-tick-tradfi-${TRADFI_OHLCV_PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
    echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[(df.venue=='KRX')&(df.data_type=='ohlcv_24h')].groupby(['symbol','capture_status']).size())\""
fi
