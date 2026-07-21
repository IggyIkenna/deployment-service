#!/usr/bin/env bash
# Epic: tradfi_master
# Lifecycle: permanent
# Delete-when: NA
# Launch CBOE daily Treasury-yield INDEX OHLCV-24h backfill VMs (one per
# year-shard).
#
# Universe: the 5 US Treasury-yield-tenor INDEX instruments in the UAC
# ``YAHOO_INDICES`` registry with venue="CBOE" — US3M/US2Y/US5Y/US10Y/US30Y
# (Yahoo tickers ^IRX / 2YY=F / ^FVX / ^TNX / ^TYX). Canonical ids
# ``CBOE:INDEX:{tenor}-USD``, instrument_type=INDEX, data_type=ohlcv_24h.
#
# Routing: VM_VENUE=CBOE + VM_DATA_TYPES=ohlcv_24h + BLANK source → the MTDS
# download handler's ``route_yahoo_tradfi("CBOE", data_types={ohlcv_24h})`` path
# (market_tick_data_service/adapters/_umi_yahoo.py) → ``fetch_yahoo_indices("CBOE")``
# → Yahoo Finance daily. CBOE is a MIXED venue: it also serves VX (VIX) FUTURES
# via Databento XCBF.PITCH (that path is the SEPARATE launch-tradfi-bf-cfe /
# -cboe-ohlcv-1m.sh launchers, ohlcv_1m/ohlcv_1s). The ohlcv_24h data_type is the
# ONLY one Yahoo serves here and the one Databento never serves for CBOE, so this
# launcher NEVER short-circuits the VX-futures capture path. A `--source` selector
# is deliberately NOT set (blank → setup-data-pipeline-vm.sh omits --source;
# venue-routed to Yahoo, mirroring the FX daily launcher).
#
# Why a dedicated launcher (not the fx or cboe/cfe launchers): the fx launcher
# hard-routes VM_VENUE=FX; the cboe/cfe launchers hard-route Databento
# ohlcv_1m/1s VX futures. Neither emits a VM_VENUE=CBOE + ohlcv_24h shard, so
# the CBOE Treasury-yield INDEX tenors had NO backfill launcher — this closes
# that gap (tradfi MVP-set expansion, operator 2026-07-21).
#
# Window: 2019-01-01 → today by default (all 5 tenors have Yahoo daily history
# well before 2019: ^IRX/^FVX/^TNX/^TYX from 2000-01-03, 2YY=F from 2018-08-13).
# NOTE: this launcher deliberately does NOT apply the UAC venue-discovery-floor
# clamp — that floor (VenueMapping "CBOE") is the DATABENTO VX-futures genesis
# (~2020-06-01), which is WRONG for the Yahoo Treasury series (which start 2000).
# Override the window with `--start-floor YYYY-MM-DD` (e.g. 2000-01-01 for the
# full ^TNX/^TYX/^IRX/^FVX history; the 2019+ default keeps 2YY=F fully covered).
#
# Per-VM shard isolation: VM_NAME + MANIFEST_PER_VM_SHARDS=true. Singleton lock
# matches ^tradfi-bf-. VMs default to SPOT (idempotent backfill).
#
# Usage:
#   bash launch-tradfi-bf-cboe-indices-ohlcv-24h.sh --dry-run
#   bash launch-tradfi-bf-cboe-indices-ohlcv-24h.sh
#   bash launch-tradfi-bf-cboe-indices-ohlcv-24h.sh --year 2025
#   bash launch-tradfi-bf-cboe-indices-ohlcv-24h.sh --start-floor 2000-01-01
#
# SSOT: codex/02-data/tradfi-databento-sourcing-ssot.md (Yahoo daily indices) +
#       unified-trading-pm/plans/active/tradfi_mvp_set_expansion_2026_07_21.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

# CBOE Treasury INDEX = Yahoo daily. The shared lib FREEZES
# TRADFI_OHLCV_DATA_TYPES / TRADFI_OHLCV_SOURCE at SOURCE time (from
# OHLCV_DATA_TYPES / OHLCV_SOURCE), so override the FROZEN lib vars DIRECTLY
# (an `export OHLCV_*` after the `source` line is a NO-OP — see the FX launcher's
# 2026-06-23 incident note). ohlcv_24h ONLY + BLANK source (venue-routed to Yahoo;
# a non-blank --source would fail-closed / mis-stamp provenance).
TRADFI_OHLCV_DATA_TYPES="${OHLCV_DATA_TYPES:-ohlcv_24h}"
TRADFI_OHLCV_SOURCE="${OHLCV_SOURCE:-}"

ohlcv_parse_common_args "$@"

ohlcv_check_singleton_lock "$FORCE" "$DRY_RUN"

# NO venue-discovery-floor clamp here (see header): the UAC CBOE floor is the
# Databento VX-futures genesis, not the Yahoo Treasury-series genesis. The
# default START_FLOOR (2019-01-01, or the caller's --start-floor) is used as-is.
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
    vm_name="tradfi-bf-cboe-idx-ohlcv-24h-${year}-${run_ts}"
    # instrument_ids="" — the Yahoo indices adapter (fetch_yahoo_indices) fetches
    # the WHOLE venue=CBOE YAHOO_INDICES set from UAC itself; no client-side list.
    # Venue MUST be CBOE (the canonical venue on the YAHOO_INDICES entries + the
    # manifest rows); ohlcv_24h routes it to the Yahoo Treasury branch, never
    # Databento (route_yahoo_tradfi).
    ohlcv_create_vm "$vm_name" "CBOE" "$start" "$end" "" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
done

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "=========================================="
    echo "DRY-RUN: CBOE Treasury INDEX OHLCV-24h (Yahoo daily) year-shards (${START_FLOOR}..today)"
    echo "=========================================="
else
    echo "CBOE Treasury INDEX OHLCV-24h (Yahoo daily) year-shards launched in ${TRADFI_OHLCV_ZONE}."
    echo "Manifest check (post-drain):"
    echo "  gsutil cp gs://market-data-tick-tradfi-${TRADFI_OHLCV_PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
    echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[(df.venue=='CBOE')&(df.data_type=='ohlcv_24h')].groupby(['symbol','capture_status']).size())\""
fi
