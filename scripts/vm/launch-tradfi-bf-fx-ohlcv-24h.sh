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

# FX = Yahoo daily. The shared lib FREEZES TRADFI_OHLCV_DATA_TYPES /
# TRADFI_OHLCV_SOURCE at SOURCE time (from OHLCV_DATA_TYPES / OHLCV_SOURCE),
# so an `export OHLCV_*` AFTER the `source` line above is a NO-OP — the lib has
# already captured the defaults (ohlcv_1m;ohlcv_1s / databento). Override the
# FROZEN lib vars DIRECTLY (these are what ohlcv_create_vm reads into
# VM_DATA_TYPES / VM_SOURCE metadata). Incident 2026-06-23: the post-source
# export left FX VMs running --data-types ohlcv_1m ohlcv_1s --source databento
# (pre-flight then drops the unsupported 1m/1s + the Yahoo branch ignores
# --source, but the manifest stamps a misleading databento provenance and the
# CLI is mis-shaped). FX is daily-only (ohlcv_24h), venue-routed to Yahoo, and
# needs NO --source selector (blank → setup-data-pipeline-vm.sh omits --source).
TRADFI_OHLCV_DATA_TYPES="${OHLCV_DATA_TYPES:-ohlcv_24h}"
TRADFI_OHLCV_SOURCE="${OHLCV_SOURCE:-}"

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
