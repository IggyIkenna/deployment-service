#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch the FRED (Federal Reserve Economic Data) macro backfill VM.
#
# Universe: FredAdapter.KEY_SERIES (29 series) — the MTDS download path routes
# VM_VENUE=FRED to market_tick_data_service.adapters._umi_fred.route_fred_tradfi,
# which fetches EVERY KEY_SERIES itself (no client-side instrument list / --source
# needed — FRED is venue-fixed, exempted from the --source REQUIRED gate exactly
# like FX/KRX/ICE). data_types = yield_curve (BOND series) + ohlcv_1d (INDEX
# series) — corrects the stale "macro_result" declaration (never emitted by the
# adapter); see unified-api-contracts@0c0f6953 / market-tick-data-service@407f69f1.
#
# Window: 1962-01-02 (coverage_starts.py's declared FRED floor) -> yesterday by
# default. Override with --start-floor. FRED has no per-venue UAC discovery-start
# override (get_instrument_discovery_start("FRED") is unset), so no floor clamp
# applies — the coverage_starts.py floor is the honest-coverage SSOT used here
# directly.
#
# Single-VM default (NOT year-sharded): FRED's rate limit is per-API-KEY, not
# per-IP like Databento (one shared `fred-api-key` Secret Manager credential for
# every VM) — unlike the CME/FX per-venue launchers built on this same shared lib,
# fanning out N year-shard VMs would NOT buy N times the throughput (all VMs
# contend for the same key-scoped quota) and would just multiply 429/retry
# thrashing. `_umi_fred.py` already fetches all 29 series concurrently per day
# (`FredAdapter`'s bounded retry-with-backoff absorbs the rest), so one VM
# processing the whole floor..today window sequentially is the correct default.
# `--year YYYY` still narrows to a single-year window (via the shared lib's
# year-shard + filter helpers) for a fast smoke-test launch.
#
# Per-VM shard isolation: VM_NAME + MANIFEST_PER_VM_SHARDS=true.
# Singleton lock matches ^tradfi-bf-.
#
# Usage:
#   bash launch-tradfi-bf-fred.sh --dry-run
#   bash launch-tradfi-bf-fred.sh                      # full 1962-01-02..yesterday, one VM
#   bash launch-tradfi-bf-fred.sh --year 2024 --dry-run  # single-year smoke test
#
# SSOT: unified-trading-pm/plans/active/issues/macro_micro_econ_data_capture_audit_2026_06_05.md,
#       codex/02-data/tradfi-databento-sourcing-ssot.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

# FRED = its own macro REST venue, no Databento/Massive dataset. Small JSON
# payloads (29 series/day) — no need for CME's e2-highmem-16 / 250GB profile.
# Direct assignment (not `:-`): the lib already set these at SOURCE time
# (line ~35/40 of _tradfi-ohlcv-launcher-lib.sh), so a self-referential
# `${TRADFI_OHLCV_MACHINE:-...}` here would be a no-op (var already non-empty).
TRADFI_OHLCV_MACHINE="e2-standard-4"
TRADFI_OHLCV_BOOT_GB="50"
# yield_curve (BOND-classified KEY_SERIES) + ohlcv_1d (INDEX-classified) — the
# adapter's real, already-correct wire contract (VENUE_DATA_TYPE_CAPABILITIES
# fixed in unified-api-contracts@0c0f6953; see _umi_fred.py docstring).
TRADFI_OHLCV_DATA_TYPES="${OHLCV_DATA_TYPES:-yield_curve;ohlcv_1d}"
# FRED is venue-fixed (tick_data_handler.py's _VENUE_FIXED_SOURCE_VENUES) —
# no --source selector; blank omits the flag entirely (setup-data-pipeline-vm.sh:
# `[[ -n "$VM_SOURCE" ]] && BASE_CLI="$BASE_CLI --source $VM_SOURCE"`).
TRADFI_OHLCV_SOURCE="${OHLCV_SOURCE:-}"

ohlcv_parse_common_args "$@"

ohlcv_check_singleton_lock "$FORCE" "$DRY_RUN"

# coverage_starts.py's declared FRED floor — NOT clamped via
# ohlcv_clamp_floor_to_venue (FRED has no venue_start_dates /
# venue_instrument_discovery_overrides entry, so that lookup is fail-open/no-op
# for FRED; the coverage_starts.py value is the real SSOT floor here).
START_FLOOR="${START_FLOOR:-1962-01-02}"
if [[ "$START_FLOOR" == "2019-01-01" ]]; then
    # ohlcv_parse_common_args's own hardcoded default (not FRED-aware) — only
    # override when the caller didn't pass an explicit --start-floor.
    START_FLOOR="1962-01-02"
fi

if [[ -n "$ONLY_YEAR" ]]; then
    # Smoke-test path: single-year window via the shared year-shard + filter
    # helpers (reuses the exact CME/FX machinery, not a hand-rolled date calc).
    YEAR_SHARDS="$(ohlcv_year_shards "${START_FLOOR:0:4}" "$START_FLOOR")"
    YEAR_SHARDS="$(ohlcv_apply_year_filter "$YEAR_SHARDS")"
    IFS=';' read -ra _shards <<< "$YEAR_SHARDS"
    if (( ${#_shards[@]} == 0 )); then
        echo "ERROR: no year-shard matches --year=${ONLY_YEAR} (floor=${START_FLOOR})" >&2
        exit 1
    fi
    for shard in "${_shards[@]}"; do
        start="${shard%%:*}"
        end="${shard##*:}"
        run_ts="$(date +%Y%m%d-%H%M%S)"
        vm_name="tradfi-bf-fred-${start:0:4}-${run_ts}"
        ohlcv_create_vm "$vm_name" "FRED" "$start" "$end" "" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
    done
else
    # Production default: ONE VM, whole floor..yesterday window (see header —
    # FRED's per-key rate limit means year-sharding would not add throughput).
    today_iso="$(date -u -d 'yesterday' +%Y-%m-%d 2>/dev/null || date -u -v-1d +%Y-%m-%d)"
    run_ts="$(date +%Y%m%d-%H%M%S)"
    vm_name="tradfi-bf-fred-full-${run_ts}"
    ohlcv_create_vm "$vm_name" "FRED" "$START_FLOOR" "$today_iso" "" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
fi

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "=========================================="
    echo "DRY-RUN: FRED macro backfill (${START_FLOOR}..today)"
    echo "=========================================="
else
    echo "FRED macro backfill VM(s) launched in ${TRADFI_OHLCV_ZONE}."
    echo "Manifest check (post-drain):"
    echo "  gsutil cp gs://market-data-tick-tradfi-${TRADFI_OHLCV_PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
    echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[df.venue=='FRED'].groupby(['data_type','capture_status']).size())\""
fi
