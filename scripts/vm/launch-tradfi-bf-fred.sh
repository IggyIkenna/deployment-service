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
# Window: 2020-01-01 (matches the rest of tradfi's Databento-sourced group —
# CME/FX/ICE, tradfi-databento-sourcing-ssot.md) -> yesterday by default.
# Override with --start-floor. Deliberately NOT coverage_starts.py's full
# 1962-01-02 FRED-availability floor: some FRED series genuinely have decades
# more history than the rest of this bucket, but co-locating that much deeper
# history in the SAME manifest inflated the consolidator's incremental-merge
# chunk count ~9x (tradfi_manifest_consolidator_fred_widespan_stall_2026_07_30.md)
# — coverage_starts.py's 1962-01-02 remains the honest AVAILABILITY floor (do
# not change it, it documents a true fact about FRED), this is a separate
# BACKFILL-SCOPE choice: capture only the window that matches the rest of
# tradfi's coverage, consistent with CME/CBOE/NASDAQ/NYSE all being clamped to
# their own venue-specific start (get_instrument_discovery_start()).
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
#   bash launch-tradfi-bf-fred.sh                      # full 2020-01-01..yesterday, one VM
#   bash launch-tradfi-bf-fred.sh --year 2024 --dry-run  # single-year smoke test
#
# SSOT: unified-trading-pm/plans/active/issues/macro_micro_econ_data_capture_audit_2026_06_05.md,
#       codex/02-data/tradfi-databento-sourcing-ssot.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

# e2-highmem-4 (32GB), NOT e2-standard-4 (16GB): confirmed OOM-kill (exit 137)
# during the 2024 smoke-test launch — chunk 1 (2024-01-01..01-07) climbed to
# ~12.4GB/16GB before being killed, chunk 2 (01-08..01-14) climbed 58%->92.5%
# RSS in 30s before being killed — same root cause + same validated fix as
# mtds_backfill_vm_memory_hang_large_chunk_2026_07_22.md's CEFI Tardis OOM and
# its sports-odds-launcher recurrence (bbce1b6): per-invocation date-iteration
# accumulation within mtds_chunk_loop.sh's multi-day chunks, NOT vendor payload
# size — the original "small JSON payloads, no need for a bigger profile"
# reasoning here was the same mistake both prior incidents already disproved.
# Direct assignment against the DIFFERENTLY-NAMED MACHINE_TYPE (not `:-` against
# TRADFI_OHLCV_MACHINE itself): the lib already set TRADFI_OHLCV_MACHINE at SOURCE
# time (line ~49 of _tradfi-ohlcv-launcher-lib.sh), so a self-referential
# `${TRADFI_OHLCV_MACHINE:-...}` here would be a no-op (var already non-empty).
# MACHINE_TYPE is the env escalation._recover_backfill_vm's OOM `bigger_machine`
# hint actually sets (KEY #4) — honoring it here lets an OOM relaunch escalate
# past this FRED-specific e2-highmem-4 default instead of silently re-OOMing on
# the same machine every time.
TRADFI_OHLCV_MACHINE="${MACHINE_TYPE:-e2-highmem-4}"
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

# Backfill-scope floor (2026-07-30 ruling — see header comment): matches the
# rest of tradfi's Databento group (CME/FX/ICE = 2020-01-01), NOT
# coverage_starts.py's full 1962-01-02 FRED-availability floor. FRED has no
# venue_start_dates / venue_instrument_discovery_overrides entry, so
# ohlcv_clamp_floor_to_venue is still a no-op for FRED — this is a direct
# literal default, the same mechanism CME/ICE/FX use via their own clamp.
START_FLOOR="${START_FLOOR:-2020-01-01}"
if [[ "$START_FLOOR" == "2019-01-01" ]]; then
    # ohlcv_parse_common_args's own hardcoded default (not FRED-aware) — only
    # override when the caller didn't pass an explicit --start-floor.
    START_FLOOR="2020-01-01"
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
