#!/usr/bin/env bash
# Launch ICE OHLCV-1m backfill VMs (one per (root, year-shard)).
#
# ICE roots per OHLCV-only MVP plan (Phase 6): currently empty until the
# tradfi instrument universe declares ICE futures. The script ships with the
# scaffolding ready — operator extends ICE_ROOTS when the corresponding rows
# land in `unified_api_contracts.registry.tradfi_ticker_universe`.
#
# Why ship empty: per CLAUDE.md "External Data Is Always Available" + the
# OHLCV-only plan coverage matrix, ICE remains BLOCKED-UNIVERSE-DECISION
# pending operator pick of the contracts to backfill (e.g. ICE Brent BRN.FUT,
# ICE Gasoil G.FUT, ICE Sugar SB.FUT). Datasets: IFEU.IMPACT (London) /
# IFUS.IMPACT (US). Once roots are declared, append entries to ICE_ROOTS
# below in the same shape as the CME wrapper.
#
# Per-VM shard isolation: VM_NAME + MANIFEST_PER_VM_SHARDS=true.
# Singleton lock matches ^tradfi-bf-.
#
# Usage:
#   bash launch-tradfi-bf-ice-ohlcv-1m.sh --dry-run
#
# SSOT: tradfi_ohlcv_only_mvp_backfill_2026_05_15.md Phase 6.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

ohlcv_parse_common_args "$@"

# ICE root universe — operator-extended once tradfi_ticker_universe declares
# ICE rows. Shape: "root|parent_symbols".
# Candidate extension (uncomment + verify against Databento ICE coverage):
#   "BRN|BRN.FUT"        # ICE Brent crude (IFEU.IMPACT)
#   "G|G.FUT"            # ICE Gasoil (IFEU.IMPACT)
#   "SB|SB.FUT"          # ICE Sugar No.11 (IFUS.IMPACT)
#   "CC|CC.FUT"          # ICE Cocoa (IFUS.IMPACT)
declare -a ICE_ROOTS=()

if (( ${#ICE_ROOTS[@]} == 0 )); then
    echo "WARN: ICE_ROOTS empty — no ICE roots declared in tradfi instrument universe yet." >&2
    echo "      This is a scaffolding launcher. Extend ICE_ROOTS when operator picks roots." >&2
    echo "      Plan reference: tradfi_ohlcv_only_mvp_backfill_2026_05_15.md Phase 6." >&2
    exit 0
fi

ohlcv_check_singleton_lock "$FORCE" "$DRY_RUN"

YEAR_SHARDS="$(ohlcv_year_shards "${START_FLOOR:0:4}" "$START_FLOOR")"
IFS=';' read -ra _shards <<< "$YEAR_SHARDS"

for spec in "${ICE_ROOTS[@]}"; do
    root="${spec%%|*}"
    syms="${spec##*|}"
    for shard in "${_shards[@]}"; do
        start="${shard%%:*}"
        end="${shard##*:}"
        year="${start:0:4}"
        run_ts="$(date +%Y%m%d-%H%M%S)"
        # bash-3-compat: ${var,,} is bash 4+; macOS ships bash 3.2.
        root_lower="$(echo "$root" | tr '[:upper:]' '[:lower:]')"
        root_safe="${root_lower//./-}"
        vm_name="tradfi-bf-ice-ohlcv-1m-${root_safe}-${year}-${run_ts}"
        ohlcv_create_vm "$vm_name" "ICE" "$start" "$end" "$syms" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
    done
done

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "=========================================="
    echo "DRY-RUN: ICE OHLCV-1m year-shards (${START_FLOOR}..today)"
    echo "=========================================="
else
    echo "ICE OHLCV-1m year-shards launched in ${TRADFI_OHLCV_ZONE}."
fi
