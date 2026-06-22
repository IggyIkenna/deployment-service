#!/usr/bin/env bash
# Launch CME OHLCV-1m backfill VMs (one per (root, year-shard)).
#
# Roots per OHLCV-only MVP plan (tradfi_ohlcv_only_mvp_backfill_2026_05_15.md
# Phase 6): ES + MES + NQ + MNQ + CL + GC + ES_OPT (CME options chain).
#
# Symbol-set: Databento parent symbology — one symbol per root pulls the full
# chain for the date window (BTC.FUT/ES.FUT/NQ.FUT/CL.FUT/...). Options use
# the 11-cluster ES_OPT_PARENTS set.
#
# Window: 2019-01-01 → today by default (per operator direction "full period
# for tradfi"). Override with `--start-floor YYYY-MM-DD`.
#
# Per-VM shard isolation: VM_NAME + MANIFEST_PER_VM_SHARDS=true (CLAUDE.md
# HARD RULE). Singleton lock matches ^tradfi-bf-.
#
# Usage:
#   bash launch-tradfi-bf-cme-ohlcv-1m.sh --dry-run
#   bash launch-tradfi-bf-cme-ohlcv-1m.sh
#   bash launch-tradfi-bf-cme-ohlcv-1m.sh --env staging --start-floor 2020-01-01
#
# SSOT: tradfi_ohlcv_only_mvp_backfill_2026_05_15.md Phase 6.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_tradfi-ohlcv-launcher-lib.sh
source "${SCRIPT_DIR}/_tradfi-ohlcv-launcher-lib.sh"

# Extract optional --only-root <ROOT> before passing remaining args to the
# common parser. Allows surgical per-root rollouts to stagger load on the
# shared GLBX.MDP3 dataset.
ONLY_ROOT=""
_remaining_args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --only-root) ONLY_ROOT="$2"; shift 2 ;;
        *)           _remaining_args+=("$1"); shift ;;
    esac
done
set -- "${_remaining_args[@]+"${_remaining_args[@]}"}"

ohlcv_parse_common_args "$@"

# CME root universe (parent-symbol set). Each entry: "root|parent_symbols"
# semicolon-delimited parent-symbols form a single VM's instrument-ids list.
# CME root universe — ALL 47 catalogue future roots (operator 2026-06-22: download
# everything, no client-side filters; the market is the filter — listed-no-trade -> empty).
# Each root fetched as <root>.FUT + <root>.OPT (options bundle is empty for futures-only roots).
declare -a CME_ROOTS=(
    "6A|6A.FUT;6A.OPT"
    "6B|6B.FUT;6B.OPT"
    "6C|6C.FUT;6C.OPT"
    "6E|6E.FUT;6E.OPT"
    "6J|6J.FUT;6J.OPT"
    "6L|6L.FUT;6L.OPT"
    "6M|6M.FUT;6M.OPT"
    "6N|6N.FUT;6N.OPT"
    "6S|6S.FUT;6S.OPT"
    "6Z|6Z.FUT;6Z.OPT"
    "BTC|BTC.FUT;BTC.OPT"
    "CL|CL.FUT;CL.OPT"
    "CT|CT.FUT;CT.OPT"
    "ES|ES.FUT;ES.OPT"
    "ETH|ETH.FUT;ETH.OPT"
    "GC|GC.FUT;GC.OPT"
    "HE|HE.FUT;HE.OPT"
    "HG|HG.FUT;HG.OPT"
    "HO|HO.FUT;HO.OPT"
    "LE|LE.FUT;LE.OPT"
    "MBT|MBT.FUT;MBT.OPT"
    "MES|MES.FUT;MES.OPT"
    "MET|MET.FUT;MET.OPT"
    "NG|NG.FUT;NG.OPT"
    "NKD|NKD.FUT;NKD.OPT"
    "NQ|NQ.FUT;NQ.OPT"
    "RB|RB.FUT;RB.OPT"
    "RTY|RTY.FUT;RTY.OPT"
    "SI|SI.FUT;SI.OPT"
    "XAB|XAB.FUT;XAB.OPT"
    "XAF|XAF.FUT;XAF.OPT"
    "XAI|XAI.FUT;XAI.OPT"
    "XAK|XAK.FUT;XAK.OPT"
    "XAP|XAP.FUT;XAP.OPT"
    "XAU|XAU.FUT;XAU.OPT"
    "XAV|XAV.FUT;XAV.OPT"
    "XAY|XAY.FUT;XAY.OPT"
    "YM|YM.FUT;YM.OPT"
    "ZB|ZB.FUT;ZB.OPT"
    "ZC|ZC.FUT;ZC.OPT"
    "ZF|ZF.FUT;ZF.OPT"
    "ZL|ZL.FUT;ZL.OPT"
    "ZM|ZM.FUT;ZM.OPT"
    "ZN|ZN.FUT;ZN.OPT"
    "ZS|ZS.FUT;ZS.OPT"
    "ZT|ZT.FUT;ZT.OPT"
    "ZW|ZW.FUT;ZW.OPT"
    "ECES|ECES.OPT"  # CME event contract (binary market), GLBX.MDP3, coverage 2025-09-28+
    "ECBTC|ECBTC.OPT"  # CME event contract (binary market), GLBX.MDP3, coverage 2025-09-28+
    "ECRTY|ECRTY.OPT"  # CME event contract (binary market), GLBX.MDP3, coverage 2025-09-28+
    "ECYM|ECYM.OPT"  # CME event contract (binary market), GLBX.MDP3, coverage 2025-09-28+
    "ECGC|ECGC.OPT"  # CME event contract (binary market), GLBX.MDP3, coverage 2025-09-28+
    "ECCL|ECCL.OPT"  # CME event contract (binary market), GLBX.MDP3, coverage 2025-09-28+
    "ECNG|ECNG.OPT"  # CME event contract (binary market), GLBX.MDP3, coverage 2025-09-28+
    "EC6E|EC6E.OPT"  # CME event contract (binary market), GLBX.MDP3, coverage 2025-09-28+
    "ECNQ|ECNQ.OPT"  # CME event contract (binary market), GLBX.MDP3, coverage 2025-09-28+
)

if [[ -n "$ONLY_ROOT" ]]; then
    _filtered=()
    for spec in "${CME_ROOTS[@]}"; do
        if [[ "${spec%%|*}" == "$ONLY_ROOT" ]]; then
            _filtered+=("$spec")
        fi
    done
    if (( ${#_filtered[@]} == 0 )); then
        echo "ERROR: --only-root '$ONLY_ROOT' does not match any CME_ROOTS entry" >&2
        echo "Available roots: ES MES NQ MNQ CL GC ES_OPT" >&2
        exit 1
    fi
    CME_ROOTS=("${_filtered[@]}")
fi

ohlcv_check_singleton_lock "$FORCE" "$DRY_RUN"

YEAR_SHARDS="$(ohlcv_year_shards "${START_FLOOR:0:4}" "$START_FLOOR")"
YEAR_SHARDS="$(ohlcv_apply_year_filter "$YEAR_SHARDS")"
IFS=';' read -ra _shards <<< "$YEAR_SHARDS"
if (( ${#_shards[@]} == 0 )); then
    echo "ERROR: no year-shards match --year=${ONLY_YEAR}" >&2; exit 1
fi

for spec in "${CME_ROOTS[@]}"; do
    root="${spec%%|*}"
    syms="${spec##*|}"
    for shard in "${_shards[@]}"; do
        start="${shard%%:*}"
        end="${shard##*:}"
        year="${start:0:4}"
        run_ts="$(date +%Y%m%d-%H%M%S)"
        # bash-3-compat (${var,,} is bash 4+; macOS + many Ubuntu images ship bash 3).
        # gcloud VM names can't contain '.' either — `ES.FUT` → `es-fut`.
        root_lower="$(echo "$root" | tr '[:upper:]' '[:lower:]')"
        root_safe="${root_lower//./-}"
        vm_name="tradfi-bf-cme-ohlcv-1m-${root_safe}-${year}-${run_ts}"
        ohlcv_create_vm "$vm_name" "CME" "$start" "$end" "$syms" "$DRY_RUN" "$DEPLOYMENT_ENV" "$FORCE_WINDOW"
    done
done

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "=========================================="
    echo "DRY-RUN: CME OHLCV-1m year-shards (${START_FLOOR}..today)"
    echo "=========================================="
else
    echo "CME OHLCV-1m year-shards launched in ${TRADFI_OHLCV_ZONE}."
    echo "Manifest check (post-drain):"
    echo "  gsutil cp gs://market-data-tick-tradfi-${TRADFI_OHLCV_PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
    echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[(df.venue=='CME')&(df.data_type=='ohlcv_1m')].groupby(['symbol','capture_status']).size())\""
fi
