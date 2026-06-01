#!/usr/bin/env bash
# Per-cluster VM backfill template
#
# Invokes the full pipeline DAG (instruments → MTDS → MDPS → features → ML)
# for a date range on a specific cluster. Uses service CLIs via the standard
# --mode batch --start-date --end-date pattern.
#
# Usage:
#   bash scripts/vm/backfill-cluster.sh --cluster cefi --start-date 2025-01-01 --end-date 2025-12-31
#   bash scripts/vm/backfill-cluster.sh --cluster sports --start-date 2025-07-01 --end-date 2025-12-31
#   bash scripts/vm/backfill-cluster.sh --cluster defi --start-date 2025-01-01 --end-date 2025-03-31 --skip-existing
#
# The script reads the cluster config to determine which services to run and
# the dependencies.yaml to determine execution order.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Defaults
CLUSTER=""
START_DATE=""
END_DATE=""
SKIP_EXISTING=""
DRY_RUN=""
ASSET_GROUP_OVERRIDE=""
STRATEGY=""
LAYER_ONLY=""
FROM_LAYER=""

usage() {
    echo "Usage: $0 --cluster <name> --start-date YYYY-MM-DD --end-date YYYY-MM-DD [options]"
    echo ""
    echo "Options:"
    echo "  --cluster        Cluster name: cefi, defi, tradfi, sports, prediction, full"
    echo "  --start-date     Start date (YYYY-MM-DD)"
    echo "  --end-date       End date (YYYY-MM-DD)"
    echo "  --skip-existing  Skip dates/entities that already have data in GCS"
    echo "  --dry-run        Log commands without executing"
    echo "  --asset-group       Override category (default: from cluster config)"
    echo "  --strategy <id>  Run single strategy within cluster (L6-L7)"
    echo "  --layer <L1-L7>  Run specific layer only"
    echo "  --from-layer <L1-L7>  Start from this layer (skip earlier layers)"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --cluster)     CLUSTER="$2"; shift 2 ;;
        --start-date)  START_DATE="$2"; shift 2 ;;
        --end-date)    END_DATE="$2"; shift 2 ;;
        --skip-existing) SKIP_EXISTING="--skip-existing"; shift ;;
        --dry-run)     DRY_RUN="true"; shift ;;
        --asset-group)    ASSET_GROUP_OVERRIDE="$2"; shift 2 ;;
        --strategy)    STRATEGY="$2"; shift 2 ;;
        --layer)       LAYER_ONLY="$2"; shift 2 ;;
        --from-layer)  FROM_LAYER="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

[[ -z "$CLUSTER" || -z "$START_DATE" || -z "$END_DATE" ]] && usage

# Layer number extraction helper
layer_num() {
    echo "${1#L}" # L3 → 3
}

# Resolve category from cluster config
if [[ -n "$ASSET_GROUP_OVERRIDE" ]]; then
    ASSET_GROUP="$ASSET_GROUP_OVERRIDE"
else
    ASSET_GROUP=$(python3 -c "
import yaml, sys
with open('${REPO_ROOT}/configs/clusters/${CLUSTER}.yaml') as f:
    cfg = yaml.safe_load(f)
print(cfg.get('category', 'ALL'))
" 2>/dev/null || echo "ALL")
fi

echo "=========================================="
echo "Cluster backfill: ${CLUSTER}"
echo "Category: ${ASSET_GROUP}"
echo "Date range: ${START_DATE} → ${END_DATE}"
echo "Skip existing: ${SKIP_EXISTING:-no}"
echo "Dry run: ${DRY_RUN:-no}"
[[ -n "$STRATEGY" ]] && echo "Strategy: ${STRATEGY}"
[[ -n "$LAYER_ONLY" ]] && echo "Layer only: ${LAYER_ONLY}"
[[ -n "$FROM_LAYER" ]] && echo "From layer: ${FROM_LAYER}"
echo "=========================================="

# Resolve workspace root (parent of deployment-service)
WORKSPACE_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"

run_service() {
    local svc="$1"
    local svc_under="${svc//-/_}"
    local svc_dir="${WORKSPACE_ROOT}/${svc}"
    local extra_args="${2:-}"

    # Post-consolidation: pnl-attribution-service, risk-and-exposure-service,
    # position-balance-monitor-service are sub-packages of strategy-service.
    # Route dir-check to strategy-service to avoid spurious SKIP.
    # (strategy_repo_consolidation_2026_05_19.md)
    case "$svc" in
        pnl-attribution-service|risk-and-exposure-service|position-balance-monitor-service)
            svc_dir="${WORKSPACE_ROOT}/strategy-service"
            ;;
    esac

    if [[ ! -d "$svc_dir" ]]; then
        echo "SKIP: ${svc} — repo not found at ${svc_dir}"
        return 0
    fi

    local cmd="cd ${svc_dir} && python -m ${svc_under} --operation compute --mode batch --start-date ${START_DATE} --end-date ${END_DATE}"

    # Add category if the service supports it
    case "$svc" in
        instruments-service)
            cmd="cd ${svc_dir} && python -m ${svc_under} --operation instruments --mode batch --asset-group ${ASSET_GROUP} --start-date ${START_DATE} --end-date ${END_DATE}"
            ;;
        market-tick-data-service)
            cmd="cd ${svc_dir} && python -m ${svc_under} --operation download --mode batch --asset-group ${ASSET_GROUP} --start-date ${START_DATE} --end-date ${END_DATE}"
            ;;
        market-data-processing-service)
            cmd="cd ${svc_dir} && MDPS_ASSET_GROUP=${ASSET_GROUP} python -m ${svc_under}.cli.main --operation process --mode batch --start-date ${START_DATE} --end-date ${END_DATE} --${ASSET_GROUP}"
            ;;
        features-sports-service)
            # Sports features use --date (single day) — iterate
            cmd="ITERATE_DATES"
            ;;
        features-*)
            cmd="cd ${svc_dir} && python -m ${svc_under} --operation compute --mode batch --asset-group ${ASSET_GROUP} --start-date ${START_DATE} --end-date ${END_DATE}"
            ;;
        ml-training-service)
            cmd="cd ${svc_dir} && python -m ${svc_under} --operation train --mode batch --asset-group ${ASSET_GROUP} --start-date ${START_DATE} --end-date ${END_DATE}"
            ;;
        ml-inference-service)
            cmd="cd ${svc_dir} && python -m ${svc_under} --operation infer --mode batch --asset-group ${ASSET_GROUP} --start-date ${START_DATE} --end-date ${END_DATE}"
            ;;
        strategy-service)
            cmd="cd ${svc_dir} && python -m ${svc_under} --operation backtest --mode batch --asset-group ${ASSET_GROUP} --start-date ${START_DATE} --end-date ${END_DATE}"
            [[ -n "$STRATEGY" ]] && cmd="${cmd} --strategy ${STRATEGY}"
            ;;
        execution-service)
            cmd="cd ${svc_dir} && python -m ${svc_under} --operation backtest --mode batch --asset-group ${ASSET_GROUP} --start-date ${START_DATE} --end-date ${END_DATE}"
            [[ -n "$STRATEGY" ]] && cmd="${cmd} --strategy ${STRATEGY}"
            ;;
        pnl-attribution-service)
            # Consolidated into strategy-service post-consolidation (strategy_repo_consolidation_2026_05_19.md).
            # cd into strategy-service dir (not the archived pnl-attribution-service dir).
            cmd="cd ${WORKSPACE_ROOT}/strategy-service && python -m strategy_service --operation pnl-attribution --mode batch --asset-group ${ASSET_GROUP} --start-date ${START_DATE} --end-date ${END_DATE}"
            [[ -n "$STRATEGY" ]] && cmd="${cmd} --strategy ${STRATEGY}"
            ;;
        risk-and-exposure-service)
            # Consolidated into strategy-service post-consolidation (strategy_repo_consolidation_2026_05_19.md).
            # cd into strategy-service dir (not the archived risk-and-exposure-service dir).
            cmd="cd ${WORKSPACE_ROOT}/strategy-service && python -m strategy_service --operation risk-monitor --mode batch --asset-group ${ASSET_GROUP} --start-date ${START_DATE} --end-date ${END_DATE}"
            [[ -n "$STRATEGY" ]] && cmd="${cmd} --strategy ${STRATEGY}"
            ;;
        position-balance-monitor-service)
            # Consolidated into strategy-service post-consolidation (strategy_repo_consolidation_2026_05_19.md).
            # cd into strategy-service dir (not the archived position-balance-monitor-service dir).
            cmd="cd ${WORKSPACE_ROOT}/strategy-service && python -m strategy_service --operation position-recon --mode batch --asset-group ${ASSET_GROUP} --start-date ${START_DATE} --end-date ${END_DATE}"
            ;;
        *)
            echo "SKIP: ${svc} — no backfill command defined"
            return 0
            ;;
    esac

    if [[ "$cmd" == "ITERATE_DATES" ]]; then
        echo "--- ${svc} (date iteration) ---"
        local current="$START_DATE"
        while [[ "$current" < "$END_DATE" || "$current" == "$END_DATE" ]]; do
            local day_cmd="cd ${svc_dir} && python -m ${svc_under} --operation compute --mode batch --date ${current} ${SKIP_EXISTING}"
            if [[ -n "$DRY_RUN" ]]; then
                echo "DRY RUN: ${day_cmd} ${extra_args}"
            else
                echo "RUN: ${day_cmd} ${extra_args}"
                eval "${day_cmd} ${extra_args}" || echo "WARN: ${svc} failed for ${current} — continuing"
            fi
            current=$(date -j -v+1d -f "%Y-%m-%d" "$current" "+%Y-%m-%d" 2>/dev/null || date -d "$current + 1 day" "+%Y-%m-%d")
        done
        return 0
    fi

    cmd="${cmd} ${extra_args} ${SKIP_EXISTING}"

    if [[ -n "$DRY_RUN" ]]; then
        echo "DRY RUN: ${cmd}"
    else
        echo "--- ${svc} ---"
        echo "RUN: ${cmd}"
        eval "$cmd" || echo "WARN: ${svc} failed — continuing with next service"
    fi
}

# ── Pipeline layers (L1-L7) ──
# Each layer is an array of "layer_number:service" pairs.
# L1: Reference data
# L2: Market data download
# L3: Market data processing
# L4: Feature computation
# L5: ML training + inference
# L6: Strategy + execution backtest
# L7: PnL + risk attribution

# L1-L3: Common to all clusters
L1_SERVICES=("instruments-service")
L2_SERVICES=("market-tick-data-service")
L3_SERVICES=("market-data-processing-service")

# L4-L7: Cluster-dependent
L4_SERVICES=()
L5_SERVICES=()
L6_SERVICES=()
L7_SERVICES=()

case "$CLUSTER" in
    cefi)
        L4_SERVICES=(
            features-calendar-service
            features-delta-one-service
            features-onchain-service
            features-cross-instrument-service
            features-multi-timeframe-service
        )
        L5_SERVICES=(ml-training-service ml-inference-service)
        L6_SERVICES=(strategy-service execution-service)
        L7_SERVICES=(pnl-attribution-service risk-and-exposure-service)
        ;;
    defi)
        L4_SERVICES=(
            features-calendar-service
            features-onchain-service
            features-delta-one-service
        )
        L5_SERVICES=()
        L6_SERVICES=(strategy-service execution-service)
        L7_SERVICES=(pnl-attribution-service risk-and-exposure-service)
        ;;
    tradfi)
        L4_SERVICES=(
            features-calendar-service
            features-delta-one-service
            features-volatility-service
            features-cross-instrument-service
            features-multi-timeframe-service
        )
        L5_SERVICES=(ml-training-service ml-inference-service)
        L6_SERVICES=(strategy-service execution-service)
        L7_SERVICES=(pnl-attribution-service risk-and-exposure-service)
        ;;
    sports)
        L4_SERVICES=(features-sports-service)
        L5_SERVICES=(ml-training-service ml-inference-service)
        L6_SERVICES=(strategy-service execution-service)
        L7_SERVICES=(pnl-attribution-service risk-and-exposure-service)
        ;;
    prediction)
        L4_SERVICES=(features-cross-instrument-service)
        L5_SERVICES=(ml-inference-service)
        L6_SERVICES=(strategy-service execution-service)
        L7_SERVICES=(pnl-attribution-service risk-and-exposure-service)
        ;;
    full)
        L4_SERVICES=(
            features-calendar-service
            features-delta-one-service
            features-volatility-service
            features-onchain-service
            features-sports-service
            features-cross-instrument-service
            features-multi-timeframe-service
            features-commodity-service
        )
        L5_SERVICES=(ml-training-service ml-inference-service)
        L6_SERVICES=(strategy-service execution-service)
        L7_SERVICES=(pnl-attribution-service risk-and-exposure-service)
        ;;
esac

# Build ordered pipeline with layer filtering
should_run_layer() {
    local layer_num="$1"
    if [[ -n "$LAYER_ONLY" ]]; then
        [[ "$layer_num" == "$(layer_num "$LAYER_ONLY")" ]]
        return $?
    fi
    if [[ -n "$FROM_LAYER" ]]; then
        [[ "$layer_num" -ge "$(layer_num "$FROM_LAYER")" ]]
        return $?
    fi
    return 0
}

PIPELINE_SERVICES=()
PIPELINE_LAYERS=()

add_layer() {
    local layer_num="$1"
    shift
    local services=("$@")
    if should_run_layer "$layer_num"; then
        for svc in "${services[@]}"; do
            PIPELINE_SERVICES+=("$svc")
            PIPELINE_LAYERS+=("L${layer_num}")
        done
    fi
}

add_layer 1 "${L1_SERVICES[@]}"
add_layer 2 "${L2_SERVICES[@]}"
add_layer 3 "${L3_SERVICES[@]}"
[[ ${#L4_SERVICES[@]} -gt 0 ]] && add_layer 4 "${L4_SERVICES[@]}"
[[ ${#L5_SERVICES[@]} -gt 0 ]] && add_layer 5 "${L5_SERVICES[@]}"
[[ ${#L6_SERVICES[@]} -gt 0 ]] && add_layer 6 "${L6_SERVICES[@]}"
[[ ${#L7_SERVICES[@]} -gt 0 ]] && add_layer 7 "${L7_SERVICES[@]}"

echo ""
echo "Pipeline order (${#PIPELINE_SERVICES[@]} services):"
for i in "${!PIPELINE_SERVICES[@]}"; do
    echo "  ${PIPELINE_LAYERS[$i]}: ${PIPELINE_SERVICES[$i]}"
done
echo ""

current_layer=""
for i in "${!PIPELINE_SERVICES[@]}"; do
    svc="${PIPELINE_SERVICES[$i]}"
    layer="${PIPELINE_LAYERS[$i]}"
    if [[ "$layer" != "$current_layer" ]]; then
        [[ -n "$current_layer" ]] && echo "--- ${current_layer} complete ---"
        echo ""
        echo "=== Layer ${layer} ==="
        current_layer="$layer"
    fi
    run_service "$svc"
    echo ""
done
[[ -n "$current_layer" ]] && echo "--- ${current_layer} complete ---"

echo "=========================================="
echo "Backfill complete for cluster: ${CLUSTER}"
echo "=========================================="
