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
CATEGORY_OVERRIDE=""

usage() {
    echo "Usage: $0 --cluster <name> --start-date YYYY-MM-DD --end-date YYYY-MM-DD [options]"
    echo ""
    echo "Options:"
    echo "  --cluster        Cluster name: cefi, defi, tradfi, sports, prediction"
    echo "  --start-date     Start date (YYYY-MM-DD)"
    echo "  --end-date       End date (YYYY-MM-DD)"
    echo "  --skip-existing  Skip dates/entities that already have data in GCS"
    echo "  --dry-run        Log commands without executing"
    echo "  --category       Override category (default: from cluster config)"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --cluster)     CLUSTER="$2"; shift 2 ;;
        --start-date)  START_DATE="$2"; shift 2 ;;
        --end-date)    END_DATE="$2"; shift 2 ;;
        --skip-existing) SKIP_EXISTING="--skip-existing"; shift ;;
        --dry-run)     DRY_RUN="true"; shift ;;
        --category)    CATEGORY_OVERRIDE="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

[[ -z "$CLUSTER" || -z "$START_DATE" || -z "$END_DATE" ]] && usage

# Resolve category from cluster config
if [[ -n "$CATEGORY_OVERRIDE" ]]; then
    CATEGORY="$CATEGORY_OVERRIDE"
else
    CATEGORY=$(python3 -c "
import yaml, sys
with open('${REPO_ROOT}/configs/clusters/${CLUSTER}.yaml') as f:
    cfg = yaml.safe_load(f)
print(cfg.get('category', 'ALL'))
" 2>/dev/null || echo "ALL")
fi

echo "=========================================="
echo "Cluster backfill: ${CLUSTER}"
echo "Category: ${CATEGORY}"
echo "Date range: ${START_DATE} → ${END_DATE}"
echo "Skip existing: ${SKIP_EXISTING:-no}"
echo "Dry run: ${DRY_RUN:-no}"
echo "=========================================="

# Resolve workspace root (parent of deployment-service)
WORKSPACE_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"

run_service() {
    local svc="$1"
    local svc_under="${svc//-/_}"
    local svc_dir="${WORKSPACE_ROOT}/${svc}"
    local extra_args="${2:-}"

    if [[ ! -d "$svc_dir" ]]; then
        echo "SKIP: ${svc} — repo not found at ${svc_dir}"
        return 0
    fi

    local cmd="cd ${svc_dir} && python -m ${svc_under} --operation compute --mode batch --start-date ${START_DATE} --end-date ${END_DATE}"

    # Add category if the service supports it
    case "$svc" in
        instruments-service)
            cmd="cd ${svc_dir} && python -m ${svc_under} --operation instruments --mode batch --category ${CATEGORY} --start-date ${START_DATE} --end-date ${END_DATE}"
            ;;
        market-tick-data-service)
            cmd="cd ${svc_dir} && python -m ${svc_under} --operation download --mode batch --category ${CATEGORY} --start-date ${START_DATE} --end-date ${END_DATE}"
            ;;
        market-data-processing-service)
            cmd="cd ${svc_dir} && MDPS_CATEGORY=${CATEGORY} python -m ${svc_under}.cli.main --operation process --mode batch --start-date ${START_DATE} --end-date ${END_DATE} --${CATEGORY}"
            ;;
        features-sports-service)
            # Sports features use --date (single day) — iterate
            cmd="ITERATE_DATES"
            ;;
        features-*)
            cmd="cd ${svc_dir} && python -m ${svc_under} --operation compute --mode batch --category ${CATEGORY} --start-date ${START_DATE} --end-date ${END_DATE}"
            ;;
        ml-training-service)
            cmd="cd ${svc_dir} && python -m ${svc_under} --operation train --mode batch --category ${CATEGORY} --start-date ${START_DATE} --end-date ${END_DATE}"
            ;;
        ml-inference-service)
            cmd="cd ${svc_dir} && python -m ${svc_under} --operation infer --mode batch --category ${CATEGORY} --start-date ${START_DATE} --end-date ${END_DATE}"
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

# Data pipeline services in DAG order (from dependencies.yaml execution_order)
# Only run data pipeline services — skip strategy/execution/risk for backfill
PIPELINE_SERVICES=(
    instruments-service
    market-tick-data-service
    market-data-processing-service
)

# Feature services depend on cluster
case "$CLUSTER" in
    cefi)
        PIPELINE_SERVICES+=(
            features-calendar-service
            features-delta-one-service
            features-onchain-service
            features-cross-instrument-service
            features-multi-timeframe-service
            ml-training-service
            ml-inference-service
        )
        ;;
    defi)
        PIPELINE_SERVICES+=(
            features-calendar-service
            features-onchain-service
        )
        ;;
    tradfi)
        PIPELINE_SERVICES+=(
            features-calendar-service
            features-delta-one-service
            features-volatility-service
            features-cross-instrument-service
            features-multi-timeframe-service
            ml-training-service
            ml-inference-service
        )
        ;;
    sports)
        PIPELINE_SERVICES+=(
            features-sports-service
            ml-training-service
            ml-inference-service
        )
        ;;
    prediction)
        PIPELINE_SERVICES+=(
            features-sports-service
            ml-inference-service
        )
        ;;
    full)
        PIPELINE_SERVICES+=(
            features-calendar-service
            features-delta-one-service
            features-volatility-service
            features-onchain-service
            features-sports-service
            features-cross-instrument-service
            features-multi-timeframe-service
            features-commodity-service
            ml-training-service
            ml-inference-service
        )
        ;;
esac

echo ""
echo "Pipeline order (${#PIPELINE_SERVICES[@]} services):"
for svc in "${PIPELINE_SERVICES[@]}"; do
    echo "  - ${svc}"
done
echo ""

for svc in "${PIPELINE_SERVICES[@]}"; do
    run_service "$svc"
    echo ""
done

echo "=========================================="
echo "Backfill complete for cluster: ${CLUSTER}"
echo "=========================================="
