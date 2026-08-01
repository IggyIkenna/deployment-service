#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Phase 2 canonical-write smoke test launcher — spawn an MTDS VM that writes
# ONE day of data per category with IS_TEST_RUN=true so writes land in
# `-test-{project_id}` buckets (market-data-tick-test-cefi-..., etc.) instead
# of production.
#
# After the VM runs, inspect GCS:
#   gsutil ls -r gs://market-data-tick-test-cefi-central-element-323112/
#   gsutil ls -r gs://lending-indices-test-central-element-323112/
#
# Every parquet landed must have:
#   - Partition path: day/category/venue/instrument_type/data_type/{file}.parquet
#     (+ chain for DeFi)
#   - `instrument_id` column (non-null, canonical VENUE:INSTRUMENT_TYPE:SYMBOL)
#   - venue column protocol-only for DeFi (no composite AAVE_V3-ETHEREUM)
#
# Usage:
#   bash launch-canonical-smoke-vm.sh cefi 2024-06-15             # CeFi Binance-Futures, 1 day
#   bash launch-canonical-smoke-vm.sh tradfi 2024-06-15           # TradFi CME, 1 day
#   bash launch-canonical-smoke-vm.sh defi 2024-06-15             # DeFi Aave V3 Ethereum, 1 day
#   bash launch-canonical-smoke-vm.sh all 2024-06-15              # all three sequentially
#   bash launch-canonical-smoke-vm.sh cefi 2024-06-15 --env staging  # smoke against staging tier
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/launcher_common.sh
source "${SCRIPT_DIR}/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# Pre-parse --env <val> + --dry-run in any position before positional args.
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --dry-run) export LC_DRY_RUN=true; shift ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done
set -- "${POSITIONAL[@]:-}"

ASSET_GROUP="${1:-all}"
SMOKE_DATE="${2:-2024-06-15}"
ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="$(lc_code_bucket "$PROJECT")"

lc_validate_env "$DEPLOYMENT_ENV"

RUN_TS="$(lc_run_ts)"

launch_vm() {
    local cat="$1"; local venue="$2"; local vm_name="canonical-smoke-${cat}-${RUN_TS}"
    local data_types="${3:-}"
    local operation="${4:-download}"
    echo "Launching $vm_name for $cat ($venue, $SMOKE_DATE, op=$operation)"

    local md="VM_TASK=canonical-smoke"
    md="${md},VM_SERVICE=market_tick_data_service"
    md="${md},VM_OPERATION=${operation}"
    md="${md},VM_ASSET_GROUP=$(echo "$cat" | tr '[:lower:]' '[:upper:]')"
    md="${md},VM_VENUE=${venue}"
    md="${md},VM_START_DATE=${SMOKE_DATE}"
    md="${md},VM_END_DATE=${SMOKE_DATE}"
    [[ -n "$data_types" ]] && md="${md},VM_DATA_TYPES=${data_types}"
    md="${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    md="${md},IS_TEST_RUN=true"

    if [[ "${DRY_RUN:-false}" != "true" ]]; then
        lc_verify_tarball_freshness "$CODE_BUCKET" \
            market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
            || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
    fi

    if [[ "${DRY_RUN:-false}" != "true" ]]; then
        lc_verify_tarball_freshness "$CODE_BUCKET" \
            market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
            || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
    fi

    lc_gcloud_create "$vm_name" "$PROJECT" "$ZONE" "e2-standard-4" "30" \
        "startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
        "purpose=canonical-smoke,category=${cat},env=${DEPLOYMENT_ENV},run-ts=${RUN_TS}" \
        "$(lc_tier_service_account "$DEPLOYMENT_ENV" "$PROJECT" "true")"
    echo "  → SSH: gcloud compute ssh $vm_name --zone=$ZONE"
    echo "  → Delete: gcloud compute instances delete $vm_name --zone=$ZONE --quiet"
}

case "$ASSET_GROUP" in
    cefi)   launch_vm cefi BINANCE-FUTURES "" download ;;
    tradfi) launch_vm tradfi CME "" download ;;
    defi)   launch_vm defi AAVE_V3-ETHEREUM lending_indices collect-lending-indices ;;
    all)
        launch_vm cefi BINANCE-FUTURES "" download
        launch_vm tradfi CME "" download
        launch_vm defi AAVE_V3-ETHEREUM lending_indices collect-lending-indices
        ;;
    *) echo "Unknown category: $ASSET_GROUP"; exit 2 ;;
esac
