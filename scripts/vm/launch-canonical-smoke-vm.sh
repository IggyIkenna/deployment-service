#!/usr/bin/env bash
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
#   - venue column protocol-only for DeFi (no composite AAVEV3-ETHEREUM)
#
# Usage:
#   bash launch-canonical-smoke-vm.sh cefi 2024-06-15             # CeFi Binance-Futures, 1 day
#   bash launch-canonical-smoke-vm.sh tradfi 2024-06-15           # TradFi CME, 1 day
#   bash launch-canonical-smoke-vm.sh defi 2024-06-15             # DeFi Aave V3 Ethereum, 1 day
#   bash launch-canonical-smoke-vm.sh all 2024-06-15              # all three sequentially
set -euo pipefail

CATEGORY="${1:-all}"
SMOKE_DATE="${2:-2024-06-15}"
ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

RUN_TS="$(date +%Y%m%d-%H%M%S)"

launch_vm() {
    local cat="$1"; local venue="$2"; local vm_name="canonical-smoke-${cat}-${RUN_TS}"
    local data_types="${3:-}"
    echo "Launching $vm_name for $cat ($venue, $SMOKE_DATE)"

    local md="VM_TASK=canonical-smoke"
    md="${md},VM_SERVICE=market_tick_data_service"
    md="${md},VM_OPERATION=download"
    md="${md},VM_CATEGORY=$(echo "$cat" | tr '[:lower:]' '[:upper:]')"
    md="${md},VM_VENUE=${venue}"
    md="${md},VM_START_DATE=${SMOKE_DATE}"
    md="${md},VM_END_DATE=${SMOKE_DATE}"
    [[ -n "$data_types" ]] && md="${md},VM_DATA_TYPES=${data_types}"
    md="${md},IS_TEST_RUN=true"

    gcloud compute instances create "$vm_name" \
        --project="$PROJECT" \
        --zone="$ZONE" \
        --machine-type=e2-medium \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --scopes=cloud-platform \
        --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
        --labels=purpose=canonical-smoke,category="${cat}",run-ts="${RUN_TS}"
    echo "  → SSH: gcloud compute ssh $vm_name --zone=$ZONE"
    echo "  → Delete: gcloud compute instances delete $vm_name --zone=$ZONE --quiet"
}

case "$CATEGORY" in
    cefi)   launch_vm cefi BINANCE-FUTURES ;;
    tradfi) launch_vm tradfi CME ;;
    defi)   launch_vm defi AAVE_V3 lending_indices ;;
    all)
        launch_vm cefi BINANCE-FUTURES
        launch_vm tradfi CME
        launch_vm defi AAVE_V3 lending_indices
        ;;
    *) echo "Unknown category: $CATEGORY"; exit 2 ;;
esac
