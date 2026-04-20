#!/usr/bin/env bash
# Tier-0 instruments-service smoke launcher — spawn an instruments-service VM
# that writes ONE day of reference data per category with IS_TEST_RUN=true
# so writes land in `instruments-store-{cat}-test-{project_id}` buckets instead
# of production.
#
# Dep-order companion to launch-canonical-smoke-vm.sh:
#   1. Run THIS first — instruments-service writes reference data.
#   2. Then launch-canonical-smoke-vm.sh — MTDS reads that reference data.
#
# MTDS raises `NO INSTRUMENTS FOUND` without this prereq (shard-level isolated,
# rc=0, but zero records written). Canary run 2026-04-20 confirmed that CEFI
# was blocked while TRADFI (CME has its own feed) and DEFI (on-chain reads)
# ran green without a Tier-0 prereq.
#
# Usage:
#   bash launch-instruments-smoke-vm.sh cefi 2026-04-19           # CeFi BINANCE-FUTURES
#   bash launch-instruments-smoke-vm.sh tradfi 2026-04-19         # TradFi CME
#   bash launch-instruments-smoke-vm.sh defi 2026-04-19           # DeFi AAVEV3-ETHEREUM
#   bash launch-instruments-smoke-vm.sh all 2026-04-19            # all three sequentially
#
# Inspect after run:
#   gsutil ls -r gs://instruments-store-cefi-test-central-element-323112/
#   gsutil ls -r gs://instruments-store-tradfi-test-central-element-323112/
#   gsutil ls -r gs://instruments-store-defi-test-central-element-323112/
set -euo pipefail

CATEGORY="${1:-all}"
SMOKE_DATE="${2:-$(date -u -v-1d +%Y-%m-%d 2>/dev/null || date -u -d 'yesterday' +%Y-%m-%d)}"
ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

RUN_TS="$(date +%Y%m%d-%H%M%S)"

launch_vm() {
    local cat="$1"; local venue="$2"; local vm_name="instruments-smoke-${cat}-${RUN_TS}"
    echo "Launching $vm_name for $cat ($venue, $SMOKE_DATE)"

    local md="VM_TASK=instruments-smoke"
    md="${md},VM_SERVICE=instruments_service"
    md="${md},VM_OPERATION=download"   # setup-data-pipeline-vm.sh rewrites download→instruments for instruments_service
    md="${md},VM_CATEGORY=$(echo "$cat" | tr '[:lower:]' '[:upper:]')"
    md="${md},VM_VENUE=${venue}"
    md="${md},VM_START_DATE=${SMOKE_DATE}"
    md="${md},VM_END_DATE=${SMOKE_DATE}"
    md="${md},IS_TEST_RUN=true"

    gcloud compute instances create "$vm_name" \
        --project="$PROJECT" \
        --zone="$ZONE" \
        --machine-type=e2-standard-4 \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --scopes=cloud-platform \
        --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
        --labels=purpose=instruments-smoke,category="${cat}",run-ts="${RUN_TS}"
    echo "  → SSH: gcloud compute ssh $vm_name --zone=$ZONE"
    echo "  → Delete: gcloud compute instances delete $vm_name --zone=$ZONE --quiet"
}

case "$CATEGORY" in
    cefi)   launch_vm cefi BINANCE-FUTURES ;;
    tradfi) launch_vm tradfi CME ;;
    defi)   launch_vm defi AAVEV3-ETHEREUM ;;
    all)
        launch_vm cefi BINANCE-FUTURES
        launch_vm tradfi CME
        launch_vm defi AAVEV3-ETHEREUM
        ;;
    *) echo "Unknown category: $CATEGORY (expected cefi|tradfi|defi|all)"; exit 2 ;;
esac
