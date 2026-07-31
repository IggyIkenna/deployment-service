#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
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
#   bash launch-instruments-smoke-vm.sh defi 2026-04-19           # DeFi AAVE_V3-ETHEREUM
#   bash launch-instruments-smoke-vm.sh all 2026-04-19            # all three sequentially
#
# Inspect after run:
#   gsutil ls -r gs://instruments-store-cefi-test-central-element-323112/
#   gsutil ls -r gs://instruments-store-tradfi-test-central-element-323112/
#   gsutil ls -r gs://instruments-store-defi-test-central-element-323112/
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
SMOKE_DATE="${2:-$(date -u -v-1d +%Y-%m-%d 2>/dev/null || date -u -d 'yesterday' +%Y-%m-%d)}"
ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="$(lc_code_bucket "$PROJECT")"

lc_validate_env "$DEPLOYMENT_ENV"

RUN_TS="$(lc_run_ts)"

launch_vm() {
    local cat="$1"; local venue="$2"; local vm_name="instruments-smoke-${cat}-${RUN_TS}"
    echo "Launching $vm_name for $cat ($venue, $SMOKE_DATE)"

    local md="VM_TASK=instruments-smoke"
    md="${md},VM_SERVICE=instruments_service"
    md="${md},VM_OPERATION=download"   # setup-data-pipeline-vm.sh rewrites download→instruments for instruments_service
    md="${md},VM_ASSET_GROUP=$(echo "$cat" | tr '[:lower:]' '[:upper:]')"
    md="${md},VM_VENUE=${venue}"
    md="${md},VM_START_DATE=${SMOKE_DATE}"
    md="${md},VM_END_DATE=${SMOKE_DATE}"
    md="${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    md="${md},IS_TEST_RUN=true"

    if [[ "${DRY_RUN:-false}" != "true" ]]; then
        lc_verify_tarball_freshness "$CODE_BUCKET" \
            instruments-service unified-api-contracts unified-trading-library deployment-service \
            || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
    fi

    lc_gcloud_create "$vm_name" "$PROJECT" "$ZONE" "e2-standard-4" "30" \
        "startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
        "purpose=instruments-smoke,category=${cat},env=${DEPLOYMENT_ENV},run-ts=${RUN_TS}" \
        "$(lc_tier_service_account "$DEPLOYMENT_ENV" "$PROJECT")"
    echo "  → SSH: gcloud compute ssh $vm_name --zone=$ZONE"
    echo "  → Delete: gcloud compute instances delete $vm_name --zone=$ZONE --quiet"
}

case "$ASSET_GROUP" in
    cefi)   launch_vm cefi BINANCE-FUTURES ;;
    tradfi) launch_vm tradfi CME ;;
    defi)   launch_vm defi AAVE_V3-ETHEREUM ;;
    all)
        launch_vm cefi BINANCE-FUTURES
        launch_vm tradfi CME
        launch_vm defi AAVE_V3-ETHEREUM
        ;;
    *) echo "Unknown category: $ASSET_GROUP (expected cefi|tradfi|defi|all)"; exit 2 ;;
esac
