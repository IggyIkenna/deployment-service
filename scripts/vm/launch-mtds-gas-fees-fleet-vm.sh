#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch 8 GCE VMs for full gas fee backfill — one VM per EVM chain.
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first (CORE tarballs include MTDS).
#
# Each VM: chain-specific start date from UAC GAS_FEE_CHAIN_START_DATES.
# Chain coverage: ETHEREUM, OPTIMISM, BSC, POLYGON, BASE, ARBITRUM, AVALANCHE, LINEA.
# Alchemy API key fetched from Secret Manager at VM boot (cloud-platform scope).
#
# Usage:
#   bash launch-mtds-gas-fees-fleet-vm.sh                  # Launch all 8 VMs
#   bash launch-mtds-gas-fees-fleet-vm.sh --dry-run        # Print plan only
#   bash launch-mtds-gas-fees-fleet-vm.sh --chain 1        # Single chain only (by chain_id)
#   bash launch-mtds-gas-fees-fleet-vm.sh --env staging    # Staging env tier
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-2}"
DRY_RUN=false
SINGLE_CHAIN=""
END_DATE="$(date +%Y-%m-%d)"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# Chain configs: chain_id:name:start_date (from UAC GAS_FEE_CHAIN_START_DATES)
CHAINS=(
  "1:ethereum:2020-01-01"
  "10:optimism:2021-11-12"
  "56:bsc:2020-09-01"
  "137:polygon:2020-06-01"
  "8453:base:2023-08-09"
  "42161:arbitrum:2021-09-01"
  "43114:avalanche:2020-09-22"
  "59144:linea:2023-07-12"
)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone)    ZONE="$2"; shift 2 ;;
    --end)     END_DATE="$2"; shift 2 ;;
    --chain)   SINGLE_CHAIN="$2"; shift 2 ;;
    --env)     DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"

echo "============================================================"
echo "Gas Fees Fleet VM Launcher (Pattern A)"
echo "  Project:  ${PROJECT_ID}"
echo "  Zone:     ${ZONE}"
echo "  Machine:  ${MACHINE_TYPE}"
echo "  End date: ${END_DATE}"
echo "  Chain:    ${SINGLE_CHAIN:-all}"
echo "  Env:      ${DEPLOYMENT_ENV}"
echo "  Tarball:  gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

launch_chain_vm() {
  local CHAIN_ID="$1"
  local CHAIN_NAME="$2"
  local START_DATE="$3"
  local VM_NAME="mtds-gas-fees-${CHAIN_NAME}"

  echo ""
  echo "--- ${CHAIN_NAME} (chain_id=${CHAIN_ID}): ${START_DATE} → ${END_DATE} ---"

  if $DRY_RUN; then
    echo "  [DRY RUN] Would create VM: ${VM_NAME}"
    echo "  VM_TASK=defi-backfill  VM_OPERATION=collect-gas-fees  VM_GAS_FEE_CHAINS=${CHAIN_ID}"
    return 0
  fi

  # Delete existing VM if present (fleet pattern — re-run safe)
  gcloud compute instances delete "${VM_NAME}" \
    --project="${PROJECT_ID}" --zone="${ZONE}" --quiet 2>/dev/null || true

  local METADATA
  METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  METADATA="${METADATA},VM_TASK=defi-backfill"
  METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
  METADATA="${METADATA},VM_OPERATION=collect-gas-fees"
  METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
  METADATA="${METADATA},VM_GAS_FEE_CHAINS=${CHAIN_ID}"
  METADATA="${METADATA},VM_GAS_FEE_SAMPLE_INTERVAL=100"
  METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
  METADATA="${METADATA},VM_NAME=${VM_NAME}"
  METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
  METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  METADATA="${METADATA},VM_START_DATE=${START_DATE}"
  METADATA="${METADATA},VM_END_DATE=${END_DATE}"

  echo "  Creating VM ${VM_NAME}..."
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --scopes=cloud-platform \
    --no-restart-on-failure \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --labels="purpose=mtds-gas-fees-fleet,env=${DEPLOYMENT_ENV},chain=${CHAIN_NAME}" \
    --metadata="${METADATA}"
  echo "  VM ${VM_NAME} created."
  echo "  Logs: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
}

for chain_spec in "${CHAINS[@]}"; do
  IFS=':' read -r CHAIN_ID CHAIN_NAME START_DATE <<< "$chain_spec"

  if [[ -n "$SINGLE_CHAIN" && "$CHAIN_ID" != "$SINGLE_CHAIN" ]]; then
    continue
  fi

  launch_chain_vm "$CHAIN_ID" "$CHAIN_NAME" "$START_DATE"
done

echo ""
echo "============================================================"
echo "Fleet launched! Monitor:"
echo "  gcloud compute instances list --filter='name~mtds-gas-fees' --project=${PROJECT_ID}"
echo "  gsutil ls gs://${CODE_BUCKET}/vm-logs/ | grep mtds-gas-fees"
echo "============================================================"
