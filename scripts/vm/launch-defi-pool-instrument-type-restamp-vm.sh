#!/usr/bin/env bash
# Epic: manifest_master
# Lifecycle: oneoff
# Delete-when: after defi_cefi_venue_chain_axis_contamination_2026_07_28.md's POOL P3
#   todo is verified done (0 instrument_type=="POOL" rows remain for
#   service_name=="market-tick-data-service" in the DeFi availability_index).
# Launch GCE VM to fold historical DeFi manifest instrument_type=="POOL" (uppercase) rows
# to canonical lowercase "pool", scoped to service_name=="market-tick-data-service" (raw-tick
# MTDS rows only -- explicitly excludes market-data-processing-service candle rows, which
# deliberately keep the uppercase form per the 2026-07-21 operator-ruled candle canonical
# shape).
#
# Pattern A (canonical tarball) — same mechanism as launch-perp-funding-manifest-restamp-vm.sh.
# Migration script: market-tick-data-service/scripts/migrate_defi_pool_instrument_type_casing_2026_08_04.py
#
# Why this needs a VM: the DeFi availability_index.parquet is a ~29M-row/~1.7GB manifest-index
# object -- a read-transform-write over the whole index is the "heavy I/O never runs from the
# operator's local machine" HARD RULE class (/codex/05-infrastructure/vm-launcher-runbook.md
# § heavy-I/O rule), independent of how many rows the migration actually mutates. A local
# interactive attempt to even INVESTIGATE (filtered read) this same index hit a 120s
# network-client timeout mid-download (IncompleteRead at 268MB of ~1.73GB) -- confirming the
# object is genuinely too large for a reliable local session read, let alone a write.
#
# Usage:
#   bash launch-defi-pool-instrument-type-restamp-vm.sh             # dry-run first (default)
#   bash launch-defi-pool-instrument-type-restamp-vm.sh --apply     # write mode
#   bash launch-defi-pool-instrument-type-restamp-vm.sh --dry-run   # print launch plan only, skip gcloud
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
VM_NAME="mtds-migrate-defi-pool-casing"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false
APPLY_FLAG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --apply)   APPLY_FLAG="--apply"; shift ;;
    --env)     DEPLOYMENT_ENV="$2"; shift 2 ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone)    ZONE="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
MIGRATION_CMD="python scripts/migrate_defi_pool_instrument_type_casing_2026_08_04.py ${APPLY_FLAG}"

echo "============================================================"
echo "DeFi POOL instrument_type Casing Re-stamp VM (Pattern A)"
echo "  Project:  ${PROJECT_ID}"
echo "  Zone:     ${ZONE}"
echo "  Machine:  ${MACHINE_TYPE}"
echo "  VM:       ${VM_NAME}"
echo "  Env:      ${DEPLOYMENT_ENV}"
echo "  Mode:     $([ -n "$APPLY_FLAG" ] && echo APPLY || echo DRY-RUN)"
echo "  Cmd:      ${MIGRATION_CMD}"
echo "============================================================"

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=canonical-migration"
  echo "  VM_MIGRATION_CMD=${MIGRATION_CMD}"
  exit 0
fi

echo "Launching VM..."
gcloud compute instances delete "${VM_NAME}" \
  --project="${PROJECT_ID}" --zone="${ZONE}" --quiet 2>/dev/null || true

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=canonical-migration"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_MIGRATION_CMD=${MIGRATION_CMD}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

lc_verify_tarball_freshness "$CODE_BUCKET" \
    market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
    || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }

gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "${PROJECT_ID}")" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
  --labels="purpose=defi-pool-casing-restamp,env=${DEPLOYMENT_ENV}",managed-by=deployment-service \
  --metadata="${METADATA}"

echo ""
echo "VM launched: ${VM_NAME}"
echo "  Monitor logs: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "  SSH:          gcloud compute ssh ${VM_NAME} --zone=${ZONE} -- tail -f /var/log/vm-setup.log"
echo "============================================================"
