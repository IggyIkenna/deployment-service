#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch GCE VM to migrate CeFi tick data instrument_type partitions.
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first (mtds-code.tar.gz).
#
# Routes via VM_TASK=canonical-migration handler. Migration script:
#   $WORKSPACE/mtds/scripts/migrate_cefi_instrument_types.py
#
# Zone corrected from us-central1-a to asia-northeast1-c (workspace rule:
# never use a different region — all GCS data is in asia-northeast1).
#
# Usage:
#   bash launch-cefi-migration-vm.sh           # Launch VM
#   bash launch-cefi-migration-vm.sh --dry-run # Print plan only
#   bash launch-cefi-migration-vm.sh --env staging
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-8}"
VM_NAME="mtds-migrate-cefi-itype"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
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

echo "============================================================"
echo "CeFi Instrument Type Migration VM (Pattern A)"
echo "  Project:  ${PROJECT_ID}"
echo "  Zone:     ${ZONE}"
echo "  Machine:  ${MACHINE_TYPE}"
echo "  VM:       ${VM_NAME}"
echo "  Env:      ${DEPLOYMENT_ENV}"
echo "  Tarball:  gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=canonical-migration"
  echo "  VM_MIGRATION_CMD=python scripts/migrate_cefi_instrument_types.py --workers 16 --mixed-workers 1 --skip-cleanup"
  exit 0
fi

echo "Launching VM..."
gcloud compute instances delete "${VM_NAME}" \
  --project="${PROJECT_ID}" --zone="${ZONE}" --quiet 2>/dev/null || true

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=canonical-migration"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_MIGRATION_CMD=python scripts/migrate_cefi_instrument_types.py --workers 16 --mixed-workers 1 --skip-cleanup"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

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
  --labels="purpose=cefi-migration,env=${DEPLOYMENT_ENV}",managed-by=deployment-service \
  --metadata="${METADATA}"

echo ""
echo "VM launched: ${VM_NAME}"
echo "  Monitor logs: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "  SSH:          gcloud compute ssh ${VM_NAME} --zone=${ZONE} -- tail -f /var/log/vm-setup.log"
echo "============================================================"
