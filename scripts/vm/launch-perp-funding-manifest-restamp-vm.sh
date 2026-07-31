#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after cefi_perp_funding_kalshi_polymarket_residual_and_capture_gap_2026_07_30.md's
#   manifest re-stamp todo is verified done (8 rows corrected)
# Launch GCE VM to re-stamp 8 cefi perp_funding manifest rows (KALSHI_PERP/POLYMARKET_PERP ->
# KALSHI-PERP/POLYMARKET-PERP, plus 2 confirmed-phantom rows re-stamped honest attempted_failed).
#
# Pattern A (canonical tarball) — same mechanism as launch-cefi-migration-vm.sh.
# Migration script: market-tick-data-service/scripts/restamp_perp_funding_venue_manifest_2026_07_30.py
#
# Why this needs a VM at all for only 8 rows: DefiManifestRecorder does a full
# read-merge-write of the ENTIRE consolidated availability_index.parquet (9.5M rows for this
# bucket) on every instantiation. 8 local attempts from an interactive session all timed out
# (120s client timeout) over ~90 minutes -- see cefi_perp_funding_kalshi_polymarket_residual_
# and_capture_gap_2026_07_30.md. This is the heavy-I/O-belongs-on-a-VM-in-region class of
# problem (CLAUDE.md VM-launcher-runbook: "manifest-index rewrites go on a VM ALWAYS"), not a
# row-count-scaled cost.
#
# Usage:
#   bash launch-perp-funding-manifest-restamp-vm.sh             # dry-run first (default)
#   bash launch-perp-funding-manifest-restamp-vm.sh --apply     # write mode
#   bash launch-perp-funding-manifest-restamp-vm.sh --dry-run   # print launch plan only, skip gcloud
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
VM_NAME="mtds-migrate-perp-funding-restamp"
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
MIGRATION_CMD="python scripts/restamp_perp_funding_venue_manifest_2026_07_30.py ${APPLY_FLAG}"

echo "============================================================"
echo "Perp-Funding Manifest Re-stamp VM (Pattern A)"
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
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
  --labels="purpose=perp-funding-manifest-restamp,env=${DEPLOYMENT_ENV}",managed-by=deployment-service \
  --metadata="${METADATA}"

echo ""
echo "VM launched: ${VM_NAME}"
echo "  Monitor logs: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "  SSH:          gcloud compute ssh ${VM_NAME} --zone=${ZONE} -- tail -f /var/log/vm-setup.log"
echo "============================================================"
