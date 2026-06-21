#!/usr/bin/env bash
# Launch GCE VM for DEX swaps backfill (Uniswap V3 / Balancer / Curve / GMX / etc. via The Graph)
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Writes to market-data-tick-defi-prd-{project_id} (canonical env-tiered DEFI tick bucket) using
# resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi").
#
# Usage:
#   bash launch-mtds-dex-swaps-backfill-vm.sh
#   bash launch-mtds-dex-swaps-backfill-vm.sh --start 2026-01-25 --end 2026-05-23
#   bash launch-mtds-dex-swaps-backfill-vm.sh --dry-run
#   bash launch-mtds-dex-swaps-backfill-vm.sh --env staging
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"
DRY_RUN=false
START_DATE="${START_DATE:-2023-01-01}"
END_DATE="${END_DATE:-$(date +%Y-%m-%d)}"
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
MTDS_TARBALL_SHA="${MTDS_TARBALL_SHA:-}"
UTL_TARBALL_SHA="${UTL_TARBALL_SHA:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)        DRY_RUN=true; shift ;;
    --project)        PROJECT_ID="$2"; shift 2 ;;
    --zone)           ZONE="$2"; shift 2 ;;
    --start)          START_DATE="$2"; shift 2 ;;
    --end)            END_DATE="$2"; shift 2 ;;
    --force)          FORCE=true; shift ;;
    --env)            DEPLOYMENT_ENV="$2"; shift 2 ;;
    --mtds-sha)       MTDS_TARBALL_SHA="$2"; shift 2 ;;
    --utl-sha)        UTL_TARBALL_SHA="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
VM_NAME="${VM_NAME:-mtds-dex-swaps-backfill}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

echo "============================================================"
echo "DEX Swaps Backfill VM Launcher (Pattern A)"
echo "  VM:        ${VM_NAME}"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Env:       ${DEPLOYMENT_ENV}"
echo "  Tarball:   gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "  Bucket:    market-data-tick-defi-prd-${PROJECT_ID}  (resolved by resolve_bucket_name)"
echo "============================================================"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^mtds-dex-swaps-\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    echo "WARN: DEX swaps VM already running: ${EXISTING}" >&2
    echo "      Use --force to bypass. Aborting." >&2
    exit 1
  fi
fi

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=defi-backfill  VM_OPERATION=collect-dex-swaps"
  exit 0
fi

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=defi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=collect-dex-swaps"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
if [[ -n "${MTDS_TARBALL_SHA}" ]]; then
  METADATA="${METADATA},MTDS_TARBALL_SHA=${MTDS_TARBALL_SHA}"
fi
if [[ -n "${UTL_TARBALL_SHA}" ]]; then
  METADATA="${METADATA},UTL_TARBALL_SHA=${UTL_TARBALL_SHA}"
fi

echo "Creating VM ${VM_NAME}..."
gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --labels="purpose=mtds-dex-swaps-backfill,env=${DEPLOYMENT_ENV}" \
  --metadata="${METADATA}"

echo ""
echo "  VM created: ${VM_NAME}"
echo "  T+10 check: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "  Logs:       gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
