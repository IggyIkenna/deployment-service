#!/usr/bin/env bash
# Launch GCE VM for MTDS sports odds backfill (Odds API historical)
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first (CORE tarballs include MTDS).
#
# Uses VM_TASK=mtds-backfill handler (chunked loop, default 7-day chunks) with
# VM_ASSET_GROUP=SPORTS. Sports MTDS does NOT accept --tier; VM_TIER is intentionally
# omitted from metadata to avoid the MTDS CLI rejecting unrecognised arguments.
#
# Usage:
#   bash launch-mtds-sports-odds-backfill-vm.sh                              # Full 5.8yr run
#   bash launch-mtds-sports-odds-backfill-vm.sh --dry-run                    # Print plan
#   bash launch-mtds-sports-odds-backfill-vm.sh --start 2025-01-04 --end 2025-01-04
#   bash launch-mtds-sports-odds-backfill-vm.sh --vm-name mtds-backfill-odds-2020 --start 2020-06-01 --end 2020-12-31 --force
#   bash launch-mtds-sports-odds-backfill-vm.sh --env staging
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
START_DATE="${START_DATE:-2020-06-01}"
END_DATE="${END_DATE:-2026-03-28}"
FORCE=false
CHUNK_SIZE="${CHUNK_SIZE:-7}"
VM_NAME_OVERRIDE=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)      DRY_RUN=true; shift ;;
    --project)      PROJECT_ID="$2"; shift 2 ;;
    --zone)         ZONE="$2"; shift 2 ;;
    --start)        START_DATE="$2"; shift 2 ;;
    --end)          END_DATE="$2"; shift 2 ;;
    --force)        FORCE=true; shift ;;
    --chunk-size)   CHUNK_SIZE="$2"; shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --vm-name)      VM_NAME_OVERRIDE="$2"; shift 2 ;;
    --env)          DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
VM_NAME="${VM_NAME_OVERRIDE:-mtds-backfill-odds-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

echo "============================================================"
echo "MTDS Sports Odds Backfill VM Launcher (Pattern A)"
echo "  VM:        ${VM_NAME}"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Chunk:     ${CHUNK_SIZE} days per batch"
echo "  Force:     ${FORCE}"
echo "  Env:       ${DEPLOYMENT_ENV}"
echo "  Tarball:   gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

# ── Singleton lock ──
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^mtds-backfill-odds-\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    echo "WARN: Sports odds VM already running: ${EXISTING}" >&2
    echo "      Use --force to bypass. Aborting." >&2
    exit 1
  fi
fi

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=mtds-backfill  VM_ASSET_GROUP=SPORTS  (no VM_TIER — sports MTDS has no --tier)"
  exit 0
fi

# ── Build metadata ──
METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=mtds-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
# NOTE: VM_TIER intentionally omitted — sports MTDS CLI has no --tier argument
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_CHUNK_DAYS=${CHUNK_SIZE}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
$FORCE && METADATA="${METADATA},VM_FORCE=true"

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
  --labels="purpose=mtds-sports-odds-backfill,env=${DEPLOYMENT_ENV}" \
  --metadata="${METADATA}"

echo ""
echo "  VM created: ${VM_NAME}"
echo "  T+10 check: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "  Logs:       gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
