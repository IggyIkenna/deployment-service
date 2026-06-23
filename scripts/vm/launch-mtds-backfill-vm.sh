#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch GCE VM for MTDS category-specific backfill (CeFi, DeFi, TradFi, etc.)
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first (CORE tarballs include MTDS).
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Usage:
#   bash launch-mtds-backfill-vm.sh --asset-group CEFI --start 2024-04-05 --end 2026-04-05
#   bash launch-mtds-backfill-vm.sh --asset-group DEFI --start 2024-04-05 --end 2026-04-05 --env staging
#   bash launch-mtds-backfill-vm.sh --asset-group TRADFI --start 2026-03-29 --end 2026-04-05
#   bash launch-mtds-backfill-vm.sh --asset-group CEFI --dry-run
#
# Pre-launch prerequisite:
#   bash deployment-service/scripts/vm/create-code-tarballs.sh   # CORE includes MTDS
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
ASSET_GROUP=""
START_DATE=""
END_DATE=""
TIER=""
FORCE=false
CHUNK_SIZE="${CHUNK_SIZE:-7}"
VM_NAME_OVERRIDE=""
VENUES=""
DATA_TYPES=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)       DRY_RUN=true; shift ;;
    --project)       PROJECT_ID="$2"; shift 2 ;;
    --zone)          ZONE="$2"; shift 2 ;;
    --asset-group)   ASSET_GROUP="$2"; shift 2 ;;
    --start)         START_DATE="$2"; shift 2 ;;
    --end)           END_DATE="$2"; shift 2 ;;
    --tier)          TIER="$2"; shift 2 ;;
    --force)         FORCE=true; shift ;;
    --chunk-size)    CHUNK_SIZE="$2"; shift 2 ;;
    --machine-type)  MACHINE_TYPE="$2"; shift 2 ;;
    --vm-name)       VM_NAME_OVERRIDE="$2"; shift 2 ;;
    --venues)        VENUES="$2"; shift 2 ;;
    --data-types)    DATA_TYPES="$2"; shift 2 ;;
    --env)           DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ -z "$ASSET_GROUP" ]]; then
  echo "ERROR: --asset-group is required (CEFI, DEFI, TRADFI, SPORTS, PREDICTION)"
  exit 1
fi

CATEGORY_LOWER=$(echo "$ASSET_GROUP" | tr '[:upper:]' '[:lower:]')
CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
VM_NAME="${VM_NAME_OVERRIDE:-mtds-backfill-${CATEGORY_LOWER}-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

echo "============================================================"
echo "MTDS Category Backfill VM Launcher (Pattern A)"
echo "  Category:  ${ASSET_GROUP}"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE:-<not set>} → ${END_DATE:-<not set>}"
echo "  Tier:      ${TIER:-N/A}"
echo "  Chunk:     ${CHUNK_SIZE} days per batch"
echo "  Force:     ${FORCE}"
echo "  Venues:    ${VENUES:-all}"
echo "  DataTypes: ${DATA_TYPES:-all}"
echo "  VM:        ${VM_NAME}"
echo "  Env:       ${DEPLOYMENT_ENV}"
echo "  Tarball:   gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

# ── Singleton lock ──
# Refuses launch if any mtds-backfill-{category}-* VM is RUNNING in the zone.
# Prevents Tardis per-IP thundering-herd (concurrent VMs share egress NAT).
VM_PREFIX="mtds-backfill-${CATEGORY_LOWER}-"
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^${VM_PREFIX}\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    echo "WARN: MTDS backfill VM already running for ${ASSET_GROUP}: ${EXISTING}" >&2
    echo "      Use --force to bypass. Aborting." >&2
    exit 1
  fi
fi

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=mtds-backfill  VM_ASSET_GROUP=${ASSET_GROUP}"
  exit 0
fi

# ── Build metadata ──
METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=mtds-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_ASSET_GROUP=${ASSET_GROUP}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_CHUNK_DAYS=${CHUNK_SIZE}"
[[ -n "$START_DATE" ]] && METADATA="${METADATA},VM_START_DATE=${START_DATE}"
[[ -n "$END_DATE" ]]   && METADATA="${METADATA},VM_END_DATE=${END_DATE}"
[[ -n "$TIER" ]]       && METADATA="${METADATA},VM_TIER=${TIER}"
[[ -n "$VENUES" ]]     && METADATA="${METADATA},VM_VENUE=${VENUES}"
[[ -n "$DATA_TYPES" ]] && METADATA="${METADATA},VM_DATA_TYPES=${DATA_TYPES}"
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
  --labels="purpose=mtds-backfill,asset-group=${CATEGORY_LOWER},env=${DEPLOYMENT_ENV}" \
  --metadata="${METADATA}"

echo ""
echo "  VM created: ${VM_NAME}"
echo "  T+10 check: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "  Logs:       gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
