#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a targeted instruments-service sports gap-fill VM.
#
# Targets a single entity type (--entity) over a specific date range
# (--start-date / --end-date). Passes VM_SPORTS_ENTITY + VM_SPORTS_PROVIDER
# metadata so the VM only fetches the one entity type with manifest skip
# for dates already captured.
#
# VM naming: instr-backfill-sports-{entity_lower}-{ts}
# Watchdog prefix: "instr-backfill-sports" (already registered, bucket = instruments-store-sports-prd-*)
# Machine: e2-standard-4 (16 GB) — single entity, much lower RSS than full backfill.
#
# Usage (from workspace root):
#   bash deployment-service/scripts/vm/launch-sports-is-gap-fill.sh \
#     --entity FIXTURE_STATS \
#     --provider API_FOOTBALL \
#     --start-date 2019-02-15 \
#     --end-date 2026-04-14 \
#     [--dry-run] [--project PROJECT_ID] [--zone ZONE]
#
# Generate entity/date args with:
#   python3 instruments-service/scripts/query_sports_is_gaps.py
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false

ENTITY=""
PROVIDER="API_FOOTBALL"
START_DATE=""
END_DATE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --entity)        ENTITY="$(echo "$2" | tr '[:lower:]' '[:upper:]')"; shift 2 ;;
    --provider)      PROVIDER="$(echo "$2" | tr '[:lower:]' '[:upper:]')"; shift 2 ;;
    --start-date)    START_DATE="$2"; shift 2 ;;
    --end-date)      END_DATE="$2"; shift 2 ;;
    --dry-run)       DRY_RUN=true; shift ;;
    --project)       PROJECT_ID="$2"; shift 2 ;;
    --zone)          ZONE="$2"; shift 2 ;;
    --machine-type)  MACHINE_TYPE="$2"; shift 2 ;;
    --env)           DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$ENTITY" || -z "$START_DATE" || -z "$END_DATE" ]]; then
  echo "ERROR: --entity, --start-date, and --end-date are required." >&2
  exit 1
fi

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

TS="$(date -u +%Y%m%d-%H%M%S)"
ENTITY_LOWER="$(echo "$ENTITY" | tr '[:upper:]' '[:lower:]' | tr '_' '-')"
# VM name matches watchdog prefix "instr-backfill-sports"
VM_NAME="instr-backfill-sports-${ENTITY_LOWER}-${TS}"

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"

echo "============================================================"
echo "Sports IS Gap-Fill VM Launcher"
echo "  Entity:   ${ENTITY}"
echo "  Provider: ${PROVIDER}"
echo "  Dates:    ${START_DATE} → ${END_DATE}"
echo "  Project:  ${PROJECT_ID}"
echo "  Zone:     ${ZONE}"
echo "  Machine:  ${MACHINE_TYPE}"
echo "  VM:       ${VM_NAME}"
echo "  DryRun:   ${DRY_RUN}"
echo "  Tarball:  gs://${CODE_BUCKET}/code/instruments-service-code.tar.gz"
echo "============================================================"

if $DRY_RUN; then
  echo "[DRY RUN] Would create VM: ${VM_NAME}"
  echo "[DRY RUN] Metadata:"
  echo "  VM_TASK=instruments-backfill"
  echo "  VM_ASSET_GROUP=SPORTS"
  echo "  VM_SPORTS_ENTITY=${ENTITY}"
  echo "  VM_SPORTS_PROVIDER=${PROVIDER}"
  echo "  VM_START_DATE=${START_DATE}"
  echo "  VM_END_DATE=${END_DATE}"
  exit 0
fi

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=instruments-backfill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_CHUNK_DAYS=30"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SPORTS_ENTITY=${ENTITY}"
METADATA="${METADATA},VM_SPORTS_PROVIDER=${PROVIDER}"
METADATA="${METADATA},SKIP_DEPENDENCY_CHECK=true"
METADATA="${METADATA},STALL_TIMEOUT_SEC=7200"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        instruments-service unified-api-contracts unified-trading-library deployment-service \
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
  --boot-disk-size=30GB \
  --labels="purpose=sports-is-gap-fill,entity=$(echo "${ENTITY}" | tr '[:upper:]' '[:lower:]'),env=${DEPLOYMENT_ENV}" \
  --metadata="${METADATA}"

echo ""
echo "VM RUNNING: ${VM_NAME}"
echo "Monitor: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} --format='value(status)'"
echo "Logs:    gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "T+10min check (mandatory):"
echo "  gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} --format='value(status)'"
