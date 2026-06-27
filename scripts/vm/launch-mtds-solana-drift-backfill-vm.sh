#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch GCE VM for Drift S3 historical data backfill (Solana DeFi)
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first (CORE tarballs include MTDS).
# Handler: VM_TASK=solana-drift-backfill added to setup-data-pipeline-vm.sh same commit.
#
# Downloads Drift's public S3 historical data (orderbook, trades, funding) and
# writes to GCS as parquet. Default market: SOL-PERP, 180 days back.
#
# Usage:
#   bash launch-mtds-solana-drift-backfill-vm.sh
#   bash launch-mtds-solana-drift-backfill-vm.sh --market BTC-PERP
#   bash launch-mtds-solana-drift-backfill-vm.sh --start 2024-01-01 --end 2026-01-01
#   bash launch-mtds-solana-drift-backfill-vm.sh --dry-run
#   bash launch-mtds-solana-drift-backfill-vm.sh --env staging
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
START_DATE="${START_DATE:-$(date -v-180d +%Y-%m-%d 2>/dev/null || date -d '180 days ago' +%Y-%m-%d)}"
END_DATE="${END_DATE:-$(date +%Y-%m-%d)}"
DRIFT_MARKET="${DRIFT_MARKET:-SOL-PERP}"
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)  DRY_RUN=true; shift ;;
    --project)  PROJECT_ID="$2"; shift 2 ;;
    --zone)     ZONE="$2"; shift 2 ;;
    --start)    START_DATE="$2"; shift 2 ;;
    --end)      END_DATE="$2"; shift 2 ;;
    --market)   DRIFT_MARKET="$2"; shift 2 ;;
    --force)    FORCE=true; shift ;;
    --env)      DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
VM_NAME="mtds-solana-drift-backfill"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

echo "============================================================"
echo "Solana Drift Backfill VM Launcher (Pattern A)"
echo "  VM:        ${VM_NAME}"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Market:    ${DRIFT_MARKET}"
echo "  Env:       ${DEPLOYMENT_ENV}"
echo "  Tarball:   gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^mtds-solana-drift-\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    echo "WARN: Solana Drift VM already running: ${EXISTING}" >&2
    echo "      Use --force to bypass. Aborting." >&2
    exit 1
  fi
fi

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=solana-drift-backfill  VM_DRIFT_MARKET=${DRIFT_MARKET}"
  exit 0
fi

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=solana-drift-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_DRIFT_MARKET=${DRIFT_MARKET}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Creating VM ${VM_NAME} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]..."
# shellcheck disable=SC2086
gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  ${PROVISIONING_FLAGS} \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --labels="purpose=mtds-solana-drift-backfill,env=${DEPLOYMENT_ENV}" \
  --metadata="${METADATA}"

echo ""
echo "  VM created: ${VM_NAME}"
echo "  T+10 check: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "  Logs:       gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
