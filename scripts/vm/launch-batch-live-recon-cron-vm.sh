#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a nightly T+1 batch-live reconciliation VM.
#
# This runs batch-live-reconciliation-service for yesterday's date (T-1),
# comparing batch pipeline outputs against live execution events and
# computing per-shard deviation metrics.
#
# Why this exists: GAP-18 (topology_qgroup_gap_closure_2026_05_09 Phase 8)
# requires "live continuous run + nightly batch replay + reconciler diff per
# shard, 7-day window passes per-pair tolerance thresholds" BEFORE May-23.
# The cron must launch by 2026-05-16 to accumulate 7 full days before the
# cutover gate fires on 2026-05-23.
#
# Output:
#   - Recon report → gs://recon-store-{pid}/reports/{date}/report.json
#   - Lifecycle events → gs://{pid}-events/events/batch-live-reconciliation-service/
#   - VM logs → gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#
# Usage:
#   bash launch-batch-live-recon-cron-vm.sh                   # yesterday (T-1)
#   bash launch-batch-live-recon-cron-vm.sh 2026-05-14        # specific date
#   bash launch-batch-live-recon-cron-vm.sh --dry-run         # smoke (no GCS writes)
#   bash launch-batch-live-recon-cron-vm.sh --force           # bypass singleton
#
# Singleton lock: refuses if a batch-live-recon-* VM is RUNNING. --force bypasses.
#
# Scheduling: run this script nightly at 03:00 UTC via Cloud Scheduler:
#   gcloud scheduler jobs create http batch-live-recon-nightly \
#     --schedule="0 3 * * *" \
#     --time-zone="UTC" \
#     --uri="https://CLOUD_RUN_TRIGGER_URL" \
#     ...
# Or trigger directly from a cron VM via a separate launcher wrapper.
#
# Cost: e2-standard-4 + 50GB, ~10-15 min per nightly run.
set -euo pipefail

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN_FLAG=""
TARGET_DATE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=true; shift ;;
        --dry-run) DRY_RUN_FLAG="--dry-run"; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --date) TARGET_DATE="$2"; shift 2 ;;
        [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]) TARGET_DATE="$1"; shift ;;
        *) echo "ERROR: Unknown arg: $1" >&2; exit 1 ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# Default to yesterday (T-1) for nightly cron.
if [[ -z "$TARGET_DATE" ]]; then
    TARGET_DATE="$(date -u -v-1d +%Y-%m-%d 2>/dev/null || date -u -d 'yesterday' +%Y-%m-%d)"
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="e2-standard-4"
BOOT_DISK_GB="50"

if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^batch-live-recon-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: batch-live-recon VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate nightly run. Wait for it to complete or
pass --force to bypass.

Inspect: gcloud compute ssh $EXISTING --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/recon.log'
GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
Delete:   gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
EOF
        exit 1
    fi
fi

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="batch-live-recon-${TARGET_DATE//\-/}-${RUN_TS}"

RECON_CMD="python -m batch_live_reconciliation_service"
RECON_CMD+=" --operation reconcile"
RECON_CMD+=" --mode batch"
RECON_CMD+=" --start-date ${TARGET_DATE}"
RECON_CMD+=" --end-date ${TARGET_DATE}"
if [[ -n "$DRY_RUN_FLAG" ]]; then
    RECON_CMD+=" ${DRY_RUN_FLAG}"
fi

# BLR_WORKSPACE is where the batch-live-reconciliation-service tarball extracts.
RECON_MODULE_PATH="/home/ikennaigboaka/workspace/blr"
BACKFILL_CMD="cd ${RECON_MODULE_PATH} && ${RECON_CMD}"

METADATA="VM_TASK=batch-live-recon"
METADATA="${METADATA},VM_SERVICE=batch_live_reconciliation_service"
METADATA="${METADATA},VM_OPERATION=reconcile"
METADATA="${METADATA},VM_RECON_DATE=${TARGET_DATE}"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

echo "Launching ${VM_NAME}: batch-live recon for date=${TARGET_DATE}${DRY_RUN_FLAG:+ (dry-run)}"

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=batch-live-recon,recon-date="${TARGET_DATE//\-/}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: ${VM_NAME}"
echo "Date:        ${TARGET_DATE}"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events:      gcloud storage ls gs://${PROJECT}-events/events/batch-live-reconciliation-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "Delete:      gcloud compute instances delete ${VM_NAME} --zone=${ZONE} --quiet"
