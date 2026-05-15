#!/usr/bin/env bash
# Launch a GCE VM that runs the daily QG snapshot (B-018 Phase 4.A).
#
# Boots an e2-small VM in asia-northeast1-c, clones the workspace repos that
# are registered in the tarballs bucket, runs snapshot.sh + snapshot_to_parquet.py,
# then auto-shuts down. Triggered daily at 06:00 UTC by Cloud Scheduler.
#
# What the VM does:
#   1. Install unified-trading-library:latest tarball (provides UTL + pyarrow).
#   2. Clone unified-trading-pm (for snapshot.sh + workspace-manifest.json).
#   3. For every repo in workspace-manifest.json, clone from GitHub if tarball
#      exists; mark as "repo_not_found_on_disk" (skipped) otherwise.
#   4. Run:
#        bash unified-trading-pm/scripts/quality_gates/snapshot.sh | \
#          python3 unified-trading-pm/scripts/quality_gates/snapshot_to_parquet.py \
#            --project-id {PROJECT_ID}
#   5. Auto-shutdown.
#
# Singleton-locked: refuses to launch if any qg-snapshot-* VM is RUNNING.
# Watchdog: prefix "qg-snapshot-" registered in vm_zombie_watchdog.py (heartbeat-only).
#
# Cloud Scheduler setup (run once after first successful smoke test):
#   gcloud scheduler jobs create http qg-snapshot-daily \
#     --schedule="0 6 * * *" \
#     --uri="https://compute.googleapis.com/compute/v1/projects/central-element-323112/zones/asia-northeast1-c/instances" \
#     --message-body="$(bash $(dirname $0)/launch-qg-snapshot-vm.sh --dry-run-scheduler-body)" \
#     --oauth-service-account-email=... \
#     --location=asia-northeast1
#
# Cost: e2-small ~$0.013/h. Snapshot runs ≤ 30 min → < $0.01/day.
#
# Usage:
#   bash launch-qg-snapshot-vm.sh                   # launch snapshot VM (prod)
#   bash launch-qg-snapshot-vm.sh --force            # bypass singleton lock
#   bash launch-qg-snapshot-vm.sh --env staging      # staging tier
#   bash launch-qg-snapshot-vm.sh --dry-run          # print gcloud command, no launch
#
# Plan: deployment_and_qg_strategy_implementation_2026_05_13.md Phase 4.A.

set -euo pipefail

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="e2-small"
BOOT_DISK_GB="30"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false

VM_PREFIX="qg-snapshot-"

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --force)     FORCE=true; shift ;;
        --dry-run)   DRY_RUN=true; shift ;;
        --env)       DEPLOYMENT_ENV="$2"; shift 2 ;;
        --project)   PROJECT="$2"; CODE_BUCKET="deployment-scripts-${PROJECT}"; shift 2 ;;
        --zone)      ZONE="$2"; shift 2 ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            echo "Usage: $0 [--force] [--dry-run] [--env prod|staging|dev] [--project PID]" >&2
            exit 1
            ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be prod|staging|dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# ── Singleton lock ────────────────────────────────────────────────────────────
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${VM_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: QG snapshot VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate (singleton lock).

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force
EOF
        exit 1
    fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="${VM_PREFIX}${RUN_TS}"

# Startup command passed to the VM via metadata
SNAPSHOT_CMD="bash /home/unified/workspace/unified-trading-pm/scripts/quality_gates/snapshot.sh"
PARQUET_CMD="python3 /home/unified/workspace/unified-trading-pm/scripts/quality_gates/snapshot_to_parquet.py --project-id ${PROJECT}"
# Full pipeline: snapshot → parquet upload → auto-shutdown
BACKFILL_CMD="${SNAPSHOT_CMD} | ${PARQUET_CMD}"

METADATA="VM_TASK=qg-snapshot"
METADATA="${METADATA},VM_SERVICE=qg_snapshot"
METADATA="${METADATA},VM_OPERATION=qg-snapshot"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},GCP_PROJECT_ID=${PROJECT}"
METADATA="${METADATA},WORKSPACE_ROOT=/home/unified/workspace"

GCLOUD_CMD=(
    gcloud compute instances create "$VM_NAME"
    --project="$PROJECT"
    --zone="$ZONE"
    --machine-type="$MACHINE_TYPE"
    --image-family=ubuntu-2404-lts-amd64
    --image-project=ubuntu-os-cloud
    --boot-disk-size="${BOOT_DISK_GB}GB"
    --scopes=cloud-platform
    "--metadata=startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}"
    "--labels=purpose=qg-snapshot,env=${DEPLOYMENT_ENV},run-ts=${RUN_TS}"
)

echo "Launching $VM_NAME: QG snapshot → GCS (env=${DEPLOYMENT_ENV})"

if $DRY_RUN; then
    echo "[DRY-RUN] Would run:"
    printf '  %s \\\n' "${GCLOUD_CMD[@]}"
    exit 0
fi

"${GCLOUD_CMD[@]}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:   gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Verify parquet upload (after ~30 min):"
echo "  gsutil ls gs://${PROJECT}-deployment-events/quality_gates_snapshot/"
echo ""
echo "Verify /api/repos/deploy-ready reads new snapshot:"
echo "  curl http://localhost:8004/api/repos/deploy-ready | python3 -m json.tool"
