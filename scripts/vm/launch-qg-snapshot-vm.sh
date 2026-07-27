#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/launcher_common.sh
source "${SCRIPT_DIR}/lib/launcher_common.sh"

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
MACHINE_TYPE="e2-small"
BOOT_DISK_GB="30"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false
DRY_RUN_SCHEDULER_BODY=false

VM_PREFIX="qg-snapshot-"

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --force)                    FORCE=true; shift ;;
        --dry-run)                  DRY_RUN=true; shift ;;
        --dry-run-scheduler-body)   DRY_RUN_SCHEDULER_BODY=true; shift ;;
        --env)       DEPLOYMENT_ENV="$2"; shift 2 ;;
        --project)   PROJECT="$2"; shift 2 ;;
        --zone)      ZONE="$2"; shift 2 ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            echo "Usage: $0 [--force] [--dry-run] [--dry-run-scheduler-body] [--env prod|staging|dev] [--project PID]" >&2
            exit 1
            ;;
    esac
done

CODE_BUCKET="$(lc_code_bucket "$PROJECT")"
lc_validate_env "$DEPLOYMENT_ENV"

# Canonical startup-script-url used by both the Cloud Scheduler JSON body and
# the direct-launch path.
STARTUP_URL="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"

# Pre-flight: verify the startup script exists in GCS before printing the
# scheduler body or launching. When absent, GCE silently skips the startup
# script entirely — the VM boots normally but the setup script never runs
# (no serial output, no run.log ever written). Root cause of cefi-015: the
# Cloud Scheduler job was created before create-code-tarballs.sh had uploaded
# the script to GCS.
# Bypass with SKIP_GCS_PREFLIGHT=true (CI / test environments).
if [[ "${SKIP_GCS_PREFLIGHT:-false}" != "true" ]]; then
    # `gcloud storage`, not `gsutil` — gsutil resolves creds from the CLI's active
    # account (a short-lived WIF token in an interactive AO slot can't refresh
    # unattended), while `gcloud storage` resolves via ADC, which stays valid. See
    # plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.
    if ! gcloud storage objects describe "$STARTUP_URL" >/dev/null 2>&1; then
        echo "ERROR: startup script not found at $STARTUP_URL" >&2
        echo "Run 'bash scripts/vm/create-code-tarballs.sh' first, then re-run." >&2
        exit 1
    fi
fi

# setup-data-pipeline-vm.sh hardcodes WORKSPACE="/home/ikennaigboaka/workspace".
# VM_BACKFILL_CMD must use this path (not /home/unified/workspace).
_VM_WORKSPACE="/home/ikennaigboaka/workspace"
SNAPSHOT_CMD="bash ${_VM_WORKSPACE}/unified-trading-pm/scripts/quality_gates/snapshot.sh"
PARQUET_CMD="python3 ${_VM_WORKSPACE}/unified-trading-pm/scripts/quality_gates/snapshot_to_parquet.py --project-id ${PROJECT}"
# Full pipeline: snapshot → parquet upload → auto-shutdown
BACKFILL_CMD="${SNAPSHOT_CMD} | ${PARQUET_CMD}"

# --dry-run-scheduler-body must stay side-effect/state-independent: it only
# renders the Cloud Scheduler REST body (JSON built below via its own
# self-contained python heredoc) and does not need to know the live fleet's
# singleton-lock state. The lock preflight (lc_singleton_check) runs ONLY on
# the real direct-launch path further down, right before the actual gcloud
# create call. See
# plans/active/issues/deployment_service_qg_red_qg_snapshot_launcher_live_vm_flake_2026_07_27.md
if $DRY_RUN_SCHEDULER_BODY; then
    # Output the Compute Engine instances.insert REST body for Cloud Scheduler HTTP target.
    # Usage:
    #   gcloud scheduler jobs create http qg-snapshot-daily \
    #     --schedule="0 6 * * *" \
    #     --uri="https://compute.googleapis.com/compute/v1/projects/${PROJECT}/zones/${ZONE}/instances" \
    #     --message-body="$(bash $(dirname $0)/launch-qg-snapshot-vm.sh --dry-run-scheduler-body)" \
    #     --oauth-service-account-email="uts-prod-batch-sa@${PROJECT}.iam.gserviceaccount.com" \
    #     --location=asia-northeast1
    python3 - <<PYEOF
import json
body = {
    "name": "${VM_PREFIX}daily",
    "zone": "projects/${PROJECT}/zones/${ZONE}",
    "machineType": "zones/${ZONE}/machineTypes/${MACHINE_TYPE}",
    "disks": [{
        "boot": True,
        "initializeParams": {
            "sourceImage": "projects/debian-cloud/global/images/family/debian-12",
            "diskSizeGb": "${BOOT_DISK_GB}"
        },
        "autoDelete": True
    }],
    "networkInterfaces": [{"accessConfigs": [{"type": "ONE_TO_ONE_NAT"}]}],
    "serviceAccounts": [{
        "email": "uts-prod-batch-sa@${PROJECT}.iam.gserviceaccount.com",
        "scopes": ["https://www.googleapis.com/auth/cloud-platform"]
    }],
    "metadata": {
        "items": [
            {"key": "startup-script-url", "value": "${STARTUP_URL}"},
            {"key": "VM_TASK", "value": "qg-snapshot"},
            {"key": "VM_SERVICE", "value": "qg_snapshot"},
            {"key": "VM_OPERATION", "value": "qg-snapshot"},
            {"key": "VM_BACKFILL_CMD", "value": "${BACKFILL_CMD}"},
            {"key": "DEPLOYMENT_ENV", "value": "${DEPLOYMENT_ENV}"},
            {"key": "VM_SHUTDOWN_ON_COMPLETION", "value": "true"},
            {"key": "GCP_PROJECT_ID", "value": "${PROJECT}"},
            {"key": "WORKSPACE_ROOT", "value": "/home/unified/workspace"}
        ]
    },
    "labels": {"purpose": "qg-snapshot", "env": "${DEPLOYMENT_ENV}"}
}
print(json.dumps(body))
PYEOF
    exit 0
fi

# Direct-launch path: define VM_NAME, METADATA, LABELS for lc_gcloud_create.
# Previously these three variables were undefined, causing an immediate exit
# under set -euo pipefail (cefi-015 secondary root cause).
RUN_TS="$(lc_run_ts)"
VM_NAME="${VM_PREFIX}${RUN_TS}"

lc_singleton_check "$VM_PREFIX" "$ZONE" "$PROJECT" "$FORCE"

METADATA="startup-script-url=${STARTUP_URL}"
METADATA="${METADATA},VM_TASK=qg-snapshot"
METADATA="${METADATA},VM_SERVICE=qg_snapshot"
METADATA="${METADATA},VM_OPERATION=qg-snapshot"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},GCP_PROJECT_ID=${PROJECT}"

LABELS="purpose=qg-snapshot,env=${DEPLOYMENT_ENV}"

echo "Launching $VM_NAME: QG snapshot → GCS (env=${DEPLOYMENT_ENV})"

if $DRY_RUN; then
    echo "[DRY-RUN] Would run: lc_gcloud_create $VM_NAME $PROJECT $ZONE $MACHINE_TYPE $BOOT_DISK_GB ..."
    echo "  metadata: $METADATA"
    echo "  labels:   $LABELS"
    exit 0
fi

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

lc_gcloud_create "$VM_NAME" "$PROJECT" "$ZONE" "$MACHINE_TYPE" "$BOOT_DISK_GB" \
    "$METADATA" "$LABELS"

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
