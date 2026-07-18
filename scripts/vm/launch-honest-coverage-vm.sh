#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a GCE VM that runs the daily honest-coverage measurement (B-018 Phase 8.A).
#
# Boots an e2-highmem-4 VM (32 GB) in asia-northeast1-c, runs
#   instruments-service/scripts/measure_honest_coverage.py --asset-group all
# then auto-shuts down. Triggered daily at 00:30 UTC by Cloud Scheduler
# (honest-coverage-daily — creation pending Ikenna for cloudscheduler.jobs.create).
#
# Output: gs://central-element-323112-honest-coverage/{date}/coverage.json
# Consumed by: deployment-api GET /api/data-status/honest-coverage (Phase 2C).
#
# This is the Cloud Scheduler-targeted launcher (always runs all asset groups).
# For ad-hoc / partial-run use, see launch-measure-honest-coverage-vm.sh
# (supports --asset-group filtering).
#
# Singleton lock: refuses to launch if any honest-coverage-* VM is RUNNING.
# Watchdog: prefix "honest-coverage-" registered in vm_zombie_watchdog.py (heartbeat-only).
#
# Usage:
#   bash launch-honest-coverage-vm.sh             # prod (all asset groups)
#   bash launch-honest-coverage-vm.sh --force     # bypass singleton lock
#   bash launch-honest-coverage-vm.sh --env staging
#   bash launch-honest-coverage-vm.sh --dry-run   # print gcloud command, no launch
#
# Cost: e2-highmem-4 (32 GB) ~5-20 min → ~$0.05/day. Right-sized 2026-07-16:
# e2-standard-2 (8 GB) OOM'd on the growing per-asset-group availability-index
# parquet reads (measure_honest_coverage._read_parquet_safe swallows the OOM and
# skips that AG), which silently produced partial (e.g. defi-only) coverage.json
# files. 32 GB gives headroom for the largest single-AG parquet read.
#
# Plan: cross_asset_group_catalogue_audit_2026_05_10.md Phase 2B / B-018 Phase 8.A.
# Scheduler setup: deployment-service/scripts/vm/setup-honest-coverage-scheduler.sh

set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="e2-highmem-4"
BOOT_DISK_GB="50"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false

VM_PREFIX="honest-coverage-"

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --force)   FORCE=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --env)     DEPLOYMENT_ENV="$2"; shift 2 ;;
        --project) PROJECT="$2"; CODE_BUCKET="deployment-scripts-${PROJECT}"; shift 2 ;;
        --zone)    ZONE="$2"; shift 2 ;;
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
ERROR: honest-coverage VM already running in $ZONE: $EXISTING
Concurrent runs race on the GCS output JSON. Use --force to bypass.

  Inspect:  gcloud compute ssh $EXISTING --zone=$ZONE
  Force:    bash $0 --force

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If confirmed stale:
  gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
EOF
        exit 1
    fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="${VM_PREFIX}${RUN_TS}"

MEASURE_SCRIPT="/home/ikennaigboaka/workspace/instruments/scripts/measure_honest_coverage.py"
BACKFILL_CMD="python ${MEASURE_SCRIPT} --asset-group all"

# VM_TASK=features-backfill: setup-data-pipeline-vm.sh reads VM_BACKFILL_CMD verbatim
# (VM_TASK=measure-honest-coverage is not a registered routing in setup-data-pipeline-vm.sh).
METADATA="VM_TASK=features-backfill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

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
    "--labels=purpose=honest-coverage,env=${DEPLOYMENT_ENV},run-ts=${RUN_TS}"
)

echo "Launching $VM_NAME: honest-coverage measurement (all asset groups, env=${DEPLOYMENT_ENV})"

if $DRY_RUN; then
    echo "[DRY-RUN] Would run:"
    printf '  %s \\\n' "${GCLOUD_CMD[@]}"
    exit 0
fi

lc_verify_tarball_freshness "$CODE_BUCKET" \
    instruments-service unified-api-contracts unified-trading-library deployment-service \
    || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }

"${GCLOUD_CMD[@]}"

echo ""
echo "VM launched: $VM_NAME"
OUTPUT_DATE="$(date +%Y-%m-%d)"
echo "Output (when complete): gsutil cat gs://${PROJECT}-honest-coverage/${OUTPUT_DATE}/coverage.json"
echo "Logs:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:   gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
