#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a GCE VM to run the nightly chaos-drill cron (Phase 6.A).
#
# Plan: disaster_recovery_circuit_breakers_2026_05_10.md Phase 6.A.
# Runner: e2e-testing/scripts/defi/run_chaos_drill.py
#
# The VM:
#   - n2-standard-2 (2 vCPU / 8GB), 20GB SSD boot disk
#   - Pulls UAC + UTL + e2e-testing tarballs from gs://deployment-scripts-{pid}/code/
#   - Runs run_chaos_drill.py (OPERATIONAL+VENUE_OUTAGE+PRICE_SHOCK scenario selection,
#     in-process kill-switch arm/disarm, parquet report to GCS events bucket).
#   - Self-deletes on completion via shutdown-script.
#
# Singleton-locked: refuses launch if a same-prefix VM is already RUNNING
# in the zone (--force bypass).
#
# Typical cadence: nightly (22:00 JST). Drill report written to
#   gs://{pid}-events/dr_drill_reports/date={date}/report.parquet
# CHAOS_DRILL_FAILED event emitted if >24h stale or overall_pass=false.
#
# After code changes to e2e-testing/scripts/defi/ or UTL, refresh tarballs:
#   bash deployment-service/scripts/vm/create-code-tarballs.sh \
#     --include unified-trading-library --include unified-api-contracts \
#     --include e2e-testing
#
# VM Naming Convention: "disaster-drill-cron-{ts}" — prefix registered in
# VM_PREFIX_TO_BUCKET (vm_zombie_watchdog.py).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$REPO_ROOT/.." && pwd)}"

# Defaults
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-2}"
DISK_SIZE="${DISK_SIZE:-20GB}"
PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
DRY_RUN=false
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
VM_NAME=""

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --zone <zone>           GCE zone (default: ${ZONE})
  --machine-type <t>      GCE machine type (default: ${MACHINE_TYPE})
  --disk-size <s>         Boot disk size (default: ${DISK_SIZE})
  --project <pid>         GCP project (default: ${PROJECT_ID})
  --env <env>             Deployment env tier (prod|staging|dev; default: ${DEPLOYMENT_ENV})
  --vm-name <name>        Override generated VM name.
  --force                 Bypass singleton lock.
  --dry-run               Print plan, do not call gcloud.
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --zone)         ZONE="$2"; shift 2 ;;
        --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
        --disk-size)    DISK_SIZE="$2"; shift 2 ;;
        --project)      PROJECT_ID="$2"; CODE_BUCKET="deployment-scripts-${PROJECT_ID}"; shift 2 ;;
        --env)          DEPLOYMENT_ENV="$2"; shift 2 ;;
        --vm-name)      VM_NAME="$2"; shift 2 ;;
        --force)        FORCE=true; shift ;;
        --dry-run)      DRY_RUN=true; shift ;;
        --help|-h)      usage ;;
        *)              echo "Unknown option: $1"; usage ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

RUN_TS="$(date +%Y%m%d-%H%M%S)"
if [[ -z "$VM_NAME" ]]; then
    VM_NAME="disaster-drill-cron-${RUN_TS}"
fi

log() { echo "$(date '+%H:%M:%S') $*"; }

log "=========================================="
log "Disaster-Drill Cron VM Launcher (Phase 6.A)"
log "=========================================="
log "VM name: ${VM_NAME}"
log "Zone:    ${ZONE}"
log "Machine: ${MACHINE_TYPE}"
log "Disk:    ${DISK_SIZE}"
log "Project: ${PROJECT_ID}"
log "Env:     ${DEPLOYMENT_ENV}"
log "=========================================="

# ── Singleton lock ──
if ! $DRY_RUN; then
    EXISTING=$(gcloud compute instances list \
        --project="${PROJECT_ID}" \
        --filter="name~^disaster-drill-cron- AND zone:(${ZONE}) AND status=RUNNING" \
        --format="value(name)" 2>/dev/null || echo "")
    if [[ -n "$EXISTING" && "$FORCE" == "false" ]]; then
        log "ERROR: disaster-drill-cron VM already RUNNING: ${EXISTING}"
        log "Pass --force to bypass the singleton lock."
        exit 3
    fi
    if [[ -n "$EXISTING" && "$FORCE" == "true" ]]; then
        log "WARNING: --force given; existing VM RUNNING but launching anyway: ${EXISTING}"
    fi
fi

RUNNER_CMD="cd /app/e2e-testing && python scripts/defi/run_chaos_drill.py"

SHUTDOWN_SCRIPT=$(cat <<'SHUTDOWN_EOF'
#!/usr/bin/env bash
ZONE=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/zone" 2>/dev/null | awk -F/ '{print $NF}')
VM_NAME=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/name" 2>/dev/null)
if [[ -n "$VM_NAME" && -n "$ZONE" ]]; then
    gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet 2>/dev/null || true
fi
SHUTDOWN_EOF
)

METADATA_ITEMS=(
    "VM_TASK=chaos-drill"
    "VM_SERVICE=chaos-drill"
    "VM_OPERATION=drill"
    "VM_ASSET_GROUP=DEFI"
    "VM_SHUTDOWN_ON_COMPLETION=true"
    "VM_BACKFILL_CMD=${RUNNER_CMD}"
    "VM_NAME=${VM_NAME}"
    "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    "startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
)

METADATA_STR=$(IFS=','; echo "${METADATA_ITEMS[*]}")

if $DRY_RUN; then
    log "[DRY RUN] Would create VM:"
    log "  Name:    ${VM_NAME}"
    log "  Zone:    ${ZONE}"
    log "  Machine: ${MACHINE_TYPE}"
    log "  Disk:    ${DISK_SIZE}"
    log "  Cmd:     ${RUNNER_CMD}"
    log "  Metadata items:"
    for item in "${METADATA_ITEMS[@]}"; do
        log "    ${item}"
    done
    exit 0
fi

SHUTDOWN_FILE=$(mktemp)
echo "$SHUTDOWN_SCRIPT" > "$SHUTDOWN_FILE"
trap 'rm -f "$SHUTDOWN_FILE"' EXIT

log "Creating VM..."
gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --boot-disk-size="${DISK_SIZE}" \
    --boot-disk-type=pd-ssd \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --scopes=cloud-platform \
    --metadata="${METADATA_STR}" \
    --metadata-from-file="shutdown-script=${SHUTDOWN_FILE}" \
    --labels=purpose=chaos-drill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

log ""
log "VM created: ${VM_NAME}"
log ""
log "Verify STARTED event (within 60s):"
log "  gcloud storage cat gs://${PROJECT_ID}-events/events/chaos-drill/\$(date +%Y-%m-%d)/${VM_NAME}/hour=*/\*.jsonl 2>/dev/null | head -5"
log ""
log "Verify drill report (after completion):"
log "  gcloud storage ls gs://${PROJECT_ID}-events/dr_drill_reports/date=\$(date +%Y-%m-%d)/"
log ""
log "VM self-deletes on completion (shutdown-script)."
log "=========================================="
