#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# One-command strategy test VM launcher.
#
# Creates code tarballs for the specified category, launches a GCE VM with
# appropriate metadata, and runs the full L1-L7 pipeline via
# setup-data-pipeline-vm.sh (startup script).
#
# Usage:
#   bash scripts/vm/launch-strategy-test-vm.sh \
#     --asset-group CEFI \
#     --strategy CEFI_MOMENTUM_BTC_5M \
#     --start-date 2026-04-01 \
#     --end-date 2026-04-07 \
#     --mode batch
#
#   bash scripts/vm/launch-strategy-test-vm.sh \
#     --asset-group DEFI \
#     --strategy DEFI_ARB_ETH_1M \
#     --start-date 2026-04-01 \
#     --end-date 2026-04-07 \
#     --dry-run
#
# The VM:
#   - Uses e2-standard-4, 50GB boot disk, Ubuntu 24.04 LTS
#   - Runs setup-data-pipeline-vm.sh as startup script
#   - Executes full pipeline (L1-L7) for the category via backfill-cluster.sh
#   - Logs to GCS: gs://{bucket}/logs/strategy-test-{strategy}-{timestamp}.log
#   - Self-deletes on completion (via shutdown script)
#
# Prerequisites:
#   - gcloud CLI authenticated with compute + storage permissions
#   - Code repos checked out locally (for tarball creation)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$REPO_ROOT/.." && pwd)}"

# Defaults
ASSET_GROUP=""
STRATEGY=""
START_DATE=""
END_DATE=""
MODE="batch"
DRY_RUN=false
ZONE="asia-northeast1-b"
MACHINE_TYPE="e2-standard-4"
DISK_SIZE="50GB"
BUCKET="deployment-scripts-central-element-323112"
PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
VM_NAME=""
SKIP_TARBALLS=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

usage() {
    echo "Usage: $0 --asset-group <CAT> --strategy <ID> --start-date YYYY-MM-DD --end-date YYYY-MM-DD [options]"
    echo ""
    echo "Required:"
    echo "  --asset-group <CAT>        CEFI, TRADFI, DEFI, SPORTS, PREDICTION"
    echo "  --strategy <ID>         Strategy ID (e.g. CEFI_MOMENTUM_BTC_5M)"
    echo "  --start-date YYYY-MM-DD Backtest start date"
    echo "  --end-date YYYY-MM-DD   Backtest end date"
    echo ""
    echo "Optional:"
    echo "  --mode <mode>           Pipeline mode: batch (default), backtest, paper, live"
    echo "  --zone <zone>           GCE zone (default: ${ZONE})"
    echo "  --machine-type <type>   GCE machine type (default: ${MACHINE_TYPE})"
    echo "  --disk-size <size>      Boot disk size (default: ${DISK_SIZE})"
    echo "  --bucket <bucket>       GCS bucket for code/logs (default: ${BUCKET})"
    echo "  --env <env>             Deployment env tier (prod|staging|dev; default: ${DEPLOYMENT_ENV})"
    echo "  --vm-name <name>        Custom VM name (default: auto-generated)"
    echo "  --skip-tarballs         Skip tarball creation (use existing in GCS)"
    echo "  --dry-run               Show what would be created without executing"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --asset-group)     ASSET_GROUP="${2^^}"; shift 2 ;;
        --strategy)     STRATEGY="$2"; shift 2 ;;
        --start-date)   START_DATE="$2"; shift 2 ;;
        --end-date)     END_DATE="$2"; shift 2 ;;
        --mode)         MODE="$2"; shift 2 ;;
        --zone)         ZONE="$2"; shift 2 ;;
        --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
        --disk-size)    DISK_SIZE="$2"; shift 2 ;;
        --bucket)       BUCKET="$2"; shift 2 ;;
        --env)          DEPLOYMENT_ENV="$2"; shift 2 ;;
        --vm-name)      VM_NAME="$2"; shift 2 ;;
        --skip-tarballs) SKIP_TARBALLS=true; shift ;;
        --dry-run)      DRY_RUN=true; shift ;;
        --help|-h)      usage ;;
        *)              echo "Unknown option: $1"; usage ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# Validate required args
if [[ -z "$ASSET_GROUP" || -z "$STRATEGY" || -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "ERROR: --asset-group, --strategy, --start-date, and --end-date are required."
    usage
fi

# Validate category
case "$ASSET_GROUP" in
    CEFI|TRADFI|DEFI|SPORTS|PREDICTION) ;;
    *) echo "ERROR: Unknown category: $ASSET_GROUP"; usage ;;
esac

# Generate VM name if not provided
TIMESTAMP=$(date '+%Y%m%d-%H%M%S')
STRATEGY_SLUG=$(echo "$STRATEGY" | tr '[:upper:]' '[:lower:]' | tr '_' '-' | cut -c1-30)
if [[ -z "$VM_NAME" ]]; then
    VM_NAME="strategy-test-${STRATEGY_SLUG}-${TIMESTAMP}"
fi

# Log path in GCS
LOG_PATH="gs://${BUCKET}/logs/strategy-test-${STRATEGY}-${TIMESTAMP}.log"

log() { echo "$(date '+%H:%M:%S') $*"; }

log "=========================================="
log "Strategy Test VM Launcher"
log "=========================================="
log "Category:    ${ASSET_GROUP}"
log "Strategy:    ${STRATEGY}"
log "Date range:  ${START_DATE} -> ${END_DATE}"
log "Mode:        ${MODE}"
log "VM name:     ${VM_NAME}"
log "Zone:        ${ZONE}"
log "Machine:     ${MACHINE_TYPE}"
log "Disk:        ${DISK_SIZE}"
log "Env:         ${DEPLOYMENT_ENV}"
log "Log path:    ${LOG_PATH}"
log "=========================================="
log ""

# ── Step 1: Create and upload code tarballs ──
if ! $SKIP_TARBALLS; then
    log "Step 1: Creating code tarballs for category ${ASSET_GROUP}..."
    TARBALL_ARGS="--asset-group ${ASSET_GROUP} --bucket ${BUCKET}"
    # Also include deployment-service itself (for backfill-cluster.sh on the VM)
    TARBALL_ARGS="${TARBALL_ARGS} --include deployment-service"

    if $DRY_RUN; then
        log "  [DRY RUN] Would run: bash ${SCRIPT_DIR}/create-code-tarballs.sh ${TARBALL_ARGS} --dry-run"
        bash "${SCRIPT_DIR}/create-code-tarballs.sh" ${TARBALL_ARGS} --dry-run
    else
        bash "${SCRIPT_DIR}/create-code-tarballs.sh" ${TARBALL_ARGS}
    fi
    log ""
else
    log "Step 1: SKIPPED (--skip-tarballs) — using existing tarballs in GCS"
    log ""
fi

# ── Step 2: Create shutdown script (self-delete + log upload) ──
SHUTDOWN_SCRIPT=$(cat <<'SHUTDOWN_EOF'
#!/usr/bin/env bash
# Upload logs to GCS and self-delete the VM on completion.
LOG_BUCKET=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/LOG_PATH" 2>/dev/null || echo "")
ZONE=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/zone" 2>/dev/null | awk -F/ '{print $NF}')
VM_NAME=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/name" 2>/dev/null)

# Upload all logs to GCS
if [[ -n "$LOG_BUCKET" ]]; then
    gsutil -m cp /home/ikennaigboaka/logs/*.log "$(dirname "$LOG_BUCKET")/" 2>/dev/null || true
    gsutil cp /var/log/vm-setup.log "$(dirname "$LOG_BUCKET")/vm-setup-${VM_NAME}.log" 2>/dev/null || true
fi

# Self-delete
if [[ -n "$VM_NAME" && -n "$ZONE" ]]; then
    gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet 2>/dev/null || true
fi
SHUTDOWN_EOF
)

# ── Step 3: Create the GCE VM ──
log "Step 2: Creating GCE VM..."

METADATA_ITEMS=(
    "VM_TASK=strategy-backtest"
    "VM_ASSET_GROUP=${ASSET_GROUP}"
    "VM_STRATEGY=${STRATEGY}"
    "VM_START_DATE=${START_DATE}"
    "VM_END_DATE=${END_DATE}"
    "VM_PIPELINE_MODE=backtest"
    "VM_OPERATION=backtest"
    "VM_SERVICE=strategy_service"
    "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    "LOG_PATH=${LOG_PATH}"
    "startup-script-url=gs://${BUCKET}/vm/setup-data-pipeline-vm.sh"
)

METADATA_STR=$(IFS=','; echo "${METADATA_ITEMS[*]}")

if $DRY_RUN; then
    log "[DRY RUN] Would create VM:"
    log "  Name:     ${VM_NAME}"
    log "  Zone:     ${ZONE}"
    log "  Machine:  ${MACHINE_TYPE}"
    log "  Disk:     ${DISK_SIZE}"
    log "  Metadata:"
    for item in "${METADATA_ITEMS[@]}"; do
        log "    ${item}"
    done
    log ""
    log "[DRY RUN] VM would self-delete on completion"
    log "[DRY RUN] Logs would be uploaded to: ${LOG_PATH}"
else
    # Write shutdown script to temp file (process substitution not reliable with gcloud)
    SHUTDOWN_FILE=$(mktemp)
    echo "$SHUTDOWN_SCRIPT" > "$SHUTDOWN_FILE"
    trap 'rm -f "$SHUTDOWN_FILE"' EXIT

    gcloud compute instances create "${VM_NAME}" \
        --project="${PROJECT_ID}" \
        --zone="${ZONE}" \
        --machine-type="${MACHINE_TYPE}" \
        --boot-disk-size="${DISK_SIZE}" \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --scopes=cloud-platform \
        --metadata="${METADATA_STR}" \
        --metadata-from-file="shutdown-script=${SHUTDOWN_FILE}" \
        --labels=purpose=strategy-test,strategy="${STRATEGY_SLUG}",env="${DEPLOYMENT_ENV}"

    log ""
    log "VM created: ${VM_NAME}"
    log ""
    log "Monitor setup progress:"
    log "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} -- tail -f /var/log/vm-setup.log"
    log ""
    log "Monitor pipeline progress:"
    log "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} -- tail -f /home/ikennaigboaka/logs/backtest-pipeline.log"
    log ""
    log "Logs will be uploaded to GCS on completion:"
    log "  ${LOG_PATH}"
    log ""
    log "VM will self-delete when the pipeline completes."
fi

log ""
log "=========================================="
log "Strategy test VM launch complete"
log "=========================================="
