#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a GCE VM to run the parallel execution-alpha measurement harness.
#
# Runs execution-service/scripts/run_execution_alpha_parallel.py on a
# c3-highcpu-176 (176 vCPU) VM, splitting the date window into 4-day chunks
# and measuring execution alpha (realistic fills vs always_fill baseline) for
# BOTH archetypes (carry_staked_basis + ARBITRAGE_PRICE_DISPERSION) in parallel.
#
# Plan: compute_optimization_mock_data_2026_05_13.md Phase 3
# "Parallel-shard wrapper: 730 days × 2 archetypes × 2 fill-modes = 2920 worker runs.
#  Fits on c3-highcpu-176 with 16-day chunks → 183 chunks per shape, all parallel."
#
# Usage:
#   bash deployment-service/scripts/vm/launch-execution-alpha-vm.sh \
#     --start 2024-01-01 --end 2025-12-31
#
#   bash deployment-service/scripts/vm/launch-execution-alpha-vm.sh \
#     --start 2024-01-01 --end 2025-12-31 \
#     --chunk-days 4 --max-workers 176 \
#     --dry-run
#
# VM naming convention: exec-alpha-{ts}
# Registered in VM_PREFIX_TO_BUCKET as "exec-alpha-" (heartbeat-only;
# output goes to strategy-store bucket, not a manifest-shard writer).
#
# Per-shippable-unit refresh: after editing this launcher OR
# execution-service/scripts/run_execution_alpha_parallel.py, refresh tarballs:
#   bash deployment-service/scripts/vm/create-code-tarballs.sh \
#     --include execution-service --include deployment-service \
#     --include unified-trading-library --include unified-api-contracts
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"


# Defaults.
START_DATE=""
END_DATE=""
CHUNK_DAYS=4
MAX_WORKERS=176
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-c3-highcpu-176}"
DISK_SIZE="${DISK_SIZE:-50GB}"
PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
OUTPUT_BUCKET="strategy-store-${PROJECT_ID}"
POLICY_VERSION="default-v1"
SEED=42
DRY_RUN=false
VM_NAME=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

usage() {
    cat <<EOF
Usage: $0 --start YYYY-MM-DD --end YYYY-MM-DD [options]

Required:
  --start YYYY-MM-DD   Backtest window start (inclusive).
  --end YYYY-MM-DD     Backtest window end (inclusive).

Optional:
  --chunk-days <N>      Days per chunk (default: ${CHUNK_DAYS}; 730 days → 183 chunks/archetype)
  --max-workers <N>     Parallel workers on VM (default: ${MAX_WORKERS})
  --output-bucket <b>   GCS base prefix for chunk JSON outputs (default: gs://${OUTPUT_BUCKET}/backtests/execution_alpha)
  --policy-version <v>  Policy version label for GroupCRunner (default: ${POLICY_VERSION})
  --seed <n>            RNG seed (default: ${SEED})
  --zone <zone>         GCE zone (default: ${ZONE})
  --machine-type <t>    GCE machine type (default: ${MACHINE_TYPE})
  --disk-size <s>       Boot disk size (default: ${DISK_SIZE})
  --project <pid>       GCP project (default: ${PROJECT_ID})
  --env <env>           Deployment env tier (prod|staging|dev; default: ${DEPLOYMENT_ENV})
  --vm-name <name>      Override generated VM name.
  --dry-run             Print VM plan, do not call gcloud.
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --start)          START_DATE="$2"; shift 2 ;;
        --end)            END_DATE="$2"; shift 2 ;;
        --chunk-days)     CHUNK_DAYS="$2"; shift 2 ;;
        --max-workers)    MAX_WORKERS="$2"; shift 2 ;;
        --output-bucket)  OUTPUT_BUCKET="$2"; shift 2 ;;
        --policy-version) POLICY_VERSION="$2"; shift 2 ;;
        --seed)           SEED="$2"; shift 2 ;;
        --zone)           ZONE="$2"; shift 2 ;;
        --machine-type)   MACHINE_TYPE="$2"; shift 2 ;;
        --disk-size)      DISK_SIZE="$2"; shift 2 ;;
        --project)        PROJECT_ID="$2"; CODE_BUCKET="deployment-scripts-${PROJECT_ID}"; shift 2 ;;
        --env)            DEPLOYMENT_ENV="$2"; shift 2 ;;
        --vm-name)        VM_NAME="$2"; shift 2 ;;
        --dry-run)        DRY_RUN=true; shift ;;
        --help|-h)        usage ;;
        *)                echo "Unknown option: $1"; usage ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "ERROR: --start and --end are required."
    usage
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
if [[ -z "$VM_NAME" ]]; then
    VM_NAME="exec-alpha-${RUN_TS}"
fi

GCS_OUTPUT_BASE="gs://${OUTPUT_BUCKET}/backtests/execution_alpha"

log() { echo "$(date '+%H:%M:%S') $*"; }

log "=========================================="
log "Execution Alpha Parallel VM Launcher"
log "=========================================="
log "Date range:   ${START_DATE} -> ${END_DATE}"
log "Chunk days:   ${CHUNK_DAYS}"
log "Max workers:  ${MAX_WORKERS}"
log "VM name:      ${VM_NAME}"
log "Zone:         ${ZONE}"
log "Machine:      ${MACHINE_TYPE}"
log "Disk:         ${DISK_SIZE}"
log "Project:      ${PROJECT_ID}"
log "Env:          ${DEPLOYMENT_ENV}"
log "Output:       ${GCS_OUTPUT_BASE}"
log "=========================================="

# ── Shutdown script (self-delete on completion) ──
SHUTDOWN_SCRIPT=$(cat <<'SHUTDOWN_EOF'
#!/usr/bin/env bash
ZONE=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/zone" 2>/dev/null | awk -F/ '{print $NF}')
VM_NAME=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/name" 2>/dev/null)
if [[ -n "$VM_NAME" && -n "$ZONE" ]]; then
    gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet 2>/dev/null || true
fi
SHUTDOWN_EOF
)

RUNNER_CMD="python scripts/run_execution_alpha_parallel.py \
--start ${START_DATE} \
--end ${END_DATE} \
--chunk-days ${CHUNK_DAYS} \
--max-workers ${MAX_WORKERS} \
--output-bucket ${GCS_OUTPUT_BASE} \
--policy-version ${POLICY_VERSION} \
--seed ${SEED}"

METADATA_ITEMS=(
    "VM_TASK=execution-alpha"
    "VM_SERVICE=execution_service"
    "VM_OPERATION=alpha-measurement"
    "VM_PIPELINE_MODE=batch"
    "VM_ASSET_GROUP=DEFI"
    "VM_START_DATE=${START_DATE}"
    "VM_END_DATE=${END_DATE}"
    "VM_CHUNK_DAYS=${CHUNK_DAYS}"
    "VM_SHUTDOWN_ON_COMPLETION=true"
    "VM_BACKFILL_CMD=${RUNNER_CMD}"
    "OUTPUT_BUCKET=${OUTPUT_BUCKET}"
    "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    "startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
)

METADATA_STR=$(IFS=','; echo "${METADATA_ITEMS[*]}")

if $DRY_RUN; then
    log "[DRY RUN] Would create VM:"
    log "  Name:     ${VM_NAME}"
    log "  Zone:     ${ZONE}"
    log "  Machine:  ${MACHINE_TYPE}"
    log "  Disk:     ${DISK_SIZE}"
    log "  Cmd:      ${RUNNER_CMD}"
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
if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        execution-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

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
    --labels=purpose=execution-alpha,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

log ""
log "VM created: ${VM_NAME}"
log ""
log "Verify event stream (90s after launch):"
log "  gcloud storage ls gs://${PROJECT_ID}-events/events/execution-service/\$(date +%Y-%m-%d)/${VM_NAME}/"
log ""
log "Tail GCS log (live):"
log "  gcloud storage cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
log ""
log "Chunk outputs (on completion):"
log "  gcloud storage ls ${GCS_OUTPUT_BASE}/"
log ""
log "VM self-deletes on completion (shutdown-script)."
log "=========================================="
