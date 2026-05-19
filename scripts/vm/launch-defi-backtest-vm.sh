#!/usr/bin/env bash
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch a GCE VM to run a DeFi strategy batch backtest (historical replay via
# the co-located engine). Closes batch_live_symmetry_2026_05_10.md Tab 8 Step 1
# (launch-defi-backtest-vm.sh greenfield ship).
#
# Per CLAUDE.md "VM launcher script SSOT" HARD RULE:
# every gcloud launcher lives under deployment-service/scripts/vm/.
#
# Usage:
#   bash deployment-service/scripts/vm/launch-defi-backtest-vm.sh \
#     --archetype carry_staked_basis \
#     --start-date 2024-01-01 \
#     --end-date 2026-05-01
#
#   bash deployment-service/scripts/vm/launch-defi-backtest-vm.sh \
#     --archetype ARBITRAGE_PRICE_DISPERSION \
#     --start-date 2026-04-01 --end-date 2026-05-19 \
#     --dry-run
#
# The VM:
#   - n2-standard-4 (4 vCPU / 16GB), 100GB SSD boot disk
#   - Pulls UAC + UTL + e2e-testing + strategy-service + execution-service
#     tarballs from gs://deployment-scripts-{pid}/code/
#   - Runs e2e-testing/scripts/defi/run-batch.sh
#     --strategy $ARCHETYPE --start-date $START_DATE --end-date $END_DATE
#     via colocated_engine.py
#   - Writes backtest scores to
#     gs://strategy-store-{pid}/backtests/defi/{archetype}/{ts}/
#   - Self-deletes on completion via shutdown-script.
#
# Singleton-locked PER ARCHETYPE: refuses launch if a same-archetype
# defi-backtest VM is already RUNNING in the zone (--force bypass).
#
# Per CLAUDE.md "No fire-and-forget VM launches" HARD RULE: verify STARTED
# event in gs://{pid}-events/ within 90s of launch. Recheck every 10-15 min
# for progress events.
#
# Watchdog: prefix `defi-backtest-` is registered in vm_zombie_watchdog.py
# VM_PREFIX_TO_BUCKET (heartbeat-only — backtest VMs write to strategy-store
# but not per-VM manifest shards, so watchdog falls back to heartbeat sidecar).
#
# After code changes to strategy-service / execution-service / e2e-testing,
# refresh tarballs before launching:
#   bash deployment-service/scripts/vm/create-code-tarballs.sh \
#     --include strategy-service --include execution-service \
#     --include e2e-testing \
#     --include unified-trading-library --include unified-api-contracts
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$REPO_ROOT/.." && pwd)}"

# Defaults
ARCHETYPE=""
START_DATE=""
END_DATE=""
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-4}"
DISK_SIZE="${DISK_SIZE:-100GB}"
PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
DRY_RUN=false
FORCE=false
VM_NAME=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

usage() {
    cat <<EOF
Usage: $0 --archetype <name> --start-date <YYYY-MM-DD> --end-date <YYYY-MM-DD> [options]

Required:
  --archetype <name>      carry_staked_basis | ARBITRAGE_PRICE_DISPERSION
  --start-date <date>     Backtest start date (YYYY-MM-DD)
  --end-date <date>       Backtest end date (YYYY-MM-DD)

Optional:
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
        --archetype)     ARCHETYPE="$2"; shift 2 ;;
        --start-date)    START_DATE="$2"; shift 2 ;;
        --end-date)      END_DATE="$2"; shift 2 ;;
        --zone)          ZONE="$2"; shift 2 ;;
        --machine-type)  MACHINE_TYPE="$2"; shift 2 ;;
        --disk-size)     DISK_SIZE="$2"; shift 2 ;;
        --project)       PROJECT_ID="$2"; CODE_BUCKET="deployment-scripts-${PROJECT_ID}"; shift 2 ;;
        --env)           DEPLOYMENT_ENV="$2"; shift 2 ;;
        --vm-name)       VM_NAME="$2"; shift 2 ;;
        --force)         FORCE=true; shift ;;
        --dry-run)       DRY_RUN=true; shift ;;
        --help|-h)       usage ;;
        *)               echo "Unknown option: $1"; usage ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ -z "$ARCHETYPE" ]]; then
    echo "ERROR: --archetype is required."
    usage
fi

if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "ERROR: --start-date and --end-date are required."
    usage
fi

case "$ARCHETYPE" in
    carry_staked_basis|ARBITRAGE_PRICE_DISPERSION) ;;
    *) echo "ERROR: --archetype must be carry_staked_basis or ARBITRAGE_PRICE_DISPERSION (got: $ARCHETYPE)"; exit 2 ;;
esac

ARCHETYPE_SLUG=$(echo "$ARCHETYPE" | tr '[:upper:]' '[:lower:]' | tr '_:' '--')
RUN_TS="$(date +%Y%m%d-%H%M%S)"
if [[ -z "$VM_NAME" ]]; then
    # GCE name limit = 63 chars. Layout:
    #   "defi-backtest-" = 14 chars
    #   slug ≤30         = 30 chars max
    #   "-YYYYMMDD-HHMMSS" = 16 chars
    #   total              = 60 chars max
    SLUG_TRUNC="${ARCHETYPE_SLUG:0:30}"
    SLUG_TRUNC="${SLUG_TRUNC%-}"
    VM_NAME="defi-backtest-${SLUG_TRUNC}-${RUN_TS}"
fi

log() { echo "$(date '+%H:%M:%S') $*"; }

log "=========================================="
log "DeFi Backtest VM Launcher"
log "=========================================="
log "Archetype:    ${ARCHETYPE}"
log "Date range:   ${START_DATE} → ${END_DATE}"
log "VM name:      ${VM_NAME}"
log "Zone:         ${ZONE}"
log "Machine:      ${MACHINE_TYPE}"
log "Disk:         ${DISK_SIZE}"
log "Project:      ${PROJECT_ID}"
log "Env:          ${DEPLOYMENT_ENV}"
log "=========================================="

# ── Singleton lock per archetype ──
if ! $DRY_RUN; then
    EXISTING=$(gcloud compute instances list \
        --project="${PROJECT_ID}" \
        --filter="name~^defi-backtest-${ARCHETYPE_SLUG} AND zone:(${ZONE}) AND status=RUNNING" \
        --format="value(name)" 2>/dev/null || echo "")
    if [[ -n "$EXISTING" && "$FORCE" == "false" ]]; then
        log "ERROR: same-archetype backtest VM already RUNNING:"
        log "  ${EXISTING}"
        log "Pass --force to bypass the singleton lock."
        exit 3
    fi
    if [[ -n "$EXISTING" && "$FORCE" == "true" ]]; then
        log "WARNING: --force given; same-archetype backtest VM is RUNNING but launching anyway:"
        log "  ${EXISTING}"
    fi
fi

# ── Build run-batch.sh command ──
BATCH_ARGS="--strategy ${ARCHETYPE} --start-date ${START_DATE} --end-date ${END_DATE}"
RUNNER_CMD="bash scripts/defi/run-batch.sh ${BATCH_ARGS}"

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

METADATA_ITEMS=(
    "VM_TASK=defi-backtest"
    "VM_SERVICE=strategy_service"
    "VM_OPERATION=batch"
    "VM_PIPELINE_MODE=batch"
    "VM_ASSET_GROUP=DEFI"
    "VM_ARCHETYPE=${ARCHETYPE}"
    "VM_BACKTEST_START=${START_DATE}"
    "VM_BACKTEST_END=${END_DATE}"
    "VM_SHUTDOWN_ON_COMPLETION=true"
    "VM_BACKFILL_CMD=${RUNNER_CMD}"
    "MANIFEST_PER_VM_SHARDS=true"
    "VM_NAME=${VM_NAME}"
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
    --labels=purpose=defi-backtest,archetype="${ARCHETYPE_SLUG}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

log ""
log "VM created: ${VM_NAME}"
log ""
log "Verify event stream (90s after launch):"
log "  gcloud storage ls gs://${PROJECT_ID}-events/events/strategy-service/\$(date +%Y-%m-%d)/${VM_NAME}/"
log ""
log "Verify STARTED event (within 90s):"
log "  gcloud storage cat gs://${PROJECT_ID}-events/events/strategy-service/\$(date +%Y-%m-%d)/${VM_NAME}/hour=*/\*.jsonl 2>/dev/null | head -5"
log ""
log "Check backtest scores after VM completes:"
log "  gcloud storage ls gs://strategy-store-${PROJECT_ID}/backtests/defi/${ARCHETYPE}/"
log ""
log "VM self-deletes on completion (shutdown-script)."
log "=========================================="
