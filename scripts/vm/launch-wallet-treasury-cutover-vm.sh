#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run verified + GCS orphan-sweep=0
# Launch a GCE VM running the wallet/treasury cutover-archetype demo client
# dry-run — full lifecycle for Phase 9.A of
# wallet_treasury_client_flow_2026_05_10.md.
#
# Lifecycle steps executed on the VM (per plan Phase 9.A):
#   1. Onboard demo client (POST /api/clients/{id})
#   2. Subscribe to both cutover archetypes at 100%:
#        carry_staked_basis + arbitrage_price_dispersion
#   3. Treasury ping all sources: Copper / CEFFU / DeFi PK / venue sub-account
#   4. Allocation step — calls allocation engine for demo client
#   5. 24h paper-trade loop (execution-service "always fill" mode)
#   6. Per-trade settle + fee-accrual + HWM-ledger update for every fill
#   7. Daily statement emission at midnight UTC (or at 24h boundary)
#   8. ≥1 automated withdrawal: REQUESTED → APPROVED-2-of-N → EXECUTED → RECONCILED
#   9. ≥1 forced period-boundary crystallization (DAILY rule; perf_fee_amount > 0)
#  10. ≥1 underwater zero-fee crystallization (force perf_fee_amount == 0 path)
#
# Each step emits a structured progress event:
#   gs://{pid}-events/events/wallet_treasury/{today}/{vm-name}/hour={H}/*.jsonl
# Required events: STARTED (within 60s) + ≥1 progress/hour + STOPPED.
#
# Singleton-locked: refuses to launch if wallet-treasury-cutover-* is RUNNING
# in the zone. Use --force to bypass.
#
# Watchdog: prefix `wallet-treasury-cutover-` registered in VM_PREFIX_TO_BUCKET
# (heartbeat-only — writes to event-archive, not per-VM manifest shards).
#
# OPERATOR NOTE (Phase 9.A): this is a 24h dry-run.
# Run this script when back from flights; then run:
#   python3 position-balance-monitor-service/scripts/capture_phase_9_evidence.py \
#     --run-id <wallet-treasury-cutover-{timestamp}>
#
# Usage:
#   bash deployment-service/scripts/vm/launch-wallet-treasury-cutover-vm.sh
#   bash deployment-service/scripts/vm/launch-wallet-treasury-cutover-vm.sh --dry-run
#   bash deployment-service/scripts/vm/launch-wallet-treasury-cutover-vm.sh --force
#   bash deployment-service/scripts/vm/launch-wallet-treasury-cutover-vm.sh \
#     --env staging --force
#
# After any code changes to position-balance-monitor-service / execution-service /
# unified-trading-library / unified-api-contracts, refresh tarballs first:
#   bash deployment-service/scripts/vm/create-code-tarballs.sh \
#     --include position-balance-monitor-service \
#     --include execution-service \
#     --include unified-trading-library \
#     --include unified-api-contracts
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$REPO_ROOT/.." && pwd)}"

# Defaults
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-4}"
DISK_SIZE="${DISK_SIZE:-50GB}"
PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false
FORCE=false
VM_NAME=""

VM_PREFIX="wallet-treasury-cutover-"

usage() {
    cat <<EOF
Usage: $0 [options]

Launch the wallet/treasury cutover dry-run VM (Phase 9.A).
Runs a 24h paper-trade lifecycle for the demo client.

Options:
  --zone <zone>         GCE zone (default: ${ZONE})
  --machine-type <t>    GCE machine type (default: ${MACHINE_TYPE})
  --disk-size <s>       Boot disk size (default: ${DISK_SIZE})
  --project <pid>       GCP project (default: ${PROJECT_ID})
  --env <env>           Deployment env tier: prod|staging|dev (default: ${DEPLOYMENT_ENV})
  --vm-name <name>      Override generated VM name.
  --force               Bypass singleton lock.
  --dry-run             Print plan, do not call gcloud.
  -h, --help            This help.

After launch, verify events within 60s:
  gcloud storage ls gs://${PROJECT_ID}-events/events/wallet_treasury/\$(date +%Y-%m-%d)/<vm-name>/

Then capture evidence after VM completes (~24h):
  python3 position-balance-monitor-service/scripts/capture_phase_9_evidence.py \\
    --run-id <vm-name>
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
        *)              echo "ERROR: Unknown option: $1" >&2; usage ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

RUN_TS="$(date +%Y%m%d-%H%M%S)"
if [[ -z "$VM_NAME" ]]; then
    # GCE name limit = 63 chars.
    # "wallet-treasury-cutover-" = 24 chars + "YYYYMMDD-HHMMSS" = 15 chars = 39 total
    VM_NAME="${VM_PREFIX}${RUN_TS}"
fi

log() { echo "$(date '+%H:%M:%S') $*"; }

log "=========================================="
log "Wallet/Treasury Cutover VM Launcher"
log "Phase 9.A — Demo client 24h dry-run"
log "=========================================="
log "VM name:      ${VM_NAME}"
log "Zone:         ${ZONE}"
log "Machine:      ${MACHINE_TYPE}"
log "Disk:         ${DISK_SIZE}"
log "Project:      ${PROJECT_ID}"
log "Env:          ${DEPLOYMENT_ENV}"
log "=========================================="

# ── Singleton lock ──
if ! $DRY_RUN; then
    EXISTING=$(gcloud compute instances list \
        --project="${PROJECT_ID}" \
        --filter="name~^${VM_PREFIX} AND zone:(${ZONE}) AND status=RUNNING" \
        --format="value(name)" 2>/dev/null || echo "")
    if [[ -n "$EXISTING" && "$FORCE" == "false" ]]; then
        log "ERROR: wallet-treasury-cutover VM already RUNNING in ${ZONE}:"
        log "  ${EXISTING}"
        log ""
        log "Options:"
        log "  Inspect:  gcloud compute ssh ${EXISTING} --zone=${ZONE}"
        log "  Events:   gcloud storage ls gs://${PROJECT_ID}-events/events/wallet_treasury/\$(date +%Y-%m-%d)/${EXISTING}/"
        log "  Delete:   gcloud compute instances delete ${EXISTING} --zone=${ZONE} --quiet"
        log "  Force:    bash $0 --force"
        exit 3
    fi
    if [[ -n "$EXISTING" && "$FORCE" == "true" ]]; then
        log "WARNING: --force given; cutover VM is RUNNING but launching anyway:"
        log "  ${EXISTING}"
    fi
fi

# ── Runner command ──
# Executed by setup-data-pipeline-vm.sh inside the workspace venv.
# Uses position-balance-monitor-service scripts/run_wallet_treasury_cutover.py
# which orchestrates all 10 lifecycle steps with per-step event emission.
RUNNER_CMD="python3 position-balance-monitor-service/scripts/run_wallet_treasury_cutover.py \
  --demo-client demo-client-001 \
  --archetypes carry_staked_basis,ARBITRAGE_PRICE_DISPERSION \
  --paper-trade-duration-hours 24 \
  --force-crystallization \
  --force-underwater-crystallization \
  --run-id ${VM_NAME} \
  --deployment-env ${DEPLOYMENT_ENV}"

# ── Shutdown script (self-delete on completion) ──
SHUTDOWN_SCRIPT=$(cat <<'SHUTDOWN_EOF'
#!/usr/bin/env bash
ZONE=$(curl -sf -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/zone" 2>/dev/null | awk -F/ '{print $NF}')
VM_NAME=$(curl -sf -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/name" 2>/dev/null)
if [[ -n "$VM_NAME" && -n "$ZONE" ]]; then
    gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet 2>/dev/null || true
fi
SHUTDOWN_EOF
)

METADATA_ITEMS=(
    "VM_TASK=wallet-treasury-cutover"
    "VM_SERVICE=wallet_treasury"
    "VM_OPERATION=cutover_dry_run"
    "VM_MODE=paper"
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
    log "  Name:     ${VM_NAME}"
    log "  Zone:     ${ZONE}"
    log "  Machine:  ${MACHINE_TYPE}"
    log "  Disk:     ${DISK_SIZE}"
    log "  Cmd:      ${RUNNER_CMD}"
    log "  Metadata items:"
    for item in "${METADATA_ITEMS[@]}"; do
        log "    ${item}"
    done
    log ""
    log "Evidence capture command (after ~24h):"
    log "  python3 position-balance-monitor-service/scripts/capture_phase_9_evidence.py \\"
    log "    --run-id ${VM_NAME}"
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
    --labels="purpose=wallet-treasury-cutover,env=${DEPLOYMENT_ENV},run-ts=${RUN_TS}"

log ""
log "VM created: ${VM_NAME}"
log ""
log "STEP 1 — Verify STARTED event (within 60s):"
log "  gcloud storage ls \\"
log "    gs://${PROJECT_ID}-events/events/wallet_treasury/\$(date +%Y-%m-%d)/${VM_NAME}/"
log ""
log "STEP 2 — Tail lifecycle progress events:"
log "  gcloud storage cat \\"
log "    'gs://${PROJECT_ID}-events/events/wallet_treasury/\$(date +%Y-%m-%d)/${VM_NAME}/hour=*/*.jsonl' \\"
log "    2>/dev/null | tail -20"
log ""
log "STEP 3 — After ~24h, capture evidence:"
log "  python3 position-balance-monitor-service/scripts/capture_phase_9_evidence.py \\"
log "    --run-id ${VM_NAME}"
log ""
log "Exit codes: 0=success 1=internal-error 2=bad-args 3=singleton-lock"
log "  Per-stage exit codes embedded in lifecycle event payloads."
log "=========================================="
