#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a GCE VM to run the per-archetype cutover DR drill (Phase 9.A).
#
# Plan: disaster_recovery_circuit_breakers_2026_05_10.md Phase 9.A.
# Runner: e2e-testing/scripts/defi/run_dr_drill_cutover.py
#
# For each DeFi archetype (carry_staked_basis + arbitrage_price_dispersion):
#   1. Kill-switch drill: arms KILL_PER_ARCHETYPE_*, checks armed_switch_ids(),
#      disarms. Records KillSwitchDrillResult.
#   2. Breaker fire + recovery drill: first 5 breakers in PER_ARCHETYPE_BREAKERS
#      registry — arm via BreakerRecoveryEngine + evaluate one recovery tick.
#      Records BreakerDrillResult.
# Evidence written as JSON to:
#   gs://{pid}-events/dr_cutover_evidence/archetype={archetype}/date={date}/evidence.json
#
# The VM:
#   - n2-standard-2 (2 vCPU / 8GB), 20GB SSD boot disk
#   - Pulls UAC + UTL + e2e-testing tarballs from gs://deployment-scripts-{pid}/code/
#   - Self-deletes on completion via shutdown-script.
#
# Singleton-locked: refuses launch if a same-prefix VM is already RUNNING
# in the zone (--force bypass).
#
# Phase 9.B done-def: ≥15 breaker fires + recoveries per archetype within SLA.
#
# After code changes to e2e-testing/scripts/defi/ or UTL, refresh tarballs:
#   bash deployment-service/scripts/vm/create-code-tarballs.sh \
#     --include unified-trading-library --include unified-api-contracts \
#     --include e2e-testing
#
# VM Naming Convention: "dr-drill-cutover-{ts}" — prefix registered in
# VM_PREFIX_TO_BUCKET (vm_zombie_watchdog.py).
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$REPO_ROOT/.." && pwd)}"

# Defaults
ARCHETYPE="all"
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
  --archetype <name>      carry_staked_basis | ARBITRAGE_PRICE_DISPERSION | all (default: all)
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
        --archetype)    ARCHETYPE="$2"; shift 2 ;;
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

case "$ARCHETYPE" in
    carry_staked_basis|CARRY_STAKED_BASIS) ARCHETYPE="CARRY_STAKED_BASIS" ;;
    arbitrage_price_dispersion|ARBITRAGE_PRICE_DISPERSION) ARCHETYPE="ARBITRAGE_PRICE_DISPERSION" ;;
    all) ;;
    *) echo "ERROR: --archetype must be carry_staked_basis, ARBITRAGE_PRICE_DISPERSION, or all (got: $ARCHETYPE)"; exit 2 ;;
esac

RUN_TS="$(date +%Y%m%d-%H%M%S)"
if [[ -z "$VM_NAME" ]]; then
    VM_NAME="dr-drill-cutover-${RUN_TS}"
fi

log() { echo "$(date '+%H:%M:%S') $*"; }

log "=========================================="
log "DR Drill Cutover VM Launcher (Phase 9.A)"
log "=========================================="
log "Archetype: ${ARCHETYPE}"
log "VM name:   ${VM_NAME}"
log "Zone:      ${ZONE}"
log "Machine:   ${MACHINE_TYPE}"
log "Disk:      ${DISK_SIZE}"
log "Project:   ${PROJECT_ID}"
log "Env:       ${DEPLOYMENT_ENV}"
log "=========================================="

# ── Singleton lock ──
if ! $DRY_RUN; then
    EXISTING=$(gcloud compute instances list \
        --project="${PROJECT_ID}" \
        --filter="name~^dr-drill-cutover- AND zone:(${ZONE}) AND status=RUNNING" \
        --format="value(name)" 2>/dev/null || echo "")
    if [[ -n "$EXISTING" && "$FORCE" == "false" ]]; then
        log "ERROR: dr-drill-cutover VM already RUNNING: ${EXISTING}"
        log "Pass --force to bypass the singleton lock."
        exit 3
    fi
    if [[ -n "$EXISTING" && "$FORCE" == "true" ]]; then
        log "WARNING: --force given; existing VM RUNNING but launching anyway: ${EXISTING}"
    fi
fi

ARCHETYPE_ARG=""
if [[ "$ARCHETYPE" != "all" ]]; then
    ARCHETYPE_ARG="--archetype ${ARCHETYPE}"
fi
RUNNER_CMD="cd /app/e2e-testing && python scripts/defi/run_dr_drill_cutover.py ${ARCHETYPE_ARG}"

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
    "VM_TASK=dr-drill-cutover"
    "VM_SERVICE=dr-drill-cutover"
    "VM_OPERATION=drill"
    "VM_ASSET_GROUP=DEFI"
    "VM_ARCHETYPE=${ARCHETYPE}"
    "VM_SHUTDOWN_ON_COMPLETION=true"
    "VM_BACKFILL_CMD=${RUNNER_CMD}"
    "VM_NAME=${VM_NAME}"
    "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    "startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
)

METADATA_STR=$(IFS=','; echo "${METADATA_ITEMS[*]}")

if $DRY_RUN; then
    log "[DRY RUN] Would create VM:"
    log "  Name:      ${VM_NAME}"
    log "  Zone:      ${ZONE}"
    log "  Machine:   ${MACHINE_TYPE}"
    log "  Disk:      ${DISK_SIZE}"
    log "  Archetype: ${ARCHETYPE}"
    log "  Cmd:       ${RUNNER_CMD}"
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
        unified-api-contracts unified-trading-library deployment-service \
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
    --labels=purpose=dr-drill-cutover,archetype="$(echo "$ARCHETYPE" | tr '[:upper:]' '[:lower:]' | tr '_' '-')",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

log ""
log "VM created: ${VM_NAME}"
log ""
log "Verify STARTED event (within 60s):"
log "  gcloud storage cat gs://${PROJECT_ID}-events/events/dr-drill-cutover/\$(date +%Y-%m-%d)/${VM_NAME}/hour=*/\*.jsonl 2>/dev/null | head -5"
log ""
log "Verify cutover evidence (after completion):"
log "  gcloud storage ls gs://${PROJECT_ID}-events/dr_cutover_evidence/"
log ""
log "VM self-deletes on completion (shutdown-script)."
log "=========================================="
