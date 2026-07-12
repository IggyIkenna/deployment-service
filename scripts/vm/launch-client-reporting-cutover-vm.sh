#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run verified + GCS orphan-sweep=0
# Launch a GCE VM running the client-reporting PnL attribution cutover demo.
# Phase 8.A of client_reporting_pnl_attribution_mvp_2026_05_10.md.
#
# Lifecycle steps executed on the VM:
#   1. 24h paper-trade attribution loop for demo_client_001
#   2. Both archetypes: carry_staked_basis + arbitrage_price_dispersion
#   3. Hourly decomposition-sum invariant checks
#   4. emit_attribution_parquet() output to client-reports GCS bucket
#
# Each step emits a structured progress event:
#   gs://{pid}-events/events/client-reporting-cutover/{today}/{vm-name}/hour={H}/*.jsonl
# Required events: STARTED (within 60s) + ≥1 progress/hour + STOPPED.
#
# Singleton-locked: refuses to launch if client-reporting-cutover-* is RUNNING
# in the zone. Use --force to bypass.
#
# Watchdog: prefix `client-reporting-cutover-` registered in VM_PREFIX_TO_BUCKET
# (heartbeat-only — writes to event-archive + client-reports bucket,
#  not per-VM manifest shards).
#
# Usage:
#   bash deployment-service/scripts/vm/launch-client-reporting-cutover-vm.sh
#   bash deployment-service/scripts/vm/launch-client-reporting-cutover-vm.sh --dry-run
#   bash deployment-service/scripts/vm/launch-client-reporting-cutover-vm.sh --force
#
# After any code changes to client-reporting-api / unified-trading-library /
# unified-api-contracts, refresh tarballs first:
#   bash deployment-service/scripts/vm/create-code-tarballs.sh \
#     --include client-reporting-api \
#     --include unified-trading-library \
#     --include unified-api-contracts
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

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

VM_PREFIX="client-reporting-cutover-"

usage() {
    cat <<EOF
Usage: $0 [options]

Launch the client-reporting PnL attribution cutover VM (Phase 8.A).
Runs a 24h paper-trade attribution loop for the demo client.

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
  gcloud storage ls gs://${PROJECT_ID}-events/events/client-reporting-cutover/\$(date +%Y-%m-%d)/<vm-name>/

Evidence capture (after ~24h):
  gcloud storage cat \\
    'gs://${PROJECT_ID}-events/events/client-reporting-cutover/\$(date +%Y-%m-%d)/<vm-name>/hour=*/\*.jsonl' \\
    2>/dev/null | grep '"event_name": "STOPPED"'
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
    # "client-reporting-cutover-" = 25 chars + "YYYYMMDD-HHMMSS" = 15 chars = 40 total
    VM_NAME="${VM_PREFIX}${RUN_TS}"
fi

log() { echo "$(date '+%H:%M:%S') $*"; }

log "=========================================="
log "Client Reporting Cutover VM Launcher"
log "Phase 8.A — Demo client 24h attribution run"
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
        log "ERROR: client-reporting-cutover VM already RUNNING in ${ZONE}:"
        log "  ${EXISTING}"
        log ""
        log "Options:"
        log "  Inspect:  gcloud compute ssh ${EXISTING} --zone=${ZONE}"
        log "  Events:   gcloud storage ls gs://${PROJECT_ID}-events/events/client-reporting-cutover/\$(date +%Y-%m-%d)/${EXISTING}/"
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
RUNNER_CMD="python3 client-reporting-api/scripts/run_client_reporting_cutover.py \
  --demo-client demo_client_001 \
  --duration-hours 24 \
  --run-id ${VM_NAME} \
  --cloud gcp \
  --project-id ${PROJECT_ID} \
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
    "VM_TASK=client-reporting-cutover"
    "VM_SERVICE=client_reporting"
    "VM_OPERATION=pnl_attribution_cutover"
    "VM_MODE=paper"
    "VM_ASSET_GROUP=defi"
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
    log "  gcloud storage cat \\"
    log "    'gs://${PROJECT_ID}-events/events/client-reporting-cutover/\$(date +%Y-%m-%d)/${VM_NAME}/hour=*/*.jsonl' \\"
    log "    2>/dev/null | grep '\"event_name\": \"STOPPED\"'"
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
    --labels="purpose=client-reporting-cutover,env=${DEPLOYMENT_ENV},run-ts=${RUN_TS}"

log ""
log "VM created: ${VM_NAME}"
log ""
log "STEP 1 — Verify STARTED event (within 60s):"
log "  gcloud storage ls \\"
log "    gs://${PROJECT_ID}-events/events/client-reporting-cutover/\$(date +%Y-%m-%d)/${VM_NAME}/"
log ""
log "STEP 2 — Tail attribution progress events:"
log "  gcloud storage cat \\"
log "    'gs://${PROJECT_ID}-events/events/client-reporting-cutover/\$(date +%Y-%m-%d)/${VM_NAME}/hour=*/*.jsonl' \\"
log "    2>/dev/null | tail -20"
log ""
log "STEP 3 — Verify parquet output (after first hour):"
log "  gcloud storage ls \\"
log "    'gs://client-reports-${PROJECT_ID}/pnl_attribution/strategy_id=*/client_id=demo_client_001/'"
log ""
log "STEP 4 — Capture STOPPED evidence (after ~24h):"
log "  gcloud storage cat \\"
log "    'gs://${PROJECT_ID}-events/events/client-reporting-cutover/\$(date +%Y-%m-%d)/${VM_NAME}/hour=*/*.jsonl' \\"
log "    2>/dev/null | grep '\"STOPPED\"'"
log ""
log "Exit codes: 0=success 1=internal-error 2=bad-args 3=singleton-lock"
log "=========================================="
