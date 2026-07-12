#!/usr/bin/env bash
# Epic: strategy_master
# Lifecycle: campaign
# Delete-when: funding ensemble paper path folded into strategy-service colocated_engine promote workflow
#
# Launch a SCHEDULED_RECURRING paper VM that runs the funding/basis ENSEMBLE
# desired-state engine on a daily cron and uploads the target book to GCS.
# NO capital at risk — funding_ensemble_engine.py prints + plots the target
# positions (per-venue balances + liquidation alerts) from live venue snapshots;
# it is the SIGNAL / desired-state stage (the journal's emitter), not a trading
# engine. The production paper EXECUTION path is strategy-service colocated_engine
# (run-paper.sh) — see launch-defi-paper-trading-vm.sh.
#
# Per CLAUDE.md "VM launcher script SSOT" HARD RULE: every gcloud launcher lives
# under deployment-service/scripts/vm/.
#
# Usage:
#   bash deployment-service/scripts/vm/launch-funding-ensemble-paper-cron-vm.sh --dry-run
#   bash deployment-service/scripts/vm/launch-funding-ensemble-paper-cron-vm.sh \
#     --capital 1000000 --data-source gcs_complete
#
# The VM:
#   - n2-standard-4 (4 vCPU / 16GB), 50GB SSD boot disk
#   - Pulls UAC + UTL + e2e-testing + strategy-service tarballs from
#     gs://deployment-scripts-{pid}/code/ via setup-data-pipeline-vm.sh
#   - Installs a daily crontab (default 00:15 UTC) that runs
#     e2e-testing/scripts/defi/funding_ensemble_engine.py and uploads the HTML +
#     positioning JSON to gs://{output-bucket}/funding_ensemble/{date}/
#   - SCHEDULED_RECURRING: stays up for the cron; operator stops it to end the run.
#
# Singleton-locked: refuses launch if a funding-ensemble-paper VM is already
# RUNNING in the zone (--force bypass).
#
# Per CLAUDE.md "No fire-and-forget VM launches" HARD RULE: verify the VM reaches
# RUNNING within 60s + the deployment-registry STARTED event, recheck at T+10min.
# NOTE: funding_ensemble_engine.py is a research script (no ServiceBootstrap
# lifecycle events). The setup-data-pipeline-vm.sh wrapper emits the VM-level
# STARTED/STOPPED; per-run progress events arrive once the engine is folded into
# strategy-service (the P1c engine follow-up) — until then verify via the VM
# RUNNING status + the daily GCS output object.
#
# After code changes to e2e-testing / strategy-service, refresh tarballs first:
#   bash deployment-service/scripts/vm/create-code-tarballs.sh \
#     --include e2e-testing --include strategy-service \
#     --include unified-trading-library --include unified-api-contracts
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

CAPITAL="1000000"
DATA_SOURCE="gcs_complete"
CRON_SCHEDULE="15 0 * * *"   # daily 00:15 UTC
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-4}"
DISK_SIZE="${DISK_SIZE:-50GB}"
PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false
FORCE=false
VM_NAME=""

usage() {
    cat <<EOF
Usage: $0 [options]

Optional:
  --capital <usd>       Notional capital for the desired-state book (default: ${CAPITAL})
  --data-source <src>   live | gcs_complete (default: ${DATA_SOURCE})
  --cron <schedule>     crontab schedule (default: "${CRON_SCHEDULE}")
  --zone <zone>         GCE zone (default: ${ZONE})
  --machine-type <t>    GCE machine type (default: ${MACHINE_TYPE})
  --disk-size <s>       Boot disk size (default: ${DISK_SIZE})
  --project <pid>       GCP project (default: ${PROJECT_ID})
  --env <env>           Deployment env tier (prod|staging|dev; default: ${DEPLOYMENT_ENV})
  --vm-name <name>      Override generated VM name.
  --force               Bypass singleton lock.
  --dry-run             Print plan, do not call gcloud.
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --capital)      CAPITAL="$2"; shift 2 ;;
        --data-source)  DATA_SOURCE="$2"; shift 2 ;;
        --cron)         CRON_SCHEDULE="$2"; shift 2 ;;
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
case "$DATA_SOURCE" in
    live|gcs_complete) ;;
    *) echo "ERROR: --data-source must be live or gcs_complete (got: $DATA_SOURCE)" >&2; exit 1 ;;
esac

RUN_TS="$(date +%Y%m%d-%H%M%S)"
if [[ -z "$VM_NAME" ]]; then
    VM_NAME="funding-ensemble-paper-${RUN_TS}"
fi

log() { echo "$(date '+%H:%M:%S') $*"; }

log "=========================================="
log "Funding Ensemble Paper Cron VM Launcher"
log "=========================================="
log "Capital:      \$${CAPITAL}"
log "Data source:  ${DATA_SOURCE}"
log "Cron:         ${CRON_SCHEDULE}"
log "VM name:      ${VM_NAME}"
log "Zone:         ${ZONE}"
log "Machine:      ${MACHINE_TYPE}"
log "Project:      ${PROJECT_ID}"
log "Env:          ${DEPLOYMENT_ENV}"
log "=========================================="

# ── Singleton lock ──
if ! $DRY_RUN; then
    EXISTING=$(gcloud compute instances list \
        --project="${PROJECT_ID}" \
        --filter="name~^funding-ensemble-paper AND zone:(${ZONE}) AND status=RUNNING" \
        --format="value(name)" 2>/dev/null || echo "")
    if [[ -n "$EXISTING" && "$FORCE" == "false" ]]; then
        log "ERROR: funding-ensemble-paper VM already RUNNING: ${EXISTING}"
        log "Pass --force to bypass the singleton lock."
        exit 3
    fi
fi

# One-shot run via VM_TASK=strategy-paper — setup-data-pipeline-vm.sh installs e2e-testing,
# cd's to $WORKSPACE/e2e-testing, symlinks .venv-workspace, then runs VM_BACKFILL_CMD via
# _launch_with_tee (emits DEPLOYMENT_STARTED/COMPLETED/FAILED + streams the log to GCS) and
# self-deletes the VM. So the cmd path is RELATIVE to e2e-testing and uses the symlinked
# .venv-workspace/bin/python (the run-paper.sh convention; this handler does NOT rewrite a
# bare `python`). The engine itself also emits STARTED/STOPPED/FAILED (setup_events). DAILY
# recurrence is an external trigger (Cloud Scheduler / a crontab invoking this launcher) —
# the canonical ephemeral-compute pattern (cf. the manifest consolidator = Scheduler + VM).
# cwd on the VM is $WORKSPACE/e2e-testing; the venv symlink is at $WORKSPACE/.venv-workspace
# → reach it with ../.venv-workspace/bin/python (run-paper.sh uses the absolute $WORKSPACE form).
RUNNER_CMD="DATA_SOURCE=${DATA_SOURCE} ../.venv-workspace/bin/python scripts/defi/funding_ensemble_engine.py --capital ${CAPITAL} --out /tmp/funding_ensemble.html && gsutil -q cp /tmp/funding_ensemble.html gs://${CODE_BUCKET}/funding_ensemble/\$(date +%Y-%m-%d)/funding_ensemble.html || true"

METADATA_ITEMS=(
    "VM_TASK=strategy-paper"
    "VM_SERVICE=strategy_service"
    "VM_OPERATION=paper"
    "VM_PIPELINE_MODE=live"
    "VM_ASSET_GROUP=DEFI"
    "VM_DATA_SOURCE=${DATA_SOURCE}"
    "VM_CAPITAL=${CAPITAL}"
    "VM_BACKFILL_CMD=${RUNNER_CMD}"
    "VM_SHUTDOWN_ON_COMPLETION=true"
    "VM_LIFECYCLE_CLASS=EPHEMERAL_EXPERIMENT"
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
    log "  Cron:     ${CRON_SCHEDULE}"
    log "  Cmd:      ${RUNNER_CMD}"
    for item in "${METADATA_ITEMS[@]}"; do
        log "    ${item}"
    done
    exit 0
fi

log "Creating VM..."
if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        strategy-service unified-api-contracts unified-trading-library deployment-service \
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
    --labels=purpose=funding-ensemble-paper,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

log ""
log "VM created: ${VM_NAME}"
log "Verify RUNNING within 60s (no-fire-and-forget T+0):"
log "  gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
log "Verify STARTED event + recheck progress at T+10min:"
log "  gcloud storage ls gs://${PROJECT_ID}-events/events/strategy-service/\$(date +%Y-%m-%d)/${VM_NAME}/"
log "Daily book output lands in:"
log "  gs://<output-bucket>/funding_ensemble/<date>/"
log "Stop the VM to end the recurring run:"
log "  gcloud compute instances stop ${VM_NAME} --zone=${ZONE}"
log "=========================================="
