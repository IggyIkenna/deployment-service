#!/usr/bin/env bash
# Launch a GCE VM to run the DeFi recursive-borrow live trading strategy for
# one variant (Family 1 lending-only OR Family 2 perp-hedged).
#
# Part of defi_recursive_borrow_archetypes_2026_05_10.md Phase 13 (live deploy).
# Per CLAUDE.md "VM launcher script SSOT" HARD RULE: every gcloud launcher lives
# under deployment-service/scripts/vm/.
#
# CRITICAL — REAL CAPITAL AT RISK (on-chain DeFi + perp hedge).
# This launcher refuses to start unless:
#   (a) --paper-smoke-passed is supplied (proves >=7d paper smoke green), OR
#   (b) --force-live is supplied (explicit operator override with full awareness).
#
# Variants:
#   CARRY_RECURSIVE_BORROW_LENDING_ONLY  — Family 1: Aave/Morpho loop only
#   CARRY_RECURSIVE_BORROW_PERP_HEDGED  — Family 2: Aave loop + CeFi perp hedge
#
# Usage:
#   bash deployment-service/scripts/vm/launch-defi-recursive-borrow-vm.sh \
#     --variant CARRY_RECURSIVE_BORROW_LENDING_ONLY \
#     --paper-smoke-passed
#
#   # Dry-run (print plan, no VM created):
#   bash deployment-service/scripts/vm/launch-defi-recursive-borrow-vm.sh \
#     --variant CARRY_RECURSIVE_BORROW_LENDING_ONLY \
#     --dry-run
#
# The VM:
#   - n2-standard-4 (4 vCPU / 16GB), 50GB SSD boot disk
#   - Pulls UAC + UTL + MTDS + deployment-service + strategy-service +
#     execution-service + e2e-testing tarballs from gs://deployment-scripts-{pid}/code/
#   - Runs e2e-testing/scripts/defi/run-live.sh --strategy $VARIANT via
#     colocated_engine.py (CLOUD_KMS_ENCRYPTED custody, mainnet).
#   - Self-deletes on completion via shutdown-script.
#
# Singleton-locked PER VARIANT: refuses launch if a same-variant defi-recursive VM
# is already RUNNING in the zone (--force bypass).
#
# Per CLAUDE.md "No fire-and-forget VM launches" HARD RULE: verify STARTED event
# in gs://{pid}-events/events/strategy-service/{today}/{vm-name}/ within 90s.
#
# Watchdog: prefix `defi-recursive-` is registered in vm_zombie_watchdog.py
# VM_PREFIX_TO_BUCKET (heartbeat-only — live VMs write to event-archive but
# not per-VM manifest shards).
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
VARIANT=""
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-4}"
DISK_SIZE="${DISK_SIZE:-50GB}"
PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
DRY_RUN=false
FORCE=false
FORCE_LIVE=false
PAPER_SMOKE_PASSED=false
VM_NAME=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

usage() {
    cat <<EOF
Usage: $0 --variant <name> --paper-smoke-passed [options]

Required:
  --variant <name>          CARRY_RECURSIVE_BORROW_LENDING_ONLY |
                            CARRY_RECURSIVE_BORROW_PERP_HEDGED
  --paper-smoke-passed      Required: confirms >=7d paper smoke (Phase 12) green.

Optional:
  --zone <zone>           GCE zone (default: ${ZONE})
  --machine-type <t>      GCE machine type (default: ${MACHINE_TYPE})
  --disk-size <s>         Boot disk size (default: ${DISK_SIZE})
  --project <pid>         GCP project (default: ${PROJECT_ID})
  --env <env>             Deployment env tier (prod|staging|dev; default: ${DEPLOYMENT_ENV})
  --vm-name <name>        Override generated VM name.
  --force                 Bypass singleton lock (allow same-variant VM RUNNING).
  --force-live            Bypass --paper-smoke-passed gate. USE WITH CAUTION.
  --dry-run               Print plan, do not call gcloud.
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --variant)            VARIANT="$2"; shift 2 ;;
        --paper-smoke-passed) PAPER_SMOKE_PASSED=true; shift ;;
        --zone)               ZONE="$2"; shift 2 ;;
        --machine-type)       MACHINE_TYPE="$2"; shift 2 ;;
        --disk-size)          DISK_SIZE="$2"; shift 2 ;;
        --project)            PROJECT_ID="$2"; CODE_BUCKET="deployment-scripts-${PROJECT_ID}"; shift 2 ;;
        --env)                DEPLOYMENT_ENV="$2"; shift 2 ;;
        --vm-name)            VM_NAME="$2"; shift 2 ;;
        --force)              FORCE=true; shift ;;
        --force-live)         FORCE_LIVE=true; shift ;;
        --dry-run)            DRY_RUN=true; shift ;;
        --help|-h)            usage ;;
        *)                    echo "Unknown option: $1"; usage ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ -z "$VARIANT" ]]; then
    echo "ERROR: --variant is required."
    usage
fi

case "$VARIANT" in
    CARRY_RECURSIVE_BORROW_LENDING_ONLY|CARRY_RECURSIVE_BORROW_PERP_HEDGED) ;;
    *)
        echo "ERROR: --variant must be CARRY_RECURSIVE_BORROW_LENDING_ONLY or CARRY_RECURSIVE_BORROW_PERP_HEDGED (got: $VARIANT)"
        exit 2
        ;;
esac

# ── Safety gate: refuse live launch without paper-smoke evidence ──
if ! $PAPER_SMOKE_PASSED && ! $FORCE_LIVE && ! $DRY_RUN; then
    cat >&2 <<'GATE_MSG'

  ╔══════════════════════════════════════════════════════════════════════╗
  ║  LIVE LAUNCH BLOCKED — paper-smoke gate not satisfied               ║
  ║                                                                      ║
  ║  This launcher requires confirmation that the Phase 12 paper-smoke  ║
  ║  (recursive_borrow_paper_smoke.py, >=7 continuous days) completed   ║
  ║  successfully before any real-capital on-chain launch is permitted.  ║
  ║                                                                      ║
  ║  To proceed:                                                         ║
  ║    1. Complete Phase 12 paper-smoke (see plan Phase 12 done-def).   ║
  ║    2. Re-run with: --paper-smoke-passed                             ║
  ║                                                                      ║
  ║  Emergency bypass (operator accepts full responsibility):           ║
  ║    Re-run with: --force-live                                        ║
  ╚══════════════════════════════════════════════════════════════════════╝

GATE_MSG
    exit 4
fi

VARIANT_SLUG=$(echo "$VARIANT" | tr '[:upper:]' '[:lower:]' | tr '_' '-')
RUN_TS="$(date +%Y%m%d-%H%M%S)"
if [[ -z "$VM_NAME" ]]; then
    # GCE name limit = 63 chars. Layout:
    #   "defi-recursive-"  = 15 chars
    #   slug trunc ≤32     = 32 chars max
    #   "-YYYYMMDD-HHMMSS" = 16 chars
    #   total              = 63 chars max
    SLUG_TRUNC="${VARIANT_SLUG:0:32}"
    SLUG_TRUNC="${SLUG_TRUNC%-}"
    VM_NAME="defi-recursive-${SLUG_TRUNC}-${RUN_TS}"
fi

log() { echo "$(date '+%H:%M:%S') $*"; }

log "==========================================="
log "DeFi Recursive-Borrow Live VM Launcher"
log "==========================================="
log "Variant:      ${VARIANT}"
log "VM name:      ${VM_NAME}"
log "Zone:         ${ZONE}"
log "Machine:      ${MACHINE_TYPE}"
log "Disk:         ${DISK_SIZE}"
log "Project:      ${PROJECT_ID}"
log "Env:          ${DEPLOYMENT_ENV}"
log "Paper smoke:  PASSED (--paper-smoke-passed=${PAPER_SMOKE_PASSED})"
log "==========================================="

# ── Singleton lock per variant ──
if ! $DRY_RUN; then
    EXISTING=$(gcloud compute instances list \
        --project="${PROJECT_ID}" \
        --filter="name~^defi-recursive-${VARIANT_SLUG} AND zone:(${ZONE}) AND status=RUNNING" \
        --format="value(name)" 2>/dev/null || echo "")
    if [[ -n "$EXISTING" && "$FORCE" == "false" ]]; then
        log "ERROR: same-variant recursive-borrow VM already RUNNING:"
        log "  ${EXISTING}"
        log "Pass --force to bypass the singleton lock."
        exit 3
    fi
    if [[ -n "$EXISTING" && "$FORCE" == "true" ]]; then
        log "WARNING: --force given; same-variant VM is RUNNING but launching anyway:"
        log "  ${EXISTING}"
    fi
fi

RUNNER_CMD="bash scripts/defi/run-live.sh --strategy ${VARIANT}"

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
    "VM_TASK=defi-recursive-live"
    "VM_SERVICE=strategy_service"
    "VM_OPERATION=live"
    "VM_PIPELINE_MODE=live"
    "VM_ASSET_GROUP=defi"
    "VM_ARCHETYPE=${VARIANT}"
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
    --labels=purpose=defi-recursive-live,variant="${VARIANT_SLUG}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

log ""
log "VM created: ${VM_NAME}"
log ""
log "Verify event stream (90s after launch):"
log "  gcloud storage ls gs://${PROJECT_ID}-events/events/strategy-service/\$(date +%Y-%m-%d)/${VM_NAME}/"
log ""
log "Verify STARTED event (within 90s):"
log "  gcloud storage cat gs://${PROJECT_ID}-events/events/strategy-service/\$(date +%Y-%m-%d)/${VM_NAME}/hour=*/\*.jsonl 2>/dev/null | head -5"
log ""
log "VM self-deletes on completion (shutdown-script)."
log "==========================================="
