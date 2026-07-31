#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch a GCE VM to run the strategy live-trading mode (Copper MPC custody,
# real mainnet capital) for a single archetype.
#
# Part of promote_workflow_may23_cli_path_2026_05_10.md Phase 1 (launcher
# scripts for paper + live). Per CLAUDE.md "VM launcher script SSOT" HARD RULE:
# every gcloud launcher lives under deployment-service/scripts/vm/.
#
# CRITICAL — REAL CAPITAL AT RISK.
# This launcher refuses to start unless:
#   (a) --dry-run-live-cutover-passed is supplied (proves Phase 8 dry-run ran), OR
#   (b) --force-live is supplied (explicit operator override with full awareness).
# This gate exists to prevent accidental live launches before the Phase 8
# dry-run completes (see promote_workflow_may23_cli_path_2026_05_10.md Phase 8).
#
# Usage:
#   bash deployment-service/scripts/vm/launch-strategy-live-vm.sh \
#     --archetype carry_staked_basis \
#     --dry-run-live-cutover-passed   # required for real launch
#
#   bash deployment-service/scripts/vm/launch-strategy-live-vm.sh \
#     --archetype carry_staked_basis \
#     --dry-run   # print plan only, no VM created
#
# The VM:
#   - n2-standard-4 (4 vCPU / 16GB), 50GB SSD boot disk
#   - Pulls UAC + UTL + MTDS + deployment-service + strategy-service +
#     execution-service + e2e-testing tarballs from gs://deployment-scripts-{pid}/code/
#   - Runs e2e-testing/scripts/defi/run-live.sh --strategy $ARCHETYPE via
#     colocated_engine.py (Copper MPC custody, mainnet).
#   - Self-deletes on completion via shutdown-script.
#
# Singleton-locked PER ARCHETYPE: refuses launch if a same-archetype
# strategy-live VM is already RUNNING in the zone (--force bypass).
#
# Per CLAUDE.md "No fire-and-forget VM launches" HARD RULE: verify STARTED
# event in gs://{pid}-events/events/strategy-service/{today}/{vm-name}/ within
# 90s of launch. Recheck every 10-15 min for progress events.
#
# Watchdog: prefix `strategy-live-` is registered in vm_zombie_watchdog.py
# VM_PREFIX_TO_BUCKET (heartbeat-only — live VMs write to event-archive but
# not per-VM manifest shards, so watchdog falls back to heartbeat sidecar).
#
# After code changes to strategy-service / execution-service / e2e-testing,
# refresh tarballs before launching:
#   bash deployment-service/scripts/vm/create-code-tarballs.sh \
#     --include strategy-service --include execution-service \
#     --include e2e-testing \
#     --include unified-trading-library --include unified-api-contracts
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$REPO_ROOT/.." && pwd)}"

# Defaults
ARCHETYPE=""
SHARD_ID=0
CLIENTS_YAML_PATH=""
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-4}"
DISK_SIZE="${DISK_SIZE:-150GB}"
PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
DRY_RUN=false
FORCE=false
FORCE_LIVE=false
DRY_RUN_CUTOVER_PASSED=false
VM_NAME=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

usage() {
    cat <<EOF
Usage: $0 --archetype <name> --dry-run-live-cutover-passed [options]

Required:
  --archetype <name>              carry_staked_basis | ARBITRAGE_PRICE_DISPERSION
  --dry-run-live-cutover-passed   Required: confirms Phase 8 dry-run completed.

Optional:
  --shard <n>             Shard index (default: ${SHARD_ID}). Singleton lock key is (archetype, shard).
  --clients-yaml-path <p> Path to clients.yaml for this shard. Passed as VM_CLIENTS_YAML_PATH metadata.
  --zone <zone>           GCE zone (default: ${ZONE})
  --machine-type <t>      GCE machine type (default: ${MACHINE_TYPE})
  --disk-size <s>         Boot disk size (default: ${DISK_SIZE})
  --project <pid>         GCP project (default: ${PROJECT_ID})
  --env <env>             Deployment env tier (prod|staging|dev; default: ${DEPLOYMENT_ENV})
  --vm-name <name>        Override generated VM name.
  --force                 Bypass singleton lock (allow same-archetype+shard VM RUNNING).
  --force-live            Bypass the --dry-run-live-cutover-passed gate.
                          USE WITH EXTREME CAUTION — real capital at risk.
  --dry-run               Print plan, do not call gcloud.
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --archetype)                    ARCHETYPE="$2"; shift 2 ;;
        --shard)                        SHARD_ID="$2"; shift 2 ;;
        --clients-yaml-path)            CLIENTS_YAML_PATH="$2"; shift 2 ;;
        --dry-run-live-cutover-passed)  DRY_RUN_CUTOVER_PASSED=true; shift ;;
        --zone)                         ZONE="$2"; shift 2 ;;
        --machine-type)                 MACHINE_TYPE="$2"; shift 2 ;;
        --disk-size)                    DISK_SIZE="$2"; shift 2 ;;
        --project)                      PROJECT_ID="$2"; CODE_BUCKET="deployment-scripts-${PROJECT_ID}"; shift 2 ;;
        --env)                          DEPLOYMENT_ENV="$2"; shift 2 ;;
        --vm-name)                      VM_NAME="$2"; shift 2 ;;
        --force)                        FORCE=true; shift ;;
        --force-live)                   FORCE_LIVE=true; shift ;;
        --dry-run)                      DRY_RUN=true; shift ;;
        --help|-h)                      usage ;;
        *)                              echo "Unknown option: $1"; usage ;;
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

case "$ARCHETYPE" in
    carry_staked_basis|ARBITRAGE_PRICE_DISPERSION) ;;
    *) echo "ERROR: --archetype must be carry_staked_basis or ARBITRAGE_PRICE_DISPERSION (got: $ARCHETYPE)"; exit 2 ;;
esac

# ── Safety gate: refuse live launch without Phase 8 dry-run evidence ──
if ! $DRY_RUN_CUTOVER_PASSED && ! $FORCE_LIVE && ! $DRY_RUN; then
    cat >&2 <<'GATE_MSG'

  ╔══════════════════════════════════════════════════════════════════════╗
  ║  LIVE LAUNCH BLOCKED — Phase 8 dry-run gate not satisfied           ║
  ║                                                                      ║
  ║  This launcher requires confirmation that the Phase 8 dry-run       ║
  ║  (promote_workflow_may23_cli_path_2026_05_10.md Phase 8) completed  ║
  ║  successfully before any real-capital launch is permitted.          ║
  ║                                                                      ║
  ║  To proceed:                                                         ║
  ║    1. Complete Phase 8 dry-run (see plan Phase 8 done-definition).  ║
  ║    2. Re-run with: --dry-run-live-cutover-passed                    ║
  ║                                                                      ║
  ║  Emergency bypass (operator accepts full responsibility):           ║
  ║    Re-run with: --force-live                                        ║
  ╚══════════════════════════════════════════════════════════════════════╝

GATE_MSG
    exit 4
fi

ARCHETYPE_SLUG=$(echo "$ARCHETYPE" | tr '[:upper:]' '[:lower:]' | tr '_:' '--')
RUN_TS="$(date +%Y%m%d-%H%M%S)"
if [[ -z "$VM_NAME" ]]; then
    # GCE name limit = 63 chars. Layout:
    #   "strategy-live-"  = 14 chars
    #   slug ≤22          = 22 chars max (reduced to fit shard suffix)
    #   "-shardN"         = 7 chars max
    #   "-YYYYMMDD-HHMMSS" = 16 chars
    #   total              = 59 chars max
    SLUG_TRUNC="${ARCHETYPE_SLUG:0:22}"
    SLUG_TRUNC="${SLUG_TRUNC%-}"
    VM_NAME="strategy-live-${SLUG_TRUNC}-shard${SHARD_ID}-${RUN_TS}"
fi

log() { echo "$(date '+%H:%M:%S') $*"; }

log "=========================================="
log "Strategy Live VM Launcher — REAL CAPITAL"
log "=========================================="
log "Archetype:    ${ARCHETYPE}"
log "Shard:        ${SHARD_ID}"
log "VM name:      ${VM_NAME}"
log "Zone:         ${ZONE}"
log "Machine:      ${MACHINE_TYPE}"
log "Disk:         ${DISK_SIZE}"
log "Project:      ${PROJECT_ID}"
log "Env:          ${DEPLOYMENT_ENV}"
log "Cutover gate: PASSED (--dry-run-live-cutover-passed=${DRY_RUN_CUTOVER_PASSED})"
log "=========================================="

# ── Singleton lock per (archetype, shard) triplet ──
if ! $DRY_RUN; then
    EXISTING=$(gcloud compute instances list \
        --project="${PROJECT_ID}" \
        --filter="name~^strategy-live-${ARCHETYPE_SLUG}-shard${SHARD_ID} AND zone:(${ZONE}) AND status=RUNNING" \
        --format="value(name)" 2>/dev/null || echo "")
    if [[ -n "$EXISTING" && "$FORCE" == "false" ]]; then
        log "ERROR: same-archetype+shard live VM already RUNNING:"
        log "  ${EXISTING}"
        log "Pass --force to bypass the singleton lock."
        exit 3
    fi
    if [[ -n "$EXISTING" && "$FORCE" == "true" ]]; then
        log "WARNING: --force given; same-archetype+shard live VM is RUNNING but launching anyway:"
        log "  ${EXISTING}"
    fi
fi

# ── Build run-live.sh command ──
RUNNER_CMD="bash scripts/defi/run-live.sh --strategy ${ARCHETYPE}"

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
    "VM_TASK=strategy-live"
    "VM_SERVICE=strategy_service"
    "VM_OPERATION=live"
    "VM_PIPELINE_MODE=live"
    "VM_ASSET_GROUP=DEFI"
    "VM_ARCHETYPE=${ARCHETYPE}"
    "VM_SHARD_ID=${SHARD_ID}"
    "VM_SHUTDOWN_ON_COMPLETION=true"
    "VM_BACKFILL_CMD=${RUNNER_CMD}"
    "MANIFEST_PER_VM_SHARDS=true"
    "VM_NAME=${VM_NAME}"
    "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    "startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
)
if [[ -n "$CLIENTS_YAML_PATH" ]]; then
    METADATA_ITEMS+=("VM_CLIENTS_YAML_PATH=${CLIENTS_YAML_PATH}")
fi

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
        strategy-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

# Data-writing VM: disk sized for sustained writes. A pd-standard 50GB boot disk
# sustains only ~6 MB/s and collapsed the CeFi backfill to 2.36 MB/s after ~7.5GB
# (2026-07-18). Enforced by scripts/quality_gates/check_backfill_vm_disk_provisioning.py.
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
    --labels=purpose=strategy-live,archetype="${ARCHETYPE_SLUG}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

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
log "=========================================="
