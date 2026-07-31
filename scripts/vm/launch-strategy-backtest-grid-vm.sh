#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch a GCE VM to run the 2-yr config-grid backtest for a single
# DeFi archetype (carry_staked_basis | ARBITRAGE_PRICE_DISPERSION).
#
# Closes audit Item #2 + master-plan Group F Item 18 operationally — the
# strategy-service grid runner (strategy-service@3dea3c7) was code-shipped
# but never executed end-to-end on real infra. Per CLAUDE.md "Plans Run
# To Actual Completion" HARD RULE, code-shipped is NOT operationally-shipped;
# this launcher fires the actual run.
#
# Usage:
#   bash deployment-service/scripts/vm/launch-strategy-backtest-grid-vm.sh \
#     --archetype carry_staked_basis \
#     --start 2024-01-01 --end 2026-05-01 \
#     --grid-density medium
#
#   bash deployment-service/scripts/vm/launch-strategy-backtest-grid-vm.sh \
#     --archetype ARBITRAGE_PRICE_DISPERSION \
#     --start 2024-01-01 --end 2026-05-01 \
#     --grid-density medium
#
# Singleton-locked PER ARCHETYPE: refuses launch if a same-archetype VM is
# already RUNNING in the zone (`--force` bypass). Prevents the operator
# from accidentally double-spawning two runs for the same archetype +
# wasting ~16h compute.
#
# The VM:
#   - n2-standard-4 (4 vCPU / 16GB), 100GB SSD boot disk
#   - Pulls strategy-service + UAC + UTL + deployment-service tarballs
#     from gs://deployment-scripts-{pid}/code/
#   - Runs strategy_service.scripts.run_2yr_config_grid_backtest with the
#     supplied --archetype / --start / --end / --grid-density
#   - Writes per-config + summary parquets to
#     gs://strategy-store-central-element-323112/backtests/config_grid_2yr/{archetype}/{run_id}/
#   - Self-deletes on completion via shutdown-script
#
# Per-shippable-unit refresh: after editing this launcher OR
# strategy-service/scripts/run_2yr_config_grid_backtest.py, refresh
# tarballs:
#   bash deployment-service/scripts/vm/create-code-tarballs.sh \
#     --include strategy-service --include deployment-service \
#     --include unified-trading-library --include unified-api-contracts
#
# Watchdog: prefix `strategy-backtest-grid-` is registered in
# vm_zombie_watchdog.py VM_PREFIX_TO_BUCKET (heartbeat-only — strategy-service
# does not write per-VM manifest shards, so the watchdog falls back to the
# GCS heartbeat sidecar at gs://deployment-scripts-{pid}/vm-heartbeat/).
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$REPO_ROOT/.." && pwd)}"

# Defaults
ARCHETYPE=""
START_DATE=""
END_DATE=""
GRID_DENSITY="medium"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-4}"
DISK_SIZE="${DISK_SIZE:-100GB}"
PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
OUTPUT_BUCKET="strategy-store-${PROJECT_ID}"
DRY_RUN=false
FORCE=false
SMOKE=false
VM_NAME=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

usage() {
    cat <<EOF
Usage: $0 --archetype <name> --start YYYY-MM-DD --end YYYY-MM-DD [options]

Required:
  --archetype <name>      One of: carry_staked_basis | ARBITRAGE_PRICE_DISPERSION
  --start YYYY-MM-DD      Backtest start (inclusive).
  --end YYYY-MM-DD        Backtest end (inclusive).

Optional:
  --grid-density <d>      coarse | medium (default) | fine
  --output-bucket <b>     GCS bucket for backtest outputs (default: ${OUTPUT_BUCKET})
  --zone <zone>           GCE zone (default: ${ZONE})
  --machine-type <t>      GCE machine type (default: ${MACHINE_TYPE})
  --disk-size <s>         Boot disk size (default: ${DISK_SIZE})
  --project <pid>         GCP project (default: ${PROJECT_ID})
  --env <env>             Deployment env tier (prod|staging|dev; default: ${DEPLOYMENT_ENV})
  --vm-name <name>        Override generated VM name.
  --smoke                 Forwarded to runner — 5-config / 2-slot smoke run.
  --force                 Bypass singleton lock (allow same-archetype VM RUNNING).
  --dry-run               Print plan, do not call gcloud.
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --archetype)     ARCHETYPE="$2"; shift 2 ;;
        --start)         START_DATE="$2"; shift 2 ;;
        --end)           END_DATE="$2"; shift 2 ;;
        --grid-density)  GRID_DENSITY="$2"; shift 2 ;;
        --output-bucket) OUTPUT_BUCKET="$2"; shift 2 ;;
        --zone)          ZONE="$2"; shift 2 ;;
        --machine-type)  MACHINE_TYPE="$2"; shift 2 ;;
        --disk-size)     DISK_SIZE="$2"; shift 2 ;;
        --project)       PROJECT_ID="$2"; CODE_BUCKET="deployment-scripts-${PROJECT_ID}"; shift 2 ;;
        --env)           DEPLOYMENT_ENV="$2"; shift 2 ;;
        --vm-name)       VM_NAME="$2"; shift 2 ;;
        --smoke)         SMOKE=true; shift ;;
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

# Validate required args
if [[ -z "$ARCHETYPE" || -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "ERROR: --archetype, --start, and --end are required."
    usage
fi

case "$ARCHETYPE" in
    carry_staked_basis|ARBITRAGE_PRICE_DISPERSION) ;;
    *) echo "ERROR: --archetype must be carry_staked_basis or ARBITRAGE_PRICE_DISPERSION (got: $ARCHETYPE)"; exit 2 ;;
esac

# Slugify archetype for VM name (lowercase + dashes; truncate to fit 63-char GCE name limit).
ARCHETYPE_SLUG=$(echo "$ARCHETYPE" | tr '[:upper:]' '[:lower:]' | tr '_' '-')
RUN_TS="$(date +%Y%m%d-%H%M%S)"
if [[ -z "$VM_NAME" ]]; then
    # GCE name limit = 63 chars. Layout:
    #   "strategy-backtest-grid-" = 23 chars
    #   slug (≤24)                = 24 chars max
    #   "-YYYYMMDD-HHMMSS"         = 16 chars
    #   total                      = 63 chars max
    # ARBITRAGE_PRICE_DISPERSION → "arbitrage-price-dispersi" (24 chars).
    SLUG_TRUNC="${ARCHETYPE_SLUG:0:24}"
    # Strip trailing dash if truncation landed on one (cleaner names).
    SLUG_TRUNC="${SLUG_TRUNC%-}"
    VM_NAME="strategy-backtest-grid-${SLUG_TRUNC}-${RUN_TS}"
fi

log() { echo "$(date '+%H:%M:%S') $*"; }

log "=========================================="
log "Strategy Backtest Grid VM Launcher"
log "=========================================="
log "Archetype:    ${ARCHETYPE}"
log "Date range:   ${START_DATE} -> ${END_DATE}"
log "Grid density: ${GRID_DENSITY}"
log "Smoke:        ${SMOKE}"
log "VM name:      ${VM_NAME}"
log "Zone:         ${ZONE}"
log "Machine:      ${MACHINE_TYPE}"
log "Disk:         ${DISK_SIZE}"
log "Project:      ${PROJECT_ID}"
log "Env:          ${DEPLOYMENT_ENV}"
log "Output:       gs://${OUTPUT_BUCKET}/backtests/config_grid_2yr/${ARCHETYPE}/"
log "=========================================="

# ── Singleton lock per archetype ──
# Refuse to launch if another VM matching the same archetype slug is RUNNING
# in the zone (prevents double-spawn waste; --force overrides).
if ! $DRY_RUN; then
    EXISTING=$(gcloud compute instances list \
        --project="${PROJECT_ID}" \
        --filter="name~^strategy-backtest-grid-${ARCHETYPE_SLUG} AND zone:(${ZONE}) AND status=RUNNING" \
        --format="value(name)" 2>/dev/null || echo "")
    if [[ -n "$EXISTING" && "$FORCE" == "false" ]]; then
        log "ERROR: same-archetype VM already RUNNING:"
        log "  ${EXISTING}"
        log "Pass --force to bypass the singleton lock (will run two backtest VMs in parallel)."
        exit 3
    fi
    if [[ -n "$EXISTING" && "$FORCE" == "true" ]]; then
        log "WARNING: --force given; same-archetype VM is RUNNING but launching anyway:"
        log "  ${EXISTING}"
    fi
fi

# ── Shutdown script (self-delete on completion) ──
SHUTDOWN_SCRIPT=$(cat <<'SHUTDOWN_EOF'
#!/usr/bin/env bash
# Self-delete on shutdown. The runner script writes /tmp/backtest-done with
# its exit code; the heartbeat daemon emits STOPPED/FAILED before this fires.
ZONE=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/zone" 2>/dev/null | awk -F/ '{print $NF}')
VM_NAME=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/name" 2>/dev/null)
if [[ -n "$VM_NAME" && -n "$ZONE" ]]; then
    gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet 2>/dev/null || true
fi
SHUTDOWN_EOF
)

# ── Build metadata ──
SMOKE_FLAG=""
$SMOKE && SMOKE_FLAG=" --smoke"

# We re-use the `phantom-recon` / `mdps-backfill` style VM_BACKFILL_CMD
# routing — setup-data-pipeline-vm.sh already has a branch matching
# `strategy-backtest-grid` that cd's into $WORKSPACE/strategy and execs
# VM_BACKFILL_CMD via _launch_with_tee.
#
# IMPORTANT: invoke as `python scripts/run_2yr_config_grid_backtest.py`
# (script path), NOT as `python -m strategy_service.scripts.run_...` —
# `scripts/` lives at the repo root and is NOT part of the
# `strategy_service` package (no __init__.py there). The 2026-05-10 first
# launch failed with `ModuleNotFoundError: No module named
# 'strategy_service.scripts'` because of this confusion.
RUNNER_CMD="python scripts/run_2yr_config_grid_backtest.py \
--archetype ${ARCHETYPE} \
--start ${START_DATE} \
--end ${END_DATE} \
--grid-density ${GRID_DENSITY}${SMOKE_FLAG}"

METADATA_ITEMS=(
    "VM_TASK=strategy-backtest-grid"
    "VM_SERVICE=strategy_service"
    "VM_OPERATION=backtest"
    "VM_PIPELINE_MODE=batch"
    "VM_ASSET_GROUP=DEFI"
    "VM_ARCHETYPE=${ARCHETYPE}"
    "VM_START_DATE=${START_DATE}"
    "VM_END_DATE=${END_DATE}"
    "VM_GRID_DENSITY=${GRID_DENSITY}"
    "VM_SHUTDOWN_ON_COMPLETION=true"
    "VM_BACKFILL_CMD=${RUNNER_CMD}"
    "OUTPUT_BUCKET=${OUTPUT_BUCKET}"
    "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    "startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
)

METADATA_STR=$(IFS=','; echo "${METADATA_ITEMS[*]}")

# ── Launch ──
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
    --labels=purpose=strategy-backtest-grid,archetype="${ARCHETYPE_SLUG}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

log ""
log "VM created: ${VM_NAME}"
log ""
log "Verify event stream (90s after launch):"
log "  gcloud storage ls gs://${PROJECT_ID}-events/events/strategy-service/\$(date +%Y-%m-%d)/${VM_NAME}/"
log ""
log "Tail GCS log (live):"
log "  gcloud storage cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
log ""
log "Backtest output (on completion):"
log "  gs://${OUTPUT_BUCKET}/backtests/config_grid_2yr/${ARCHETYPE}/"
log ""
log "VM self-deletes on completion (shutdown-script)."
log "=========================================="
