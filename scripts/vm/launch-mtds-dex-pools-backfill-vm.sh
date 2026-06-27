#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch GCE VM for DEX pools backfill (Uniswap V3 / SushiSwap / Curve via The Graph)
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first (CORE tarballs include MTDS).
#
# Usage:
#   bash launch-mtds-dex-pools-backfill-vm.sh
#   bash launch-mtds-dex-pools-backfill-vm.sh --start 2024-01-01 --end 2026-01-01
#   bash launch-mtds-dex-pools-backfill-vm.sh --dry-run
#   bash launch-mtds-dex-pools-backfill-vm.sh --env staging
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
START_DATE="${START_DATE:-2023-01-01}"
END_DATE="${END_DATE:-$(date +%Y-%m-%d)}"
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false
# TheGraph key-pool sharding (Part 4): SHARD_INDEX selects this VM's starting key
# (key_number = SHARD_INDEX % pool_size + 1) so a multi-VM DeFi subgraph fan-out
# spreads load across the 9-key thegraph-api-key[-2..9] SM pool — each VM begins
# on a distinct key, and the handler round-robins the full pool per request.
SHARD_INDEX="${SHARD_INDEX:-0}"
FLEET_VMS="${FLEET_VMS:-1}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)     DRY_RUN=true; shift ;;
    --project)     PROJECT_ID="$2"; shift 2 ;;
    --zone)        ZONE="$2"; shift 2 ;;
    --start)       START_DATE="$2"; shift 2 ;;
    --end)         END_DATE="$2"; shift 2 ;;
    --force)       FORCE=true; shift ;;
    --env)         DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    --preemptible) shift ;;  # deprecated no-op: SPOT is now the default
    --shard-index) SHARD_INDEX="$2"; shift 2 ;;
    --fleet-vms)   FLEET_VMS="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
VM_NAME="${VM_NAME:-mtds-dex-pools-backfill}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

echo "============================================================"
echo "DEX Pools Backfill VM Launcher (Pattern A)"
echo "  VM:        ${VM_NAME}"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Env:       ${DEPLOYMENT_ENV}"
echo "  Tarball:   gs://${CODE_BUCKET}/code/mtds-code.tar.gz"

# ── TheGraph key-pool capacity model (Part 4) ──
# Resolve the pooled ceiling (per_key_rpm × pool_size) from the registry SSOT so
# the launch sizes its fan-out against the 9-key pool rather than a single key.
# SHARD_INDEX picks this VM's starting key (handler round-robins the rest).
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PY="${REPO_ROOT}/.venv/bin/python"
[[ -x "$PY" ]] || PY="python3"
POOL_LINE="$(
  PYTHONPATH="${REPO_ROOT}" "$PY" - <<'PYEOF' 2>/dev/null || true
from deployment_service.data_pipeline_monitors.launch_budget_registry import key_pool_capacity_for_source
k = key_pool_capacity_for_source("thegraph")
if k is not None:
    print(f"{k.per_key_rpm} {k.pool_size} {k.effective_rpm}")
PYEOF
)"
THEGRAPH_POOL_SIZE=9
if [[ -n "$POOL_LINE" ]]; then
  read -r _PER_KEY_RPM THEGRAPH_POOL_SIZE _EFFECTIVE_RPM <<<"$POOL_LINE"
  echo "  Key-pool:  TheGraph ${THEGRAPH_POOL_SIZE}-key pool, per-key ${_PER_KEY_RPM} req/min → effective ${_EFFECTIVE_RPM} req/min; this VM starts on key $(( (SHARD_INDEX % THEGRAPH_POOL_SIZE) + 1 )) (SHARD_INDEX=${SHARD_INDEX}, fleet=${FLEET_VMS})"
else
  echo "  Key-pool:  TheGraph registry unavailable — handler round-robins the 9-key pool; SHARD_INDEX=${SHARD_INDEX} start key $(( (SHARD_INDEX % 9) + 1 ))" >&2
fi
echo "============================================================"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^mtds-dex-pools-\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    echo "WARN: DEX pools VM already running: ${EXISTING}" >&2
    echo "      Use --force to bypass. Aborting." >&2
    exit 1
  fi
fi

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=defi-backfill  VM_OPERATION=collect-dex-pools"
  exit 0
fi

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=defi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=collect-dex-pools"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
# Part 4: SHARD_INDEX selects this VM's starting TheGraph key (key_number =
# SHARD_INDEX % 9 + 1); the handler round-robins the full 9-key pool per request.
METADATA="${METADATA},SHARD_INDEX=${SHARD_INDEX}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"

# SPOT by default (--no-restart-on-failure already passed below); --on-demand clears it.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Creating VM ${VM_NAME} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]..."
# shellcheck disable=SC2086
gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  ${PROVISIONING_FLAGS} \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --labels="purpose=mtds-dex-pools-backfill,env=${DEPLOYMENT_ENV}" \
  --metadata="${METADATA}"

echo ""
echo "  VM created: ${VM_NAME}"
echo "  T+10 check: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "  Logs:       gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
