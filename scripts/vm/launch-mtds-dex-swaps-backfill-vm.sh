#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch GCE VM for DEX swaps backfill (Uniswap V3 / Balancer / Curve / GMX / etc. via The Graph)
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Writes to market-data-tick-defi-prd-{project_id} (canonical env-tiered DEFI tick bucket) using
# resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi").
#
# Usage:
#   bash launch-mtds-dex-swaps-backfill-vm.sh
#   bash launch-mtds-dex-swaps-backfill-vm.sh --start 2026-01-25 --end 2026-05-23
#   bash launch-mtds-dex-swaps-backfill-vm.sh --dry-run
#   bash launch-mtds-dex-swaps-backfill-vm.sh --env staging
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
START_DATE="${START_DATE:-2023-01-01}"
END_DATE="${END_DATE:-$(date +%Y-%m-%d)}"
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
MTDS_TARBALL_SHA="${MTDS_TARBALL_SHA:-}"
UTL_TARBALL_SHA="${UTL_TARBALL_SHA:-}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false
# TheGraph key-pool sharding (Part 4): SHARD_INDEX selects this VM's starting key
# (key_number = SHARD_INDEX % pool_size + 1) so a multi-VM DeFi subgraph fan-out
# spreads load across the 9-key thegraph-api-key[-2..9] SM pool — each VM begins
# on a distinct key, and the handler round-robins the full pool per request.
SHARD_INDEX="${SHARD_INDEX:-250}"
FLEET_VMS="${FLEET_VMS:-250}"
# --protocols: comma-separated allowlist for dex_swaps_handler.py's
# --dex-swaps-protocols (nargs='+'), mirrors launch-mtds-dex-pools-backfill-vm.sh
# / launch-mtds-lending-indices-backfill-vm.sh's VM_*_PROTOCOLS passthrough.
PROTOCOLS=""
# Diagnostic override: keep the VM alive post-crash for SSH/dmesg inspection
# instead of self-deleting (mtds_backfill_vm_startup_oom_rc137_2026_07_14 issue —
# every prior rc=137 crash self-deleted before anyone could attach). Default
# true (self-shutdown) preserves normal backfill behavior for every other caller.
VM_SHUTDOWN_ON_COMPLETION="${VM_SHUTDOWN_ON_COMPLETION:-true}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)        DRY_RUN=true; shift ;;
    --project)        PROJECT_ID="$2"; shift 2 ;;
    --zone)           ZONE="$2"; shift 2 ;;
    --start)          START_DATE="$2"; shift 2 ;;
    --end)            END_DATE="$2"; shift 2 ;;
    --force)          FORCE=true; shift ;;
    --env)            DEPLOYMENT_ENV="$2"; shift 2 ;;
    --mtds-sha)       MTDS_TARBALL_SHA="$2"; shift 2 ;;
    --utl-sha)        UTL_TARBALL_SHA="$2"; shift 2 ;;
    --on-demand)      ON_DEMAND=true; shift ;;
    --preemptible)    shift ;;  # deprecated no-op: SPOT is now the default
    --shard-index)    SHARD_INDEX="$2"; shift 2 ;;
    --fleet-vms)      FLEET_VMS="$2"; shift 2 ;;
    --protocols)      PROTOCOLS="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
VM_NAME="${VM_NAME:-mtds-dex-swaps-backfill}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

echo "============================================================"
echo "DEX Swaps Backfill VM Launcher (Pattern A)"
echo "  VM:        ${VM_NAME}"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Env:       ${DEPLOYMENT_ENV}"
echo "  Tarball:   gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "  Bucket:    market-data-tick-defi-prd-${PROJECT_ID}  (resolved by resolve_bucket_name)"

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
    --filter="name~\"^mtds-dex-swaps-\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    echo "WARN: DEX swaps VM already running: ${EXISTING}" >&2
    echo "      Use --force to bypass. Aborting." >&2
    exit 1
  fi
fi

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=defi-backfill  VM_OPERATION=collect-dex-swaps"
  exit 0
fi

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=defi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=collect-dex-swaps"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=${VM_SHUTDOWN_ON_COMPLETION}"
# Part 4: SHARD_INDEX selects this VM's starting TheGraph key (key_number =
# SHARD_INDEX % 9 + 1); the handler round-robins the full 9-key pool per request.
METADATA="${METADATA},SHARD_INDEX=${SHARD_INDEX}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
if [[ -n "$PROTOCOLS" ]]; then
  METADATA="${METADATA},VM_DEX_SWAPS_PROTOCOLS=${PROTOCOLS}"
fi
if [[ -n "${MTDS_TARBALL_SHA}" ]]; then
  METADATA="${METADATA},MTDS_TARBALL_SHA=${MTDS_TARBALL_SHA}"
fi
if [[ -n "${UTL_TARBALL_SHA}" ]]; then
  METADATA="${METADATA},UTL_TARBALL_SHA=${UTL_TARBALL_SHA}"
fi

# Durable pin registry — instance metadata (above) covers this VM only while it
# RUNS. The record below is what the retention sweep and a preemption relaunch
# read once the instance is gone, which is exactly the state the 2026-07-20
# incident left every VM in.
lc_write_tarball_pin_record "$VM_NAME" "$PROJECT_ID" "launch-mtds-dex-swaps-backfill-vm.sh" \
  "MTDS_TARBALL_SHA=${MTDS_TARBALL_SHA}" \
  "UTL_TARBALL_SHA=${UTL_TARBALL_SHA}"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Creating VM ${VM_NAME} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]..."
# shellcheck disable=SC2086
if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  ${PROVISIONING_FLAGS} \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
  --labels="purpose=mtds-dex-swaps-backfill,env=${DEPLOYMENT_ENV}" \
  --metadata="${METADATA}"

echo ""
echo "  VM created: ${VM_NAME}"
echo "  T+10 check: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "  Logs:       gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
