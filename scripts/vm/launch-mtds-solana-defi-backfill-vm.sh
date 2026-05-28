#!/usr/bin/env bash
# Launch GCE VM for Solana DeFi multi-protocol backfill (collect-solana-defi op).
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Routes through VM_TASK=solana-defi-backfill in setup-data-pipeline-vm.sh.
#
# Covers the Solana DeFi protocols routed through SolanaDefiHandler that are
# NOT already covered by dedicated launchers:
#   - MARGINFI    -> lending_indices (DeFiLlama protocol-level TVL)
#   - SOLEND/SAVE -> lending_indices (DeFiLlama yields + chart replay)
#   - KAMINO      -> dex_pools       (Kamino strategies API)
#   - KAMINO_LEND -> lending_indices (DeFiLlama kamino-lend yields)
#   - RAYDIUM     -> dex_pools       (Raydium v3 pools)
#   - ORCA        -> dex_pools       (Orca Whirlpool list)
#   - PHOENIX     -> dex_pools       (Jupiter quote routing — CLOB DEX)
#   - JITO        -> lst_rates       (Jito stake pool stats)
#
# Excluded (dedicated launchers already cover):
#   - DRIFT       -> launch-mtds-solana-drift-backfill-vm.sh (Drift S3)
#   - MARINADE    -> launch-marinade-solana-backfill-vm.sh   (mSOL APY)
#
# All listed protocols use public APIs (DeFiLlama, Kamino, Raydium, Orca,
# Phoenix-via-Jupiter, Jito Stakenet) — no Helius / Alchemy credentials needed.
# --solana-lending-backfill is set so MARGINFI/SOLEND backfill historically
# rather than reading today's snapshot for every date in the range.
#
# Usage:
#   bash launch-mtds-solana-defi-backfill-vm.sh
#   bash launch-mtds-solana-defi-backfill-vm.sh --start 2025-01-17 --end 2026-05-28
#   bash launch-mtds-solana-defi-backfill-vm.sh --dry-run
#   bash launch-mtds-solana-defi-backfill-vm.sh --env staging
#   bash launch-mtds-solana-defi-backfill-vm.sh --protocols "marginfi solend"
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
START_DATE="${START_DATE:-$(date -v-180d +%Y-%m-%d 2>/dev/null || date -d '180 days ago' +%Y-%m-%d)}"
END_DATE="${END_DATE:-$(date +%Y-%m-%d)}"
# Default protocol set excludes drift+marinade (dedicated launchers).
PROTOCOLS="${PROTOCOLS:-marginfi solend kamino kamino_lending raydium orca phoenix jito}"
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)    DRY_RUN=true; shift ;;
    --project)    PROJECT_ID="$2"; shift 2 ;;
    --zone)       ZONE="$2"; shift 2 ;;
    --start)      START_DATE="$2"; shift 2 ;;
    --end)        END_DATE="$2"; shift 2 ;;
    --protocols)  PROTOCOLS="$2"; shift 2 ;;
    --force)      FORCE=true; shift ;;
    --env)        DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
VM_NAME="mtds-solana-defi-backfill"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

# Metadata transport: VM_SOLANA_PROTOCOLS uses ';' (not ',') because gcloud
# uses ',' as the key separator in --metadata.  setup-data-pipeline-vm.sh
# translates ';' back to spaces before passing to --solana-protocols.
PROTOCOLS_META="$(echo "$PROTOCOLS" | tr ' ' ';')"

echo "============================================================"
echo "Solana DeFi Multi-Protocol Backfill VM Launcher (Pattern A)"
echo "  VM:        ${VM_NAME}"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Protocols: ${PROTOCOLS}"
echo "  Env:       ${DEPLOYMENT_ENV}"
echo "  Tarball:   gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name=\"${VM_NAME}\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    echo "WARN: Solana DeFi backfill VM already running: ${EXISTING}" >&2
    echo "      Use --force to bypass. Aborting." >&2
    exit 1
  fi
fi

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=solana-defi-backfill  VM_SOLANA_PROTOCOLS=${PROTOCOLS_META}"
  exit 0
fi

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=solana-defi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=collect-solana-defi"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_SOLANA_PROTOCOLS=${PROTOCOLS_META}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"

echo "Creating VM ${VM_NAME}..."
gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --labels="purpose=mtds-solana-defi-backfill,env=${DEPLOYMENT_ENV}" \
  --metadata="${METADATA}"

echo ""
echo "  VM created: ${VM_NAME}"
echo "  T+10 check: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "  Logs:       gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
