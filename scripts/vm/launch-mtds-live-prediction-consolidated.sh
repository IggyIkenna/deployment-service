#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
#
# Launch ONE consolidated GCE VM that runs ALL prediction live-capture shards as
# separate background processes on a single host.
#
# MOTIVATION (2026-06-28): 4 separate VMs (POLYMARKET×trades, POLYMARKET×book_snapshot_5,
# KALSHI×trades, KALSHI×book_snapshot_5) cost ~$16/day. All 4 shards fit comfortably on
# one e2-highmem-4 (4 vCPU, 32 GB RAM), reducing cost to ~$3/day on-demand.
#
# Prediction shards launched:
#   POLYMARKET: trades, book_snapshot_5
#   KALSHI:     trades, book_snapshot_5
#
# Singleton lock: refuses to launch if any "mtds-live-prediction-consolidated-*" VM is
# already RUNNING. The operator must stop existing prediction VMs before launching.
# Use --force to bypass the singleton guard (e.g. replacing a stale VM).
#
# Machine type: e2-highmem-4 (4 vCPU, 32 GB RAM) — 4 prediction shards are lightweight
# websocket connections, each under ~2 GB RSS. If RSS exceeds 24 GB after 15 min,
# upgrade to e2-highmem-8.
#
# Monitoring:
#   gcloud compute ssh <VM_NAME> --zone=asia-northeast1-c --command 'ps aux | grep market_tick'
#   gcloud compute ssh <VM_NAME> --zone=asia-northeast1-c --command 'cat /home/ikennaigboaka/logs/live-*.log | tail -50'
#   gcloud compute ssh <VM_NAME> --zone=asia-northeast1-c --command "free -h && ps aux --sort=-%mem | head -20"
#
# SSOT: codex/05-infrastructure/live-pipeline-architecture.md
set -euo pipefail

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false
MACHINE_TYPE="${MACHINE_TYPE:-e2-highmem-4}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    *) echo "ERROR: unknown flag '$1'" >&2; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
VM_PREFIX="mtds-live-prediction-consolidated"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^${VM_PREFIX}-\" AND status=RUNNING" \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: Consolidated prediction live VM already running: $EXISTING
Refusing to launch a duplicate.

Options:
  Inspect:  gcloud compute ssh $EXISTING --zone=$ZONE
  Stop:     gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:    FORCE=true bash $0 --env ${DEPLOYMENT_ENV}
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="${VM_PREFIX}-${RUN_TS}"

echo "Uploading startup script to GCS..."
STARTUP_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STARTUP_SCRIPT="${STARTUP_SCRIPT_DIR}/setup-prediction-live-consolidated-vm.sh"
if [[ ! -f "$STARTUP_SCRIPT" ]]; then
  echo "ERROR: startup script not found: $STARTUP_SCRIPT" >&2
  exit 1
fi
gsutil -q cp "$STARTUP_SCRIPT" "gs://${CODE_BUCKET}/vm/setup-prediction-live-consolidated-vm.sh"
echo "Startup script uploaded to gs://${CODE_BUCKET}/vm/setup-prediction-live-consolidated-vm.sh"

echo "Launching $VM_NAME: consolidated prediction live capture (${MACHINE_TYPE})"

METADATA="VM_TASK=mtds-live"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=live_websocket"
METADATA="${METADATA},VM_MODE=live"
METADATA="${METADATA},VM_ASSET_GROUP=PREDICTION"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"

if $DRY_RUN; then
  echo "[DRY-RUN] Would create VM: $VM_NAME (${MACHINE_TYPE})"
  echo "[DRY-RUN] startup-script-url=gs://${CODE_BUCKET}/vm/setup-prediction-live-consolidated-vm.sh"
  echo "[DRY-RUN] metadata=${METADATA}"
  exit 0
fi

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type="${MACHINE_TYPE}" \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --service-account="unified-trading-sa@${PROJECT}.iam.gserviceaccount.com" \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-prediction-live-consolidated-vm.sh,${METADATA}" \
  --labels=purpose=mtds-live-consolidated,asset-group=prediction,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Machine type: ${MACHINE_TYPE}"
echo ""
echo "Monitor all shards (after ~5min boot):"
echo "  gcloud compute ssh $VM_NAME --zone=$ZONE --command 'ps aux | grep websocket'"
echo "  gcloud compute ssh $VM_NAME --zone=$ZONE --command 'ls -la /home/ikennaigboaka/logs/live-*.log'"
echo "  gcloud compute ssh $VM_NAME --zone=$ZONE --command 'free -h && ps aux --sort=-%mem | head -25'"
echo ""
echo "GCS log tee: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete:       gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Cost estimate:"
echo "  e2-highmem-4 (32 GB, 4 vCPU): ~\$0.13/hr = ~\$3/day on-demand"
echo "  vs old 4 x e2-standard-2:     ~\$16/day"
echo "  Savings:                       ~\$13/day (81%)"
