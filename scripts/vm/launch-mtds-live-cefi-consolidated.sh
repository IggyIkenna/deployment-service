#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
#
# Launch ONE consolidated GCE VM that runs ALL MVP CeFi live-capture shards as
# separate background processes on a single host.
#
# MOTIVATION (2026-06-27): The original design (16 × e2-standard-8 VMs, one per
# venue×data_type shard) cost ~$103/day. MVP instrument scoping (fewer order-book
# symbols per shard) dramatically reduces per-process RSS, making it plausible to
# co-locate all MVP CeFi shards on one generously-sized VM.
#
# MVP CeFi shards launched (per is_in_mvp_capture_universe SSOT):
#   Perps/spot with-perp → trades + book_snapshot_5:
#     BINANCE-FUTURES: trades, book_snapshot_5
#     BYBIT-FUTURES:   trades, book_snapshot_5, derivative_ticker
#     HYPERLIQUID:     trades, book_snapshot_5, derivative_ticker
#     KRAKEN-FUTURES:  trades, book_snapshot_5, derivative_ticker
#     OKX-FUTURES:     trades, book_snapshot_5, derivative_ticker
#   Deribit BTC/ETH options → derivative_ticker ONLY (options_chain WS not yet wired):
#     DERIBIT:         derivative_ticker
#   ASTER → book_snapshot_5 + liquidations ONLY, both LIVE-ONLY data_types (added
#   2026-07-28 per operator ruling folding the ASTER live-connector ask into this
#   consolidation instead of a standalone VM; trades stays batch-captured):
#     ASTER:           book_snapshot_5, liquidations
#
# Note: Deribit trades + book_snapshot_5 are NOT in the MVP CeFi live scope (MVP spec:
# Deribit = options_chain only; options_chain live WS is a future Phase 3.5 item).
# BINANCE-DELIVERY is explicitly excluded per MVP spec.
#
# Startup: uses setup-cefi-live-consolidated-vm.sh (uploaded to GCS alongside this
# script) which launches N websocket-streaming processes in the background and
# monitors them. The VM name starts with "mtds-live-cefi-" so the zombie-watchdog
# + launcher-registry already cover it.
#
# Singleton lock: refuses to launch if any "mtds-live-cefi-consolidated-*" VM is
# already RUNNING. Other per-shard "mtds-live-cefi-*" VMs are NOT blocked —
# operator must manually stop those first (or use --force).
#
# Machine type: e2-highmem-16 (16 vCPU, 128 GB RAM) — generous enough to ceiling-test
# all MVP shards in-process. If RSS stays well under 96 GB after 15 min, shrink to
# n2-highmem-8. Cost: e2-highmem-16 ≈ $0.71/hr = ~$17/day on-demand.
#
# Zone: default asia-northeast1-c. On a ZONE_RESOURCE_POOL_EXHAUSTED stockout, retry
# with --zone asia-northeast1-b (or -a) — same-region fallback ONLY, cross-region is
# forbidden (all GCS data lives in asia-northeast1). SSOT: codex/05-infrastructure/
# strategy-shard-vm-topology.md § Zone.
#
# Monitoring:
#   gcloud compute ssh <VM_NAME> --zone=asia-northeast1-c --command 'ps aux | grep market_tick'
#   gcloud compute ssh <VM_NAME> --zone=asia-northeast1-c --command 'cat /home/ikennaigboaka/logs/live-*.log | tail -50'
#   gcloud compute ssh <VM_NAME> --zone=asia-northeast1-c --command "free -h && ps aux --sort=-%mem | head -20"
#
# SSOT: codex/05-infrastructure/live-pipeline-architecture.md
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false
MACHINE_TYPE="${MACHINE_TYPE:-e2-highmem-16}"
# Same-region stockout fallback ONLY (asia-northeast1-b/-a) — cross-region is FORBIDDEN,
# all GCS data lives in asia-northeast1 (codex/05-infrastructure/strategy-shard-vm-topology.md
# § Zone). Default stays -c; override only when GCE returns ZONE_RESOURCE_POOL_EXHAUSTED.
ZONE_OVERRIDE="${ZONE_OVERRIDE:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --zone) ZONE_OVERRIDE="$2"; shift 2 ;;
    *) echo "ERROR: unknown flag '$1'" >&2; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
if [[ -n "$ZONE_OVERRIDE" ]]; then
  case "$ZONE_OVERRIDE" in
    asia-northeast1-*) ZONE="$ZONE_OVERRIDE" ;;
    *) echo "ERROR: --zone must stay in asia-northeast1 (same-region stockout fallback only — all GCS data lives in asia-northeast1; got: $ZONE_OVERRIDE)" >&2; exit 1 ;;
  esac
fi
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
VM_PREFIX="mtds-live-cefi-consolidated"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^${VM_PREFIX}-\" AND status=RUNNING" \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: Consolidated CeFi live VM already running: $EXISTING
Refusing to launch a duplicate.

Options:
  Inspect:  gcloud compute ssh $EXISTING --zone=$ZONE
  Force:    FORCE=true bash $0 --env ${DEPLOYMENT_ENV}

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If confirmed stale:
  gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="${VM_PREFIX}-${RUN_TS}"

echo "Uploading startup script to GCS..."
STARTUP_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STARTUP_SCRIPT="${STARTUP_SCRIPT_DIR}/setup-cefi-live-consolidated-vm.sh"
if [[ ! -f "$STARTUP_SCRIPT" ]]; then
  echo "ERROR: startup script not found: $STARTUP_SCRIPT" >&2
  exit 1
fi
# `gcloud storage`, not `gsutil` — gsutil resolves creds from the CLI's active
# account (a short-lived WIF token in an interactive AO slot can't refresh
# unattended), while `gcloud storage` resolves via ADC, which stays valid. See
# plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.
gcloud storage cp "$STARTUP_SCRIPT" "gs://${CODE_BUCKET}/vm/setup-cefi-live-consolidated-vm.sh" --quiet
echo "Startup script uploaded to gs://${CODE_BUCKET}/vm/setup-cefi-live-consolidated-vm.sh"

echo "Launching $VM_NAME: consolidated CeFi live capture (${MACHINE_TYPE})"

METADATA="VM_TASK=mtds-live"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=live_websocket"
METADATA="${METADATA},VM_MODE=live"
METADATA="${METADATA},VM_ASSET_GROUP=CEFI"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"

if $DRY_RUN; then
  echo "[DRY-RUN] Would create VM: $VM_NAME (${MACHINE_TYPE})"
  echo "[DRY-RUN] startup-script-url=gs://${CODE_BUCKET}/vm/setup-cefi-live-consolidated-vm.sh"
  echo "[DRY-RUN] metadata=${METADATA}"
  exit 0
fi

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type="${MACHINE_TYPE}" \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-cefi-live-consolidated-vm.sh,${METADATA}" \
  --labels=purpose=mtds-live-consolidated,asset-group=cefi,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

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
echo "  e2-highmem-16 (128 GB, 16 vCPU): ~\$0.71/hr = ~\$17/day on-demand"
echo "  vs old 16 x e2-standard-8:       ~\$103/day"
echo "  Savings:                          ~\$86/day (83%)"
