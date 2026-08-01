#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run verified + GCS orphan-sweep=0
# Launch a one-time OHLCV backfill VM for EXTENDED-STARKNET.
#
# Coverage window: 2024-07-26 → 2025-07-31
# Data types:     trades + book_snapshot_5  (OHLCV only)
# Instruments:    BTC, ETH, SOL
#
# WHY trades/book_snapshot_5 only (no perp_funding):
#   Extended-Starknet funding data is available from 2025-08-01 onward.
#   The forward-poll launcher (launch-cefi-onchain-forward-poll.sh) already
#   captures funding from 2025-08-01+. Pre-2025-08-01 funding dates are emitted
#   by MTDS as record_empty(EXPECTED_PRE_VENUE_LAUNCH) — no backfill needed.
#
# VM naming:   cefi-ext-bfill-{YYYYMMDD-HHMMSS}
# Watchdog:    registered under "cefi-ext-bfill-" in vm_zombie_watchdog.py
# Lifecycle:   EPHEMERAL_BATCH — terminates on completion
# Singleton:   one backfill VM at a time (same prefix lock); pass --force to bypass
#
# Invocation inside VM (assembled by setup-data-pipeline-vm.sh from metadata):
#   python -m market_tick_data_service \
#     --operation download --mode batch --asset-group CEFI \
#     --venues EXTENDED-STARKNET \
#     --start-date 2024-07-26 --end-date 2025-07-31 \
#     --data-types trades book_snapshot_5 \
#     --force-window
#
# Usage:
#   bash launch-mtds-extended-ohlcv-backfill.sh                # prod, fixed window
#   bash launch-mtds-extended-ohlcv-backfill.sh --force        # bypass singleton lock
#   bash launch-mtds-extended-ohlcv-backfill.sh --env staging  # staging env tier
#
# Operator go-ahead required before running:
#   BLOCKED-OPERATOR-DECISION — window + VM cost approved in
#   dex_perp_and_venue_data_expansion_2026_05_12.md § 2F.
#   Do NOT run without operator ack on that plan item.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false

DRY_RUN=false
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    *) echo "ERROR: Unknown argument '$1'" >&2; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
RUN_TS="$(date +%Y%m%d-%H%M%S)"

VM_PREFIX="cefi-ext-bfill-"
START_DATE="2024-07-26"
END_DATE="2025-07-31"
VENUE="EXTENDED-STARKNET"
DATA_TYPES="trades;book_snapshot_5"
INSTRUMENTS="BTC;ETH;SOL"

# Singleton lock — one backfill VM per prefix at a time.
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^${VM_PREFIX}\" AND status=RUNNING" \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    echo "SKIP — backfill VM already running: $EXISTING" >&2
    echo "Pass --force to bypass the singleton lock or wait for it to finish." >&2
    exit 1
  fi
fi

VM_NAME="${VM_PREFIX}${RUN_TS}"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Launching $VM_NAME [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
echo "  Venue:      $VENUE"
echo "  Window:     $START_DATE → $END_DATE"
echo "  Data types: $DATA_TYPES"
echo "  Env:        $DEPLOYMENT_ENV"

METADATA="VM_TASK=cefi-extended-ohlcv-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=download"
METADATA="${METADATA},VM_ASSET_GROUP=CEFI"
METADATA="${METADATA},VM_VENUE=${VENUE}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_DATA_TYPES=${DATA_TYPES}"
METADATA="${METADATA},VM_INSTRUMENT_IDS=${INSTRUMENTS}"
METADATA="${METADATA},VM_FORCE_WINDOW=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  # shellcheck disable=SC2086
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
    --zone="$ZONE" \
    --machine-type=e2-standard-4 \
    ${PROVISIONING_FLAGS} \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=cefi-extended-ohlcv-backfill,venue="${VENUE,,}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service
fi

echo ""
echo "Launched: $VM_NAME"
echo "  Log:    gcloud compute ssh $VM_NAME --zone=$ZONE --command 'sudo tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "  GCS:    gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "  Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "T+10min check (post-launch verification per CLAUDE.md):"
echo "  gcloud compute instances describe $VM_NAME --zone=$ZONE --format='value(status)'"
