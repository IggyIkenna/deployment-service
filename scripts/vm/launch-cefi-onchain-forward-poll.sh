#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch forward-poll VMs for on-chain CeFi perp venues.
#
# Covers: LIGHTER-ZKSYNC + EXTENDED-STARKNET + HYPERLIQUID (+ ASTER for parity).
# Each venue gets its own singleton-locked VM so one stalled venue never blocks the others.
#
# Purpose: ingest perp_funding + trades + book_snapshot_5 for on-chain perp venues.
# Writes to:
#   gs://market-data-tick-cefi-central-element-323112/raw_tick_data/by_date/
#     day={D}/asset_group=cefi/venue={VENUE}/instrument_type=perpetual/
#       data_type={perp_funding,trades,book_snapshot_5}/{symbol}.parquet
#
# Runs the unified MTDS CLI — same code path as historical backfills.
#
# Invocation inside the VM (assembled by setup-data-pipeline-vm.sh from metadata):
#   python -m market_tick_data_service \
#     --operation download --mode batch --asset-group CEFI \
#     --venues <VENUE> \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE \
#     --data-types perp_funding trades book_snapshot_5 \
#     --force-window
#
# Usage:
#   bash launch-cefi-onchain-forward-poll.sh                         # all venues, yesterday
#   bash launch-cefi-onchain-forward-poll.sh --venue LIGHTER-ZKSYNC  # single venue, yesterday
#   bash launch-cefi-onchain-forward-poll.sh 2026-05-18 2026-05-19   # explicit date window
#   bash launch-cefi-onchain-forward-poll.sh --force                 # bypass all singleton locks
#   bash launch-cefi-onchain-forward-poll.sh --env staging           # staging env tier
#
# Singleton lock: per-venue, refuses to launch if a same-prefix VM is already RUNNING.
# Pass --force to bypass (e.g. for legitimate parallel investigations or backfill overlap).
#
# VM prefix → watchdog registration mapping (all in vm_zombie_watchdog.py VM_PREFIX_TO_BUCKET):
#   LIGHTER-ZKSYNC     → cefi-lighter-
#   EXTENDED-STARKNET  → cefi-extended-
#   HYPERLIQUID        → cefi-hyperliquid-
#   ASTER              → aster-fwd-  (also has dedicated launch-aster-forward-poll.sh)
#
# Cost: e2-standard-2 for ~5-15 min per venue per run.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
SINGLE_VENUE=""

_positional=()
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --venue) SINGLE_VENUE="$2"; shift 2 ;;
    *) _positional+=("$1"); shift ;;
  esac
done
set -- "${_positional[@]+"${_positional[@]}"}"

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ $# -eq 2 ]]; then
  START_DATE="$1"
  END_DATE="$2"
else
  START_DATE="$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)"
  END_DATE="$START_DATE"
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
RUN_TS="$(date +%Y%m%d-%H%M%S)"

# venue → (vm_prefix, data_types_semicolon_separated, instrument_ids_semicolon_separated)
declare -A VENUE_PREFIX
declare -A VENUE_DATA_TYPES
declare -A VENUE_INSTRUMENTS
VENUE_PREFIX["LIGHTER-ZKSYNC"]="cefi-lighter-"
VENUE_DATA_TYPES["LIGHTER-ZKSYNC"]="perp_funding;trades;book_snapshot_5"
VENUE_INSTRUMENTS["LIGHTER-ZKSYNC"]="BTC;ETH;SOL;HYPE;TON"

VENUE_PREFIX["EXTENDED-STARKNET"]="cefi-extended-"
VENUE_DATA_TYPES["EXTENDED-STARKNET"]="perp_funding;trades;book_snapshot_5"
VENUE_INSTRUMENTS["EXTENDED-STARKNET"]="BTC;ETH;SOL"

VENUE_PREFIX["HYPERLIQUID"]="cefi-hyperliquid-"
VENUE_DATA_TYPES["HYPERLIQUID"]="perp_funding;trades;book_snapshot_5;derivative_ticker"
VENUE_INSTRUMENTS["HYPERLIQUID"]="BTC;ETH;SOL"

VENUE_PREFIX["ASTER"]="aster-fwd-"
VENUE_DATA_TYPES["ASTER"]="trades;derivative_ticker"
VENUE_INSTRUMENTS["ASTER"]="BTC;ETH"

ALL_VENUES=("LIGHTER-ZKSYNC" "EXTENDED-STARKNET" "HYPERLIQUID" "ASTER")

if [[ -n "$SINGLE_VENUE" ]]; then
  if [[ -z "${VENUE_PREFIX[$SINGLE_VENUE]:-}" ]]; then
    echo "ERROR: Unknown venue '$SINGLE_VENUE'. Valid: ${ALL_VENUES[*]}" >&2
    exit 1
  fi
  VENUES=("$SINGLE_VENUE")
else
  VENUES=("${ALL_VENUES[@]}")
fi

launched=()
skipped=()

for VENUE in "${VENUES[@]}"; do
  PREFIX="${VENUE_PREFIX[$VENUE]}"

  # ── Per-venue singleton lock ─────────────────────────────────────────────
  if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
      --filter="name~\"^${PREFIX}\" AND status=RUNNING" \
      --zones="$ZONE" \
      --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
      echo "SKIP $VENUE — VM already running: $EXISTING (pass --force to override)" >&2
      skipped+=("$VENUE ($EXISTING)")
      continue
    fi
  fi

  VM_NAME="${PREFIX}${RUN_TS}"
  DATA_TYPES="${VENUE_DATA_TYPES[$VENUE]}"
  INSTRUMENTS="${VENUE_INSTRUMENTS[$VENUE]}"

  echo "Launching $VM_NAME: $VENUE ${START_DATE}..${END_DATE}"

  METADATA="VM_TASK=cefi-onchain-forward-poll"
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
    if [[ "${DRY_RUN:-false}" != "true" ]]; then
        lc_verify_tarball_freshness "$CODE_BUCKET" \
            market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
            || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
    fi

    gcloud compute instances create "$VM_NAME" \
      --project="$PROJECT" \
      --zone="$ZONE" \
      --machine-type=e2-standard-2 \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
      --scopes=cloud-platform \
      --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
      --labels=purpose=cefi-onchain-forward-poll,venue="${VENUE,,}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
  fi

  launched+=("$VM_NAME")
done

echo ""
if [[ ${#launched[@]} -gt 0 ]]; then
  echo "Launched ${#launched[@]} VM(s):"
  for vm in "${launched[@]}"; do
    echo "  $vm"
    echo "    Log:    gcloud compute ssh $vm --zone=$ZONE --command 'sudo tail -f /home/ikennaigboaka/logs/backfill.log'"
    echo "    GCS:    gsutil cat gs://${CODE_BUCKET}/vm-logs/${vm}/run.log"
    echo "    Delete: gcloud compute instances delete $vm --zone=$ZONE --quiet"
  done
fi
if [[ ${#skipped[@]} -gt 0 ]]; then
  echo ""
  echo "Skipped ${#skipped[@]} venue(s) (already running): ${skipped[*]}"
fi
