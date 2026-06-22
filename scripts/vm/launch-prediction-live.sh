#!/usr/bin/env bash
# Epic: predictions_master
# Lifecycle: permanent
#
# Launch a long-running GCE VM that runs MTDS in live WS mode for a single
# Prediction (POLYMARKET|KALSHI) × data_type shard.  The runner resolves the
# full active instrument universe from the instruments-service IS parquet for
# today — NO --instrument-ids required.
#
# Purpose: subscribe to the Prediction CLOB WebSocket (Polymarket order-book /
# Kalshi REST-polled feed), aggregate ticks per UTC-aligned 1-minute boundary,
# write per-instrument parquets + per-VM manifest shards under
#   gs://market-data-tick-prediction-{pid}/.../pipeline_mode=live_{source}/...
# and publish CandleBoundaryCrossedEvent to the prediction Redis Stream for
# the downstream MDPS features-service pipeline.
#
# This is the LIVE counterpart to launch-prediction-forward-poll.sh (which
# runs the daily batch catch-up).  One VM per (venue × data_type) shard;
# singleton-locked per shard so duplicate VMs never race on the WS feed.
#
# IS→MTDS contract: instrument universe is resolved at runtime from
#   gs://instruments-store-prediction-{pid}/instrument_availability/by_date/
#     day={today}/venue={VENUE}/instruments.parquet
# (plan item 83: catalog-aware enumeration).  If the IS parquet is missing /
# empty, the runner emits honest-absence (SOURCE_RETURNED_ZERO) and exits.
#
# Supported shards:
#   --venue POLYMARKET --data-type trades
#   --venue POLYMARKET --data-type book_snapshot_5
#   --venue KALSHI    --data-type trades
#   --venue KALSHI    --data-type book_snapshot_5
#
# Usage:
#   bash launch-prediction-live.sh --venue POLYMARKET --data-type trades
#   bash launch-prediction-live.sh --venue KALSHI --data-type trades --env staging
#   bash launch-prediction-live.sh --venue POLYMARKET --data-type book_snapshot_5 --force
#   bash launch-prediction-live.sh --venue POLYMARKET --data-type trades --dry-run
#
# Singleton lock: refuses to launch if a prediction-live-{venue_slug}-{dt_slug}-*
# VM is already RUNNING in the zone for the same shard.
#
# Bucket-naming SSOT: bucket names resolved via UTL build_bucket() at VM
# startup (VM metadata DEPLOYMENT_ENV drives env-aware resolution).
#
# Cost: e2-standard-4 — lighter than generic MTDS-live (no instrument list to
# pass; IS parquet read is fast); long-lived continuous stream.
#
# Prerequisites:
#   - Tarballs uploaded: bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group PREDICTION
#   - polymarket-* + kalshi-api-key in Secret Manager
#   - Instrument index parquet current in instruments-store-prediction-{pid} bucket
#   - Redis endpoint available (MTDS_STREAMING_REDIS_URL in VM metadata or SM)
set -euo pipefail

VENUE=""
DATA_TYPE=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --venue)      VENUE="${2^^}"; shift 2 ;;    # normalise to uppercase
    --data-type)  DATA_TYPE="$2"; shift 2 ;;
    --env)        DEPLOYMENT_ENV="$2"; shift 2 ;;
    --force)      FORCE=true; shift ;;
    --dry-run)    DRY_RUN=true; shift ;;
    *) echo "ERROR: unknown flag '$1'" >&2; exit 1 ;;
  esac
done

# ------------------------------------------------------------------
# Validate required args
# ------------------------------------------------------------------
if [[ -z "$VENUE" ]]; then
  cat >&2 <<EOF
ERROR: --venue required.

Usage:
  bash launch-prediction-live.sh --venue <POLYMARKET|KALSHI> --data-type <trades|book_snapshot_5>
    [--env prod|staging|dev] [--force] [--dry-run]

Supported shards:
  POLYMARKET × trades
  POLYMARKET × book_snapshot_5
  KALSHI     × trades
  KALSHI     × book_snapshot_5
EOF
  exit 1
fi

if [[ -z "$DATA_TYPE" ]]; then
  echo "ERROR: --data-type required (trades or book_snapshot_5)" >&2
  exit 1
fi

case "$VENUE" in
  POLYMARKET|KALSHI) ;;
  *) echo "ERROR: --venue must be POLYMARKET or KALSHI (got: $VENUE)" >&2; exit 1 ;;
esac

case "$DATA_TYPE" in
  trades|book_snapshot_5) ;;
  *) echo "ERROR: --data-type must be one of: trades, book_snapshot_5 (got: $DATA_TYPE)" >&2; exit 1 ;;
esac

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# ------------------------------------------------------------------
# Build GCE-safe slug: lowercase, colons/underscores/slashes → hyphens
# ------------------------------------------------------------------
VENUE_SLUG="${VENUE,,}"
DT_SLUG="${DATA_TYPE//_/-}"
DT_SLUG="${DT_SLUG,,}"
VM_PREFIX="prediction-live-${VENUE_SLUG}-${DT_SLUG}"

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

# shard_spec for the MTDS CLI: prediction:VENUE:data_type
SHARD_SPEC="prediction:${VENUE}:${DATA_TYPE}"

# ------------------------------------------------------------------
# Singleton lock — one live WS producer per (venue × data_type) shard
# ------------------------------------------------------------------
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^${VM_PREFIX}-\" AND status=RUNNING" \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: Prediction-live VM already running for ${SHARD_SPEC} in ${ZONE}: ${EXISTING}
Refusing to launch a duplicate — per-shard WebSocket feed must be singleton.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --venue ${VENUE} --data-type ${DATA_TYPE} --env ${DEPLOYMENT_ENV} --force
EOF
    exit 1
  fi

  # Fallback zone checks — within-region only (GCS data is asia-northeast1)
  for FALLBACK_ZONE in "asia-northeast1-b" "asia-northeast1-a"; do
    FB_EXISTING="$(gcloud compute instances list \
      --filter="name~\"^${VM_PREFIX}-\" AND status=RUNNING" \
      --zones="$FALLBACK_ZONE" \
      --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$FB_EXISTING" ]]; then
      cat >&2 <<EOF
ERROR: Prediction-live VM already running in fallback zone ${FALLBACK_ZONE}: ${FB_EXISTING}
Stop it before launching in ${ZONE}.
EOF
      exit 1
    fi
  done
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="${VM_PREFIX}-${RUN_TS}"

echo "Launching ${VM_NAME}: Prediction live WS producer ${SHARD_SPEC} env=${DEPLOYMENT_ENV}"

# ------------------------------------------------------------------
# VM metadata — consumed by setup-data-pipeline-vm.sh at startup
# ------------------------------------------------------------------
METADATA="VM_TASK=mtds-live"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=live_websocket"
METADATA="${METADATA},VM_MODE=live"
METADATA="${METADATA},VM_ASSET_GROUP=PREDICTION"
METADATA="${METADATA},VM_SHARD_SPEC=${SHARD_SPEC}"
# NO VM_INSTRUMENT_IDS — runner resolves full IS universe automatically
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: ${VM_NAME}"
  echo "[DRY-RUN] SHARD_SPEC=${SHARD_SPEC}"
  echo "[DRY-RUN] VM_PREFIX for singleton lock: ${VM_PREFIX}"
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT}" \
    --zone="${ZONE}" \
    --machine-type=e2-standard-4 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=prediction-live,venue="${VENUE_SLUG}",data-type="${DT_SLUG}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: ${VM_NAME}"
echo "Logs: gcloud compute ssh ${VM_NAME} --zone=${ZONE} --command 'tail -f /home/ikennaigboaka/logs/live.log'"
echo "GCS log: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete:  gcloud compute instances delete ${VM_NAME} --zone=${ZONE} --quiet"
