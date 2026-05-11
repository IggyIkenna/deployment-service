#!/usr/bin/env bash
# Launch a long-running GCE VM that runs MTDS in live mode for a given asset_group.
#
# Purpose: subscribe to per-venue WebSocket feeds for the operator-expected
# coverage set per asset_group (CeFi/DeFi/TradFi/Sports/Prediction), aggregate
# ticks per UTC-aligned timeframe boundary, and emit `CandleBoundaryCrossedEvent`
# to `streaming.{asset_group}.candle_boundary_crossed` Redis Stream for MDPS to
# consume. This is the LIVE producer half of the live-pipeline cascade
# (MTDS → MDPS → features-service), per
# `unified-trading-pm/codex/05-infrastructure/live-pipeline-architecture.md`.
#
# Companion VM: launch-mdps-features-live.sh (per asset_group). One MTDS-live VM
# per asset_group; co-located in the same region with the MDPS+features VM for
# sub-ms Redis Stream latency.
#
# Bucket-naming SSOT: this launcher uses the (b+) env-aware shape codified
# 2026-05-11 per `bucket_name_ssot_canonicalisation_2026_05_10.md`. The
# `--env $DEPLOYMENT_ENV` flag is propagated to VM metadata so MTDS resolves
# buckets via `unified_trading_library.cloud_interface.bucket_naming.resolve_bucket_name(
# cloud=, kind=, asset_group=, env=)`. Bucket NAMES carry env tier; bucket
# PATHS carry pipeline_mode (`live_websocket` in this case).
#
# Singleton-locked per (asset_group): refuses to launch if a same-prefix VM
# (`mtds-live-{asset_group}-*`) is already RUNNING in the zone. Live producers
# write to per-VM manifest shards under `_index/per_vm/{vm_name}.parquet`; two
# concurrent producers for the same asset_group thrash on the WS feed + race
# on the Redis Stream consumer group.
#
# Operational launch boundary: Phase 15 of
# `unified-trading-pm/plans/active/live_pipeline_mtds_mdps_features_2026_05_08.md`
# is the named runner for ACTUAL live VM bootstrap. This script ships code-
# ready as part of Phase 13; operational launch awaits:
#   - Harsh slot 5 per-service MDPS/features consumer wiring
#   - Phase 12 batch-vs-live reconciliation gate green
#   - Tarball refresh via `create-code-tarballs.sh --all`
#
# Usage:
#   bash launch-mtds-live.sh --asset-group cefi                       # prod (default)
#   bash launch-mtds-live.sh --asset-group defi --env staging         # staging cluster
#   bash launch-mtds-live.sh --asset-group cefi --force               # bypass singleton lock
#
# Cost: e2-standard-8 ~24/7 — live producer; auto-shutdown on STOPPED event only.
set -euo pipefail

ASSET_GROUP=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --asset-group) ASSET_GROUP="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    *) echo "ERROR: unknown flag '$1'" >&2; exit 1 ;;
  esac
done

if [[ -z "$ASSET_GROUP" ]]; then
  cat >&2 <<EOF
ERROR: --asset-group required.

Usage:
  bash launch-mtds-live.sh --asset-group <cefi|defi|tradfi|sports|prediction> [--env prod|staging|dev] [--force]

One MTDS-live VM per asset_group. Singleton-locked per (asset_group).
EOF
  exit 1
fi

case "$ASSET_GROUP" in
  cefi|defi|tradfi|sports|prediction) ;;
  *) echo "ERROR: --asset-group must be one of cefi/defi/tradfi/sports/prediction (got: $ASSET_GROUP)" >&2; exit 1 ;;
esac

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^mtds-live-${ASSET_GROUP}-\" AND status=RUNNING" \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: MTDS-live VM already running for asset_group=${ASSET_GROUP} in $ZONE: $EXISTING
Refusing to launch a duplicate — per-asset_group WebSocket feed + Redis Stream
consumer group must be singleton.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --asset-group ${ASSET_GROUP} --env ${DEPLOYMENT_ENV} --force
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="mtds-live-${ASSET_GROUP}-${RUN_TS}"

echo "Launching $VM_NAME: MTDS live producer asset_group=${ASSET_GROUP} env=${DEPLOYMENT_ENV}"

METADATA="VM_TASK=mtds-live"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=live_websocket"
METADATA="${METADATA},VM_MODE=live"
METADATA="${METADATA},VM_ASSET_GROUP=${ASSET_GROUP^^}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type=e2-standard-8 \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --labels=purpose=mtds-live,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/live.log'"
echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events: gcloud storage ls gs://${PROJECT}-events/events/market_tick_data_service/\$(date +%Y-%m-%d)/${VM_NAME}/"
echo "Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
