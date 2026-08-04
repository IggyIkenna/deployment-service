#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a long-running GCE VM that runs MDPS + features-service (asset-scoped)
# in live mode for a given asset_group. The two services are co-located in one
# VM so MDPS→features handoff stays in-process / sub-ms.
#
# Purpose: MDPS subscribes to `streaming.{asset_group}.candle_boundary_crossed`
# (emitted by `launch-mtds-live.sh --asset-group ${asset_group}`), aggregates
# ticks → OHLCV candles → publishes to `streaming.{asset_group}.candle_computed`.
# features-service (asset-scoped flavor) subscribes to candle_computed → runs
# per-family feature compute → publishes `streaming.{asset_group}.features_computed`.
# Per CLAUDE.md "Live = batch" — same code path as batch mode, only WS feed +
# Redis Stream transport differ.
#
# Bucket-naming SSOT: this launcher uses the (b+) env-aware shape codified
# 2026-05-11 per `bucket_name_ssot_canonicalisation_2026_05_10.md`. The
# `--env $DEPLOYMENT_ENV` flag is propagated to VM metadata so MDPS + features
# resolve buckets via `unified_trading_library.cloud_interface.bucket_naming.resolve_bucket_name(
# cloud=, kind=, asset_group=, env=)`. Bucket NAMES carry env tier; bucket
# PATHS carry pipeline_mode (`live_websocket` for live candles + features).
#
# Singleton-locked per (asset_group): refuses to launch if a same-prefix VM
# (`mdps-features-live-{asset_group}-*`) is already RUNNING in the zone. Two
# concurrent consumers on the same Redis Stream + asset_group group thrash on
# XAUTOCLAIM + race per-VM manifest shards.
#
# Operational launch boundary: Phase 15 of
# `unified-trading-pm/plans/active/live_pipeline_mtds_mdps_features_2026_05_08.md`
# is the named runner for ACTUAL live VM bootstrap. This script ships code-
# ready as part of Phase 13; operational launch awaits Harsh slot 5
# per-service consumer wiring + Phase 12 reconciliation gate green.
#
# Usage:
#   bash launch-mdps-features-live.sh --asset-group cefi                       # prod
#   bash launch-mdps-features-live.sh --asset-group defi --env staging         # staging cluster
#   bash launch-mdps-features-live.sh --asset-group cefi --force               # bypass singleton lock
#
# Cost: e2-standard-8 ~24/7 — live consumer; auto-shutdown on STOPPED event only.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

ASSET_GROUP=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false

DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
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
  bash launch-mdps-features-live.sh --asset-group <cefi|defi|tradfi|sports|prediction> [--env prod|staging|dev] [--force]

One MDPS+features-live VM per asset_group, co-located with the matching MTDS-live VM.
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
CODE_BUCKET="deployment-scripts-${PROJECT}"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^mdps-features-live-${ASSET_GROUP}-\" AND status=RUNNING" \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: MDPS+features-live VM already running for asset_group=${ASSET_GROUP} in $ZONE: $EXISTING
Refusing to launch a duplicate — per-asset_group Redis Stream consumer-group
must be singleton (one MDPS XREADGROUP + one features-service XREADGROUP per ag).

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Force:     bash $0 --asset-group ${ASSET_GROUP} --env ${DEPLOYMENT_ENV} --force

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect/Tail
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
VM_NAME="mdps-features-live-${ASSET_GROUP}-${RUN_TS}"

echo "Launching $VM_NAME: MDPS+features-asset-scoped live asset_group=${ASSET_GROUP} env=${DEPLOYMENT_ENV}"

METADATA="VM_TASK=mdps-features-live"
METADATA="${METADATA},VM_SERVICE=market_data_processing_service+features_service"
METADATA="${METADATA},VM_OPERATION=streaming-aggregation"
METADATA="${METADATA},VM_MODE=live"
METADATA="${METADATA},VM_ASSET_GROUP=${ASSET_GROUP^^}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          market-data-processing-service market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
    --zone="$ZONE" \
    --machine-type=e2-standard-8 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=mdps-features-live,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/live.log'"
echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events (MDPS): gcloud storage ls gs://${PROJECT}-events/events/market_data_processing_service/\$(date +%Y-%m-%d)/${VM_NAME}/"
echo "Events (features): gcloud storage ls gs://${PROJECT}-events/events/features_service/\$(date +%Y-%m-%d)/${VM_NAME}/"
echo "Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
