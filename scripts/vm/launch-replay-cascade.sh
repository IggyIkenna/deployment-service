#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch the SINGLETON replay-cascade VM that bridges historical batch sources
# → Redis Streams the live pipeline consumes, providing smooth handoff to live
# producers on mid-day restart.
#
# Purpose: when a live MTDS VM crashes or restarts mid-day, the gap window has
# no manifest row. The replay subsystem fetches the missing historical window
# (Databento / Tardis / exchange REST snapshot — same sources as backfill),
# iterates aligned timeframe boundaries, builds CandleBoundaryCrossedEvent
# preserving the ORIGINAL window's live-arrival `available_at` time (per
# CLAUDE.md `available_at` is-per-row-write-time rule), publishes via
# `ReplayPublisher` (UTL@`f24e651b`) to the same Redis Streams the live
# producer uses. Per-shard watermark KV at
# `replay_watermark.{asset_group}.{shard_key}` guards double-publish at the
# handoff boundary.
#
# Architecture: separate process from MTDS-live (NOT folded in) — same code
# path as MTDS for tick-aggregation but a different entry-point. Per
# `unified-trading-pm/codex/05-infrastructure/replay-subsystem.md`. Consumer
# (MDPS + features) doesn't know or care whether an event is replay or live;
# only the timestamps differ. Live publisher's next-aligned-boundary check
# sees the watermark KV + skips emission for `period_end <= watermark`.
#
# Bucket-naming SSOT: this launcher uses the (b+) env-aware shape codified
# 2026-05-11 per `bucket_name_ssot_canonicalisation_2026_05_10.md`. Replay
# reads from historical batch buckets per `pipeline_mode=batch_*` paths;
# writes are reused live-pipeline paths (`pipeline_mode=live_websocket`)
# preserving the original `available_at`. Resolver:
# `unified_trading_library.cloud_interface.bucket_naming.resolve_bucket_name(
# cloud=, kind=, asset_group=, env=)`.
#
# Multi-hour-outage backstop: if replay can't catch up to live (e.g. multi-
# hour outage caused a >24h gap), the replay finalizer halts at the
# historical-source coverage limit + emits a `REPLAY_BACKSTOP_REACHED` event.
# Strategy-service Phase 9 wires this to a manual-intervention gate — operator
# must explicitly resume after batch backfill catches up.
#
# Singleton-locked: refuses to launch if a same-prefix VM (`replay-*`) is
# already RUNNING in the zone. Two concurrent replay producers race on the
# watermark KV CAS + double-publish at the handoff boundary.
#
# Operational launch boundary: Phase 15 of
# `unified-trading-pm/plans/active/live_pipeline_mtds_mdps_features_2026_05_08.md`
# is the named runner for ACTUAL live VM bootstrap. This script ships code-
# ready as part of Phase 13.
#
# Usage:
#   bash launch-replay-cascade.sh --start 2026-05-11T12:00:00Z --end 2026-05-11T13:30:00Z --asset-group cefi --shard-key BTC-USDT
#   bash launch-replay-cascade.sh --start ... --end ... --asset-group cefi --shard-key BTC-USDT --env staging
#   bash launch-replay-cascade.sh --start ... --end ... --asset-group cefi --shard-key BTC-USDT --force
#
# Cost: e2-standard-8 — one-shot per replay window; auto-shutdown on STOPPED event.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

START_ISO=""
END_ISO=""
ASSET_GROUP=""
SHARD_KEY=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false

DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --start) START_ISO="$2"; shift 2 ;;
    --end) END_ISO="$2"; shift 2 ;;
    --asset-group) ASSET_GROUP="$2"; shift 2 ;;
    --shard-key) SHARD_KEY="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    *) echo "ERROR: unknown flag '$1'" >&2; exit 1 ;;
  esac
done

if [[ -z "$START_ISO" || -z "$END_ISO" || -z "$ASSET_GROUP" || -z "$SHARD_KEY" ]]; then
  cat >&2 <<EOF
ERROR: --start, --end, --asset-group, --shard-key are all required.

Usage:
  bash launch-replay-cascade.sh \\
    --start <ISO-8601-UTC> --end <ISO-8601-UTC> \\
    --asset-group <cefi|defi|tradfi|sports|prediction> \\
    --shard-key <instrument-or-canonical-key> \\
    [--env prod|staging|dev] [--force]

Example:
  bash launch-replay-cascade.sh --start 2026-05-11T12:00:00Z --end 2026-05-11T13:30:00Z \\
    --asset-group cefi --shard-key BTC-USDT --env prod
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
    --filter='name~"^replay-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: replay-cascade VM already running in $ZONE: $EXISTING
Singleton VM — only one replay producer per environment (watermark KV CAS).

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Force:     bash $0 --start ${START_ISO} --end ${END_ISO} --asset-group ${ASSET_GROUP} --shard-key ${SHARD_KEY} --env ${DEPLOYMENT_ENV} --force

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect/Tail
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If, after that check, it is genuinely stale, construct the delete
command yourself — do not copy-paste one from this message — per infra.md
STEP 0.65's 3-signal staleness check (heartbeat age, run.log tail, manifest
mtime); when unsure, escalate instead of deleting.
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="replay-${ASSET_GROUP}-${RUN_TS}"

echo "Launching $VM_NAME: replay-cascade asset_group=${ASSET_GROUP} shard=${SHARD_KEY} window=[${START_ISO}, ${END_ISO}] env=${DEPLOYMENT_ENV}"

METADATA="VM_TASK=replay-cascade"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=replay"
METADATA="${METADATA},VM_MODE=replay"
METADATA="${METADATA},VM_ASSET_GROUP=${ASSET_GROUP^^}"
METADATA="${METADATA},VM_SHARD_KEY=${SHARD_KEY}"
METADATA="${METADATA},VM_START_ISO=${START_ISO}"
METADATA="${METADATA},VM_END_ISO=${END_ISO}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
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
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
    --zone="$ZONE" \
    --machine-type=e2-standard-8 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=replay-cascade,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/replay.log'"
echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events: gcloud storage ls gs://${PROJECT}-events/events/market_tick_data_service/\$(date +%Y-%m-%d)/${VM_NAME}/"
echo "Watermark: gcloud redis instances ... (per replay_watermark.${ASSET_GROUP}.${SHARD_KEY} KV)"
echo "Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
