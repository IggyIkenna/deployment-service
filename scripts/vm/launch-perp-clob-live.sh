#!/usr/bin/env bash
# Epic: predictions_master
# Lifecycle: permanent
# Delete-when: NA
#
# Launch a long-running GCE VM that runs MTDS in live mode, recording CLOB
# book-snapshot ticks for perp-CLOB venues (Kalshi-perp + Polymarket-perp).
#
# Venue mapping (cefi asset_group — these are crypto perps with real funding,
# not prediction-market outcomes; they share the cefi market-data bucket):
#   KALSHI-PERP   → wss://api.elections.kalshi.com/trade-api/ws/v2 (public read, no auth)
#   POLYMARKET-PERP → BLOCKED-UPSTREAM-OUTAGE: perps-api.polymarket.com DNS NXDOMAIN
#                     (verified 2026-06-21). This launcher gates the venue and skips
#                     launch rather than creating a doomed crash-loop VM.
#
# Live = batch schema parity:
#   Both live and batch modes write the same UAC `book_snapshot` data_type schema
#   via the MTDS connector.  The only difference between modes is pipeline_mode:
#     live:  live_kalshi_perp  (this launcher)
#     batch: batch_kalshi_perp (forward-poll / historical download)
#   Parquet field names, types, and manifest semantics are identical in both modes.
#
# Singleton-locked per shard_spec: refuses to launch if a same-prefix VM is already
# RUNNING in the zone.  Use --force to bypass.
#
# VM prefix: mtds-live-cefi-kalshi-perp-book-snapshot-{ts}
#   Covered by "mtds-live-cefi-" in VM_PREFIX_TO_BUCKET → _TICK_CEFI / LONG_LIVED_LIVE.
#
# Usage:
#   bash launch-perp-clob-live.sh --venue KALSHI-PERP --instrument-ids "BTC-PERP;ETH-PERP"
#   bash launch-perp-clob-live.sh --venue KALSHI-PERP --instrument-ids "BTC-PERP" --env staging
#   bash launch-perp-clob-live.sh --venue KALSHI-PERP --instrument-ids "BTC-PERP" --force
#   bash launch-perp-clob-live.sh --venue POLYMARKET-PERP  # BLOCKED-UPSTREAM, exits cleanly
#
# Mirrors: launch-mtds-live.sh (general MTDS-live pattern)
#          launch-prediction-forward-poll.sh (perp CLOB batch/forward-poll)
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

VENUE=""
INSTRUMENT_IDS=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --venue) VENUE="$2"; shift 2 ;;
    --instrument-ids) INSTRUMENT_IDS="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    *) echo "ERROR: unknown flag '$1'" >&2; exit 1 ;;
  esac
done

if [[ -z "$VENUE" ]]; then
  cat >&2 <<EOF
ERROR: --venue required.

Usage:
  bash launch-perp-clob-live.sh --venue <KALSHI-PERP|POLYMARKET-PERP> \\
    --instrument-ids <semicolon-separated> \\
    [--env prod|staging|dev] [--force]

Supported venues:
  KALSHI-PERP     live CLOB book_snapshot via Kalshi perp WebSocket (public read, no auth)
  POLYMARKET-PERP BLOCKED-UPSTREAM-OUTAGE — DNS NXDOMAIN as of 2026-06-21; skips cleanly

Examples:
  bash launch-perp-clob-live.sh --venue KALSHI-PERP --instrument-ids "BTC-PERP;ETH-PERP"
EOF
  exit 1
fi

case "$VENUE" in
  KALSHI-PERP|POLYMARKET-PERP) ;;
  *) echo "ERROR: --venue must be KALSHI-PERP or POLYMARKET-PERP (got: $VENUE)" >&2; exit 1 ;;
esac

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# ---------------------------------------------------------------------------
# BLOCKED-UPSTREAM-OUTAGE gate: Polymarket-perp endpoint DNS-dead (2026-06-21)
# perps-api.polymarket.com returns NXDOMAIN — no live WebSocket available.
# Skip launch immediately rather than create a VM that will immediately crash.
# When the endpoint resolves (check: host perps-api.polymarket.com), remove
# this gate and run the launcher normally.
# ---------------------------------------------------------------------------
if [[ "$VENUE" == "POLYMARKET-PERP" ]]; then
  cat <<EOF
BLOCKED-UPSTREAM-OUTAGE: Polymarket-perp live launcher skipped.

  Status:  BLOCKED-UPSTREAM-OUTAGE (closed-set status — third-party degraded)
  Venue:   POLYMARKET-PERP
  Reason:  perps-api.polymarket.com DNS NXDOMAIN as of 2026-06-21.
           Launching a VM would create a crash-loop with no recovery path.
  Plan:    prediction_venue_perps_and_live_clob_depth_2026_06_20.md
  Unblock: Verify resolution with \`host perps-api.polymarket.com\`.
           Once live, remove this gate and launch normally.

No VM was created.
EOF
  exit 0
fi

if [[ -z "$INSTRUMENT_IDS" ]]; then
  echo "ERROR: --instrument-ids required (semicolon-separated, e.g. BTC-PERP;ETH-PERP)" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Build shard spec and GCE-safe VM name slug.
# Shard spec format: "asset_group:venue:data_type" — passed verbatim to MTDS
# CLI via VM metadata; underscores/colons are valid in the spec itself.
# GCE instance names allow ONLY [a-z0-9-]: colons and underscores must become
# hyphens in the NAME slug (pattern from launch-mtds-live.sh).
# ---------------------------------------------------------------------------
ASSET_GROUP="cefi"
DATA_TYPE="book_snapshot"
SHARD_SPEC="${ASSET_GROUP}:${VENUE}:${DATA_TYPE}"

SHARD_SLUG="${SHARD_SPEC//:/-}"
SHARD_SLUG="${SHARD_SLUG//_/-}"  # GCE name: underscores → hyphens
SHARD_SLUG="${SHARD_SLUG,,}"     # lowercase
# Result: cefi-kalshi-perp-book-snapshot
# VM prefix: mtds-live-cefi-kalshi-perp-book-snapshot-
# Covered by "mtds-live-cefi-" entry in VM_PREFIX_TO_BUCKET (vm_zombie_watchdog.py).

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

VM_PREFIX="mtds-live-${SHARD_SLUG}"

# ---------------------------------------------------------------------------
# Singleton lock: one live recorder per shard_spec to avoid duplicate WS feeds
# racing on the Redis Stream consumer group and manifest shard writes.
# ---------------------------------------------------------------------------
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^${VM_PREFIX}-\" AND status=RUNNING" \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: live VM already running for shard_spec=${SHARD_SPEC} in $ZONE: $EXISTING
Refusing to launch a duplicate — per-shard WebSocket feed must be singleton.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --venue ${VENUE} --instrument-ids "${INSTRUMENT_IDS}" --env ${DEPLOYMENT_ENV} --force
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="${VM_PREFIX}-${RUN_TS}"

echo "Launching $VM_NAME: MTDS perp-CLOB live producer venue=${VENUE} shard=${SHARD_SPEC} env=${DEPLOYMENT_ENV}"

METADATA="VM_TASK=mtds-live"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=live_websocket"
METADATA="${METADATA},VM_MODE=live"
METADATA="${METADATA},VM_ASSET_GROUP=${ASSET_GROUP^^}"
METADATA="${METADATA},VM_SHARD_SPEC=${SHARD_SPEC}"
METADATA="${METADATA},VM_INSTRUMENT_IDS=${INSTRUMENT_IDS}"
METADATA="${METADATA},VM_LIVE_SOURCE=native"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: $VM_NAME"
  echo "[DRY-RUN] Metadata: $METADATA"
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
    --machine-type=e2-standard-8 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=mtds-live,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",shard-slug="${SHARD_SLUG:0:63}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/live.log'"
echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events: gcloud storage ls gs://${PROJECT}-events/events/market_tick_data_service/\$(date +%Y-%m-%d)/${VM_NAME}/"
echo "Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
