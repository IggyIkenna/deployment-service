#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
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
#   bash launch-mtds-live.sh --asset-group cefi --shard-spec cefi:HYPERLIQUID:trades --instrument-ids BTC-USD;ETH-USD
#   bash launch-mtds-live.sh --asset-group defi --shard-spec defi:UNISWAP_V3:trade --instrument-ids WETH-USDC --env staging
#   bash launch-mtds-live.sh --asset-group cefi --shard-spec cefi:BINANCE-FUTURES:perp_funding --instrument-ids BTC;ETH --force
#
# --shard-spec: required for setup-data-pipeline-vm.sh to pass --shard-spec to the MTDS CLI.
#   Format: "asset_group:venue:data_type" (e.g. "cefi:HYPERLIQUID:trades").
# --instrument-ids: semicolon-separated (e.g. "BTC-USD;ETH-USD"). setup-data-pipeline-vm.sh
#   converts semicolons to spaces for CLI nargs='+'.
#
# Singleton-locked per (asset_group, shard_spec): refuses to launch if a same-prefix VM
# (`mtds-live-{asset_group}-{shard_slug}-*`) is already RUNNING. Per-shard lock allows
# multiple data_type shards per asset_group without conflict.
#
# Cost: e2-standard-8 ~24/7 — live producer; auto-shutdown on STOPPED event only.
#
# --test-run: test-bucket-routed, BOUNDED, auto-shutdown live smoke check (plan todo 16,
#   data_pipeline_e2e_check_2026_07_10.md). Sets IS_TEST_RUN=true + MANIFEST_ALLOW_STALE_FALLBACK=true
#   (writes land on the -test- sibling bucket, never PROD), flips VM_SHUTDOWN_ON_COMPLETION=true, and
#   uses a DISTINCT `mtds-live-smoke-` VM-name prefix (never collides with the real per-shard
#   `mtds-live-{ag}-*` singleton lock). Pair with --max-duration-seconds so the run actually
#   terminates (the live WS producer has no other natural completion signal).
# --max-duration-seconds N: bounds the live run to N seconds (WebsocketStreamingHandler calls
#   request_stop() after N seconds), then the VM exits + self-deletes if VM_SHUTDOWN_ON_COMPLETION=true.
#   Only meaningful with --test-run; a real (non-test) live producer runs forever by design.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

ASSET_GROUP=""
SHARD_SPEC=""
INSTRUMENT_IDS=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
LIVE_SOURCE="native"
FORCE=false
TEST_RUN=false
MAX_DURATION_SECONDS=""
VM_NAME_OVERRIDE=""

DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --asset-group) ASSET_GROUP="$2"; shift 2 ;;
    --shard-spec) SHARD_SPEC="$2"; shift 2 ;;
    --instrument-ids) INSTRUMENT_IDS="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --live-source) LIVE_SOURCE="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --test-run) TEST_RUN=true; shift ;;
    --max-duration-seconds) MAX_DURATION_SECONDS="$2"; shift 2 ;;
    --vm-name) VM_NAME_OVERRIDE="$2"; shift 2 ;;
    *) echo "ERROR: unknown flag '$1'" >&2; exit 1 ;;
  esac
done

if [[ -z "$ASSET_GROUP" ]]; then
  cat >&2 <<EOF
ERROR: --asset-group required.

Usage:
  bash launch-mtds-live.sh --asset-group <cefi|defi|tradfi|sports|prediction> \
    --shard-spec <asset_group:venue:data_type> \
    --instrument-ids <semicolon-separated> \
    [--env prod|staging|dev] [--live-source native|tardis-machine] [--force] \
    [--test-run] [--max-duration-seconds N] [--vm-name NAME]

One VM per (asset_group, shard_spec). Singleton-locked per shard.
--test-run + --max-duration-seconds: bounded, test-bucket-routed, auto-shutdown live smoke
check (distinct mtds-live-smoke- prefix — never collides with a real live producer).
--vm-name: caller-supplied exact VM name (skips the internal RUN_TS timestamp) — use this
whenever a caller (e.g. pipeline_e2e_check.py) needs to poll for the VM by a name it already
knows, so it never has to predict/reconstruct the launcher's own timestamp.

Examples:
  bash launch-mtds-live.sh --asset-group cefi --shard-spec cefi:HYPERLIQUID:trades --instrument-ids "BTC-USD;ETH-USD;SOL-USD"
  bash launch-mtds-live.sh --asset-group cefi --shard-spec cefi:ASTER:derivative_ticker --instrument-ids "BTC;ETH"
EOF
  exit 1
fi

if [[ -z "$SHARD_SPEC" ]]; then
  echo "ERROR: --shard-spec required (format: asset_group:venue:data_type, e.g. cefi:HYPERLIQUID:trades)" >&2
  exit 1
fi

if [[ -z "$INSTRUMENT_IDS" ]]; then
  echo "ERROR: --instrument-ids required (semicolon-separated, e.g. BTC-USD;ETH-USD)" >&2
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

case "$LIVE_SOURCE" in
  native|tardis-machine) ;;
  *) echo "ERROR: --live-source must be one of native/tardis-machine (got: $LIVE_SOURCE)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

# Build a slug from the shard spec for VM naming and singleton lock.
# cefi:HYPERLIQUID:trades → cefi-hyperliquid-trades (lowercase, colons→hyphens)
# GCE instance names allow ONLY [a-z0-9-]; data_types with underscores (book_snapshot,
# perp_funding, derivative_ticker, …) must have _ → - in the NAME slug (the real shard-spec
# passed to the MTDS CLI via VM_SHARD_SPEC keeps the underscore). Fixes the
# "instances.create Could not fetch resource" invalid-name failure on underscore data_types.
SHARD_SLUG="${SHARD_SPEC//:/-}"
SHARD_SLUG="${SHARD_SLUG//_/-}"  # GCE name: underscores → hyphens
SHARD_SLUG="${SHARD_SLUG,,}"  # lowercase
# --test-run uses a DISTINCT prefix root (never `mtds-live-${SHARD_SLUG}`) so a bounded
# smoke-check run never collides with — or singleton-locks against — a real live producer
# for the same shard.
if $TEST_RUN; then
  VM_PREFIX="mtds-live-smoke-${SHARD_SLUG}"
else
  VM_PREFIX="mtds-live-${SHARD_SLUG}"
fi

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^${VM_PREFIX}-\" AND status=RUNNING" \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: MTDS-live VM already running for shard_spec=${SHARD_SPEC} in $ZONE: $EXISTING
Refusing to launch a duplicate — per-shard WebSocket feed must be singleton.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Force:     bash $0 --asset-group ${ASSET_GROUP} --shard-spec ${SHARD_SPEC} --instrument-ids "${INSTRUMENT_IDS}" --env ${DEPLOYMENT_ENV} --force

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

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="${VM_NAME_OVERRIDE:-${VM_PREFIX}-${RUN_TS}}"

echo "Launching $VM_NAME: MTDS live producer asset_group=${ASSET_GROUP} shard=${SHARD_SPEC} env=${DEPLOYMENT_ENV}"

METADATA="VM_TASK=mtds-live"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=live_websocket"
METADATA="${METADATA},VM_MODE=live"
METADATA="${METADATA},VM_ASSET_GROUP=${ASSET_GROUP^^}"
METADATA="${METADATA},VM_SHARD_SPEC=${SHARD_SPEC}"
METADATA="${METADATA},VM_INSTRUMENT_IDS=${INSTRUMENT_IDS}"
METADATA="${METADATA},VM_LIVE_SOURCE=${LIVE_SOURCE}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
# Tardis concurrency lease passthrough (opt-in, mirrors launch-cefi-sharded-backfill.sh):
# lets checker re-runs of Tardis venues SERIALIZE against production lease-enabled
# waves instead of 403ing on the single-concurrent-IP key
# (tardis_concurrent_ip_lockout_2026_07_12.md).
[[ "${TARDIS_CONCURRENCY_LEASE:-}" == "1" ]] && METADATA="${METADATA},TARDIS_CONCURRENCY_LEASE=1"
[[ -n "${TARDIS_CONCURRENCY_LEASE_BUCKET:-}" ]] && METADATA="${METADATA},TARDIS_CONCURRENCY_LEASE_BUCKET=${TARDIS_CONCURRENCY_LEASE_BUCKET}"

if $TEST_RUN; then
  # 24h consolidator-staleness budget for -test- buckets (no standing cron; the
  # checker Phase-0-consolidates once) — mirrors launch-mtds-backfill-vm.sh.
  METADATA="${METADATA},IS_TEST_RUN=true,MANIFEST_ALLOW_STALE_FALLBACK=true,MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
  METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
  if [[ -n "$MAX_DURATION_SECONDS" ]]; then
    METADATA="${METADATA},VM_MAX_DURATION_SECONDS=${MAX_DURATION_SECONDS}"
  fi
else
  METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"
fi

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
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT" "${TEST_RUN}")" \
    --zone="$ZONE" \
    --machine-type=e2-standard-8 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=mtds-live,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",shard-slug="${SHARD_SLUG:0:63}",managed-by=deployment-service
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/live.log'"
echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events: gcloud storage ls gs://${PROJECT}-events/events/market_tick_data_service/\$(date +%Y-%m-%d)/${VM_NAME}/"
echo "Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
