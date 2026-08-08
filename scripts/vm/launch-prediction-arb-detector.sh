#!/usr/bin/env bash
# Epic: predictions_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a long-running GCE VM that runs the LIVE cross-venue (Kalshi ↔ Polymarket)
# prediction-market ARB DETECTOR in PAPER mode: it reads both venues' live
# `book_snapshot_5` from GCS, normalizes to YES-probability, flags PURE_ARB /
# QUOTABLE_ARB (fee-net), and STREAMS every opportunity to the GCS arb store
# (`features-cross-instrument` pred bucket, `cross_venue_arb/by_date/...`).
#
# It runs the features-service cross_instrument CLI:
#   python -m features_service.cross_instrument --operation arb-detect --mode live --asset-group PREDICTION
# reusing the shipped `run_prediction_cross_venue_dispersion` (matcher → book →
# align → kernel) UNCHANGED. Per CLAUDE.md "Live = batch" the SAME detection runs
# in batch (one tick) and live (the poll loop).
#
# Design SSOT: codex/04-architecture/cross-venue-prediction-arb-detection.md.
# Plan: unified-trading-pm/plans/active/prediction_venue_perps_and_live_clob_depth_2026_06_20.md.
#
# LONG_LIVED_LIVE: VM_SHUTDOWN_ON_COMPLETION=false; the loop runs until SIGTERM
# (graceful) or --max-duration. Classified LIVE via the `prediction-arb-detector-`
# prefix in vm_zombie_watchdog.VM_PREFIX_TO_BUCKET (heartbeat-only — the detector
# writes the arb store, not per-VM tick-manifest shards).
#
# Singleton-locked: refuses a duplicate if a `prediction-arb-detector-*` VM is
# already RUNNING in the zone (two detectors would double-write the arb store).
#
# Usage:
#   bash launch-prediction-arb-detector.sh                          # prod, indefinite
#   bash launch-prediction-arb-detector.sh --max-duration 90000     # ~25h validation run
#   bash launch-prediction-arb-detector.sh --env staging --force
#
# Cost: e2-standard-4 ~24/7 — light live consumer; auto-shutdown on STOPPED event only.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false
SCAN_DAYS=""
POLL_INTERVAL=""
MAX_DURATION=""
ENTRY_THRESHOLD=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --scan-days) SCAN_DAYS="$2"; shift 2 ;;
    --poll-interval-seconds) POLL_INTERVAL="$2"; shift 2 ;;
    --max-duration) MAX_DURATION="$2"; shift 2 ;;
    --entry-threshold) ENTRY_THRESHOLD="$2"; shift 2 ;;
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
VM_PREFIX="prediction-arb-detector"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^${VM_PREFIX}-\" AND status=RUNNING" \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: prediction-arb-detector VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — a second detector would double-write the GCS arb store.

Options:
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Force:     bash $0 --env ${DEPLOYMENT_ENV} --force

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
VM_NAME="${VM_PREFIX}-${RUN_TS}"

echo "Launching $VM_NAME: cross-venue prediction arb detector (paper, live) env=${DEPLOYMENT_ENV}"

METADATA="VM_TASK=prediction-arb-detect"
METADATA="${METADATA},VM_SERVICE=features_service"
METADATA="${METADATA},VM_OPERATION=arb-detect"
METADATA="${METADATA},VM_MODE=live"
METADATA="${METADATA},VM_ASSET_GROUP=PREDICTION"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"
[[ -n "$SCAN_DAYS" ]] && METADATA="${METADATA},VM_ARB_SCAN_DAYS=${SCAN_DAYS}"
[[ -n "$POLL_INTERVAL" ]] && METADATA="${METADATA},VM_ARB_POLL_INTERVAL_SECONDS=${POLL_INTERVAL}"
[[ -n "$MAX_DURATION" ]] && METADATA="${METADATA},VM_ARB_MAX_DURATION_SECONDS=${MAX_DURATION}"
[[ -n "$ENTRY_THRESHOLD" ]] && METADATA="${METADATA},VM_ARB_ENTRY_THRESHOLD=${ENTRY_THRESHOLD}"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        features-service market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi
[[ "${DRY_RUN:-false}" == "true" ]] && export LC_DRY_RUN=true
lc_gcloud_create "$VM_NAME" "$PROJECT" "$ZONE" "e2-standard-4" "50" \
    "startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    "purpose=prediction-arb-detector,asset-group=prediction,env=${DEPLOYMENT_ENV},run-ts=${RUN_TS}" \
    "$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")"

echo ""
echo "VM launched: $VM_NAME"
echo "GCS log tail:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Arb store:     gsutil ls gs://features-xinstrument-pred-${PROJECT}/cross_venue_arb/by_date/"
echo "Delete:        gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
