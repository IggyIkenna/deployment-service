#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch a short-lived GCE VM that forward-polls TradFi venues.
#
# Purpose: ingest a single day of TradFi market-data ticks for the operator-
# expected coverage set in EXPECTED_COVERAGE_BY_ASSET_GROUP['tradfi']
# (CME, ICE, CBOE, FX, YAHOO_FINANCE) — NOT NASDAQ/NYSE/BARCHART (out of
# scope per operator policy 2026-04-28). Data types per venue:
#   - CME, ICE: trades + ohlcv_1m + tbbo via Databento
#                (gated by TRADFI_TICK_DATA_WINDOWS — outside windows
#                 only ohlcv_1m flows; trades + tbbo are skipped to
#                 manage Databento cost).
#   - CBOE: ohlcv_15m (VIX) via Databento.
#   - FX: ohlcv_24h (KRW/USD) via Yahoo Finance.
#   - YAHOO_FINANCE: ohlcv_15m rolling VIX + ohlcv_24h FX.
#
# Same code path as launch-tradfi-backfill-vm.sh; this launcher just runs the
# rolling 1-day pass.
#
# Writes to: gs://market-data-tick-tradfi-central-element-323112/by_date/...
#
# Invocation inside the VM:
#   python -m market_tick_data_service \
#     --operation backfill --mode batch --asset-group TRADFI \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE
#
# Default: yesterday only (T-1).
#
# Prerequisites:
#   - Tarballs uploaded: bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group TRADFI
#   - databento-api-key + yahoo-finance access (no key) in Secret Manager
#
# Usage:
#   bash launch-tradfi-forward-poll.sh                       # yesterday (T-1)
#   bash launch-tradfi-forward-poll.sh 2026-04-15 2026-04-18 # explicit window
#   bash launch-tradfi-forward-poll.sh --force ...           # bypass singleton
#
# Cost: e2-standard-4 ~10-20 min per run.
#
# Singleton lock: refuses to launch if any tradfi-fwd-* VM is already running
# in the zone. Databento has per-key cost meters; concurrent VMs duplicate
# downloads. Yahoo Finance has soft IP rate-limits.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false

_positional=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) _positional+=("$1"); shift ;;
  esac
done
set -- "${_positional[@]+"${_positional[@]}"}"  # bash-3-safe empty-array guard under set -u

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

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^tradfi-fwd-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: TradFi VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — Databento has per-key cost meters
and Yahoo Finance soft-rate-limits. Concurrent VMs duplicate downloads
and burn budget.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force ${START_DATE} ${END_DATE}
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="tradfi-fwd-${RUN_TS}"

echo "Launching $VM_NAME: TradFi market-data ${START_DATE}..${END_DATE}"

# VM_TASK=mtds-backfill routes to the chunked MTDS-download branch that builds the
# CLI with --source (REQUIRED for a TradFi OHLCV run). The prior cefi-backfill task
# + missing --source made every forward-poll fail "--source ... is REQUIRED" + write
# 0 rows. VM_SOURCE=databento = the 3-dataset subscription path (GLBX/DBEQ/XCBF);
# VM_DATA_TYPES=ohlcv_1m;ohlcv_1s mirrors the batch backfill (live = batch, same
# data_types; both L0/free, 1s aggregates downstream to 15m/1h/24h).
METADATA="VM_TASK=mtds-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=download"
METADATA="${METADATA},VM_ASSET_GROUP=TRADFI"
METADATA="${METADATA},VM_DATA_TYPES=ohlcv_1m;ohlcv_1s"
METADATA="${METADATA},VM_SOURCE=databento"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if $DRY_RUN; then
  echo "[DRY-RUN] Would create VM: $VM_NAME"
  echo "[DRY-RUN]   project=$PROJECT zone=$ZONE machine=e2-standard-4 disk=50GB"
  echo "[DRY-RUN]   metadata=startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}"
  echo "[DRY-RUN]   labels=purpose=tradfi-forward-poll,env=${DEPLOYMENT_ENV},run-ts=${RUN_TS}"
  echo "[DRY-RUN] No VM created."
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
  --machine-type=e2-standard-4 \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --labels=purpose=tradfi-forward-poll,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete when done: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
