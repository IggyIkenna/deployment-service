#!/usr/bin/env bash
# Launch a short-lived GCE VM that forward-polls Prediction CLOBs (Polymarket,
# Kalshi).
#
# Purpose: ingest a single day of CLOB trades for the operator-expected
# coverage set in EXPECTED_COVERAGE_BY_ASSET_GROUP['prediction']
# (POLYMARKET, KALSHI). Both venues emit a single canonical data_type
# `trades` (book_snapshot_5 was retired 2026-04-19 because neither adapter
# captures order-book snapshots).
#
# Same code path as launch-mtds-prediction-backfill-vm.sh — both use the
# Polymarket/Kalshi MTDS adapters with shard-level failure isolation. This
# launcher runs the rolling 1-day pass; the historical backfill launcher
# fans out multi-month windows.
#
# Writes to: gs://market-data-tick-prediction-central-element-323112/by_date/...
#
# Invocation inside the VM:
#   python -m market_tick_data_service \
#     --operation backfill --mode batch --asset-group PREDICTION \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE
#
# Default: yesterday only (T-1).
#
# Prerequisites:
#   - Tarballs uploaded: bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group PREDICTION
#   - polymarket-* + kalshi-api-key in Secret Manager
#   - Instrument index parquet up-to-date in instruments-store-prediction-* bucket
#     (per-conditionId rows; "OTHER" instrument_type bucket already populated
#     by polymarket reference adapter for non-mainstream sports props)
#
# Usage:
#   bash launch-prediction-forward-poll.sh                       # yesterday (T-1)
#   bash launch-prediction-forward-poll.sh 2026-04-15 2026-04-18 # explicit window
#   bash launch-prediction-forward-poll.sh --force ...           # bypass singleton
#
# Cost: e2-standard-2 ~5-15 min per run.
#
# Singleton lock: refuses to launch if any prediction-fwd-* VM is already
# running in the zone. The MTDS Polymarket adapter shares the gamma API
# rate-limit per IP — concurrent VMs from the same IP egress thrash on 429s.
# Same protocol as launch-mtds-prediction-backfill-vm.sh.
set -euo pipefail

FORCE=false
if [[ "${1:-}" == "--force" ]]; then
  FORCE=true
  shift
fi

if [[ $# -eq 2 ]]; then
  START_DATE="$1"
  END_DATE="$2"
else
  START_DATE="$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)"
  END_DATE="$START_DATE"
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^prediction-fwd-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: Prediction VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — Polymarket gamma API is per-IP
rate-limited; concurrent VMs from the same egress thrash on 429s
without producing useful data.

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
VM_NAME="prediction-fwd-${RUN_TS}"

echo "Launching $VM_NAME: Prediction CLOB trades ${START_DATE}..${END_DATE}"

METADATA="VM_TASK=prediction-forward-poll"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=backfill"
METADATA="${METADATA},VM_ASSET_GROUP=PREDICTION"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type=e2-standard-2 \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --labels=purpose=prediction-forward-poll,run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete when done: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
