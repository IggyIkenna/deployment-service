#!/usr/bin/env bash
# Launch a short-lived GCE VM that forward-polls DeFi protocols.
#
# Purpose: ingest a single day of DeFi market-data for the operator-expected
# coverage set in EXPECTED_COVERAGE_BY_ASSET_GROUP['defi'] — DEX swaps/pools,
# lending indices, perp funding, oracle prices, gas fees, LST rates, plus
# the Phase-1-expansion event types (liquidation_events, flash_loan_events,
# staking_yields, etc.) across the protocol-chain composite venues
# (UNISWAPV2/3/4, CURVE, BALANCER, AAVEV3, COMPOUNDV3, MORPHO, FLUID, LIDO,
# ETHERFI, ETHENA, JITO).
#
# Same code path as launch-canonical-migration-vm.sh DEFI path / the existing
# DEFI MTDS adapter set — this launcher runs the rolling 1-day pass.
#
# Writes to: gs://market-data-tick-defi-central-element-323112/by_date/...
#
# Invocation inside the VM:
#   python -m market_tick_data_service \
#     --operation backfill --mode batch --asset-group DEFI \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE
#
# Default: yesterday only (T-1).
#
# Prerequisites:
#   - Tarballs uploaded: bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group DEFI
#   - alchemy-api-key + chainlink-feeds + protocol subgraph access in Secret Manager
#   - Instrument index parquet up-to-date in instruments-store-defi-* bucket
#
# Usage:
#   bash launch-defi-forward-poll.sh                       # yesterday (T-1)
#   bash launch-defi-forward-poll.sh 2026-04-15 2026-04-18 # explicit window
#   bash launch-defi-forward-poll.sh --force ...           # bypass singleton
#
# Cost: e2-standard-4 ~15-30 min per run depending on chain breadth.
#
# Singleton lock: refuses to launch if any defi-fwd-* VM is already running
# in the zone. Alchemy + The Graph have per-key compute-unit budgets;
# concurrent VMs hit shared rate ceilings.
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
    --filter='name~"^defi-fwd-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: DeFi VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — Alchemy + The Graph share per-key
compute-unit budgets and concurrent VMs hit rate ceilings.

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
VM_NAME="defi-fwd-${RUN_TS}"

echo "Launching $VM_NAME: DeFi market-data ${START_DATE}..${END_DATE}"

METADATA="VM_TASK=defi-forward-poll"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=backfill"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type=e2-standard-4 \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --labels=purpose=defi-forward-poll,run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete when done: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
