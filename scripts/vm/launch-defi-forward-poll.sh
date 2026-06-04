#!/usr/bin/env bash
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
# Applied to the future-template below (the stub above exits before reaching it).
#
# DeFi forward-poll launcher — STUB.
#
# Status (2026-04-28): NOT FUNCTIONAL on the standard ``--operation download``
# CLI path. DeFi MTDS adapters expose 19 separate ``--operation collect-*``
# choices (collect-gas-fees, collect-oracle-prices, collect-dex-pools,
# collect-dex-swaps, collect-lending-indices, collect-perp-funding,
# collect-lst-rates, collect-liquidations, collect-liquidation-events,
# collect-flash-loan-events, collect-staking-yields, collect-position-data,
# collect-token-transfers, collect-bridge-events, collect-governance-events,
# collect-mev-events, collect-eigenlayer-rewards, collect-solana-defi,
# collect-evm-defi). A single-VM forward-poll for DeFi therefore needs to
# fan out a sequence of ``collect-*`` operations per protocol/chain/data_type
# (or ``setup-data-pipeline-vm.sh`` needs a new task branch that loops).
#
# Today the DeFi rolling-poll story is: features-onchain-service runs
# ``collect-*`` ops on its own cadence (per protocol). This stub stays in
# place as a placeholder so the launcher inventory remains symmetric across
# asset_groups; revisit once a unified DeFi forward-poll task lands in
# setup-data-pipeline-vm.sh.
#
# Purpose (when functional): ingest a single day of DeFi market-data for
# the operator-expected coverage set in
# EXPECTED_COVERAGE_BY_ASSET_GROUP['defi'] — DEX swaps/pools, lending
# indices, perp funding, oracle prices, gas fees, LST rates, plus the
# Phase-1-expansion event types (liquidation_events, flash_loan_events,
# staking_yields, etc.) across the protocol-chain composite venues
# (UNISWAP_V2/3/4, CURVE, BALANCER, AAVE_V3, COMPOUND_V3, MORPHO, FLUID, LIDO,
# ETHERFI, ETHENA, JITO).
#
# Same code path as launch-canonical-migration-vm.sh DEFI path / the existing
# DEFI MTDS adapter set — this launcher runs the rolling 1-day pass.
#
# Writes to: gs://market-data-tick-defi-prd-central-element-323112/by_date/...
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

cat >&2 <<'STUB'
ERROR: launch-defi-forward-poll.sh is a STUB.

DeFi adapters require per-data-type ``--operation collect-*`` calls (19
separate operation choices) — the standard ``--operation download`` path
used by the other forward-poll launchers does not work for DEFI. A unified
DeFi forward-poll task needs to land in setup-data-pipeline-vm.sh first.

Until then, use features-onchain-service per-protocol cadence (its
existing collect-* runs cover the rolling window).
STUB
exit 1

# ─────────── unreachable below — kept as the future template ───────────

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false

_positional=()
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) _positional+=("$1"); shift ;;
  esac
done
set -- "${_positional[@]}"

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
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-standard-4 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=defi-forward-poll,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete when done: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
