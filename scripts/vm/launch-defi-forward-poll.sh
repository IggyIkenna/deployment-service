#!/usr/bin/env bash
# Epic: mtds_mdps_master
# Lifecycle: permanent
# Delete-when: N/A (permanent forward-poll launcher)
#
# DeFi forward-poll launcher — LST-rates live mode (minimal viable).
#
# Implemented 2026-06-21 (was a STUB since 2026-04-28). DeFi adapters use
# per-data-type ``--operation collect-*`` calls; the standard ``--operation
# download`` path used by CeFi/Prediction does not work for DeFi. This launcher
# uses the generic task branch in setup-data-pipeline-vm.sh (any VM_TASK that
# doesn't match a named branch falls through to the generic handler at line 1259
# which reads VM_OPERATION and VM_MODE from metadata).
#
# Runs ``collect-lst-rates --mode live`` for the current day (today, T-0).
# LST-rates is the cheapest DeFi data_type (Alchemy eth_call + Pyth Hermes price
# feeds; ~30 s per run). This proves the defi live pipeline end-to-end and
# produces ≥1 ``pipeline_mode=live_onchain_subgraph`` row in the consolidated
# index market-data-tick-defi-prd-central-element-323112.
#
# To add more data_types: launch additional VMs with VM_OPERATION=collect-<op>.
# Full DeFi-live scheduled fan-out is tracked in
# plans/active/data_completion_to_100_all_ag_2026_06_21.md.
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11.
# DEPLOYMENT_ENV propagated via metadata → DEPLOYMENT_ENV_SHORT resolved inside
# setup-data-pipeline-vm.sh → correct prd/stg/dev bucket tier.
#
# Writes to:
#   gs://market-data-tick-defi-prd-central-element-323112/
#     raw_tick_data/by_date/day=TODAY/
#     pipeline_mode=live_onchain_subgraph/asset_group=defi/...
#
# Catalog-gate requirement: instruments env-less index MUST be fresh in
# gs://instruments-store-defi-{pid} (assert_defi_catalog_fresh preflight).
# The batch backfill already synced this (2026-06-21). Pass
# MANIFEST_CONSOLIDATED_STALENESS_SEC=86400 so the manifest reader uses the
# consolidated index (avoids OOM on per-VM shard merge).
#
# Prerequisites:
#   bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group DEFI
#   alchemy-api-key + chainlink-feeds + protocol subgraph access in Secret Manager
#   Instruments index parquet up-to-date in instruments-store-defi-* bucket
#
# Usage:
#   bash launch-defi-forward-poll.sh              # today (T-0), prod
#   bash launch-defi-forward-poll.sh --dry-run    # print gcloud cmd, no VM
#   bash launch-defi-forward-poll.sh --force      # bypass singleton
#   bash launch-defi-forward-poll.sh --env staging
#
# Cost: e2-standard-8 ~15-30 min per run (LST-rates only).
#
# Singleton lock: refuses to launch if any defi-fwd-* VM is already running
# in the zone. Alchemy + The Graph share per-key compute-unit budgets.
set -euo pipefail

# Script-homes lifecycle marker above. Bucket-naming SSOT: resolve_bucket_name().

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false

_positional=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force)   FORCE=true; shift ;;
    --env)     DEPLOYMENT_ENV="$2"; shift 2 ;;
    *)         _positional+=("$1"); shift ;;
  esac
done
set -- "${_positional[@]}"

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# Default: today (T-0). Live mode targets the current day.
TODAY="$(date -u +%Y-%m-%d)"
START_DATE="${1:-$TODAY}"
END_DATE="${2:-$TODAY}"

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
  Force:     bash $0 --force
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="defi-fwd-${RUN_TS}"

echo "Launching $VM_NAME: DeFi LST-rates live ${START_DATE}..${END_DATE} (env=$DEPLOYMENT_ENV)"

# Generic task handler in setup-data-pipeline-vm.sh (line ~1259):
# elif [ -n "$VM_TASK" ]; then
#   _OP="$VM_OPERATION"
#   _MODE="${VM_MODE_LIVE:-batch}"  ← VM_MODE=live sets this to live
#   CLI_ARGS="--operation $_OP --mode $_MODE --asset-group $VM_ASSET_GROUP ..."
#
# catalog-gate metadata:
#   MANIFEST_CONSOLIDATED_STALENESS_SEC=86400  → use consolidated index (avoids OOM)
#   MANIFEST_PER_VM_SHARDS=true               → write per-vm shard to _index/per_vm/
METADATA="VM_TASK=defi-live-lst"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=collect-lst-rates"
METADATA="${METADATA},VM_MODE=live"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: $VM_NAME"
  echo "[DRY-RUN] metadata: $METADATA"
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
  exit 0
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
  --labels=purpose=defi-forward-poll,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Monitor (T+10min):"
echo "  gcloud compute instances describe $VM_NAME --zone=$ZONE --format='value(status)'"
echo "  gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
