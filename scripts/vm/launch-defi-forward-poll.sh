#!/usr/bin/env bash
# Epic: mtds_mdps_master
# Lifecycle: permanent
# Delete-when: N/A (permanent forward-poll launcher)
#
# DeFi forward-poll launcher — parameterized over the MTDS DeFi operation.
#
# Implemented 2026-06-21 (was a STUB since 2026-04-28); parameterized over
# --operation 2026-06-22. DeFi adapters use per-data-type ``--operation
# collect-*`` calls; the standard ``--operation download`` path used by
# CeFi/Prediction does not work for DeFi. This launcher uses the generic task
# branch in setup-data-pipeline-vm.sh (any VM_TASK that doesn't match a named
# branch falls through to the generic handler which reads VM_OPERATION + VM_MODE
# from metadata and runs ``--operation $VM_OPERATION --mode $VM_MODE``).
#
# Runs ``$VM_OPERATION --mode live`` for the current day (today, T-0). Supported
# operations (each MTDS handler tags pipeline_mode=live_* on --mode live):
#   collect-lst-rates       (default; LST APRs — daily-cadence is fine)
#   collect-dex-swaps       (DEX swap events — PRICE-SENSITIVE, high-freq)
#   collect-dex-pools       (DEX pool state  — PRICE-SENSITIVE, high-freq)
#   collect-oracle-prices   (Chainlink/Pyth  — PRICE-SENSITIVE, high-freq)
# Produces ≥1 ``pipeline_mode=live_*`` row in the consolidated index
# market-data-tick-defi-prd-central-element-323112.
#
# The PRICE-SENSITIVE ops run on a */5 cadence via Cloud Scheduler
# (terraform/gcp/defi_forward_poll_scheduler.tf). lst-rates/lending-indices stay
# daily. Full DeFi-live scheduled fan-out is tracked in
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
#   bash launch-defi-forward-poll.sh                                  # collect-lst-rates, T-0, prod
#   bash launch-defi-forward-poll.sh --operation collect-oracle-prices
#   bash launch-defi-forward-poll.sh --operation collect-dex-swaps --dry-run
#   bash launch-defi-forward-poll.sh --operation collect-dex-pools --force
#   bash launch-defi-forward-poll.sh --operation collect-oracle-prices --env staging
#
# Cost: e2-standard-8 ~15-30 min per run.
#
# Singleton lock: PER-OPERATION — refuses to launch if a defi-fwd-<op>-* VM is
# already running in the zone (so the three price-sensitive ops run concurrently
# but never two of the SAME op). Alchemy + The Graph share per-key compute-unit
# budgets, so a same-op duplicate would hit rate ceilings.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

# Script-homes lifecycle marker above. Bucket-naming SSOT: resolve_bucket_name().

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false
# Default operation = LST-rates (the original behaviour). The price-sensitive
# DeFi data_types for the live arb/paper engine are collect-dex-swaps /
# collect-dex-pools / collect-oracle-prices (run on a high-frequency cadence
# by the Cloud Scheduler in terraform/gcp/defi_forward_poll_scheduler.tf).
VM_OPERATION="collect-lst-rates"

_positional=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)   DRY_RUN=true; shift ;;
    --force)     FORCE=true; shift ;;
    --env)       DEPLOYMENT_ENV="$2"; shift 2 ;;
    --operation) VM_OPERATION="$2"; shift 2 ;;
    *)           _positional+=("$1"); shift ;;
  esac
done
set -- "${_positional[@]}"

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# Closed set of DeFi forward-poll operations (each maps to an MTDS handler that
# accepts --mode live and tags pipeline_mode=live_* on its writes).
case "$VM_OPERATION" in
  collect-lst-rates|collect-dex-swaps|collect-dex-pools|collect-oracle-prices) ;;
  *) echo "ERROR: --operation must be one of collect-lst-rates/collect-dex-swaps/collect-dex-pools/collect-oracle-prices (got: $VM_OPERATION)" >&2; exit 1 ;;
esac
# Short slug for the VM name + singleton lock key (must stay <= the GCE name budget).
_OP_SLUG="${VM_OPERATION#collect-}"   # lst-rates / dex-swaps / dex-pools / oracle-prices

# Default: today (T-0). Live mode targets the current day.
TODAY="$(date -u +%Y-%m-%d)"
START_DATE="${1:-$TODAY}"
END_DATE="${2:-$TODAY}"

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

if ! $FORCE; then
  # Singleton lock is PER-OPERATION: refuse only if the SAME op is already
  # running (so dex-swaps + dex-pools + oracle-prices can run concurrently, but
  # never two dex-swaps). VM name prefix encodes the op slug: defi-fwd-<slug>-.
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^defi-fwd-${_OP_SLUG}-\" AND status=RUNNING" \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: DeFi VM already running for operation ${VM_OPERATION} in $ZONE: $EXISTING
Refusing to launch a duplicate of the SAME op — Alchemy + The Graph share
per-key compute-unit budgets and concurrent same-op VMs hit rate ceilings.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Force:     bash $0 --force

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
VM_NAME="defi-fwd-${_OP_SLUG}-${RUN_TS}"

echo "Launching $VM_NAME: DeFi ${VM_OPERATION} live ${START_DATE}..${END_DATE} (env=$DEPLOYMENT_ENV)"

# Generic task handler in setup-data-pipeline-vm.sh (line ~1259):
# elif [ -n "$VM_TASK" ]; then
#   _OP="$VM_OPERATION"
#   _MODE="${VM_MODE_LIVE:-batch}"  ← VM_MODE=live sets this to live
#   CLI_ARGS="--operation $_OP --mode $_MODE --asset-group $VM_ASSET_GROUP ..."
#
# catalog-gate metadata:
#   MANIFEST_CONSOLIDATED_STALENESS_SEC=86400  → use consolidated index (avoids OOM)
#   MANIFEST_PER_VM_SHARDS=true               → write per-vm shard to _index/per_vm/
METADATA="VM_TASK=defi-live-${_OP_SLUG}"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=${VM_OPERATION}"
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
  --labels=purpose=defi-forward-poll,operation="${_OP_SLUG}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Monitor (T+10min):"
echo "  gcloud compute instances describe $VM_NAME --zone=$ZONE --format='value(status)'"
echo "  gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
