#!/usr/bin/env bash
# Launch a short-lived GCE VM that backfills per-chain gas-fee history via the
# unified MTDS collect-gas-fees CLI.
#
# Purpose: ingest per-block base + priority gas fees for every chain in
# DEFAULT_GAS_FEE_CHAINS. Writes to:
#   gs://gas-fees-central-element-323112/raw_tick_data/by_date/day={D}/
#     asset_group=defi/venue={CHAIN}/chain={CHAIN}/
#     instrument_type=spot_asset/data_type=gas_fees/...
#
# Chain coverage (gas_fee_handler.DEFAULT_GAS_FEE_CHAINS):
#   Tier 1+2 (Alchemy):  ETHEREUM, OPTIMISM, BSC, POLYGON, BASE, ARBITRUM,
#                        AVALANCHE, LINEA  (8 chains)
#   Tier 4 (public RPC): FANTOM, METIS, MOONBEAM, MANTLE, CELO, AURORA
#                        (6 chains, defi_pipeline_extension_followups Phase 5)
#
# Per-chain mainnet-genesis dates clip pre-existing days (UAC SSOT
# GAS_FEE_CHAIN_START_DATES). Date-range smaller than any chain's start
# is skipped without burning RPC quota.
#
# Usage:
#   bash launch-mtds-gas-fees-backfill-vm.sh                       # T-1 single day
#   bash launch-mtds-gas-fees-backfill-vm.sh 2025-01-01 2025-01-31 # explicit window
#   bash launch-mtds-gas-fees-backfill-vm.sh --force ...           # bypass singleton
#
# Cost: e2-standard-4 + 50GB. ~30s/chain/day for Tier 1+2 (block-fees endpoint
# fast on Alchemy archival). Tier-4 public RPCs are slower (~60-120s each).
# Single-day across 14 chains: ~5-15min.
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --) shift; while [[ $# -gt 0 ]]; do POSITIONAL+=("$1"); shift; done; break ;;
        -*) echo "ERROR: unknown flag '$1'" >&2; exit 1 ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ ${#POSITIONAL[@]} -eq 2 ]]; then
    START_DATE="${POSITIONAL[0]}"
    END_DATE="${POSITIONAL[1]}"
elif [[ ${#POSITIONAL[@]} -eq 0 ]]; then
    START_DATE="$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)"
    END_DATE="$START_DATE"
else
    cat >&2 <<EOF
Usage: $0 [--force] [--env prod|staging|dev] [START_DATE END_DATE]

Defaults to yesterday (T-1). Pass two YYYY-MM-DD dates for an explicit window.
Pass --force to bypass the singleton lock.
Pass --env to override the env tier (default: \$DEPLOYMENT_ENV or 'prod').
EOF
    exit 1
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"
MACHINE_TYPE="e2-standard-4"
BOOT_DISK_GB="50"

if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^mtds-gas-fees-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: gas-fees VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — Alchemy compute-units are shared per-key
and concurrent VMs burn budget without speedup.

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
VM_NAME="mtds-gas-fees-${RUN_TS}"

echo "Launching $VM_NAME: gas-fees ${START_DATE}..${END_DATE}"

# Metadata follows the cefi-backfill convention — setup-data-pipeline-vm.sh
# routes VM_TASK=cefi-backfill through the generic MTDS CLI assembly (the task
# name is misleadingly category-specific; the routing is category-agnostic).
METADATA="VM_TASK=cefi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=collect-gas-fees"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=mtds-gas-fees-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Manifest:    gsutil cp gs://gas-fees-central-element-323112/_index/availability_index.parquet /tmp/g.parquet"
echo "Inspect:     python -c \"import pandas as pd; df = pd.read_parquet('/tmp/g.parquet'); print(df[df.data_type=='gas_fees'].groupby('chain')['capture_status'].value_counts())\""
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
