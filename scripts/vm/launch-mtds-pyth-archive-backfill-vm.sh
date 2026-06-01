#!/usr/bin/env bash
# Launch a short-lived GCE VM that backfills Pyth Hermes archive SOL/USD
# (and other Solana feeds) for the pre-archive gap window 2022-11 -> 2023-10.
#
# Purpose: carry_staked_basis Solana leg requires jitoSOL/mSOL on-chain USD
# valuation for batch backtest. Pyth Hermes archive
# (https://hermes.pyth.network/v2/updates/price/{publish_time}) starts
# ~2023-10-01 per UAC ORACLE_COVERAGE_START SSOT (UAC@3adee82 2026-05-08).
# Pre-2023-10 timestamps return 404 "Update data not found", verified live
# via Tab 1 sub-agent probes 2026-05-08.
#
# Source cascade (oracle_prices_handler):
#   1. Pyth Hermes /v2/updates/price/{ts} -- 2023-10-01 onward (preferred).
#   2. Pythnet historical RPC (https://pythnet.rpcpool.com or similar) --
#      pre-2023-10 fallback for SOL/USD price account on-chain reads.
#      Free tier 100 req/min; expect 3-7 days wall-clock for full gap.
#   3. CoinGecko historical /coins/solana/history (daily granularity) --
#      free fallback if Pythnet unavailable.
#
# Birdeye paid-tier ($50/mo enterprise) is the alternative-fast path but
# requires operator API-key provisioning + Secret Manager wiring. Not
# auto-launched until operator approves -- see
# unified-trading-pm/plans/active/defi_master.md
# (Pyth Hermes coverage SSOT todo + operator decision pending).
#
# Writes to: gs://market-data-tick-defi-${PID}/raw_tick_data/
#   by_date/day={D}/category=defi/.../data_type=oracle_prices/
#     chain=SOLANA/protocol=pyth/...
#
# Default: archive gap window 2022-11-01 .. 2023-09-30 (jitoSOL genesis
# through Hermes archive start). Pass two dates for an explicit window.
#
# Usage:
#   bash launch-mtds-pyth-archive-backfill-vm.sh                       # default gap window
#   bash launch-mtds-pyth-archive-backfill-vm.sh 2022-11-01 2023-09-30 # explicit window
#   bash launch-mtds-pyth-archive-backfill-vm.sh --force ...           # bypass singleton
#
# Cost: e2-standard-4 + 50GB. Pythnet RPC 100 req/min cap means ~14k req/day
# slot; SOL/USD = 1 feed = ~1 req per noon-UTC sample = ~333 days backfill
# in <30 min. Wall-clock dominated by RPC scheduling; expect 1-2 hours for
# the 11-month gap.
#
# Singleton lock: refuses to launch if another mtds-pyth-archive-* VM is
# RUNNING in the zone -- Pythnet RPC pool keys are per-IP rate-limited and
# concurrent VMs can race. --force bypasses for legitimate parallel runs
# (e.g. operator-approved Birdeye + Pythnet parallel paths).
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
POSITIONAL=()
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true; shift ;;
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
    # Default: jitoSOL genesis (2022-11-01 per LST_TOKEN_GENESIS) through
    # Hermes archive start (2023-09-30, the day before ORACLE_COVERAGE_START).
    START_DATE="2022-11-01"
    END_DATE="2023-09-30"
else
    cat >&2 <<EOF
Usage: $0 [--force] [--env prod|staging|dev] [START_DATE END_DATE]

Defaults to the Pyth Hermes archive gap window (2022-11-01 .. 2023-09-30).
Pass two YYYY-MM-DD dates for an explicit window.
Pass --force to bypass the singleton lock.
Pass --env to override the env tier (default: \$DEPLOYMENT_ENV or 'prod').
EOF
    exit 1
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="e2-standard-4"
BOOT_DISK_GB="50"

if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^mtds-pyth-archive-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: Pyth-archive VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate -- Pythnet RPC pool keys are per-IP
rate-limited and concurrent VMs race on the cap.

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
VM_NAME="mtds-pyth-archive-${RUN_TS}"

echo "Launching $VM_NAME: Pyth Hermes archive backfill ${START_DATE}..${END_DATE}"

# Metadata follows the cefi-backfill convention -- setup-data-pipeline-vm.sh
# routes VM_TASK=cefi-backfill through the generic MTDS CLI assembly (the task
# name is misleadingly category-specific; the routing is category-agnostic).
METADATA="VM_TASK=cefi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=collect-oracle-prices"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
# Per-VM shard isolation per CLAUDE.md "Per-VM shard isolation for
# concurrent backfills". The consolidator daemon merges per-VM shards
# into the canonical manifest with last-writer-wins on row_key.
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  gcloud compute instances create "$VM_NAME" \
      --project="$PROJECT" \
      --zone="$ZONE" \
      --machine-type="$MACHINE_TYPE" \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size="${BOOT_DISK_GB}GB" \
      --scopes=cloud-platform \
      --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
      --labels=purpose=mtds-pyth-archive-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events:      gsutil ls gs://${PROJECT}-events/events/market-tick-data-service/\$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "Manifest:    gsutil cp gs://market-data-tick-defi-${PROJECT}/_index/per_vm/${VM_NAME}.parquet /tmp/per_vm.parquet"
echo "Inspect:     python3 -c \"import pandas as pd; df = pd.read_parquet('/tmp/per_vm.parquet'); m=df[(df.data_type=='oracle_prices')&(df.chain=='SOLANA')]; print(m.groupby('capture_status').size())\""
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
