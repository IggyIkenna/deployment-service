#!/usr/bin/env bash
# Launch a short-lived GCE VM that backfills Pyth Hermes archive oracle_prices
# for Solana LST feeds (JitoSOL/USD, mSOL/USD, bSOL/USD, INF/USD).
#
# Covers the Pyth Hermes archive window: 2023-10-01 → today.
# Pre-archive gap (2022-11-01 .. 2023-09-30) is handled by a separate script:
#   launch-mtds-pyth-archive-backfill-vm.sh
#
# Purpose: carry_staked_basis Solana leg requires LST USD prices from
# oracle_prices_handler for the full backtest window. All 4 feeds are
# verified in _PYTH_FEEDS (MTDS@0636dd4 2026-05-14):
#   - JitoSOL/USD: 0x67be9f519b95cf24338801051f9a808eff0a578024dc6081fd0837ff4fe1ccf8
#   - mSOL/USD:    0xc2289a6a43d2ce91c6f55caec370f4acc38a2ed477f58813334c6d03749ff2a4
#   - bSOL/USD:    0x89875379e70f8fbe69ad7f9edc1c4f15b2b5e27b9a84c5e42ddc2f76c04f3dd
#   - INF/USD:     0xe10a78bcff24b64f5e7d38d9e7ee34e6f6ef2c74c7ae43d9cf5caf35a4e0ab9
#
# GCS BACKFILL RULE (MANDATORY): This script covers 7+ months of data.
# DO NOT LAUNCH without operator [ack] in ikenna_orchestrator/pings/slot_2.md.
# See plans/active/solana_lst_native_staking_adapters_2026_05_14.md Phase 4.
# Script is ready; launch is blocked pending operator ping [ack].
#
# Usage:
#   bash launch-mtds-pyth-lst-backfill-vm.sh                         # default: 2023-10-01 → today
#   bash launch-mtds-pyth-lst-backfill-vm.sh 2023-10-01 2026-05-14   # explicit window
#   bash launch-mtds-pyth-lst-backfill-vm.sh --force ...             # bypass singleton
#   bash launch-mtds-pyth-lst-backfill-vm.sh --env staging ...       # staging env
#
# Cost: e2-standard-4 + 50GB. Pyth Hermes rate-limit: 100 req/min free tier.
# 4 feeds × ~960 days = ~3840 requests; expect <1 hour wall-clock.
#
# Singleton lock: refuses to launch if another pyth-lst-backfill-* VM is
# RUNNING in the zone.
#
# Bucket-naming SSOT: env-aware per bucket_name_ssot_canonicalisation_2026_05_10.md.
set -euo pipefail

FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
POSITIONAL=()
DRY_RUN=false
PREEMPTIBLE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)     DRY_RUN=true; shift ;;
        --force)       FORCE=true; shift ;;
        --env)         DEPLOYMENT_ENV="$2"; shift 2 ;;
        --preemptible) PREEMPTIBLE=true; shift ;;
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
    # Default: Pyth Hermes archive start → today (LST feeds).
    START_DATE="2023-10-01"
    END_DATE="$(date -u +%Y-%m-%d)"
else
    cat >&2 <<EOF
Usage: $0 [--force] [--env prod|staging|dev] [START_DATE END_DATE]

Defaults to the Pyth Hermes archive window for LST feeds (2023-10-01 → today).
Pass two YYYY-MM-DD dates for an explicit window.
Pass --force to bypass the singleton lock.
Pass --env to override the env tier (default: \$DEPLOYMENT_ENV or 'prod').

GCS BACKFILL RULE: This covers 7+ months — requires operator [ack] in
ikenna_orchestrator/pings/slot_2.md before launch.
EOF
    exit 1
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
BOOT_DISK_GB="50"

if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^pyth-lst-backfill-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: Pyth-LST-backfill VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate -- Pyth Hermes rate-limit is per-IP.

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
VM_NAME="pyth-lst-backfill-${RUN_TS}"

echo "Launching $VM_NAME: Pyth LST oracle_prices backfill ${START_DATE}..${END_DATE}"

METADATA="VM_TASK=cefi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=collect-oracle-prices"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  PREEMPTIBLE_ARG=""
  $PREEMPTIBLE && PREEMPTIBLE_ARG="--preemptible"
  gcloud compute instances create "$VM_NAME" \
      --project="$PROJECT" \
      --zone="$ZONE" \
      --machine-type="$MACHINE_TYPE" \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size="${BOOT_DISK_GB}GB" \
      --scopes=cloud-platform \
      --no-restart-on-failure \
      ${PREEMPTIBLE_ARG} \
      --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
      --labels=purpose=pyth-lst-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events:      gsutil ls gs://${PROJECT}-events/events/market-tick-data-service/\$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "Manifest:    gsutil cp gs://market-data-tick-defi-${PROJECT}/_index/per_vm/${VM_NAME}.parquet /tmp/per_vm.parquet"
echo "Inspect:     python3 -c \"import pandas as pd; df = pd.read_parquet('/tmp/per_vm.parquet'); m=df[(df.data_type=='oracle_prices')&(df.chain=='SOLANA')]; print(m.groupby('capture_status').size())\""
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
