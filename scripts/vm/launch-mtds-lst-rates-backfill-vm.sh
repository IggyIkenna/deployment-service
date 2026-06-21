#!/usr/bin/env bash
# Launch a short-lived GCE VM that backfills DeFi LST exchange-rates via the
# unified MTDS collect-lst-rates CLI.
#
# Purpose: ingest LST/LRT exchange-rates for a date window. Writes to:
#   gs://market-data-tick-defi-central-element-323112/raw_tick_data/
#     by_date/day={D}/category=defi/.../data_type=lst_rates/...
#
# Token coverage (LstRatesHandler._LST_TOKENS):
#   EVM (Ethereum):  stETH, wstETH, rETH, cbETH, sUSDe, sDAI, mETH, swETH,
#                    ETHx, osETH, ankrETH, weETH, pufETH  (13 tokens)
#   Solana:          mSOL (Marinade), jitoSOL (Jito)      (2 tokens)
#
# Runs the unified MTDS CLI via setup-data-pipeline-vm.sh metadata routing —
# same generic VM_TASK code path as the cefi/prediction backfill fleet:
#   python -m market_tick_data_service \
#     --operation collect-lst-rates --mode batch --asset-group DEFI \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE
#
# The handler iterates _LST_TOKENS internally per date, so no --venues /
# --data-types args are needed.
#
# Default: yesterday only (T-1). Pass two dates for an explicit window.
#
# Usage:
#   bash launch-mtds-lst-rates-backfill-vm.sh                       # T-1 single day
#   bash launch-mtds-lst-rates-backfill-vm.sh 2025-01-01 2025-01-31 # explicit window
#   bash launch-mtds-lst-rates-backfill-vm.sh --force ...           # bypass singleton
#
# Cost: e2-standard-4 + 50GB. ~30s/day (one block resolution + 13 eth_calls
# + 2 REST calls). 30-day backfill ~15min, 365-day ~3hr.
#
# Singleton lock: refuses to launch if another mtds-lst-rates-* VM is RUNNING
# in the zone. Alchemy compute-units are shared per-key and concurrent VMs
# burn budget without speedup. --force bypasses for legitimate parallel runs.
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
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"
BOOT_DISK_GB="50"

if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^mtds-lst-rates-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: LST-rates VM already running in $ZONE: $EXISTING
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
VM_NAME="mtds-lst-rates-${RUN_TS}"

echo "Launching $VM_NAME: DeFi LST rates ${START_DATE}..${END_DATE}"

# Metadata follows the cefi-backfill convention — setup-data-pipeline-vm.sh
# routes VM_TASK=cefi-backfill through the generic MTDS CLI assembly (the task
# name is misleadingly category-specific; the routing is category-agnostic).
METADATA="VM_TASK=cefi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=collect-lst-rates"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
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
      --labels=purpose=mtds-lst-rates-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Manifest:    gsutil cp gs://market-data-tick-defi-central-element-323112/_index/availability_index.parquet /tmp/d.parquet"
echo "Inspect:     python -c \"import pandas as pd; df = pd.read_parquet('/tmp/d.parquet'); print(df[df.data_type=='lst_rates']['capture_status'].value_counts())\""
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
