#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a short-lived GCE VM that backfills MARINADE-SOLANA LST rates (mSOL APY)
# via the unified MTDS collect-lst-rates CLI.
#
# Covers the Marinade mainnet window: 2021-02-01 → today.
# Marinade Finance launched February 2021; earlier dates emit record_empty
# with EXPECTED_NOT_YET_LAUNCHED.
#
# Purpose: carry_staked_basis Solana leg requires historical mSOL staking APY.
# The LstRatesHandler._LST_TOKENS includes mSOL (Marinade) alongside EVM tokens;
# this launcher provides a dedicated Marinade-scoped entry point with the correct
# start date and watchdog prefix.
#
# GCS BACKFILL RULE (MANDATORY): ~5 years of data → operator [ack] REQUIRED.
# Ping filed in harsh_orchestrator/pings/slot_2.md (2026-05-14). DO NOT LAUNCH
# without operator [ack]. Script is ready; launch is blocked pending ack.
# See plans/active/solana_lst_native_staking_adapters_2026_05_14.md Phase 6.
#
# Usage:
#   bash launch-marinade-solana-backfill-vm.sh                       # 2021-02-01 → today
#   bash launch-marinade-solana-backfill-vm.sh 2021-02-01 2026-05-14 # explicit window
#   bash launch-marinade-solana-backfill-vm.sh --force ...           # bypass singleton
#   bash launch-marinade-solana-backfill-vm.sh --env staging ...     # staging env
#
# Cost: e2-standard-4 + 50GB. ~5 years × 13 EVM + 2 Solana tokens. Expect 3-6h.
#
# Singleton lock: refuses to launch if another marinade-backfill-* VM is RUNNING
# in the zone to prevent Alchemy compute-unit budget burn.
#
# Bucket-naming SSOT: env-aware per bucket_name_ssot_canonicalisation_2026_05_10.md.
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
    # Default: Marinade Finance mainnet launch → today.
    START_DATE="2021-02-01"
    END_DATE="$(date -u +%Y-%m-%d)"
else
    cat >&2 <<EOF
Usage: $0 [--force] [--env prod|staging|dev] [START_DATE END_DATE]

Defaults to the Marinade Finance mainnet window (2021-02-01 → today).
Pass two YYYY-MM-DD dates for an explicit window.
Pass --force to bypass the singleton lock.
Pass --env to override the env tier (default: \$DEPLOYMENT_ENV or 'prod').

GCS BACKFILL RULE: ~5 years of data — requires operator [ack] before launch.
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
        --filter='name~"^marinade-backfill-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: Marinade backfill VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — Alchemy compute-units are shared per-key.

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
VM_NAME="marinade-backfill-${RUN_TS}"

echo "Launching $VM_NAME: MARINADE-SOLANA lst_rates backfill ${START_DATE}..${END_DATE}"

METADATA="VM_TASK=cefi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=collect-lst-rates"
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
  gcloud compute instances create "$VM_NAME" \
      --project="$PROJECT" \
      --zone="$ZONE" \
      --machine-type="$MACHINE_TYPE" \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size="${BOOT_DISK_GB}GB" \
      --scopes=cloud-platform \
      --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
      --labels=purpose=marinade-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Manifest:    gsutil cp gs://market-data-tick-defi-${PROJECT}/_index/per_vm/${VM_NAME}.parquet /tmp/per_vm.parquet"
echo "Inspect:     python3 -c \"import pandas as pd; df = pd.read_parquet('/tmp/per_vm.parquet'); m=df[df.data_type=='lst_rates']; print(m.groupby(['venue','capture_status']).size())\""
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
