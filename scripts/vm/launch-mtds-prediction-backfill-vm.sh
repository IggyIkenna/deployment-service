#!/usr/bin/env bash
# Launch a short-lived GCE VM that downloads Polymarket prediction-market trades
# via the unified MTDS download CLI — the canonical replacement for the Apr 14
# one-off `mtds-prediction-backfill` VM (no launcher script existed at the time;
# see memory/project_polymarket_clob_and_bucket_audit_2026_04_14.md).
#
# Purpose: ingest Polymarket trades for a given date window. Writes to:
#   gs://market-data-tick-prediction-central-element-323112/raw_tick_data/
#     by_date/day={D}/instrument_type=BTC|ETH|FOOTBALL|OTHER/data_type=trades/...
#
# Manifest:
#   gs://market-data-tick-prediction-central-element-323112/_index/availability_index.parquet
#
# Runs the unified MTDS CLI via setup-data-pipeline-vm.sh metadata routing —
# same code path as the production CeFi/TradFi backfill fleet:
#   python -m market_tick_data_service \
#     --operation download --mode batch --asset-group PREDICTION \
#     --venues POLYMARKET --data-types trades \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE
#
# Phase B integration (honest-coverage-metrics, 2026-04-19):
#   * MTDS engine/orchestrator.py now records empty_confirmed for conditionIds
#     that didn't trade on the date and attempted_failed for gamma API errors.
#   * Phase A UTL ManifestWriter v5 captures these in availability_index.parquet
#     so PREDICTION attempt_coverage rises from ~23% (capture-only) to ~99%.
#
# Default: yesterday only (T-1). Pass two dates for an explicit window.
#
# Usage:
#   bash launch-mtds-prediction-backfill-vm.sh                       # T-1 single day
#   bash launch-mtds-prediction-backfill-vm.sh 2025-04-01 2025-04-01 # explicit 1-day
#   bash launch-mtds-prediction-backfill-vm.sh 2025-03-14 2025-04-30 # window
#   bash launch-mtds-prediction-backfill-vm.sh --force 2025-04-01 2025-04-01
#                                                                    # bypass singleton lock
#
# Cost: e2-standard-4 + 50GB. Single-day backfill ~10-25 min wall time
# (gamma pagination + per-conditionId fetches; rate limited but not throttled
# as aggressively as RapidAPI SFI).
#
# Singleton lock: refuses to launch if another mtds-prediction-* VM is RUNNING
# in the zone. Polymarket gamma API rate-limits per-IP and concurrent VMs from
# the same project share the egress NAT, so multiple concurrent runs collide
# on 429 backoffs. --force bypasses for legitimate parallel investigations.
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

# Default: yesterday only. Pass two dates for explicit window.
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
MACHINE_TYPE="e2-standard-4"
BOOT_DISK_GB="50"

# ── Singleton lock: gamma API per-IP rate-limit + shared NAT ──
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^mtds-prediction-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: Polymarket VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — Polymarket gamma rate-limits per-IP and
concurrent VMs share the project egress NAT, thrashing on 429 backoffs.

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
VM_NAME="mtds-prediction-${RUN_TS}"

echo "Launching $VM_NAME: Polymarket trades ${START_DATE}..${END_DATE}"

# Metadata follows the cefi-backfill convention — setup-data-pipeline-vm.sh
# routes VM_TASK=cefi-backfill through the generic MTDS CLI assembly (the task
# name is misleadingly category-specific; the routing is category-agnostic).
METADATA="VM_TASK=cefi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=download"
METADATA="${METADATA},VM_ASSET_GROUP=PREDICTION"
METADATA="${METADATA},VM_VENUE=POLYMARKET"
METADATA="${METADATA},VM_DATA_TYPES=trades"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
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
      --labels=purpose=mtds-prediction-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Manifest:    gsutil cp gs://market-data-tick-prediction-central-element-323112/_index/availability_index.parquet /tmp/p.parquet"
echo "Inspect:     python -c \"import pandas as pd; df = pd.read_parquet('/tmp/p.parquet'); print(df['capture_status'].value_counts())\""
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
