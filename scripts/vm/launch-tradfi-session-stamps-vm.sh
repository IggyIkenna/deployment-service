#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# TradFi OHLCV session/phase backfill VM launcher.
#
# Walks the TradFi raw-tick bucket, reads every OHLCV parquet missing
# session/phase columns, and re-writes it with classify_session(venue, ts)
# stamps. Idempotent: parquets that already have both columns are skipped.
#
# Plan: tradfi_master.md § "Databento session-type awareness" +
#       work_split_2026_05_15_ikenna.md slot 5 item 2.
# Script: market-tick-data-service/scripts/migrate_tradfi_ohlcv_session_stamps.py
# VM prefix: canonical-migration-tradfi-session-stamps-* (matches existing
#            canonical-migration-tradfi- registration in VM_PREFIX_TO_BUCKET).
#
# Usage:
#   bash launch-tradfi-session-stamps-vm.sh 2024-01-01 2026-05-14 dry
#   bash launch-tradfi-session-stamps-vm.sh 2024-01-01 2026-05-14 full
#   bash launch-tradfi-session-stamps-vm.sh 2024-01-01 2026-05-14 full CME
#
# Boot disk: 50GB (parquets read into RAM during transform).
set -euo pipefail

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done
set -- "${POSITIONAL[@]:-}"

START_DATE="${1:-}"
END_DATE="${2:-}"
MODE="${3:-dry}"        # dry | full
VENUE_FILTER="${4:-}"   # optional single-venue scope (e.g. CME)
ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
BOOT_DISK_GB="${BOOT_DISK_GB:-50}"

if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "Usage: $0 [--env prod|staging|dev] <start-date> <end-date> [dry|full] [venue]"
    exit 2
fi

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="canonical-migration-tradfi-session-stamps-${RUN_TS}"

# Build the migration command. Default is --dry-run via script default; full
# mode adds --no-dry-run explicitly. UTL's `_find_workspace_root` walks up
# from `unified_trading_library/cloud_interface/bucket_naming.py` looking for
# a `deployment-service/` sibling — but the VM tarball extracts deployment to
# `$WORKSPACE/deployment/` (no `-service` suffix), so the SSOT yaml is not
# auto-found. Override via env var.
CMD="UNIFIED_TRADING_CLOUD_PROVIDERS_YAML=/home/ikennaigboaka/workspace/deployment/configs/cloud-providers.yaml"
CMD="$CMD python scripts/migrate_tradfi_ohlcv_session_stamps.py"
CMD="$CMD --project ${PROJECT}"
CMD="$CMD --start-date ${START_DATE}"
CMD="$CMD --end-date ${END_DATE}"
if [[ -n "$VENUE_FILTER" ]]; then
    CMD="$CMD --venue ${VENUE_FILTER}"
fi
if [[ "$MODE" == "full" ]]; then
    CMD="$CMD --no-dry-run"
else
    CMD="$CMD --dry-run"
fi

echo "Launching $VM_NAME — $CMD"

# VM_TASK=canonical-migration routes through setup-data-pipeline-vm.sh's
# canonical-migration branch (reads VM_MIGRATION_CMD metadata, runs in mtds
# venv). Re-use the existing branch rather than adding a new task type.
md="VM_TASK=canonical-migration"
md="${md},VM_SERVICE=market_tick_data_service"
md="${md},VM_OPERATION=migrate-tradfi-session-stamps"
md="${md},VM_ASSET_GROUP=TRADFI"
md="${md},VM_START_DATE=${START_DATE}"
md="${md},VM_END_DATE=${END_DATE}"
md="${md},VM_MIGRATION_CMD=${CMD}"
md="${md},VM_MIGRATION_MODE=${MODE}"
md="${md},GCP_PROJECT_ID=${PROJECT}"
md="${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
md="${md},VM_SHUTDOWN_ON_COMPLETION=true"

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-standard-8 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
    --labels=purpose=session-stamps,asset-group=tradfi,mode="${MODE}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM:       $VM_NAME"
echo "Mode:     $MODE (dry = --dry-run; full = live writes)"
echo "Range:    ${START_DATE} → ${END_DATE}"
[[ -n "$VENUE_FILTER" ]] && echo "Venue:    $VENUE_FILTER"
echo "SSH:      gcloud compute ssh $VM_NAME --zone=$ZONE"
echo "Logs:     gcloud compute instances get-serial-port-output $VM_NAME --zone=$ZONE | tail -200"
echo "Delete:   gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
