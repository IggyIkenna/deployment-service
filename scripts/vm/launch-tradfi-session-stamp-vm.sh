#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# TradFi OHLCV session-stamp backfill VM launcher.
#
# Walks the TradFi raw-tick-data bucket, reads every OHLCV parquet missing
# `session` and `phase` columns, computes them from `timestamp` via UAC
# `classify_session(venue, dt)` and writes the parquet back. Idempotent —
# blobs already carrying both columns are skipped.
#
# Underlying script: market-tick-data-service/scripts/migrate_tradfi_ohlcv_session_stamps.py
# (already shipped at MTDS@6873955; reads `databento-api-key` is NOT required —
# this migration runs against existing GCS parquets, not Databento).
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Usage:
#   bash launch-tradfi-session-stamp-vm.sh --dry-run
#   bash launch-tradfi-session-stamp-vm.sh
#   bash launch-tradfi-session-stamp-vm.sh --start-date 2024-01-01 --end-date 2026-05-15
#   bash launch-tradfi-session-stamp-vm.sh --venue CME    # scope to one venue
#
# VM prefix `canonical-migration-tradfi-` is registered in vm_zombie_watchdog
# (line 651). e2-standard-8, asia-northeast1-c, self-shutdown on completion.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

# ── Defaults + arg parsing ──
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
START_DATE=""
END_DATE=""
VENUE=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --dry-run)    DRY_RUN=true; shift ;;
        --start-date) START_DATE="$2"; shift 2 ;;
        --end-date)   END_DATE="$2"; shift 2 ;;
        --venue)      VENUE="$2"; shift 2 ;;
        --env)        DEPLOYMENT_ENV="$2"; shift 2 ;;
        --help|-h)
            grep '^#' "$0" | head -30
            exit 0
            ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="canonical-migration-tradfi-sessionstamp-${RUN_TS}"

# Build the command to run on the VM. The canonical-migration dispatch in
# setup-data-pipeline-vm.sh cd's into $WORKSPACE/mtds first, so `python ...`
# resolves relative to that repo root. The setup script rewrites the leading
# `python ` → `$VENV/bin/python ` so we just use bare `python`.
CMD="python scripts/migrate_tradfi_ohlcv_session_stamps.py --project ${PROJECT}"
[[ -n "$START_DATE" ]] && CMD="$CMD --start-date ${START_DATE}"
[[ -n "$END_DATE" ]]   && CMD="$CMD --end-date ${END_DATE}"
[[ -n "$VENUE" ]]      && CMD="$CMD --venue ${VENUE}"
if $DRY_RUN; then
    CMD="$CMD --dry-run"
    MODE="dry"
else
    CMD="$CMD --no-dry-run"
    MODE="full"
fi

echo "Launching $VM_NAME — $CMD"

# Use VM_TASK=canonical-migration so setup-data-pipeline-vm.sh dispatches to the
# same generic migration path used by launch-canonical-migration-vm.sh (cd into
# mtds repo dir + run VM_MIGRATION_CMD verbatim via _launch_with_tee).
md="VM_TASK=canonical-migration"
md="${md},VM_SERVICE=market_tick_data_service"
md="${md},VM_OPERATION=migrate-tradfi-session-stamps"
md="${md},VM_ASSET_GROUP=TRADFI"
md="${md},VM_MIGRATION_CMD=${CMD}"
md="${md},VM_MIGRATION_MODE=${MODE}"
[[ -n "$START_DATE" ]] && md="${md},VM_START_DATE=${START_DATE}"
[[ -n "$END_DATE" ]]   && md="${md},VM_END_DATE=${END_DATE}"
md="${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
md="${md},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
    --zone="$ZONE" \
    --machine-type=e2-standard-8 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
    --labels=purpose=session-stamp-migration,category=tradfi,mode="${MODE}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo "  SSH:     gcloud compute ssh $VM_NAME --zone=$ZONE"
echo "  Log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/$VM_NAME/run.log"
echo "  Delete:  gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo "  Status:  gcloud compute instances describe $VM_NAME --zone=$ZONE --format='value(status)'"
