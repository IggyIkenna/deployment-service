#!/usr/bin/env bash
# Phase 5b.3 MDPS backfill VM launcher — re-aggregate tick data into canonical
# processed_candles/ for each category using the strict-mode writer landed in
# MDPS commit c1cb73c (canonical_writer.py wrapping StreamingParquetWriter +
# SchemaContract + ManifestWriter).
#
# Input buckets:    gs://market-data-tick-{category}-*/raw_tick_data/by_date/
# Output subtree:   gs://market-data-tick-{category}-*/processed_candles/by_date/
#                       day=/timeframe=/data_type=/instrument_type=/venue=/
# Manifest:         gs://market-data-tick-{category}-*/_index/availability_index.parquet
#
# Scope per category (per plan Phase 5b.3):
#   CeFi:       2020-01-01 → 2026-04-18
#   TradFi:     2020-01-01 → 2026-04-18 (1m pass-through + re-aggregated higher)
#   DeFi:       2020-01-01 → 2026-04-18
#   Sports:     2019-01-01 → 2026-04-18
#   Prediction: 2025-03-14 → 2026-04-18 (Polymarket only)
#
# Operational safety:
#   * 50 GB boot disk (default 10 GB causes OOM on long ranges per Agent 4 note).
#   * DEPLOYMENT_STARTED/PROGRESS/COMPLETED events via deployment_heartbeat.py.
#   * stdout/stderr tee'd to GCS via vm-exec-with-gcs-tee.sh.
#   * asia-northeast1-c to stay in-region with the source tick buckets.
#   * Dry-run supported so the launcher can be tested without mutating GCS.
#
# Usage:
#   bash launch-mdps-backfill-vm.sh cefi       2020-01-01 2026-04-18 dry
#   bash launch-mdps-backfill-vm.sh tradfi     2020-01-01 2026-04-18 full
#   bash launch-mdps-backfill-vm.sh defi       2020-01-01 2026-04-18 full
#   bash launch-mdps-backfill-vm.sh sports     2019-01-01 2026-04-18 full
#   bash launch-mdps-backfill-vm.sh prediction 2025-03-14 2026-04-18 full
#   bash launch-mdps-backfill-vm.sh all        2020-01-01 2026-04-18 full

set -euo pipefail

CATEGORY="${1:-}"
START_DATE="${2:-}"
END_DATE="${3:-}"
MODE="${4:-dry}"  # dry | full

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"
MACHINE_TYPE="e2-standard-8"
BOOT_DISK_GB="50"

if [[ -z "$CATEGORY" || -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "Usage: $0 <cefi|tradfi|defi|sports|prediction|all> <start-date> <end-date> [dry|full]"
    exit 2
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"

_category_flag_for() {
    case "$1" in
        cefi)       echo "--CEFI" ;;
        tradfi)     echo "--TRADFI" ;;
        defi)       echo "--DEFI" ;;
        sports)     echo "--SPORTS" ;;
        prediction) echo "--PREDICTION" ;;
        *) echo ""; return 1 ;;
    esac
}

_launch() {
    local cat="$1"
    local flag; flag="$(_category_flag_for "$cat")" || { echo "Unknown category: $cat"; return 1; }
    local vm_name="mdps-backfill-${cat}-${RUN_TS}"

    # Strict-mode canonical write path is unconditional on the service side
    # (MDPS commit c1cb73c); the backfill CLI invokes the same service code.
    local cmd="python -m market_data_processing_service process"
    cmd="$cmd --start-date $START_DATE --end-date $END_DATE $flag"
    if [[ "$MODE" == "dry" ]]; then
        cmd="$cmd --dry-run"
    fi

    echo "Launching $vm_name"
    echo "  cmd: $cmd"
    echo "  zone: $ZONE, machine: $MACHINE_TYPE, boot: ${BOOT_DISK_GB}G"

    local md="VM_TASK=mdps-backfill"
    md="${md},VM_SERVICE=market-data-processing-service"
    md="${md},VM_OPERATION=backfill-${cat}"
    md="${md},VM_CATEGORY=$(echo "$cat" | tr '[:lower:]' '[:upper:]')"
    md="${md},VM_START_DATE=${START_DATE}"
    md="${md},VM_END_DATE=${END_DATE}"
    md="${md},VM_BACKFILL_CMD=${cmd}"
    md="${md},VM_BACKFILL_MODE=${MODE}"

    gcloud compute instances create "$vm_name" \
        --project="$PROJECT" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        --boot-disk-size="${BOOT_DISK_GB}GB" \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --scopes=cloud-platform \
        --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
        --labels=purpose=mdps-backfill,category="${cat}",mode="${MODE}",run-ts="${RUN_TS}"
    echo "  SSH: gcloud compute ssh $vm_name --zone=$ZONE"
    echo "  Delete: gcloud compute instances delete $vm_name --zone=$ZONE --quiet"
    echo ""
}

case "$CATEGORY" in
    cefi|tradfi|defi|sports|prediction) _launch "$CATEGORY" ;;
    all)
        _launch cefi
        _launch tradfi
        _launch defi
        _launch sports
        _launch prediction
        ;;
    *) echo "Unknown category: $CATEGORY"; exit 2 ;;
esac

echo "Run timestamp: $RUN_TS"
echo "Mode: $MODE (dry = --dry-run; full = live writes)"
echo ""
echo "Reminder: post-backfill, run manifest reconciliation via:"
echo "  python -c \"from unified_trading_library.manifest_writer import rebuild_manifest_from_canonical_paths; \\"
echo "    rebuild_manifest_from_canonical_paths('market-data-tick-${CATEGORY}-central-element-323112', \\"
echo "      service_name='market-data-processing-service', prefix='processed_candles/by_date')\""
