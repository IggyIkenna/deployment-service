#!/usr/bin/env bash
# Phase 3.4 canonical-migration VM launcher — rewrite historical GCS data to
# the canonical partition layout for each category. Each VM runs
# migrate_{category}_canonical.py in dry-run OR full mode.
#
# Prefer DRY-RUN first: observer sees the planned rewrites in VM logs before
# committing to actual mutations. After reviewing a dry-run run log, re-fire
# without --dry-run to do the actual migration.
#
# Usage:
#   bash launch-canonical-migration-vm.sh cefi       2020-01-01 2024-12-31 dry
#   bash launch-canonical-migration-vm.sh tradfi     2023-01-01 2024-12-31 dry
#   bash launch-canonical-migration-vm.sh defi       2023-01-01 2024-12-31 dry
#   bash launch-canonical-migration-vm.sh prediction 2025-03-14 2026-04-18 dry
#   bash launch-canonical-migration-vm.sh all        2020-01-01 2024-12-31 dry
#   bash launch-canonical-migration-vm.sh cefi       2020-01-01 2024-12-31 full
#
# Boot disk: 50GB (MDPS/features launchers' default; 10GB default was
# causing disk-pressure OOMs on long ranges).
set -euo pipefail

CATEGORY="${1:-}"
START_DATE="${2:-}"
END_DATE="${3:-}"
MODE="${4:-dry}"  # dry | full
ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"
BOOT_DISK_GB="${BOOT_DISK_GB:-50}"

if [[ -z "$CATEGORY" || -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "Usage: $0 <cefi|tradfi|defi|prediction|all> <start-date> <end-date> [dry|full]"
    exit 2
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
RUN_TS_LABEL="$(date +%Y%m%d-%H%M%S)"

_script_for() {
    case "$1" in
        cefi)       echo "python scripts/migrate_cefi_v2.py --start-date $START_DATE --end-date $END_DATE --workers 32" ;;
        tradfi)     echo "python -m market_tick_data_service.scripts.migrate_tradfi_canonical --start-date $START_DATE --end-date $END_DATE --workers 32" ;;
        defi)       echo "python -m market_tick_data_service.scripts.migrate_defi_canonical --buckets all --start-date $START_DATE --end-date $END_DATE --workers 32" ;;
        prediction) echo "python -m market_tick_data_service.scripts.migrate_polymarket_canonical --start-date $START_DATE --end-date $END_DATE --workers 32" ;;
        *) echo ""; return 1 ;;
    esac
}

_launch() {
    local cat="$1"
    local vm_name="canonical-migration-${cat}-${RUN_TS}"
    local cmd; cmd="$(_script_for "$cat")"
    [[ -z "$cmd" ]] && { echo "Unknown category: $cat"; return 1; }
    if [[ "$MODE" == "dry" ]]; then cmd="$cmd --dry-run"; fi

    echo "Launching $vm_name — $cmd"
    local md="VM_TASK=canonical-migration"
    md="${md},VM_SERVICE=market_tick_data_service"
    md="${md},VM_OPERATION=migrate-${cat}"
    md="${md},VM_CATEGORY=$(echo "$cat" | tr '[:lower:]' '[:upper:]')"
    md="${md},VM_START_DATE=${START_DATE}"
    md="${md},VM_END_DATE=${END_DATE}"
    md="${md},VM_MIGRATION_CMD=${cmd}"
    md="${md},VM_MIGRATION_MODE=${MODE}"

    gcloud compute instances create "$vm_name" \
        --project="$PROJECT" \
        --zone="$ZONE" \
        --machine-type=e2-standard-8 \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size="${BOOT_DISK_GB}GB" \
        --scopes=cloud-platform \
        --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
        --labels=purpose=canonical-migration,category="${cat}",mode="${MODE}",run-ts="${RUN_TS_LABEL}"
    echo "  SSH: gcloud compute ssh $vm_name --zone=$ZONE"
    echo "  Delete: gcloud compute instances delete $vm_name --zone=$ZONE --quiet"
}

case "$CATEGORY" in
    cefi|tradfi|defi|prediction) _launch "$CATEGORY" ;;
    all)
        _launch cefi
        _launch tradfi
        _launch defi
        _launch prediction
        ;;
    *) echo "Unknown category: $CATEGORY"; exit 2 ;;
esac

echo ""
echo "Run timestamp: $RUN_TS"
echo "Mode: $MODE (dry = --dry-run; full = live writes)"
