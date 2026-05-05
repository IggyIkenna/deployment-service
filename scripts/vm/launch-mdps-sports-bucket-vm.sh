#!/usr/bin/env bash
# MDPS sports bucket-pass VM launcher (Pass K of sports_predictions_e2e_2026_05_05).
#
# Runs reprocess_sports_odds.py over a date slice on a same-region GCE VM,
# producing per-(league_id, horizon) bucketed parquets at:
#   processed/by_date/day=D/data_type=odds_horizon_bucket/
#       league_id=L/timeframe=H/bucketed.parquet
# and emitting per-shard manifest rows so FSS / honest-coverage can query
# specific (date, league_id, timeframe) tuples directly.
#
# Why a dedicated launcher: the existing launch-canonical-migration-vm.sh
# handles the MTDS legacy→canonical re-key, not the MDPS bucket adapter.
# Pass K is the downstream step that runs on top of the migrated data.
#
# Multi-VM rollout (target <1hr total wall-clock):
#   Shard the full 2020-06-06 → 2026-04-14 range across 4-6 VMs, each on
#   a date slice. Per-VM throughput at --workers 16 ≈ 250-350 days/hour;
#   with 4 VMs that puts the slowest slice well under the 1hr ceiling.
#   Example slicing for the 2,139-day full range:
#     bash launch-mdps-sports-bucket-vm.sh 2020-06-06 2021-12-31 force
#     bash launch-mdps-sports-bucket-vm.sh 2022-01-01 2023-06-30 force
#     bash launch-mdps-sports-bucket-vm.sh 2023-07-01 2024-12-31 force
#     bash launch-mdps-sports-bucket-vm.sh 2025-01-01 2026-04-14 force
#
# Boot disk: 50GB (matches sibling MDPS launchers; 10GB default OOMs on
# long ranges per Agent 4 note in launch-mdps-backfill-vm.sh).
#
# Usage:
#   bash launch-mdps-sports-bucket-vm.sh <start-date> <end-date> [dry|full|force]
#     dry   — --dry-run, no GCS writes
#     full  — live writes, manifest pre-flight skip enabled (resume-friendly)
#     force — live writes, --force (re-bucket even already-bucketed days;
#             needed once after the 2026-05-05 layout refactor that split
#             the monolithic bucketed.parquet into per-(league,horizon))
set -euo pipefail

START_DATE="${1:-}"
END_DATE="${2:-}"
MODE="${3:-dry}"  # dry | full | force
WORKERS="${WORKERS:-16}"
ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"
BOOT_DISK_GB="${BOOT_DISK_GB:-50}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"

if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "Usage: $0 <start-date> <end-date> [dry|full|force]"
    echo "  dates: YYYY-MM-DD"
    echo "  mode:  dry (--dry-run) | full (resume-friendly) | force (re-bucket all)"
    exit 2
fi

case "$MODE" in
    dry|full|force) ;;
    *) echo "Unknown mode: $MODE (expected dry|full|force)"; exit 2 ;;
esac

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="mdps-sports-bucket-${RUN_TS}"

# Build the python command. ``setup-data-pipeline-vm.sh`` performs two
# things on this metadata before launch: (1) ``cd $WORKSPACE/mdps``, and
# (2) ``${VM_MIGRATION_CMD/python /$VENV/bin/python }`` — the FIRST
# ``python `` token gets replaced with the venv interpreter path. So this
# CMD must (a) start with bare ``python `` (no path), and (b) NOT prepend
# its own ``cd`` (the setup script already CDs to MDPS workspace dir).
# ``--force`` bypasses both the legacy single-file pre-flight skip AND
# the manifest pre-flight skip — needed for the first sweep after the
# per-(league, horizon) refactor since legacy single-file outputs would
# otherwise short-circuit the day.
CMD="python scripts/reprocess_sports_odds.py \
--start-date ${START_DATE} --end-date ${END_DATE} --workers ${WORKERS}"
case "$MODE" in
    dry)   CMD="${CMD} --dry-run" ;;
    force) CMD="${CMD} --force" ;;
    full)  ;;  # no flag — resume-friendly
esac

echo "Launching ${VM_NAME}"
echo "  Range:   ${START_DATE} → ${END_DATE}"
echo "  Mode:    ${MODE}"
echo "  Workers: ${WORKERS}"
echo "  Cmd:     ${CMD}"

md="VM_TASK=mdps-sports-bucket"
md="${md},VM_SERVICE=market_data_processing_service"
md="${md},VM_OPERATION=reprocess-sports-odds"
md="${md},VM_ASSET_GROUP=SPORTS"
md="${md},VM_START_DATE=${START_DATE}"
md="${md},VM_END_DATE=${END_DATE}"
md="${md},VM_MIGRATION_CMD=${CMD}"
md="${md},VM_MIGRATION_MODE=${MODE}"

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="${MACHINE_TYPE}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
    --labels=purpose=mdps-sports-bucket,mode="${MODE}",run-ts="${RUN_TS}"

echo ""
echo "  SSH:    gcloud compute ssh ${VM_NAME} --zone=${ZONE}"
echo "  Logs:   gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "  Delete: gcloud compute instances delete ${VM_NAME} --zone=${ZONE} --quiet"
