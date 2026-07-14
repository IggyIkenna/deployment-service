#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
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
#   bash launch-mdps-sports-bucket-vm.sh <start-date> <end-date> [dry|full|force] [--on-demand]
#     dry   — --dry-run, no GCS writes
#     full  — live writes, manifest pre-flight skip enabled (resume-friendly)
#     force — live writes, --force (re-bucket even already-bucketed days;
#             needed once after the 2026-05-05 layout refactor that split
#             the monolithic bucketed.parquet into per-(league,horizon); also
#             the only mode that re-attempts a day whose COARSE per-day manifest
#             key is already `captured` but is still missing some fine-grained
#             (league_id, timeframe) shards — the coarse pre-flight key has no
#             league_id/timeframe dimension, so `full` mode would otherwise
#             skip the whole day and never fill the gap)
#
# Idempotent backfill defaults to SPOT (~60-91% cheaper); --on-demand /
# ON_DEMAND=true forces standard provisioning. SSOT:
# codex/05-infrastructure/spot-vms-for-backfill.md. (Added 2026-07-14 —
# this launcher previously had no provisioning flag at all, defaulting to
# on-demand, a bug per that SSOT's HARD RULE for every backfill launcher.)
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
ON_DEMAND="${ON_DEMAND:-false}"

# Pre-parse --env / --on-demand before positional args.
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --on-demand) ON_DEMAND=true; shift ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done
set -- "${POSITIONAL[@]:-}"

START_DATE="${1:-}"
END_DATE="${2:-}"
MODE="${3:-dry}"  # dry | full | force
WORKERS="${WORKERS:-16}"
ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
BOOT_DISK_GB="${BOOT_DISK_GB:-50}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"

if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "Usage: $0 [--env prod|staging|dev] <start-date> <end-date> [dry|full|force]"
    echo "  dates: YYYY-MM-DD"
    echo "  mode:  dry (--dry-run) | full (resume-friendly) | force (re-bucket all)"
    exit 2
fi

case "$MODE" in
    dry|full|force) ;;
    *) echo "Unknown mode: $MODE (expected dry|full|force)"; exit 2 ;;
esac

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
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
md="${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
md="${md},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        market-data-processing-service market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
if [[ "$ON_DEMAND" == "true" ]]; then PROVISIONING_FLAGS=""; fi
echo "  Provisioning: $([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)"

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="${MACHINE_TYPE}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" \
    --scopes=cloud-platform \
    ${PROVISIONING_FLAGS} \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
    --labels=purpose=mdps-sports-bucket,mode="${MODE}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "  SSH:    gcloud compute ssh ${VM_NAME} --zone=${ZONE}"
echo "  Logs:   gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "  Delete: gcloud compute instances delete ${VM_NAME} --zone=${ZONE} --quiet"
