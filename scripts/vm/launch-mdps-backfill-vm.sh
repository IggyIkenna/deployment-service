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
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.

set -euo pipefail

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
SOURCE_BUCKET_OVERRIDE=""

# Pre-parse --env <val> and --source-bucket <val> before positional args.
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --source-bucket) SOURCE_BUCKET_OVERRIDE="$2"; shift 2 ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done
set -- "${POSITIONAL[@]:-}"

ASSET_GROUP="${1:-}"
START_DATE="${2:-}"
END_DATE="${3:-}"
MODE="${4:-dry}"  # dry | full

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="e2-standard-8"
BOOT_DISK_GB="50"

if [[ -z "$ASSET_GROUP" || -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "Usage: $0 [--env prod|staging|dev] <cefi|tradfi|defi|sports|prediction|all> <start-date> <end-date> [dry|full]"
    exit 2
fi

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

RUN_TS="$(date +%Y%m%d-%H%M%S)"

_category_upper_for() {
    case "$1" in
        cefi)       echo "CEFI" ;;
        tradfi)     echo "TRADFI" ;;
        defi)       echo "DEFI" ;;
        sports)     echo "SPORTS" ;;
        prediction) echo "PREDICTION" ;;
        *) echo ""; return 1 ;;
    esac
}

_launch() {
    local cat="$1"
    local cat_upper; cat_upper="$(_category_upper_for "$cat")" || { echo "Unknown category: $cat"; return 1; }
    local bucket_suffix=""
    if [[ -n "$SOURCE_BUCKET_OVERRIDE" ]]; then
        bucket_suffix="-$(echo "$SOURCE_BUCKET_OVERRIDE" | sed 's/-central-element-323112//' | sed "s/^market-data-tick-${cat}/main/" | sed 's/-central-element-323112//' | cut -c1-16 | tr '_' '-')"
    fi
    local vm_name="mdps-backfill-${cat}${bucket_suffix}-${RUN_TS}"

    # MDPS CLI quirk: service uses ServiceBootstrap at the top level (which
    # requires --operation and --mode) BUT has add_asset_group_arg=False, so
    # --asset-group is NOT a recognised top-level arg. The legacy `process`
    # subparser accepts per-asset-group `--CEFI/--DEFI/...` flags, but those
    # are only reachable if the bootstrap bridge (_build_legacy_argv in
    # cli/main.py) reads MDPS_ASSET_GROUP env var and translates to --CEFI.
    # Hence: export MDPS_ASSET_GROUP in the command string so the Python
    # bridge picks it up. --asset-group on the top-level fails with
    # "unrecognized arguments" (2026-04-19 VM launches proved this).
    # Source-bucket env var is read by MDPS config.py (line 75-80) to locate the
    # raw tick bucket. Without it, every shard fails with "No source bucket
    # configured for category=…" before any candle is produced.
    local source_bucket="${SOURCE_BUCKET_OVERRIDE:-market-data-tick-${cat}-${PROJECT}}"
    local cmd="PROTOCOL_DATA_SOURCE_BUCKET_${cat_upper}=${source_bucket}"
    cmd="$cmd MDPS_ASSET_GROUP=$cat_upper"
    # Sports MDPS catch-up: pre-2026 dates often lack upstream raw because
    # sports forward-poll has only been running recently. The dependency
    # check would otherwise abort the run on the first empty date. Bridge
    # the existing ``--skip-dependency-check`` flag (legacy subparser) via
    # the SKIP_DEPENDENCY_CHECK env var that ``cli/main.py::_build_legacy_argv``
    # honours at translate time. Other asset_groups keep the dep check on so
    # we fail fast if upstream is genuinely missing.
    if [[ "$cat" == "sports" ]]; then
        cmd="$cmd SKIP_DEPENDENCY_CHECK=true"
    fi
    cmd="$cmd python -m market_data_processing_service --operation process --mode batch"
    cmd="$cmd --start-date $START_DATE --end-date $END_DATE"
    if [[ "$MODE" == "dry" ]]; then
        cmd="$cmd --dry-run"
    fi

    echo "Launching $vm_name"
    echo "  cmd: $cmd"
    echo "  zone: $ZONE, machine: $MACHINE_TYPE, boot: ${BOOT_DISK_GB}G"

    local md="VM_TASK=mdps-backfill"
    md="${md},VM_SERVICE=market_data_processing_service"
    md="${md},VM_OPERATION=backfill-${cat}"
    md="${md},VM_ASSET_GROUP=$(echo "$cat" | tr '[:lower:]' '[:upper:]')"
    md="${md},VM_START_DATE=${START_DATE}"
    md="${md},VM_END_DATE=${END_DATE}"
    md="${md},VM_BACKFILL_CMD=${cmd}"
    md="${md},VM_BACKFILL_MODE=${MODE}"
    md="${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    md="${md},VM_SHUTDOWN_ON_COMPLETION=true"
    [[ -n "${UTL_TARBALL_SHA:-}" ]]  && md="${md},UTL_TARBALL_SHA=${UTL_TARBALL_SHA}"
    [[ -n "${MDPS_TARBALL_SHA:-}" ]] && md="${md},MDPS_TARBALL_SHA=${MDPS_TARBALL_SHA}"

    gcloud compute instances create "$vm_name" \
        --project="$PROJECT" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        --boot-disk-size="${BOOT_DISK_GB}GB" \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --scopes=cloud-platform \
        --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
        --labels=purpose=mdps-backfill,category="${cat}",mode="${MODE}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
    echo "  SSH: gcloud compute ssh $vm_name --zone=$ZONE"
    echo "  Delete: gcloud compute instances delete $vm_name --zone=$ZONE --quiet"
    echo ""
}

case "$ASSET_GROUP" in
    cefi|tradfi|defi|sports|prediction) _launch "$ASSET_GROUP" ;;
    all)
        _launch cefi
        _launch tradfi
        _launch defi
        _launch sports
        _launch prediction
        ;;
    *) echo "Unknown category: $ASSET_GROUP"; exit 2 ;;
esac

echo "Run timestamp: $RUN_TS"
echo "Mode: $MODE (dry = --dry-run; full = live writes)"
echo ""
echo "Reminder: post-backfill, run manifest reconciliation via:"
echo "  python -c \"from unified_trading_library.manifest_writer import rebuild_manifest_from_canonical_paths; \\"
echo "    rebuild_manifest_from_canonical_paths('market-data-tick-${ASSET_GROUP}-central-element-323112', \\"
echo "      service_name='market-data-processing-service', prefix='processed_candles/by_date')\""
