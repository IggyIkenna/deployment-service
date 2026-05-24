#!/usr/bin/env bash
# Launch sharded MDPS catch-up VMs (one VM per asset_group × year shard).
#
# Why: single-VM MDPS catch-up takes too long for the full historical window
# (e.g. CeFi observed ~14 min/day → 22 days for 2020-01-01..today on one VM).
# This launcher fans out across years so the wallclock ETA drops to
# ``ceil(window / longest_year_runtime)`` ≈ 2-4 days regardless of
# asset_group. Each VM processes one calendar year independently; manifest
# rows merge cleanly because UTL ManifestWriter writes per-VM shards under
# ``_index/per_vm/`` that the manifest reader unions at read time.
#
# Same code path as launch-mdps-backfill-vm.sh — same VM_TASK=mdps-backfill
# routing, same env-var prefixes (PROTOCOL_DATA_SOURCE_BUCKET_*,
# MDPS_ASSET_GROUP), same SKIP_DEPENDENCY_CHECK for sports.
#
# Per asset_group year ranges (data start → today):
#   CeFi:       2020-2026  (7 VMs)
#   TradFi:     2020-2026  (7 VMs)
#   Sports:     2020-2026  (7 VMs, SKIP_DEPENDENCY_CHECK=true; pre-2020 sports raw absent)
#   Prediction: 2025-2026  (2 VMs; data starts 2025-03-14)
#   DeFi:       SKIPPED — DeFi data types are pass-through (NEEDS_CANDLE_PROCESSING=False)
#               so there is no MDPS work to do. blocked_on_raw stays 0 by design.
#
# Usage:
#   bash launch-mdps-sharded-backfill.sh --preview              # plan only, no VMs created
#   bash launch-mdps-sharded-backfill.sh                        # full fan-out across all 4 asset_groups
#   bash launch-mdps-sharded-backfill.sh cefi tradfi            # subset by asset_group
#   bash launch-mdps-sharded-backfill.sh --dry                  # MDPS --dry-run inside VMs (no GCS writes; VMs still spawn)
#   bash launch-mdps-sharded-backfill.sh cefi --year 2024 2025  # subset by year
#
# Cost: e2-standard-8 × N VMs where N = sum of years per asset_group selected
# (default 7+7+7+2 = 23 VMs). Each VM ~3-12 hours wallclock depending on data
# density. Tarball pull + venv setup ~3-5 min per VM (one-time).
#
# Each VM auto-deletes via VM_SHUTDOWN_ON_COMPLETION=true.
#
# Bucket-naming SSOT: env-aware shape per `bucket_name_ssot_canonicalisation_2026_05_10.md`
# Phase 0f. Source bucket resolves to market-data-tick-{ag}-{env_short}-{project}.
# For prod: market-data-tick-{ag}-prd-central-element-323112.
# Legacy 2024/2025 DeFi re-launches (dex_pool_swaps in flat bucket) must pass
# --source-bucket-override market-data-tick-defi-central-element-323112.
set -euo pipefail

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Default e2-standard-8 (32GB) is sufficient for cefi/defi/sports/prediction.
# TradFi options-heavy days (legacy ticks.parquet bundles with 4000+ symbols
# loaded into one Polars DataFrame) hit OOM/SIGKILL on 32GB — incidents
# 2026-05-06 (2 of 7 sharded VMs killed mid-flight) + 2026-05-07
# (mdps-tradfi-2025 OOM-killed at 86.5% RSS).
#
# Auto-defaults below are chosen per-asset_group; explicit MACHINE_TYPE /
# MDPS_MAX_WORKERS env overrides still win. TradFi gets e2-highmem-8 (64GB) +
# max-workers=2 (halves concurrent peak footprint vs default 4) until the MDPS
# streaming refactor lazifies the bundle reader. Operator override pattern:
#   MACHINE_TYPE=e2-standard-8 MDPS_MAX_WORKERS=4 bash launch-mdps-sharded-backfill.sh tradfi
MACHINE_TYPE_OVERRIDE="${MACHINE_TYPE:-}"
MDPS_MAX_WORKERS_OVERRIDE="${MDPS_MAX_WORKERS:-}"

# Optional CLI flag --max-workers N forwards to the in-VM MDPS CLI.
CLI_MAX_WORKERS=""
BOOT_DISK_GB="50"
# Per-tarball SHA pins — prevents race condition where another slot rebuilds the
# fixed-name tarball between tarball build and VM boot.
# Reads from env or CLI --utl-sha / --mdps-sha flags.
UTL_TARBALL_SHA_PIN="${UTL_TARBALL_SHA:-}"
MDPS_TARBALL_SHA_PIN="${MDPS_TARBALL_SHA:-}"
STARTUP="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
# Override the computed env-tiered source bucket for all asset_groups.
# Required for 2024/2025 DeFi re-launches where dex_pool_swaps data lives
# in the legacy flat bucket (market-data-tick-defi-central-element-323112)
# rather than the canonical env-tiered bucket.
SOURCE_BUCKET_OVERRIDE=""

# ─── Per-asset_group year ranges ─────────────────────────────────────────────
CEFI_YEARS="2019 2020 2021 2022 2023 2024 2025 2026"
TRADFI_YEARS="2020 2021 2022 2023 2024 2025 2026"
DEFI_YEARS="2022 2023 2024 2025 2026"
SPORTS_YEARS="2020 2021 2022 2023 2024 2025 2026"
PREDICTION_YEARS="2025 2026"

DRY=false       # MDPS --dry-run (in-VM, no GCS writes; VMs still spawn)
PREVIEW=false   # local preview only — print plan, no VM creation
SELECTED_AGS=""
SELECTED_YEARS=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry) DRY=true; shift ;;
        --preview) PREVIEW=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --year)
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                SELECTED_YEARS="$SELECTED_YEARS $1"
                shift
            done
            ;;
        --max-workers)
            shift
            CLI_MAX_WORKERS="${1:-}"
            if [[ -z "$CLI_MAX_WORKERS" ]]; then
                echo "--max-workers requires a value"
                exit 2
            fi
            shift
            ;;
        --utl-sha)
            shift; UTL_TARBALL_SHA_PIN="${1:-}"; shift ;;
        --mdps-sha)
            shift; MDPS_TARBALL_SHA_PIN="${1:-}"; shift ;;
        --source-bucket-override)
            shift; SOURCE_BUCKET_OVERRIDE="${1:-}"; shift ;;
        cefi|tradfi|defi|sports|prediction)
            SELECTED_AGS="$SELECTED_AGS $1"
            shift
            ;;
        *) echo "Unknown arg: $1"; echo "Usage: $0 [cefi|tradfi|defi|sports|prediction ...] [--year YYYY ...] [--dry] [--preview] [--max-workers N] [--env prod|staging|dev] [--source-bucket-override BUCKET]"; exit 2 ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# Default: all 4 asset_groups
if [[ -z "$SELECTED_AGS" ]]; then
    SELECTED_AGS="cefi tradfi defi sports prediction"
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"

_year_range_for() {
    case "$1" in
        cefi) echo "$CEFI_YEARS" ;;
        tradfi) echo "$TRADFI_YEARS" ;;
        defi) echo "$DEFI_YEARS" ;;
        sports) echo "$SPORTS_YEARS" ;;
        prediction) echo "$PREDICTION_YEARS" ;;
        *) echo "" ;;
    esac
}

_cat_upper_for() {
    case "$1" in
        cefi) echo "CEFI" ;;
        tradfi) echo "TRADFI" ;;
        defi) echo "DEFI" ;;
        sports) echo "SPORTS" ;;
        prediction) echo "PREDICTION" ;;
        *) echo "" ;;
    esac
}

_filter_year() {
    # Returns 0 if year is in SELECTED_YEARS (or SELECTED_YEARS is empty).
    local year="$1"
    if [[ -z "$SELECTED_YEARS" ]]; then return 0; fi
    for y in $SELECTED_YEARS; do
        [[ "$y" == "$year" ]] && return 0
    done
    return 1
}

# Per-asset-group resource defaults.
# tradfi: high-memory + halved concurrency (legacy ticks.parquet bundles with
# 4000+ symbols hit OOM on 32GB; halving max_workers further bounds peak).
# All others: standard 32GB / default workers.
_machine_type_for() {
    case "$1" in
        tradfi) echo "e2-highmem-8" ;;
        *) echo "e2-standard-8" ;;
    esac
}

_max_workers_for() {
    case "$1" in
        tradfi) echo "2" ;;
        *) echo "" ;;  # empty = use MDPS CLI default (4)
    esac
}

launch_year_shard() {
    local cat="$1"
    local year="$2"
    local cat_upper; cat_upper="$(_cat_upper_for "$cat")"
    local vm_name="mdps-${cat}-${year}-${RUN_TS}"
    local start_date="${year}-01-01"
    local end_date="${year}-12-31"
    # 2026: clamp end_date to today (don't try to process future dates).
    if [[ "$year" == "2026" ]]; then
        end_date="$(date +%Y-%m-%d)"
    fi
    # Prediction 2025: clamp start to data-availability start.
    if [[ "$cat" == "prediction" && "$year" == "2025" ]]; then
        start_date="2025-03-14"
    fi
    # DeFi 2022: clamp start to data-availability start (raw_tick_data begins 2022-11-01).
    if [[ "$cat" == "defi" && "$year" == "2022" ]]; then
        start_date="2022-11-01"
    fi

    # Resolve machine type + max-workers (env override > CLI flag > per-AG default).
    local machine_type
    if [[ -n "$MACHINE_TYPE_OVERRIDE" ]]; then
        machine_type="$MACHINE_TYPE_OVERRIDE"
    else
        machine_type="$(_machine_type_for "$cat")"
    fi

    local resolved_max_workers=""
    if [[ -n "$MDPS_MAX_WORKERS_OVERRIDE" ]]; then
        resolved_max_workers="$MDPS_MAX_WORKERS_OVERRIDE"
    elif [[ -n "$CLI_MAX_WORKERS" ]]; then
        resolved_max_workers="$CLI_MAX_WORKERS"
    else
        resolved_max_workers="$(_max_workers_for "$cat")"
    fi

    local env_short
    case "$DEPLOYMENT_ENV" in
        prod)    env_short="prd" ;;
        staging) env_short="staging" ;;
        dev)     env_short="dev" ;;
        *)       env_short="$DEPLOYMENT_ENV" ;;
    esac
    local source_bucket
    if [[ -n "$SOURCE_BUCKET_OVERRIDE" ]]; then
        source_bucket="$SOURCE_BUCKET_OVERRIDE"
    else
        source_bucket="market-data-tick-${cat}-${env_short}-${PROJECT}"
    fi
    local cmd="PROTOCOL_DATA_SOURCE_BUCKET_${cat_upper}=${source_bucket}"
    cmd="$cmd MDPS_ASSET_GROUP=$cat_upper"
    if [[ "$cat" == "sports" || "$cat" == "prediction" ]]; then
        # Sports: IS instrument_availability by day not populated — bypass IS dep check.
        # Prediction: IS instrument_availability uses canonical_question_group partition
        # (instrument_availability/by_date/canonical_question_group=X/day=Y/venue=Z/) rather than
        # flat day= prefix that MDPS dep_checker expects. Bypass IS dep check; raw tick data is present.
        cmd="$cmd SKIP_DEPENDENCY_CHECK=true"
    fi
    # MAX_WORKERS is read from env by MDPS config.py — not a CLI flag.
    if [[ -n "$resolved_max_workers" ]]; then
        cmd="MAX_WORKERS=$resolved_max_workers $cmd"
    fi
    cmd="$cmd python -m market_data_processing_service --operation process --mode batch"
    cmd="$cmd --start-date $start_date --end-date $end_date"
    if $DRY; then
        cmd="$cmd --dry-run"
    fi

    echo "[$cat $year] $vm_name  ${start_date}..${end_date}  (machine=$machine_type, max_workers=${resolved_max_workers:-default})"
    echo "  cmd: $cmd"

    if $PREVIEW; then
        echo "  → PREVIEW (no VM created)"
        return 0
    fi

    local md="VM_TASK=mdps-backfill"
    md="${md},VM_SERVICE=market_data_processing_service"
    md="${md},VM_OPERATION=backfill-${cat}"
    md="${md},VM_ASSET_GROUP=${cat_upper}"
    md="${md},VM_START_DATE=${start_date}"
    md="${md},VM_END_DATE=${end_date}"
    md="${md},VM_BACKFILL_CMD=${cmd}"
    md="${md},VM_BACKFILL_MODE=$($DRY && echo dry || echo full)"
    md="${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    md="${md},VM_SHUTDOWN_ON_COMPLETION=true"
    [[ -n "$UTL_TARBALL_SHA_PIN" ]] && md="${md},UTL_TARBALL_SHA=${UTL_TARBALL_SHA_PIN}"
    [[ -n "$MDPS_TARBALL_SHA_PIN" ]] && md="${md},MDPS_TARBALL_SHA=${MDPS_TARBALL_SHA_PIN}"

    gcloud compute instances create "$vm_name" \
        --project="$PROJECT" \
        --zone="$ZONE" \
        --machine-type="$machine_type" \
        --boot-disk-size="${BOOT_DISK_GB}GB" \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --scopes=cloud-platform \
        --metadata="startup-script-url=${STARTUP},${md}" \
        --labels=purpose=mdps-sharded-backfill,asset_group="${cat}",year="${year}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}" \
        > /dev/null
    echo "  → RUNNING"
}

TOTAL=0
for ag in $SELECTED_AGS; do
    YEARS="$(_year_range_for "$ag")"
    [[ -z "$YEARS" ]] && { echo "Unknown asset_group: $ag"; continue; }
    for year in $YEARS; do
        if _filter_year "$year"; then
            launch_year_shard "$ag" "$year"
            TOTAL=$((TOTAL + 1))
        fi
    done
done

echo ""
echo "Launched $TOTAL MDPS shard VMs (run-ts=$RUN_TS)"
echo ""
echo "Monitor:"
echo "  gcloud compute instances list --filter='labels.run-ts=${RUN_TS}' --format='table(name,status)'"
echo ""
echo "Tail any VM:"
echo "  gsutil cat gs://${CODE_BUCKET}/vm-logs/<vm-name>/run.log"
echo ""
echo "Reminder: post-backfill, run manifest reconciliation per asset_group:"
echo "  python -c \"from unified_trading_library.manifest_writer import rebuild_manifest_from_canonical_paths; \\"
echo "    rebuild_manifest_from_canonical_paths('market-data-tick-<ag>-${PROJECT}', \\"
echo "      service_name='market-data-processing-service', prefix='processed_candles/by_date')\""
