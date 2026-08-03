#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
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
#   DeFi:       2022-2026  (5 VMs; raw_tick_data begins 2022-11-01, 2022 shard clamps its
#               start accordingly). Stale-doc note (2026-07-28): DeFi was originally
#               pass-through/no-op at launcher creation (2026-04-28) but gained real MDPS
#               candle-derivation support in 489ec0e (2026-05-05, DEFI_YEARS added below) —
#               this comment was never updated to match; DeFi is NOT skipped.
#
# Usage:
#   bash launch-mdps-sharded-backfill.sh --preview              # plan only, no VMs created
#   bash launch-mdps-sharded-backfill.sh                        # full fan-out across all 4 asset_groups
#   bash launch-mdps-sharded-backfill.sh cefi tradfi            # subset by asset_group
#   bash launch-mdps-sharded-backfill.sh --dry                  # MDPS --dry-run inside VMs (no GCS writes; VMs still spawn)
#   bash launch-mdps-sharded-backfill.sh cefi --year 2024 2025  # subset by year
#   bash launch-mdps-sharded-backfill.sh cefi --date-concurrency 4   # R1 throughput lever (opt-in)
#
# R1 throughput lever (--date-concurrency N / MDPS_DATE_CONCURRENCY env, opt-in, DEFAULT
# UNSET = today's exact per-year serial behaviour): dispatches up to N date-subprocesses
# concurrently WITHIN each year-shard VM (each date is already its own subprocess via the
# default --subprocess-per-date; this only stops blocking between them). Compounds with
# this launcher's own year-fanout lever. Months->weeks lever
# (data_pipeline_check_mdps_features_2026_07_20.md R1); PROVEN 4.12s@N=1 -> 1.04s@N=4 on 4
# real subprocesses (mtds@b3376b8). RSS scales ~linearly with N (~0.3 GB per concurrent
# in-date instrument-day) — size to the VM's RAM; e2-standard-8 (32GB): start N=2-4.
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

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Default e2-standard-8 (32GB) is sufficient for cefi/defi/sports/prediction.
# TradFi options-heavy days use e2-highmem-8 (64GB) + max-workers=2; see below.
#
# TradFi memory mitigation — attribution (2026-05-28 audit, plan task 4.2):
#   This mitigation targets the BUNDLE-READER issue, NOT the filter-pushdown bug
#   fixed at MDPS commit e47205d. Root cause: legacy ticks.parquet bundles contain
#   4000+ symbols in one DataFrame; one bundle load occupies ~25-30 GB before
#   any per-instrument work begins. Incidents: 2026-05-06 (2/7 sharded VMs
#   OOM-killed mid-flight) + 2026-05-07 (mdps-tradfi-2025 at 86.5% RSS).
#   The filter-pushdown fix shrinks the FILE LIST the scanner returns; it does NOT
#   shrink any single bundle file's in-memory footprint. Reverting this mitigation
#   would still OOM. Unblock: bundle-reader streaming refactor (lazify so only the
#   requested symbols are loaded per bundle), tracked in
#   plans/active/mdps_long_running_multi_shard_architecture_audit_2026_05_28.md.
#   Leave this mitigation in place until that refactor ships.
#
# Auto-defaults below are chosen per-asset_group; explicit MACHINE_TYPE /
# MDPS_MAX_WORKERS env overrides still win. Operator override pattern:
#   MACHINE_TYPE=e2-standard-8 MDPS_MAX_WORKERS=4 bash launch-mdps-sharded-backfill.sh tradfi
MACHINE_TYPE_OVERRIDE="${MACHINE_TYPE:-}"
MDPS_MAX_WORKERS_OVERRIDE="${MDPS_MAX_WORKERS:-}"
# R1 throughput lever (data_pipeline_check_mdps_features_2026_07_20.md) — opt-in,
# empty by default = MDPS's own MDPS_DATE_CONCURRENCY config default of 1 (SERIAL,
# byte-for-byte today's behaviour).
MDPS_DATE_CONCURRENCY_OVERRIDE="${MDPS_DATE_CONCURRENCY:-}"

# Optional CLI flag --max-workers N forwards to the in-VM MDPS CLI.
CLI_MAX_WORKERS=""
CLI_DATE_CONCURRENCY=""
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"
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
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry) DRY=true; shift ;;
        --preview) PREVIEW=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --on-demand)   ON_DEMAND=true; shift ;;
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
        --date-concurrency)
            shift
            CLI_DATE_CONCURRENCY="${1:-}"
            if [[ -z "$CLI_DATE_CONCURRENCY" ]]; then
                echo "--date-concurrency requires a value"
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
        *) echo "Unknown arg: $1"; echo "Usage: $0 [cefi|tradfi|defi|sports|prediction ...] [--year YYYY ...] [--dry] [--preview] [--max-workers N] [--date-concurrency N] [--env prod|staging|dev] [--source-bucket-override BUCKET]"; exit 2 ;;
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
# tradfi: e2-highmem-8 + max-workers=2 — bundle-reader memory mitigation
# (see header comment for attribution; leave until streaming refactor ships).
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

    # R1 throughput lever resolution (env override > CLI flag > unset/serial default).
    local resolved_date_concurrency=""
    if [[ -n "$MDPS_DATE_CONCURRENCY_OVERRIDE" ]]; then
        resolved_date_concurrency="$MDPS_DATE_CONCURRENCY_OVERRIDE"
    elif [[ -n "$CLI_DATE_CONCURRENCY" ]]; then
        resolved_date_concurrency="$CLI_DATE_CONCURRENCY"
    fi

    local env_short
    case "$DEPLOYMENT_ENV" in
        prod)    env_short="prd" ;;
        staging) env_short="stg" ;;
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
    # R1 throughput lever — opt-in, unset by default (today's exact per-year serial behaviour).
    if [[ -n "$resolved_date_concurrency" ]]; then
        cmd="$cmd --date-concurrency $resolved_date_concurrency"
    fi

    # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
    local PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
    if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

    echo "[$cat $year] $vm_name  ${start_date}..${end_date}  (machine=$machine_type, max_workers=${resolved_max_workers:-default}, date_concurrency=${resolved_date_concurrency:-1}) [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
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
    [[ -n "$resolved_date_concurrency" ]] && md="${md},VM_DATE_CONCURRENCY=${resolved_date_concurrency}"
    md="${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    md="${md},VM_SHUTDOWN_ON_COMPLETION=true"
    [[ -n "$UTL_TARBALL_SHA_PIN" ]] && md="${md},UTL_TARBALL_SHA=${UTL_TARBALL_SHA_PIN}"
    [[ -n "$MDPS_TARBALL_SHA_PIN" ]] && md="${md},MDPS_TARBALL_SHA=${MDPS_TARBALL_SHA_PIN}"
    # Durable pin registry — see lc_write_tarball_pin_record. This launcher fans
    # out one VM per asset_group × YEAR, so a single run can hold a pin across
    # dozens of long-lived VMs; losing that pin to the mtime-ranked sweep bricks
    # the relaunch of every one of them at once.
    lc_write_tarball_pin_record "$vm_name" "$PROJECT" "launch-mdps-sharded-backfill.sh" \
        "UTL_TARBALL_SHA=${UTL_TARBALL_SHA_PIN}" \
        "MDPS_TARBALL_SHA=${MDPS_TARBALL_SHA_PIN}"
    # Sports MDPS processes long empty-date stretches (no betting events) that
    # produce no log output, triggering the stall watchdog at the default 1800s.
    # 7200s = 2h gives enough headroom for a full year's empty-season gap
    # without letting a truly stalled VM idle indefinitely. Applied to all 5
    # categories (cefi/tradfi/defi/sports/prediction) — they all run through the
    # same `--operation process --mode batch` entrypoint (see cmd construction
    # above), so the same headroom rationale holds regardless of category
    # (vm_exec_stall_watchdog_checkpoint_regex_mismatch_2026_08_03.md todo 6:
    # was gated to sports only for no category-specific reason).
    md="${md},STALL_TIMEOUT_SEC=7200"
    # Per-shard progress watchdog (backfill_vm_silent_worker_stall_watchdog P2): with the blunt 2h
    # threshold above, a GENUINE mid-date hang (a stalled provider socket) idles for up to 2h before
    # the size-based watchdog trips. Reset the timer only on a real per-date PROGRESS line instead.
    # `Processing` = "Processing candles for <date>" (per processed date, incl. empty-within-window)
    # + "📦 Processing data_type: <dt>" (per data_type within a date); `Skipping` = "Skipping <date> -
    # before earliest data availability" / "Skipping <date> — candles already exist" (per pre-skipped
    # date). The per-date loop logs exactly one of these for EVERY date it touches
    # (process_handler.py:517/540/582 — proven invariant), so the marker resets on every healthy date
    # advance and only fails to reset during a genuine mid-date hang; it errs toward NOT killing.
    # Category-agnostic shared-orchestration markers (verified against a live mdps-sports run.log).
    # Applied to all 5 categories: cefi/tradfi/defi/sports/prediction all invoke the IDENTICAL
    # entrypoint (`python -m market_data_processing_service --operation process --mode batch`)
    # through the same process_handler.py per-date loop, so the "proven invariant" cited above for
    # sports holds identically for the other 4 — they were previously left on the default 1800s
    # timeout with zero regex, exposed to the always-on PIPELINE_HEARTBEAT byte-growth-defeat gap
    # (vm_exec_stall_watchdog_checkpoint_regex_mismatch_2026_08_03.md todo 6).
    # =/space/comma-free → metadata-safe.
    md="${md},STALL_PROGRESS_REGEX=Processing|Skipping"

    # SPOT preemption contract (vm_fleet_preemption_autorecovery_gap_2026_07_23.md
    # item 9): lc_write_preemption_signal_file marks a SPOT shutdown as an expected
    # preemption for fleet monitors (instead of an unexplained DP_VM_GONE_NO_CAPTURE).
    lc_write_preemption_signal_file

    # shellcheck disable=SC2086
    # Download-heavy backfill VM: pd-balanced >=250GB is MANDATORY. A pd-standard 50GB
    # boot disk sustains only ~6 MB/s of writes and its burst credits deplete by CUMULATIVE
    # BYTES WRITTEN — measured 2026-07-18, it throttled the CeFi backfill to 2.36 MB/s after
    # ~7.5GB (iostat %util 99.94, w_await 1015ms, CPU idle, RAM free). On pd-balanced 250GB the
    # same workload sustained 11.1 MB/s to 18.7GB+ with peaks of 18.15 MB/s — a 4.7x gain.
    # Enforced by scripts/quality_gates/check_backfill_vm_disk_provisioning.py.
    gcloud compute instances create "$vm_name" \
        --project="$PROJECT" \
        --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
        --zone="$ZONE" \
        --machine-type="$machine_type" \
        --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --scopes=cloud-platform \
        ${PROVISIONING_FLAGS} \
        --metadata="startup-script-url=${STARTUP},${md}" \
        --metadata-from-file="shutdown-script=${PREEMPTION_SIGNAL_FILE}" \
        --labels=purpose=mdps-sharded-backfill,asset_group="${cat}",year="${year}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service \
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
echo "Reminder: post-backfill, run manifest reconciliation per asset_group (ADDITIVE merge only —"
echo "rebuild_manifest_from_canonical_paths() wholesale-replaces a shared bucket's WHOLE manifest"
echo "index and would delete this bucket's raw_tick manifest rows; see"
echo "rebuild_manifest_from_canonical_paths_prefix_scoped_wipe_2026_07_27.md):"
echo "  python -c \"from unified_trading_library.manifest_writer import merge_manifest_from_canonical_paths; \\"
echo "    merge_manifest_from_canonical_paths('market-data-tick-<ag>-${PROJECT}', \\"
echo "      service_name='market-data-processing-service', prefix='processed_candles/by_date')\""
