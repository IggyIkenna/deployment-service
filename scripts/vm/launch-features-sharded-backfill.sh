#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape per `bucket_name_ssot_canonicalisation_2026_05_10.md`
# Phase 0f. `--env $DEPLOYMENT_ENV` propagates to VM metadata for bucket resolution.
#
# Fleet-wide, YEAR-SHARDED features-service backfill launcher — the features-side
# counterpart to `launch-mdps-sharded-backfill.sh`. Learning-from-cefi optimization
# (data_pipeline_check_mdps_features_2026_07_20.md todo 12): `launch-features-vm.sh`
# launches exactly ONE VM per (feature_family x asset_group) cell that walks the WHOLE
# date range serially (e.g. 2020-01-01..today on one box) — features/MDPS are NOT
# Tardis-IP-capped (only cefi/MTDS Tardis pulls are), so fleet-wide parallelism across
# many concurrent VMs is a free lever this launcher was missing. This launcher fans a
# (family, asset_group) cell's date range out across one VM PER YEAR, cutting wall-clock
# from O(years) to O(1 longest year) — same wall-clock-reduction shape as MDPS's sharded
# launcher, applied to the features backfill-processing path.
#
# Each shard VM still runs the SAME `python -m features_service --feature-family ...`
# command `launch-features-vm.sh` builds (same CLI axes, same env passthrough), so this
# launcher does not duplicate service-CLI logic — it only fans the date range + VM
# creation out. Manifest rows merge cleanly: UTL ManifestWriter's per-VM shards under
# `_index/per_vm/` union at read time (same mechanism the MDPS sharded launcher relies on).
#
# Within-VM multiproc (learning-from-cefi/MDPS R1): delta_one / volatility / onchain
# already expose `--max-workers` on their service CLIs (default 4) but no launcher ever
# threaded it through — MAX_WORKERS env here passes `--max-workers <N>` for those 3
# families (the other families' CLIs don't accept the flag; passing it would be a hard
# argparse failure, so it is intentionally scoped to the 3 that support it).
#
# Disk (learning-from-cefi, `check_backfill_vm_disk_provisioning.py`-enforced): 250GB
# pd-balanced — a pd-standard 50GB boot disk sustains only ~6 MB/s and collapsed the CeFi
# backfill to 2.36 MB/s after ~7.5GB (measured 2026-07-18); pd-balanced 250GB sustains
# ~70 MB/s, matching launch-features-vm.sh's own existing default.
#
# Faster-libs/Rust: NOT touched by this launcher (infra craft scope stops at VM/launcher
# config, not service-code kernels — see `unified-trading-pm/agents/infra.md` "does_not:
# Python service business logic"). Already assessed in the 2026-07-20 audit
# (`plans/archive/2026_07/data_pipeline_check_mdps_features_history_2026_07_24.md`
# "OPTIMIZATION TARGETS learning-from-cefi") as LOW priority for MDPS's candle kernel
# (polars core groupby already fast; Rust would shave only ~6% of wall-clock) — no
# infra-side action follows from that finding; any further vectorization is
# backend_engineer-scoped service-code work, not this launcher.
#
# Usage:
#   bash launch-features-sharded-backfill.sh --preview                         # plan only
#   bash launch-features-sharded-backfill.sh delta_one CEFI                    # full fan-out, all viable years
#   bash launch-features-sharded-backfill.sh delta_one CEFI --year 2024 2025   # subset by year
#   bash launch-features-sharded-backfill.sh onchain DEFI --dry                # in-VM --dry-run (no GCS writes; VMs still spawn)
#   MAX_WORKERS=8 bash launch-features-sharded-backfill.sh delta_one CEFI      # within-VM multiproc override
#
# Per-(family) asset_group year ranges mirror launch-mdps-sharded-backfill.sh's own
# tables (features consume MDPS candles, so the useful backfill window is the same):
#   CeFi/TradFi:  2019/2020-2026   DeFi: 2022-2026 (clamped to 2022-11-01)
#   Sports:       2020-2026        Prediction: 2025-2026 (clamped to 2025-03-14)
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
STARTUP="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits exhausted
# 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false
DRY=false       # in-VM --dry-run (no GCS writes; VMs still spawn)
PREVIEW=false   # local preview only — print plan, no VM creation
SELECTED_YEARS=""
FAMILY=""
ASSET_GROUP=""

print_usage() {
    cat <<'EOF'
Usage: launch-features-sharded-backfill.sh <feature_family> <ASSET_GROUP> [--year YYYY ...]
           [--dry] [--preview] [--env <prod|staging|dev>] [--on-demand]

feature_family ∈ { delta_one, volatility, onchain, sports, calendar, multi_timeframe,
                    cross_instrument, commodity }
ASSET_GROUP    ∈ { CEFI, DEFI, TRADFI, SPORTS, PREDICTION, GLOBAL }

Optional env overrides (same names as launch-features-vm.sh):
  MAX_WORKERS=<N>           within-VM multiproc (delta_one/volatility/onchain only)
  FEATURE_GROUP=<group>     narrower-than-family selector
  TIMEFRAME=<tf>            delta_one/volatility --timeframe override (required for
                             any TRADFI delta_one/volatility year-shard)
  SKIP_DEPENDENCY_CHECK=1   bypass global preflight
  FORCE=1                   rewrite parquets even if manifest shows captured
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry) DRY=true; shift ;;
        --preview) PREVIEW=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --on-demand) ON_DEMAND=true; shift ;;
        --year)
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                SELECTED_YEARS="$SELECTED_YEARS $1"
                shift
            done
            ;;
        -h|--help) print_usage; exit 0 ;;
        *)
            if [[ -z "$FAMILY" ]]; then
                FAMILY="$1"
            elif [[ -z "$ASSET_GROUP" ]]; then
                ASSET_GROUP="$1"
            else
                echo "Unknown argument: $1" >&2
                print_usage
                exit 2
            fi
            shift
            ;;
    esac
done

lc_validate_env "$DEPLOYMENT_ENV"

if [[ -z "$FAMILY" || -z "$ASSET_GROUP" ]]; then
    print_usage
    exit 2
fi

# ---------- viability matrix (mirrors launch-features-vm.sh's own matrix) ----------
_is_viable_cell() {
    local family="$1" ag="$2"
    case "$family/$ag" in
        calendar/CEFI|calendar/TRADFI) return 0 ;;
        commodity/TRADFI) return 0 ;;
        cross_instrument/CEFI|cross_instrument/TRADFI|cross_instrument/PREDICTION) return 0 ;;
        delta_one/CEFI|delta_one/DEFI|delta_one/TRADFI|delta_one/PREDICTION) return 0 ;;
        multi_timeframe/CEFI|multi_timeframe/DEFI|multi_timeframe/TRADFI) return 0 ;;
        onchain/DEFI) return 0 ;;
        sports/SPORTS) return 0 ;;
        volatility/CEFI|volatility/TRADFI) return 0 ;;
        *) return 1 ;;
    esac
}

if ! _is_viable_cell "$FAMILY" "$ASSET_GROUP"; then
    echo "Not a viable (feature_family x asset_group) cell: $FAMILY x $ASSET_GROUP" >&2
    echo "See header for the viable matrix." >&2
    exit 2
fi

# Families whose service CLI accepts --max-workers (checked against each family's
# parser at write-time — sports/calendar/multi_timeframe/cross_instrument/commodity do
# NOT accept the flag, so MAX_WORKERS is only threaded through for these 3).
_supports_max_workers() {
    case "$1" in
        delta_one|volatility|onchain) return 0 ;;
        *) return 1 ;;
    esac
}

# ---------- year ranges (mirrors launch-mdps-sharded-backfill.sh's own tables) ----------
_year_range_for() {
    case "$1" in
        CEFI|TRADFI) echo "2020 2021 2022 2023 2024 2025 2026" ;;
        DEFI)        echo "2022 2023 2024 2025 2026" ;;
        SPORTS)      echo "2020 2021 2022 2023 2024 2025 2026" ;;
        PREDICTION)  echo "2025 2026" ;;
        *)           echo "" ;;
    esac
}

_filter_year() {
    local year="$1"
    [[ -z "$SELECTED_YEARS" ]] && return 0
    for y in $SELECTED_YEARS; do
        [[ "$y" == "$year" ]] && return 0
    done
    return 1
}

FAMILY_DASHED="$(echo "$FAMILY" | tr '_' '-')"
ASSET_GROUP_LOWER="$(echo "$ASSET_GROUP" | tr '[:upper:]' '[:lower:]')"
RUN_TS="$(lc_run_ts)"

# ── Auto-pin UAC to the current LDR-tip tarball (P0 contract-propagation fix,
# mirrors launch-features-vm.sh) — an operator-exported UAC_TARBALL_SHA still wins.
if [[ -z "${UAC_TARBALL_SHA:-}" ]]; then
    UAC_TARBALL_SHA="$(lc_resolve_tarball_sha "$CODE_BUCKET" unified-api-contracts)"
fi

launch_year_shard() {
    local year="$1"
    local start_date="${year}-01-01"
    local end_date="${year}-12-31"
    if [[ "$year" == "2026" ]]; then
        end_date="$(date -u +%Y-%m-%d)"
    fi
    # Clamp to real data-availability floors (mirrors launch-mdps-sharded-backfill.sh).
    if [[ "$ASSET_GROUP" == "DEFI" && "$year" == "2022" ]]; then
        start_date="2022-11-01"
    fi
    if [[ "$ASSET_GROUP" == "PREDICTION" && "$year" == "2025" ]]; then
        start_date="2025-03-14"
    fi

    local vm_name="features-${FAMILY_DASHED}-${ASSET_GROUP_LOWER}-${year}-${RUN_TS}"

    local cmd="python -m features_service --feature-family ${FAMILY}"
    cmd="$cmd --operation compute --mode batch"
    cmd="$cmd --start-date ${start_date} --end-date ${end_date}"
    if [[ "$FAMILY" != "calendar" ]]; then
        cmd="$cmd --asset-group ${ASSET_GROUP}"
    fi
    case "$FAMILY" in
        delta_one|volatility|onchain) cmd="$cmd --feature-group ${FEATURE_GROUP:-ALL}" ;;
    esac
    case "$FAMILY" in
        delta_one|volatility) [[ -n "${TIMEFRAME:-}" ]] && cmd="$cmd --timeframe ${TIMEFRAME}" ;;
    esac
    local applied_max_workers="<cli-default>"
    if _supports_max_workers "$FAMILY" && [[ -n "${MAX_WORKERS:-}" ]]; then
        cmd="$cmd --max-workers ${MAX_WORKERS}"
        applied_max_workers="$MAX_WORKERS"
    elif ! _supports_max_workers "$FAMILY"; then
        applied_max_workers="n/a (${FAMILY} CLI has no --max-workers)"
    fi
    [[ -n "${SKIP_DEPENDENCY_CHECK:-}" ]] && cmd="$cmd --skip-dependency-check"
    [[ -n "${FORCE:-}" ]] && cmd="$cmd --force"
    if $DRY; then
        cmd="$cmd --dry-run"
    fi

    echo "[$FAMILY $ASSET_GROUP $year] $vm_name  ${start_date}..${end_date}  (max_workers=${applied_max_workers}) [$($ON_DEMAND && echo on-demand || echo SPOT)]"
    echo "  cmd: $cmd"

    if $PREVIEW; then
        echo "  -> PREVIEW (no VM created)"
        return 0
    fi

    local md="VM_TASK=features-backfill"
    md="${md},VM_SERVICE=features_service"
    md="${md},VM_FEATURE_FAMILY=${FAMILY}"
    md="${md},VM_OPERATION=compute-${FAMILY_DASHED}-${ASSET_GROUP_LOWER}"
    md="${md},VM_ASSET_GROUP=${ASSET_GROUP}"
    md="${md},VM_START_DATE=${start_date}"
    md="${md},VM_END_DATE=${end_date}"
    md="${md},VM_BACKFILL_CMD=${cmd}"
    md="${md},VM_BACKFILL_MODE=$($DRY && echo dry || echo full)"
    md="${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    # Per-VM shard isolation (workspace rule, codified 2026-05-06) — REQUIRED here since,
    # unlike launch-features-vm.sh's one-VM-per-cell shape, multiple VMs now write the
    # SAME (family, asset_group) bucket concurrently (one per year).
    md="${md},VM_NAME=${vm_name}"
    md="${md},MANIFEST_PER_VM_SHARDS=true"
    md="${md},VM_SHUTDOWN_ON_COMPLETION=true"
    [[ -n "${UAC_TARBALL_SHA:-}" ]] && md="${md},UAC_TARBALL_SHA=${UAC_TARBALL_SHA}"

    # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
    local prov_flags="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
    if $ON_DEMAND; then prov_flags=""; fi

    if [[ "${DRY_RUN:-false}" != "true" ]]; then
        lc_verify_tarball_freshness "$CODE_BUCKET" \
            features-service market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
            || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
    fi

    lc_write_tarball_pin_record "$vm_name" "$PROJECT" "launch-features-sharded-backfill.sh" \
        "UTL_TARBALL_SHA=${UTL_TARBALL_SHA:-}" \
        "UAC_TARBALL_SHA=${UAC_TARBALL_SHA:-}"

    # SPOT preemption contract (vm_fleet_preemption_autorecovery_gap_2026_07_23.md
    # item 9): lc_write_preemption_signal_file marks a SPOT shutdown as an expected
    # preemption for fleet monitors (instead of an unexplained DP_VM_GONE_NO_CAPTURE).
    lc_write_preemption_signal_file

    # Download-heavy backfill VM: pd-balanced >=250GB is MANDATORY (learning-from-cefi;
    # see header). Enforced by scripts/quality_gates/check_backfill_vm_disk_provisioning.py.
    # shellcheck disable=SC2086
    gcloud compute instances create "$vm_name" \
        --project="$PROJECT" \
        --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        ${prov_flags} \
        --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --scopes=cloud-platform \
        --metadata="startup-script-url=${STARTUP},${md}" \
        --metadata-from-file="shutdown-script=${PREEMPTION_SIGNAL_FILE}" \
        --labels=purpose=features-sharded-backfill,family="${FAMILY_DASHED}",category="${ASSET_GROUP_LOWER}",year="${year}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service \
        > /dev/null
    echo "  -> RUNNING"
}

TOTAL=0
YEARS="$(_year_range_for "$ASSET_GROUP")"
if [[ -z "$YEARS" ]]; then
    echo "Unknown asset_group year range: $ASSET_GROUP" >&2
    exit 2
fi
for year in $YEARS; do
    if _filter_year "$year"; then
        launch_year_shard "$year"
        TOTAL=$((TOTAL + 1))
    fi
done

echo ""
echo "Launched $TOTAL features shard VM(s) for ${FAMILY} x ${ASSET_GROUP} (run-ts=$RUN_TS)"
echo ""
echo "Monitor:"
echo "  gcloud compute instances list --filter='labels.run-ts=${RUN_TS}' --format='table(name,status)'"
echo ""
echo "Tail any VM:"
echo "  gsutil cat gs://${CODE_BUCKET}/vm-logs/<vm-name>/run.log"
echo ""
# Build the correct manifest prefix for the post-backfill reminder.
# Most families write under '<family>/by_date'; verified exceptions noted below.
# SSOT: each family's feature_writer/dependency_checker prefix constant.
case "$FAMILY" in
    sports)           _MF_PREFIX="sports_features/by_date" ;;
    cross_instrument) _MF_PREFIX="cross_venue_arb/by_date" ;;
    calendar)         _MF_PREFIX="calendar" ;;
    multi_timeframe)  _MF_PREFIX="mtf" ;;
    *)                _MF_PREFIX="${FAMILY}/by_date" ;;  # delta_one, volatility, onchain, commodity
esac
echo "Post-backfill manifest merge (additive — safe, no existing rows deleted; one per"
echo "features bucket, family prefix = '${_MF_PREFIX}'):"
echo "  python -c \"from unified_trading_library import resolve_bucket_name; \\"
echo "    from unified_trading_library.manifest_writer import merge_manifest_from_canonical_paths; \\"
echo "    merge_manifest_from_canonical_paths( \\"
echo "      resolve_bucket_name(cloud='gcp', kind='features', asset_group='${ASSET_GROUP_LOWER}'), \\"
echo "      service_name='features-service', prefix='${_MF_PREFIX}')\""
