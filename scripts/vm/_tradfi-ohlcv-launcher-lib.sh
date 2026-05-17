#!/usr/bin/env bash
# Shared helpers for the per-venue tradfi OHLCV-1m backfill launchers
# (`launch-tradfi-bf-{cme,ice,nasdaq,nyse}-ohlcv-1m.sh`).
#
# OHLCV-only path: each VM downloads ohlcv_1m for a single (venue, symbol-set,
# year-shard) tuple. Year-sharding so VM logs/parquets stay bounded; per-VM
# shard isolation via VM_NAME + MANIFEST_PER_VM_SHARDS=true (CLAUDE.md HARD
# RULE).
#
# Sourced by the 4 venue wrappers. Singleton lock matches ^tradfi-bf- so all
# wrappers serialize on the shared Databento PAYG account.
#
# SSOT: plans/active/tradfi_ohlcv_only_mvp_backfill_2026_05_15.md Phase 6.
set -euo pipefail

# Caller-provided (each wrapper overrides if needed):
TRADFI_OHLCV_ZONE="${TRADFI_OHLCV_ZONE:-asia-northeast1-c}"
TRADFI_OHLCV_PROJECT="${TRADFI_OHLCV_PROJECT:-central-element-323112}"
TRADFI_OHLCV_MACHINE="${TRADFI_OHLCV_MACHINE:-e2-standard-4}"
TRADFI_OHLCV_BOOT_GB="${TRADFI_OHLCV_BOOT_GB:-50}"
TRADFI_OHLCV_CODE_BUCKET="${TRADFI_OHLCV_CODE_BUCKET:-deployment-scripts-${TRADFI_OHLCV_PROJECT}}"
TRADFI_OHLCV_STARTUP="${TRADFI_OHLCV_STARTUP:-gs://${TRADFI_OHLCV_CODE_BUCKET}/vm/setup-data-pipeline-vm.sh}"

# Singleton-lock check: refuse to launch if any tradfi-bf-* VM is RUNNING in
# the zone, unless $FORCE=true or $DRY_RUN=true.
ohlcv_check_singleton_lock() {
    local force="${1:-false}"
    local dry_run="${2:-false}"
    if [[ "$force" == "true" || "$dry_run" == "true" ]]; then return 0; fi
    local existing
    existing="$(gcloud compute instances list \
        --filter='name~"^tradfi-bf-" AND status=RUNNING' \
        --zones="$TRADFI_OHLCV_ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$existing" ]]; then
        cat >&2 <<EOF
ERROR: TRADFI backfill VM already running in $TRADFI_OHLCV_ZONE: $existing
Refusing to launch a duplicate — Databento PAYG account is shared.
Inspect:   gcloud compute ssh $existing --zone=$TRADFI_OHLCV_ZONE
Tail log:  gsutil cat gs://${TRADFI_OHLCV_CODE_BUCKET}/vm-logs/${existing}/run.log
Stop:      gcloud compute instances delete $existing --zone=$TRADFI_OHLCV_ZONE --quiet
Force:     pass --force to bypass.
EOF
        exit 1
    fi
}

# Create one OHLCV-1m backfill VM.
# Args:
#   $1 vm_name           — must start with tradfi-bf- (zombie-watchdog prefix)
#   $2 vm_venue          — CME | ICE | NASDAQ | NYSE
#   $3 start_date        — YYYY-MM-DD
#   $4 end_date          — YYYY-MM-DD
#   $5 instrument_ids    — semicolon-delimited list of symbols/parent-symbols
#   $6 dry_run           — "true" | "false"
#   $7 deployment_env    — prod | staging | dev
#   $8 force_window      — true | false (--force-window pass-through)
ohlcv_create_vm() {
    local vm_name="$1"
    local vm_venue="$2"
    local start_date="$3"
    local end_date="$4"
    local instrument_ids="$5"
    local dry_run="$6"
    local deployment_env="$7"
    local force_window="$8"

    # Normalise underscores in vm_name (gcloud naming rule).
    local vm_name_safe="${vm_name//_/-}"

    local metadata="VM_TASK=cefi-backfill"
    metadata="${metadata},VM_SERVICE=market_tick_data_service"
    metadata="${metadata},VM_OPERATION=download"
    metadata="${metadata},VM_ASSET_GROUP=TRADFI"
    metadata="${metadata},VM_VENUE=${vm_venue}"
    metadata="${metadata},VM_FORCE_WINDOW=${force_window}"
    metadata="${metadata},VM_START_DATE=${start_date}"
    metadata="${metadata},VM_END_DATE=${end_date}"
    metadata="${metadata},VM_DATA_TYPES=ohlcv_1m"
    metadata="${metadata},VM_INSTRUMENT_IDS=${instrument_ids}"
    metadata="${metadata},DEPLOYMENT_ENV=${deployment_env}"
    metadata="${metadata},VM_NAME=${vm_name_safe}"
    metadata="${metadata},MANIFEST_PER_VM_SHARDS=true"
    metadata="${metadata},VM_SHUTDOWN_ON_COMPLETION=true"

    local run_ts
    run_ts="$(date +%Y%m%d-%H%M%S)"

    if [[ "$dry_run" == "true" ]]; then
        echo "[DRY-RUN] $vm_name_safe"
        echo "          venue=$vm_venue ${start_date}..${end_date}"
        echo "          instruments=${instrument_ids}"
        echo "          machine=${TRADFI_OHLCV_MACHINE} zone=${TRADFI_OHLCV_ZONE}"
    else
        echo "Launching $vm_name_safe (venue=$vm_venue ${start_date}..${end_date})"
        gcloud compute instances create "$vm_name_safe" \
            --project="$TRADFI_OHLCV_PROJECT" \
            --zone="$TRADFI_OHLCV_ZONE" \
            --machine-type="$TRADFI_OHLCV_MACHINE" \
            --image-family=ubuntu-2404-lts-amd64 \
            --image-project=ubuntu-os-cloud \
            --boot-disk-size="${TRADFI_OHLCV_BOOT_GB}GB" \
            --scopes=cloud-platform \
            --metadata="startup-script-url=${TRADFI_OHLCV_STARTUP},${metadata}" \
            --labels=purpose=tradfi-bf-ohlcv,env="${deployment_env}",run-ts="${run_ts}",venue="${vm_venue,,}"
        echo "  VM launched: $vm_name_safe"
        sleep 3
    fi
}

# Build year-shard windows for a [floor, today] range. Echoes "YYYY-MM-DD:YYYY-MM-DD"
# semicolon-delimited.
# Args:
#   $1 floor_year       — e.g. 2019
#   $2 floor_iso        — e.g. 2019-01-01 (clip earlier dates here)
ohlcv_year_shards() {
    local floor_year="$1"
    local floor_iso="$2"
    local current_year
    current_year="$(date +%Y)"
    local today_iso
    today_iso="$(date +%Y-%m-%d)"
    local out=""
    for ((y = floor_year; y <= current_year; y++)); do
        local start="${y}-01-01"
        local end="${y}-12-31"
        if (( y == floor_year )) && [[ -n "$floor_iso" ]]; then
            start="$floor_iso"
        fi
        if (( y == current_year )); then
            end="$today_iso"
        fi
        out="${out}${out:+;}${start}:${end}"
    done
    printf '%s' "$out"
}

# Standard arg parser used by each wrapper. Sets globals:
#   FORCE | DRY_RUN | DEPLOYMENT_ENV | START_FLOOR | FORCE_WINDOW
ohlcv_parse_common_args() {
    FORCE=false
    DRY_RUN=false
    DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
    START_FLOOR="2019-01-01"
    FORCE_WINDOW="true"

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --force)            FORCE=true; shift ;;
            --dry-run)          DRY_RUN=true; shift ;;
            --no-force-window)  FORCE_WINDOW="false"; shift ;;
            --env)              DEPLOYMENT_ENV="$2"; shift 2 ;;
            --start-floor)      START_FLOOR="$2"; shift 2 ;;
            --help|-h)
                grep '^#' "${BASH_SOURCE[1]}" | head -40
                exit 0
                ;;
            *)
                echo "Unknown arg: $1" >&2
                echo "Usage: ${BASH_SOURCE[1]##*/} [--dry-run] [--force] [--no-force-window] [--env prod|staging|dev] [--start-floor YYYY-MM-DD]" >&2
                exit 1
                ;;
        esac
    done

    case "$DEPLOYMENT_ENV" in
        prod|staging|dev) ;;
        *) echo "ERROR: --env must be prod|staging|dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
    esac
}
