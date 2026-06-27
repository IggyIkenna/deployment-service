#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Shared helpers for the per-venue tradfi OHLCV-1m backfill launchers
# (`launch-tradfi-bf-{cme,ice,nasdaq,nyse}-ohlcv-1m.sh`).
#
# OHLCV-only path: each VM downloads ohlcv_1m for a single (venue, symbol-set,
# year-shard) tuple. Year-sharding so VM logs/parquets stay bounded; per-VM
# shard isolation via VM_NAME + MANIFEST_PER_VM_SHARDS=true (CLAUDE.md HARD
# RULE).
#
# Sourced by the venue wrappers. Fleet concurrency cap (OHLCV_FLEET_CONCURRENCY_CAP,
# default 20) prevents overloading the shared Databento subscription account.
#
# SSOT: plans/active/tradfi_ohlcv_only_mvp_backfill_2026_05_15.md Phase 6.
set -euo pipefail

# Caller-provided (each wrapper overrides if needed):
TRADFI_OHLCV_ZONE="${TRADFI_OHLCV_ZONE:-asia-northeast1-c}"
TRADFI_OHLCV_PROJECT="${TRADFI_OHLCV_PROJECT:-central-element-323112}"
# e2-highmem-4 (32GB) — tradfi OHLCV backfill peaks ~15GB/chunk (a per-date transient
# spike on a heavy fetch: liquid GC.OPT ohlcv_1s expiry day, or NASDAQ/NYSE many-symbol
# ohlcv_1m week) that sits right at e2-standard-4's 16GB ceiling → OOM-crash-loop on 16GB
# (verified 2026-06-24: gc-2025 cleared >1 7-day chunk on 32GB, zero OOM, peak RSS 15.3GB).
# The per-date footprint itself is the real bug (tiny output, ~15GB transient) — reduce it
# (memray) + revert to e2-standard-4. SSOT: tradfi_backfill_oom_remediation_2026_06_24.md.
TRADFI_OHLCV_MACHINE="${TRADFI_OHLCV_MACHINE:-e2-highmem-4}"
TRADFI_OHLCV_BOOT_GB="${TRADFI_OHLCV_BOOT_GB:-50}"
TRADFI_OHLCV_CODE_BUCKET="${TRADFI_OHLCV_CODE_BUCKET:-deployment-scripts-${TRADFI_OHLCV_PROJECT}}"
TRADFI_OHLCV_STARTUP="${TRADFI_OHLCV_STARTUP:-gs://${TRADFI_OHLCV_CODE_BUCKET}/vm/setup-data-pipeline-vm.sh}"

# Provisioning model — backfill is idempotent (per-shard manifest resume via
# VM_NAME + MANIFEST_PER_VM_SHARDS) so it DEFAULTS TO SPOT (~60-91% cheaper).
# GCP promo credits were exhausted 2026-06-20, so on-demand backfill now burns
# real cash. SPOT (not legacy --preemptible) so heavy shards aren't force-killed
# at 24h; --instance-termination-action=DELETE avoids orphaned stopped VMs (their
# disks accrue cost) since a preempted shard re-runs cleanly from the manifest.
# Escape hatch: TRADFI_OHLCV_ON_DEMAND=true (env) or --on-demand (flag) forces
# standard provisioning for a deadline-critical wave that can't absorb preemption.
# Flags are computed at create time (ohlcv_create_vm) so --on-demand, parsed after
# this lib is sourced, takes effect. SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
TRADFI_OHLCV_ON_DEMAND="${TRADFI_OHLCV_ON_DEMAND:-false}"

# --source provenance selector (REQUIRED for a TradFi OHLCV download, 2026-06-19):
# selects the fetching adapter AND stamps row-level provenance. Default = databento
# (the 3-dataset subscription path: GLBX.MDP3 CME / DBEQ.BASIC equities / XCBF.PITCH
# CFE — databento is capable for every tradfi OHLCV venue; only `massive` is excluded
# for CBOE/VX). Override with OHLCV_SOURCE=massive for a Massive-sourced leg.
# SSOT: codex/02-data/tradfi-databento-sourcing-ssot.md.
TRADFI_OHLCV_SOURCE="${OHLCV_SOURCE:-databento}"

# Data-types to fetch. Default = BOTH ohlcv_1m + ohlcv_1s: the Databento
# subscription includes both at L0/free 16y and the registry expects them
# "fetched alongside" (1s aggregates downstream to 15m/1h/24h via MDPS). Override
# e.g. OHLCV_DATA_TYPES=ohlcv_1s for a 1s-only backfill wave (no 1m re-fetch).
# Semicolon-delimited (gcloud metadata-safe; startup splits [,;] → spaces).
TRADFI_OHLCV_DATA_TYPES="${OHLCV_DATA_TYPES:-ohlcv_1m;ohlcv_1s}"

# Concurrency-cap check: refuse to launch when the count of RUNNING tradfi-bf-*
# VMs in the zone reaches OHLCV_FLEET_CONCURRENCY_CAP.
#
# RATIONALE: Databento documents 100 concurrent connections per IP and 100
# timeseries requests/second per IP (DatabentoIPRateLimiter._RAW_LIMITS).
# Empirically, 18 tradfi-bf-* VMs ran concurrently without triggering 429s
# (instruments_foundation_completeness_2026_06_24.md — "killed the 18 RUNNING
# tradfi-bf-* OHLCV backfills"). A singleton (cap=1) serialises the whole fleet
# for no safety reason. Cap=20 is the minimum safe floor above the empirical
# 18-VM high-water mark, well within the 100-connection hard limit.
# Override: OHLCV_FLEET_CONCURRENCY_CAP=N (env) to raise/lower at call time.
OHLCV_FLEET_CONCURRENCY_CAP="${OHLCV_FLEET_CONCURRENCY_CAP:-20}"

ohlcv_check_singleton_lock() {
    local force="${1:-false}"
    local dry_run="${2:-false}"
    if [[ "$force" == "true" || "$dry_run" == "true" ]]; then return 0; fi
    local running_count
    running_count="$(gcloud compute instances list \
        --filter='name~"^tradfi-bf-" AND status=RUNNING' \
        --zones="$TRADFI_OHLCV_ZONE" \
        --format='value(name)' 2>/dev/null | wc -l)"
    if (( running_count >= OHLCV_FLEET_CONCURRENCY_CAP )); then
        cat >&2 <<EOF
ERROR: ${running_count} tradfi-bf-* VM(s) already RUNNING in $TRADFI_OHLCV_ZONE.
Fleet concurrency cap reached (OHLCV_FLEET_CONCURRENCY_CAP=${OHLCV_FLEET_CONCURRENCY_CAP}).
Inspect:  gcloud compute instances list --filter='name~"^tradfi-bf-" AND status=RUNNING' --zones=$TRADFI_OHLCV_ZONE
Force:    pass --force to bypass.
Override: OHLCV_FLEET_CONCURRENCY_CAP=N bash <launcher> to raise the cap.
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

    # VM_TASK=mtds-backfill routes to the chunked MTDS download branch in
    # setup-data-pipeline-vm.sh that builds the CLI with --source (REQUIRED for a
    # TradFi OHLCV run). The prior `cefi-backfill` task did NOT pass --source → every
    # payload failed ("--source databento|massive is REQUIRED") + 0 rows at rc=0/1.
    local metadata="VM_TASK=mtds-backfill"
    metadata="${metadata},VM_SERVICE=market_tick_data_service"
    metadata="${metadata},VM_OPERATION=download"
    metadata="${metadata},VM_ASSET_GROUP=TRADFI"
    metadata="${metadata},VM_VENUE=${vm_venue}"
    metadata="${metadata},VM_FORCE_WINDOW=${force_window}"
    metadata="${metadata},VM_START_DATE=${start_date}"
    metadata="${metadata},VM_END_DATE=${end_date}"
    metadata="${metadata},VM_DATA_TYPES=${TRADFI_OHLCV_DATA_TYPES}"
    metadata="${metadata},VM_SOURCE=${TRADFI_OHLCV_SOURCE}"
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
        # Idempotent backfill → SPOT by default (~60-91% cheaper); --on-demand /
        # TRADFI_OHLCV_ON_DEMAND=true forces standard. Unquoted expansion is
        # intentional (word-split the flag string; gcloud flags carry no spaces).
        local provisioning_flags=""
        if [[ "$TRADFI_OHLCV_ON_DEMAND" != "true" ]]; then
            provisioning_flags="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
        fi
        echo "Launching $vm_name_safe (venue=$vm_venue ${start_date}..${end_date}) [$([[ -n "$provisioning_flags" ]] && echo SPOT || echo on-demand)]"
        # shellcheck disable=SC2086
        gcloud compute instances create "$vm_name_safe" \
            --project="$TRADFI_OHLCV_PROJECT" \
            --zone="$TRADFI_OHLCV_ZONE" \
            --machine-type="$TRADFI_OHLCV_MACHINE" \
            ${provisioning_flags} \
            --image-family=ubuntu-2404-lts-amd64 \
            --image-project=ubuntu-os-cloud \
            --boot-disk-size="${TRADFI_OHLCV_BOOT_GB}GB" \
            --scopes=cloud-platform \
            --metadata="startup-script-url=${TRADFI_OHLCV_STARTUP},${metadata}" \
            --labels=purpose=tradfi-bf-ohlcv,env="${deployment_env}",run-ts="${run_ts}",venue="$(echo "$vm_venue" | tr '[:upper:]' '[:lower:]')"
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
    # End the current-year shard at YESTERDAY (UTC): Databento historical data is
    # T+1, so requesting today returns `DATA_NOT_AVAILABLE: is in the future`.
    local today_iso
    today_iso="$(date -u -d 'yesterday' +%Y-%m-%d 2>/dev/null || date -u -v-1d +%Y-%m-%d)"
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
#   FORCE | DRY_RUN | DEPLOYMENT_ENV | START_FLOOR | FORCE_WINDOW | ONLY_YEAR
ohlcv_parse_common_args() {
    FORCE=false
    DRY_RUN=false
    DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
    START_FLOOR="2019-01-01"
    FORCE_WINDOW="true"
    ONLY_YEAR=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --force)            FORCE=true; shift ;;
            --dry-run)          DRY_RUN=true; shift ;;
            --on-demand)        TRADFI_OHLCV_ON_DEMAND=true; shift ;;
            --no-force-window)  FORCE_WINDOW="false"; shift ;;
            --env)              DEPLOYMENT_ENV="$2"; shift 2 ;;
            --start-floor)      START_FLOOR="$2"; shift 2 ;;
            --year)             ONLY_YEAR="$2"; shift 2 ;;
            --help|-h)
                grep '^#' "${BASH_SOURCE[1]}" | head -40
                exit 0
                ;;
            *)
                echo "Unknown arg: $1" >&2
                echo "Usage: ${BASH_SOURCE[1]##*/} [--dry-run] [--force] [--on-demand] [--no-force-window] [--year YYYY] [--env prod|staging|dev] [--start-floor YYYY-MM-DD]" >&2
                exit 1
                ;;
        esac
    done

    case "$DEPLOYMENT_ENV" in
        prod|staging|dev) ;;
        *) echo "ERROR: --env must be prod|staging|dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
    esac

    if [[ -n "$ONLY_YEAR" ]] && ! [[ "$ONLY_YEAR" =~ ^[0-9]{4}$ ]]; then
        echo "ERROR: --year must be a 4-digit year (got: $ONLY_YEAR)" >&2; exit 1
    fi
}

# Filter year-shards down to a single year if ONLY_YEAR is set. Echoes the
# (possibly-filtered) shard list. Pass the original shard string as $1.
ohlcv_apply_year_filter() {
    local shards="$1"
    if [[ -z "$ONLY_YEAR" ]]; then
        printf '%s' "$shards"
        return
    fi
    local out=""
    local IFS=';'
    for shard in $shards; do
        local start="${shard%%:*}"
        if [[ "${start:0:4}" == "$ONLY_YEAR" ]]; then
            out="${out}${out:+;}${shard}"
        fi
    done
    printf '%s' "$out"
}
