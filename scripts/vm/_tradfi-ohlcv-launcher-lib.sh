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
# SSOT: plans/active/tradfi_backfill_throughput_followups_2026_07_24.md.
set -euo pipefail

# Caller-provided (each wrapper overrides if needed):
TRADFI_OHLCV_ZONE="${TRADFI_OHLCV_ZONE:-asia-northeast1-c}"
TRADFI_OHLCV_PROJECT="${TRADFI_OHLCV_PROJECT:-central-element-323112}"
# e2-highmem-16 (128GB) — raised from e2-highmem-4 2026-07-25 per the measured A3.1
# throughput analysis (tradfi_backfill_throughput_followups_2026_07_24.md): every real
# measurement of the 1.56x date-fanout lever (--batch-date-concurrency) and the
# 46.9k rows/min/VM CME rate was taken ON e2-highmem-16 — e2-highmem-4 was still the
# launcher default while the whole throughput model assumed 16 vCPU, understating the
# real backfill ETA by ~1.5x. Still comfortably clears the 2026-06-24 OOM floor below
# (15.3GB peak RSS on 32GB e2-highmem-4; 128GB leaves far more headroom).
# Prior context (why e2-highmem-4 over e2-standard-4): tradfi OHLCV backfill peaks
# ~15GB/chunk (a per-date transient spike on a heavy fetch: liquid GC.OPT ohlcv_1s
# expiry day, or NASDAQ/NYSE many-symbol ohlcv_1m week) that sits right at
# e2-standard-4's 16GB ceiling → OOM-crash-loop on 16GB (verified 2026-06-24: gc-2025
# cleared >1 7-day chunk on 32GB, zero OOM). SSOT: tradfi_backfill_oom_remediation_2026_06_24.md,
# tradfi_backfill_throughput_followups_2026_07_24.md (A3.1 + the OHLCV_FLEET_CONCURRENCY_CAP raise below).
TRADFI_OHLCV_MACHINE="${TRADFI_OHLCV_MACHINE:-e2-highmem-16}"
# pd-balanced 250GB: pd-standard throttles sustained writes hard (~0.17 MB/s/GB,
# measured disk-bound on cefi backfills); pd-balanced is SSD-backed and scales
# write throughput with size, so a larger balanced disk lifts the download→parquet
# write ceiling that gated ohlcv_1m throughput. Env-overridable.
TRADFI_OHLCV_BOOT_GB="${TRADFI_OHLCV_BOOT_GB:-250}"
TRADFI_OHLCV_BOOT_TYPE="${TRADFI_OHLCV_BOOT_TYPE:-pd-balanced}"
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

# --force-recapture (corrective re-capture only). When true, ohlcv_create_vm
# stamps VM_FORCE=true into the VM metadata, which setup-data-pipeline-vm.sh
# turns into the MTDS CLI `--force` flag. `--force` makes the MTDS download
# handler BYPASS `_apply_freshness_skip()` (tick_data_handler.py), so the VM
# re-fetches the dates IN ITS WINDOW even when the manifest already has some
# CME rows for those dates (the freshness check otherwise marks an
# already-partially-captured historical date as "fresh" → skips it → a windowed
# corrective re-capture silently does nothing + emits no per-VM manifest shard).
# DELIBERATELY OFF BY DEFAULT: a steady-state full backfill must stay
# idempotent (skip already-captured dates, never re-fetch / overwrite good
# data). Only a surgical corrective window (e.g. re-capturing dates whose
# options legs failed schema validation under an old bug) sets this.
# SSOT: codex/02-data/tradfi-databento-sourcing-ssot.md (freshness-skip vs VM_FORCE).
OHLCV_FORCE_RECAPTURE="${OHLCV_FORCE_RECAPTURE:-false}"

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

# Databento concurrency knobs.
#
# TRADFI_OHLCV_BATCH_DATE_CONCURRENCY is now ON BY DEFAULT for databento-sourced
# launches (2026-07-20). It was shipped opt-in/default-OFF pending the UTL
# ServiceCLI `--batch-date-concurrency` rollout; that landed (utl@7b4ed95d +
# dep@ac5d166), was MEASURED at **1.56x** on a real Databento VM (16 vCPU
# e2-highmem-16, 6 heavy CME roots, Jan-2024: serial 27.3 min vs conc=20
# 17.5 min for the same 820,639 rows; serial CPU idle 18/56 samples vs
# concurrent 0/36 — the date-fanout overlaps one date's fetch latency with
# another's parse/write), and then was never turned on, so every tradfi OHLCV
# backfill has been running SERIAL and leaving that 1.56x on the table.
#
# The default is DERIVED FROM THE MACHINE, not a flat constant, because the win
# scales with vCPU: the same change measured only ~4% on a 4-vCPU/1-root run
# (20-way fanout was CPU-bound there). `ohlcv_default_date_concurrency` scales
# ~1.25 dates-in-flight per vCPU (the measured 16 vCPU → 20 ratio) and CAPS at
# the Databento effective per-IP budget.
#
# Explicitly setting TRADFI_OHLCV_BATCH_DATE_CONCURRENCY overrides the derived
# default; setting it to 1 restores fully-serial behavior.
#
# NOT applied to non-databento launches (FX → Yahoo daily): the per-IP budget
# below is a Databento limit and Yahoo has its own, much tighter, throttle.
# SSOT: codex/02-data/tradfi-databento-sourcing-ssot.md.
TRADFI_OHLCV_BATCH_DATE_CONCURRENCY="${TRADFI_OHLCV_BATCH_DATE_CONCURRENCY:-}"
TRADFI_OHLCV_DATABENTO_MAX_CONCURRENT="${TRADFI_OHLCV_DATABENTO_MAX_CONCURRENT:-}"

# Databento's documented per-IP limits are 100 concurrent connections / 100
# timeseries req/s, applied at 0.8 target utilization by
# `DatabentoIPRateLimiter` (market_tick_data_service .../databento_key_cache.py)
# → 80 effective. The limits are per-IP and NOT per-key, and every backfill VM
# is created with its own ephemeral external IP, so this budget is PER VM — it
# is NOT a fleet-wide budget and is therefore NOT the Tardis-style storm risk
# (which shares one IP). The fleet cap below stays a separate, courtesy limit.
OHLCV_DATABENTO_PER_IP_BUDGET="${OHLCV_DATABENTO_PER_IP_BUDGET:-80}"

# Echo the default `--batch-date-concurrency` for this launch, or "" when the
# lever does not apply (non-databento source). Scales with the machine's vCPU
# count (parsed off the trailing number of the GCE machine type, e.g.
# e2-highmem-4 → 4) at ~1.25 dates in flight per vCPU, clamped to
# [2, OHLCV_DATABENTO_PER_IP_BUDGET]. Machine types with no trailing vCPU count
# (e2-micro / e2-small) assume vcpu=2.
#
# Worked examples: e2-highmem-4 (the default) → 5; e2-highmem-16 → 20, which
# exactly reproduces the configuration the 1.56x was measured on;
# e2-standard-96 → 80, clamped by the per-IP budget.
ohlcv_default_date_concurrency() {
    [[ "$TRADFI_OHLCV_SOURCE" == "databento" ]] || { printf '%s' ""; return 0; }
    local vcpu="${TRADFI_OHLCV_MACHINE##*-}"
    [[ "$vcpu" =~ ^[0-9]+$ ]] || vcpu=2
    local conc=$(( (vcpu * 125 + 99) / 100 ))
    (( conc < 2 )) && conc=2
    (( conc > OHLCV_DATABENTO_PER_IP_BUDGET )) && conc="$OHLCV_DATABENTO_PER_IP_BUDGET"
    printf '%s' "$conc"
}

# Concurrency-cap check: refuse to launch when the count of RUNNING tradfi-bf-*
# VMs in the zone reaches OHLCV_FLEET_CONCURRENCY_CAP.
#
# RATIONALE: Databento documents 100 concurrent connections per IP and 100
# timeseries requests/second per IP (DatabentoIPRateLimiter._RAW_LIMITS).
# Empirically, 18 tradfi-bf-* VMs ran concurrently without triggering 429s
# (instruments_foundation_completeness_2026_06_24.md — "killed the 18 RUNNING
# tradfi-bf-* OHLCV backfills"). A singleton (cap=1) serialises the whole fleet
# for no safety reason.
#
# THIS IS A COURTESY CAP, NOT A SAFETY CAP — and it is emphatically NOT the
# Tardis situation. The Databento limits above are PER-IP, and `ohlcv_create_vm`
# gives every VM its own ephemeral external IP (no --no-address, no shared NAT),
# so the 100-connection / 100-req-s budget is spent PER VM. Adding VMs adds
# budget; it does not divide a shared one. Do NOT apply Tardis-style cap-1
# reasoning here (Tardis shares ONE IP — see CLAUDE.md § Tardis cap).
#
# Raised 20 → 60 (2026-07-20) for the equity ticker-group re-shard: NASDAQ and
# NYSE now fan out to (ticker-group x year) instead of one VM per year, so the
# equity fleet alone is ~2 venues x 5 groups x 4 years = ~40 VMs, and 20 would
# refuse the second venue mid-rollout. 60 leaves headroom for a concurrent
# CME/ICE/CFE wave.
# Raised 60 → 150 (2026-07-25) per the tick-26 throughput re-analysis
# (tradfi_backfill_throughput_followups_2026_07_24.md): the corrected ETA is
# THROUGHPUT-bound (~999 VM-h against a cap of 60), not critical-path-bound, so
# the cap is the single highest-leverage remaining knob — same per-IP-per-VM
# reasoning as the 20->60 raise above (Databento's 100-conn/100-req-s budget is
# spent per VM, not fleet-wide; 18 concurrent VMs measured zero 429s). Projected
# ~22h -> ~9h backfill ETA. Override: OHLCV_FLEET_CONCURRENCY_CAP=N (env).
OHLCV_FLEET_CONCURRENCY_CAP="${OHLCV_FLEET_CONCURRENCY_CAP:-150}"

# Stall-watchdog progress marker (metadata-safe: no '=', space or comma).
#
# Without this, vm-exec-with-gcs-tee.sh falls back to raw LOCAL_LOG byte-growth
# detection, which the setup-data-pipeline-vm.sh vm-life-emitter defeats forever:
# its own `PIPELINE_HEARTBEAT` line every 60s keeps the log "growing" even when
# the real worker is fully hung, so the 30-min stall timer never fires. That is
# exactly how a cefi VM hung for 7+ days undetected
# (cefi_bf_2021_heavy_vm_stalled_2026_07_12). cefi / mdps / sfi / gas-fees all
# set this; tradfi was the last family still on the weak fallback.
#
# Markers verified EMPIRICALLY against a real tradfi databento run
# (gs://deployment-scripts-.../vm-logs/tradfi-bf-nasdaq-ohlcv-1m-2024-20260719-112444/run.log):
#   "StreamingParquetWriter: uploaded …/AAPL.parquet (N rows, …)"  — per-shard finalize
#   "DatabentoAdapter: streamed chunk DBEQ.BASIC/ohlcv_1m — N rows"  — per-chunk fetch
# Both are needed: `uploaded` alone would false-trip during a long fetch phase on
# a heavy CME expiry date that streams for >30 min before its first write.
TRADFI_OHLCV_STALL_PROGRESS_REGEX="${TRADFI_OHLCV_STALL_PROGRESS_REGEX:-uploaded|streamed}"

# Stall-watchdog TIMEOUT override (found + fixed 2026-07-30, tradfi_es_cme_ohlcv_zero_capture_2026_07_30.md).
#
# vm-exec-with-gcs-tee.sh's STALL_TIMEOUT_SEC default is 1800s (30min). But the
# manifest read path (unified_trading_library.manifest_writer._read_index's
# _wait_for_in_flight_cycle_then_reread) has its own legitimate, by-design
# bounded wait on a live consolidator lock — up to
# consolidator_inflight_horizon_for_bucket()'s tradfi default of 3600s (1hr).
# A VM that starts up during exactly that window (heavy concurrent fleet
# activity holding the lock) can spend its ENTIRE first 30+ minutes in that
# documented-safe wait, never emitting an `uploaded`/`streamed` progress line —
# so the stall watchdog kills it as WORKER_STALLED before the lock it is
# correctly waiting on ever clears. Observed live 2026-07-30: 3 of 7
# tradfi-bf-cme-ohlcv-1m-es-* VMs killed this way, 0/7 completed a single real
# fetch attempt. Fix: give the watchdog headroom past the lock's own horizon
# (3600s + 300s buffer) instead of the generic 1800s default.
TRADFI_OHLCV_STALL_TIMEOUT_SEC="${TRADFI_OHLCV_STALL_TIMEOUT_SEC:-3900}"

# Default number of (ticker-group x year) shards per equity venue. See
# `ohlcv_split_ticker_groups` for why equity venues shard by ticker-group.
OHLCV_TICKER_GROUPS="${OHLCV_TICKER_GROUPS:-5}"

# Default number of (root-group x year) shards for CME-style parent-symbol
# venues. See `ohlcv_split_root_groups` below for why CME bundles roots
# (opposite direction from the equity ticker-group split — CME was already
# OVER-sharded at one-VM-per-root). 58 CME roots / 8 groups ~= 7 roots/VM,
# collapsing ~58 VMs/year-shard to 8 (~406 -> ~56 VMs across 7 year-shards).
# Override with --root-groups N / OHLCV_ROOT_GROUPS=N.
OHLCV_ROOT_GROUPS="${OHLCV_ROOT_GROUPS:-8}"

# Equity sharding mode: "date-range" (default) or "ticker-group" (legacy,
# kept as an escape hatch for the pathological single-VM-memory-ceiling
# case). MEASURED (tick-26 throughput re-analysis,
# tradfi_backfill_throughput_followups_2026_07_24.md): equity per-calendar-
# date cost is ~1.46 min FIXED + 7.1e-4 min/cell, i.e. ticker-count-
# INVARIANT, so the ticker-group fan-out re-pays that fixed overhead once
# PER GROUP over the SAME calendar dates (5x compute for only ~1.0-1.2x
# wall-clock). Splitting by contiguous DATE-range instead (all tickers per
# VM) pays the fixed per-date overhead once per date TOTAL, collapsing the
# equity critical path (measured 7.1h -> 1.2h, 231 -> 46 VM-h). Override
# with --shard-mode ticker-group / OHLCV_SHARD_MODE=ticker-group.
OHLCV_SHARD_MODE="${OHLCV_SHARD_MODE:-date-range}"
# Date-slices per year-shard in date-range mode. Default 5 mirrors the prior
# ticker-group default (5 groups x ~4 years == the measured "20 date-
# slices/venue" the tick-26 analysis sized the critical-path win on).
OHLCV_DATE_SLICES="${OHLCV_DATE_SLICES:-5}"

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
    # Corrective re-capture: VM_FORCE=true → setup-data-pipeline-vm.sh adds
    # `--force` to the MTDS CLI → bypass the freshness-skip for the window.
    # Gated behind --force-recapture so steady-state backfill stays idempotent.
    metadata="${metadata},VM_FORCE=${OHLCV_FORCE_RECAPTURE}"
    metadata="${metadata},VM_START_DATE=${start_date}"
    metadata="${metadata},VM_END_DATE=${end_date}"
    metadata="${metadata},VM_DATA_TYPES=${TRADFI_OHLCV_DATA_TYPES}"
    metadata="${metadata},VM_SOURCE=${TRADFI_OHLCV_SOURCE}"
    metadata="${metadata},VM_INSTRUMENT_IDS=${instrument_ids}"
    metadata="${metadata},DEPLOYMENT_ENV=${deployment_env}"
    metadata="${metadata},VM_NAME=${vm_name_safe}"
    metadata="${metadata},MANIFEST_PER_VM_SHARDS=true"
    metadata="${metadata},VM_SHUTDOWN_ON_COMPLETION=true"
    # Real per-shard progress marker for the stall watchdog — without it the
    # PIPELINE_HEARTBEAT noise makes a hung VM look alive forever (see
    # TRADFI_OHLCV_STALL_PROGRESS_REGEX above).
    metadata="${metadata},STALL_PROGRESS_REGEX=${TRADFI_OHLCV_STALL_PROGRESS_REGEX}"
    # Headroom past the manifest-consolidator-lock's own bounded-wait horizon
    # (see TRADFI_OHLCV_STALL_TIMEOUT_SEC above) — without it, a VM correctly
    # waiting on that lock gets killed as a false-positive stall.
    metadata="${metadata},STALL_TIMEOUT_SEC=${TRADFI_OHLCV_STALL_TIMEOUT_SEC}"
    # Date-concurrency (the measured 1.56x lever). Explicit env wins; otherwise
    # the machine-derived default applies to databento-sourced launches and is
    # empty for FX/Yahoo (see ohlcv_default_date_concurrency above). Resolved
    # HERE, at create time, so a wrapper that overrides TRADFI_OHLCV_MACHINE or
    # TRADFI_OHLCV_SOURCE *after* sourcing this lib (FX does exactly that) is
    # still read correctly.
    local date_concurrency="${TRADFI_OHLCV_BATCH_DATE_CONCURRENCY:-$(ohlcv_default_date_concurrency)}"
    [[ -n "$date_concurrency" ]] && metadata="${metadata},VM_BATCH_DATE_CONCURRENCY=${date_concurrency}"
    # DATABENTO_MAX_CONCURRENT_REQUESTS stays opt-in: the MTDS config default is
    # already 100 (market_interface/config.py), which is above every derived
    # date-concurrency above, so the client-side asyncio semaphore never
    # throttles below the date fan-out and needs no launcher override.
    [[ -n "$TRADFI_OHLCV_DATABENTO_MAX_CONCURRENT" ]] && metadata="${metadata},DATABENTO_MAX_CONCURRENT_REQUESTS=${TRADFI_OHLCV_DATABENTO_MAX_CONCURRENT}"

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
            --boot-disk-type="${TRADFI_OHLCV_BOOT_TYPE}" \
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

# --- Equity ticker-group sharding -------------------------------------------
# WHY equity venues shard by (ticker-group x year), not by year alone.
#
# CME shards 47 roots x 7 years (~329 VMs) and is embarrassingly parallel. The
# equity venues did NOT: one VM per YEAR carried ALL ~622 tickers, so 207,856
# equity cells (46% of all remaining tradfi work) compressed onto ~4 year-shards
# per venue. The single longest NASDAQ VM carried ~30,106 cells = a 12.5-33 hr
# critical path, and THAT — not the vendor, not the fleet cap — was the binding
# constraint on the tradfi MVP backfill ETA.
#
# Splitting the sorted ticker universe into N contiguous groups multiplies the
# shard count by N (5 groups x 4 years = ~20 VMs/venue vs 4) and divides the
# critical path by ~N. This is SAFE to scale because the Databento budget is
# per-IP and every VM has its own IP (see OHLCV_FLEET_CONCURRENCY_CAP above) —
# more VMs buy more rate-limit budget rather than contending for one.
#
# CONTIGUOUS (not round-robin) grouping is deliberate: ohlcv row-count per
# ticker per day is bounded by the session minute count (~390) and is roughly
# uniform across tickers, so equal-COUNT groups are already load-balanced, and
# contiguity keeps a VM's scope human-readable ("g02 = BKNG..DIS") for resume
# and triage. Per-VM manifest shard isolation (VM_NAME + MANIFEST_PER_VM_SHARDS)
# already makes each group independently resumable.
#
# Echoes one group per line: "<1-based index>|<first-ticker>|<last-ticker>|<semicolon-joined tickers>"
#   $1 ticker_list — semicolon-delimited, ALREADY SORTED by the caller
#   $2 groups      — desired group count (clamped to [1, ticker_count])
ohlcv_split_ticker_groups() {
    local list="$1" groups="$2"
    local all=()
    IFS=';' read -ra all <<< "$list"
    local total="${#all[@]}"
    (( total == 0 )) && return 0
    [[ "$groups" =~ ^[0-9]+$ ]] || groups=1
    (( groups < 1 )) && groups=1
    (( groups > total )) && groups="$total"

    local base=$(( total / groups ))
    local rem=$(( total % groups ))
    local idx=0 g i size chunk first last
    for (( g = 0; g < groups; g++ )); do
        size="$base"
        # Spread the remainder over the first `rem` groups so sizes differ by
        # at most 1 — never a fat tail group.
        (( g < rem )) && size=$(( size + 1 ))
        chunk=""; first=""; last=""
        for (( i = 0; i < size; i++ )); do
            [[ -z "$first" ]] && first="${all[idx]}"
            last="${all[idx]}"
            chunk="${chunk}${chunk:+;}${all[idx]}"
            idx=$(( idx + 1 ))
        done
        printf '%s|%s|%s|%s\n' "$(( g + 1 ))" "$first" "$last" "$chunk"
    done
}

# --- CME-style root-group bundling (SINGLE_VM_QUEUE-analog) -----------------
# WHY CME bundles multiple roots per VM, instead of one VM per root.
#
# Unlike equities (one VM/year carrying ALL tickers — under-sharded, a single
# VM's critical path), CME was the OPPOSITE problem: `launch-tradfi-bf-cme-
# ohlcv-1m.sh` spawned one VM per (root, year) — ~58 roots x 7 years =~ 406
# VMs — even though CME is embarrassingly parallel and per-root cost is small
# (2 parent symbols/root). Fleet-management overhead (VM boot, singleton-lock
# scans, manifest-shard fan-out) dominates at that shard count for no
# corresponding throughput win. Bundling N roots' symbol-sets into ONE VM's
# `VM_INSTRUMENT_IDS` per year-shard collapses the VM count by ~N while each
# VM still processes its roots serially inside one MTDS CLI invocation (no
# work lost, just fewer, larger, more-saturated VMs).
#
# CONTIGUOUS (not round-robin) grouping, same rationale as
# `ohlcv_split_ticker_groups`: keeps a VM's scope human-readable for
# resume/triage and the equal-COUNT split keeps groups roughly load-balanced.
#
# Echoes one group per line: "<1-based index>|<first-root>|<last-root>|<semicolon-joined combined symbols>"
#   $1 root_specs — NEWLINE-delimited "root|semicolon-joined-symbols" entries,
#                   caller-ordered (the launcher's own ROOTS array shape)
#   $2 groups     — desired group count (clamped to [1, spec_count])
ohlcv_split_root_groups() {
    local specs="$1" groups="$2"
    local all=()
    local line
    while IFS= read -r line; do
        [[ -n "$line" ]] && all+=("$line")
    done <<< "$specs"
    local total="${#all[@]}"
    (( total == 0 )) && return 0
    [[ "$groups" =~ ^[0-9]+$ ]] || groups=1
    (( groups < 1 )) && groups=1
    (( groups > total )) && groups="$total"

    local base=$(( total / groups ))
    local rem=$(( total % groups ))
    local idx=0 g i size chunk first last spec root syms
    for (( g = 0; g < groups; g++ )); do
        size="$base"
        # Spread the remainder over the first `rem` groups so sizes differ by
        # at most 1 — never a fat tail group.
        (( g < rem )) && size=$(( size + 1 ))
        chunk=""; first=""; last=""
        for (( i = 0; i < size; i++ )); do
            spec="${all[idx]}"
            root="${spec%%|*}"
            syms="${spec#*|}"
            [[ -z "$first" ]] && first="$root"
            last="$root"
            chunk="${chunk}${chunk:+;}${syms}"
            idx=$(( idx + 1 ))
        done
        printf '%s|%s|%s|%s\n' "$(( g + 1 ))" "$first" "$last" "$chunk"
    done
}

# --- Date-range sharding (the default equity shard axis) --------------------
# Split a single [start_iso, end_iso] window into N contiguous date-range
# slices with no day lost or duplicated (same base+remainder distribution as
# `ohlcv_split_ticker_groups`, applied to calendar days instead of tickers).
# Echoes one "start_iso:end_iso" per line.
#   $1 start_iso  — YYYY-MM-DD
#   $2 end_iso    — YYYY-MM-DD
#   $3 n_slices   — desired slice count (clamped to [1, total_days])
_ohlcv_epoch_day() {
    date -u -d "$1" +%s 2>/dev/null || date -u -j -f '%Y-%m-%d' "$1" +%s
}

_ohlcv_iso_from_epoch() {
    date -u -d "@$1" +%Y-%m-%d 2>/dev/null || date -u -r "$1" +%Y-%m-%d
}

ohlcv_split_date_slices() {
    local start_iso="$1" end_iso="$2" n="$3"
    local start_epoch end_epoch total_days
    start_epoch="$(_ohlcv_epoch_day "$start_iso")"
    end_epoch="$(_ohlcv_epoch_day "$end_iso")"
    total_days=$(( (end_epoch - start_epoch) / 86400 + 1 ))
    (( total_days <= 0 )) && return 0
    [[ "$n" =~ ^[0-9]+$ ]] || n=1
    (( n < 1 )) && n=1
    (( n > total_days )) && n="$total_days"

    local base=$(( total_days / n ))
    local rem=$(( total_days % n ))
    local cursor_epoch="$start_epoch"
    local i size slice_start slice_end slice_end_epoch
    for (( i = 0; i < n; i++ )); do
        size="$base"
        # Spread the remainder over the first `rem` slices, same rule as
        # ohlcv_split_ticker_groups — never a fat tail slice.
        (( i < rem )) && size=$(( size + 1 ))
        slice_start="$(_ohlcv_iso_from_epoch "$cursor_epoch")"
        slice_end_epoch=$(( cursor_epoch + (size - 1) * 86400 ))
        slice_end="$(_ohlcv_iso_from_epoch "$slice_end_epoch")"
        printf '%s:%s\n' "$slice_start" "$slice_end"
        cursor_epoch=$(( slice_end_epoch + 86400 ))
    done
}

# --- Per-venue discovery-floor clamp ----------------------------------------
# UAC ``VenueMapping.get_instrument_discovery_start`` is the SSOT for the
# earliest date a (venue, date) shard can produce records. Dates below it are
# EXPECTED-ABSENT (the archive / IS catalog simply has nothing there), so
# launching a year-shard entirely below the floor spawns a VM that boots,
# churns every chunk, captures ZERO rows, self-deletes rc=0, and fires a
# CRITICAL ``DP_VM_GONE_NO_CAPTURE`` "silent zero" alert. This bit the 2019 CME
# OHLCV shards (2026-07-16: every date < the CME floor 2020-01-01 hit "No active
# venues for date=… asset_groups=['TRADFI']" → 0 rows), and the same latent bug
# hits CBOE (2020-06-01) and NASDAQ/NYSE (2023-04-15). The clamp raises
# START_FLOOR to the UAC floor so no doomed sub-floor shard is ever created. It
# only ever RAISES the floor (monotone max), so a deliberately stricter
# wrapper-set floor (e.g. CBOE 2026-01-01) is preserved. Resolved at launch-time
# from UAC — never a hardcoded copy (SSOT-in-UAC; supersedes the ad-hoc
# per-wrapper hardcodes). SSOT: codex/02-data/tradfi-databento-sourcing-ssot.md
# § "Per-venue genesis / discovery-start floors".
_OHLCV_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
_OHLCV_PY="${_OHLCV_REPO_ROOT}/.venv/bin/python"
[[ -x "$_OHLCV_PY" ]] || _OHLCV_PY="python3"

# Echo the UAC instrument-discovery-start floor (YYYY-MM-DD) for a canonical
# venue, or empty string if none is configured / the lookup fails (fail-open:
# no clamp, preserving the caller's START_FLOOR).
ohlcv_venue_discovery_floor() {
    PYTHONPATH="${_OHLCV_REPO_ROOT}" "$_OHLCV_PY" - "$1" <<'PYEOF' 2>/dev/null || true
import sys

from unified_api_contracts.registry.venue_mapping import VenueMapping

print(VenueMapping().get_instrument_discovery_start(sys.argv[1]) or "")
PYEOF
}

# Echo max(start_floor_iso, venue_discovery_floor). ISO YYYY-MM-DD strings sort
# lexicographically, so a plain ``[[ a > b ]]`` string compare is a correct date
# max. When it raises the floor it emits a one-line note to stderr, so a
# ``--start-floor 2019-01-01`` CME run visibly clips to 2020-01-01 instead of
# silently launching a no-op VM.
#   $1 venue        — canonical venue (e.g. CME)
#   $2 start_floor  — current START_FLOOR (YYYY-MM-DD)
ohlcv_clamp_floor_to_venue() {
    local venue="$1" start_floor="$2" vfloor
    vfloor="$(ohlcv_venue_discovery_floor "$venue")"
    if [[ -n "$vfloor" && "$vfloor" > "$start_floor" ]]; then
        echo "Clamped --start-floor ${start_floor} → ${vfloor} (UAC discovery floor for ${venue}; earlier dates are expected-absent — no data)" >&2
        printf '%s' "$vfloor"
    else
        printf '%s' "$start_floor"
    fi
}

# Standard arg parser used by each wrapper. Sets globals:
#   FORCE | DRY_RUN | DEPLOYMENT_ENV | START_FLOOR | FORCE_WINDOW | ONLY_YEAR
#   | OHLCV_FORCE_RECAPTURE (--force-recapture: stamp VM_FORCE=true → MTDS
#     `--force` → bypass the freshness-skip for a corrective windowed re-fetch)
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
            --force-recapture)  OHLCV_FORCE_RECAPTURE=true; FORCE=true; shift ;;
            --dry-run)          DRY_RUN=true; shift ;;
            --on-demand)        TRADFI_OHLCV_ON_DEMAND=true; shift ;;
            --no-force-window)  FORCE_WINDOW="false"; shift ;;
            --env)              DEPLOYMENT_ENV="$2"; shift 2 ;;
            --start-floor)      START_FLOOR="$2"; shift 2 ;;
            --year)             ONLY_YEAR="$2"; shift 2 ;;
            --ticker-groups)    OHLCV_TICKER_GROUPS="$2"; shift 2 ;;
            --root-groups)      OHLCV_ROOT_GROUPS="$2"; shift 2 ;;
            --shard-mode)       OHLCV_SHARD_MODE="$2"; shift 2 ;;
            --date-slices)      OHLCV_DATE_SLICES="$2"; shift 2 ;;
            --help|-h)
                grep '^#' "${BASH_SOURCE[1]}" | head -40
                exit 0
                ;;
            *)
                echo "Unknown arg: $1" >&2
                echo "Usage: ${BASH_SOURCE[1]##*/} [--dry-run] [--force] [--force-recapture] [--on-demand] [--no-force-window] [--year YYYY] [--shard-mode date-range|ticker-group] [--date-slices N] [--ticker-groups N] [--root-groups N] [--env prod|staging|dev] [--start-floor YYYY-MM-DD]" >&2
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

    if ! [[ "$OHLCV_TICKER_GROUPS" =~ ^[0-9]+$ ]] || (( OHLCV_TICKER_GROUPS < 1 )); then
        echo "ERROR: --ticker-groups must be a positive integer (got: $OHLCV_TICKER_GROUPS)" >&2; exit 1
    fi

    if ! [[ "$OHLCV_ROOT_GROUPS" =~ ^[0-9]+$ ]] || (( OHLCV_ROOT_GROUPS < 1 )); then
        echo "ERROR: --root-groups must be a positive integer (got: $OHLCV_ROOT_GROUPS)" >&2; exit 1
    fi

    case "$OHLCV_SHARD_MODE" in
        date-range|ticker-group) ;;
        *) echo "ERROR: --shard-mode must be date-range|ticker-group (got: $OHLCV_SHARD_MODE)" >&2; exit 1 ;;
    esac

    if ! [[ "$OHLCV_DATE_SLICES" =~ ^[0-9]+$ ]] || (( OHLCV_DATE_SLICES < 1 )); then
        echo "ERROR: --date-slices must be a positive integer (got: $OHLCV_DATE_SLICES)" >&2; exit 1
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
