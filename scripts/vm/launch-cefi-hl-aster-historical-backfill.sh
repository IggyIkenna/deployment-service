#!/usr/bin/env bash
# Epic: mtds_mdps_master
# Lifecycle: campaign
# Delete-when: HL/ASTER/LIGHTER/EXTENDED historical backfill complete (denominator
#   holes closed per mvp_backfill_cefi_tick_v10_2026_06_27.md G4 gate)
#
# Launch year-sharded backfill VMs for on-chain-perp cefi venues via the generic
# OnchainPerpBatchHandler (VM_OPERATION=collect-onchain-perp-batch): HYPERLIQUID (S3
# requester-pays), ASTER (REST), LIGHTER-ZKSYNC / EXTENDED-STARKNET
# (native REST via _umi_lighter.py / _umi_extended.py in
# umi_tick_provider.py). These venues were previously blocked by an orchestrator defi-strip
# that misclassified them as DeFi despite their failed cells living in the cefi manifest.
#
# Root cause fix (UAC@defi_venues.py): these venues removed from ALL_DEFI_VENUES +
# DEFI_VENUE_PHASE → VENUE_TO_ASSET_GROUP now maps them to "cefi" → both orchestrator
# defi-strip and umi_tick_provider defi guard now pass → the per-venue _fetch_* routes
# in umi_tick_provider.py are reachable.
#
# Data sources:
#   HYPERLIQUID:       requester-pays S3 via HyperliquidS3Downloader
#                       auth: aws-hyperliquid-s3 Secret Manager key (EXISTS)
#   ASTER:              REST API via _fetch_aster_rest (Binance Futures-compatible)
#   LIGHTER-ZKSYNC:     REST via _umi_lighter.py (zkSync Era)
#   EXTENDED-STARKNET:  REST via _umi_extended.py (Starknet)
#
# Coverage ranges:
#   HYPERLIQUID:        2023-01-01 → today (30,835 cells, cefi manifest audit 2026-06-21)
#   ASTER:               2024-01-01 → today (17,675 cells, cefi manifest audit 2026-06-21)
#   LIGHTER-ZKSYNC:      2024-08-01 → today (UAC VENUE_DATA_TYPE_CAPABILITIES start_date)
#   EXTENDED-STARKNET:   2024-10-01 → today (UAC VENUE_DATA_TYPE_CAPABILITIES start_date)
#
# Data types: trades, derivative_ticker (+ book_snapshot_5 for HL/LIGHTER/EXTENDED
#   — the handler EXCLUDES per-venue live-only/dropped data_types automatically):
#   - ASTER book_snapshot_5 + liquidations: LIVE-ONLY (Binance-compat WS captures
#     them going forward) → excluded from the batch universe by the handler.
#   - HYPERLIQUID liquidations: DROPPED entirely (HL publishes no liq feed anywhere).
#   SSOT: cefi_hl_aster_batch_data_gaps_2026_06_22.md BUG #4;
#   cefi_layer1_denominator_gaps_2026_07_03.md 2f (LIGHTER/EXTENDED share the
#   ASTER live-forward profile at the book5/derivative_ticker/trades grain).
#
# VM prefixes (already registered in vm_zombie_watchdog.py VM_PREFIX_TO_BUCKET):
#   cefi-hyperliquid-  → EPHEMERAL_BATCH → tick-cefi bucket
#   cefi-aster-        → EPHEMERAL_BATCH → tick-cefi bucket
#   cefi-lighter-      → EPHEMERAL_BATCH → tick-cefi bucket (matches cefi-lighter-zksync-*)
#   cefi-extended-     → EPHEMERAL_BATCH → tick-cefi bucket (matches cefi-extended-starknet-*)
#
# Instrument universe (BUG #4): SYMBOLS=ALL → the OnchainPerpBatchHandler enumerates
#   the FULL active perp universe per venue from the IS catalogue
#   (instruments-store-cefi-prd/prod/catalog.parquet) — ~100+ ASTER / ~150+ HL — so
#   funding rates for every (incl. small/illiquid) instrument are attempted, not 9.
#   Override SYMBOLS="BTC;ETH;..." for a surgical re-run of named symbols.
#
# Dry-run: DRY_RUN=1 bash scripts/vm/launch-cefi-hl-aster-historical-backfill.sh
set -e

PROJECT=central-element-323112
ZONE=asia-northeast1-c
STARTUP=gs://deployment-scripts-central-element-323112/vm/setup-data-pipeline-vm.sh

DRY_RUN="${DRY_RUN:-250}"
FORCE="${FORCE:-250}"
MAX_CONCURRENT="${MAX_CONCURRENT:-250}"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND="${ON_DEMAND:-false}"

# YEARS: optional space-separated allowlist to skip already-resolved historical
# year-shards on a re-run (mirrors launch-cefi-sharded-backfill.sh's YEARS=).
# Empty (default) = every year from VENUE_START_YEAR through CURRENT_YEAR, same
# as before this override existed.
YEARS="${YEARS:-}"

# ── Instrument universe ───────────────────────────────────────────────────────
# BUG #4 (2026-06-22): catalogue-driven universe. The ALL sentinel makes the
# OnchainPerpBatchHandler enumerate the FULL active perp universe per venue from
# the IS catalogue (prod/catalog.parquet) — ~100+ ASTER / ~150+ HL — so funding
# rates for every (incl. small/illiquid) instrument are attempted, not the static 9.
# Override SYMBOLS="BTC;ETH;..." for a surgical re-run of named symbols.
SYMBOLS="${SYMBOLS:-ALL}"

# data_types: trades, book_snapshot_5, derivative_ticker (env-overridable).
# The handler EXCLUDES per-venue live-only (ASTER book/liq) + dropped (HL liq) data_types
# automatically, so a uniform list is safe across venues. liquidations is NOT in the
# default — ASTER liq is live-only (WS) + HL liq is dropped (no feed); never batch-attempted.
# SSOT: cefi_hl_aster_batch_data_gaps_2026_06_22.md BUG #4.
DATA_TYPES="${DATA_TYPES:-trades;book_snapshot_5;derivative_ticker}"

# ── Year shards ───────────────────────────────────────────────────────────────
# HL:                2023-01-01 → today
# ASTER:             2024-01-01 → today
# LIGHTER-ZKSYNC:    2024-08-01 → today
# EXTENDED-STARKNET: 2024-10-01 → today
CURRENT_YEAR=$(date +%Y)
CUTOFF_DATE="${CUTOFF_DATE:-$(date +%F)}"

declare -A VENUE_START_YEAR
VENUE_START_YEAR["HYPERLIQUID"]=2023
VENUE_START_YEAR["ASTER"]=2024
VENUE_START_YEAR["LIGHTER-ZKSYNC"]=2024
VENUE_START_YEAR["EXTENDED-STARKNET"]=2024

# Precise first-shard start date (UAC VENUE_DATA_TYPE_CAPABILITIES start_date) — the
# first year-shard begins here instead of Jan-1 so the VM doesn't request dates the
# source venue didn't exist for yet.
declare -A VENUE_START_DATE
VENUE_START_DATE["HYPERLIQUID"]="2023-01-01"
VENUE_START_DATE["ASTER"]="2024-01-01"
VENUE_START_DATE["LIGHTER-ZKSYNC"]="2024-08-01"
VENUE_START_DATE["EXTENDED-STARKNET"]="2024-10-01"

# VENUES to launch — env-overridable so a re-run can target a subset.
VENUES="${VENUES:-HYPERLIQUID ASTER LIGHTER-ZKSYNC EXTENDED-STARKNET}"

# ── Helper ────────────────────────────────────────────────────────────────────
running_jobs=0

wait_for_slot() {
    while [[ $running_jobs -ge $MAX_CONCURRENT ]]; do
        wait -n 2>/dev/null || true
        running_jobs=$((running_jobs - 1))
    done
}

launch_shard() {
    local venue="$1"
    local year="$2"
    local venue_lower="${venue,,}"
    local start_date="${year}-01-01"
    local end_date

    # First shard year for this venue starts at its real capability start_date
    # (not Jan-1) so the VM doesn't request pre-source-coverage dates.
    if [[ "$year" -eq "${VENUE_START_YEAR[$venue]}" ]]; then
        start_date="${VENUE_START_DATE[$venue]}"
    fi

    if [[ "$year" -ge "$CURRENT_YEAR" ]]; then
        end_date="$CUTOFF_DATE"
    else
        end_date="${year}-12-31"
    fi

    local vm_name="${venue_lower//_/-}-${year}-${RUN_TS}"
    # Prepend cefi- prefix to match registered vm_zombie_watchdog.py prefixes:
    #   cefi-hyperliquid- / cefi-aster- / cefi-lighter- / cefi-extended-
    vm_name="cefi-${vm_name}"

    local machine="e2-highmem-8"

    local meta="startup-script-url=${STARTUP}"
    meta+=",VM_TASK=cefi-hl-aster-backfill"
    meta+=",VM_SERVICE=market_tick_data_service"
    # collect-onchain-perp-batch drives HyperliquidS3Downloader + AsterAdapter DIRECTLY,
    # bypassing the orchestrator DeFi-strip that no-ops VM_OPERATION=download for HL/ASTER.
    # Writes cefi canonical parquet + manifest (source=hyperliquid/aster, batch_<source>).
    # SSOT: market-tick-data-service OnchainPerpBatchHandler
    # (live_tardis_machine_and_hl_aster_s3_batch_2026_06_21 §2).
    meta+=",VM_OPERATION=collect-onchain-perp-batch"
    meta+=",VM_ASSET_GROUP=cefi"
    meta+=",VM_VENUE=${venue}"
    meta+=",VM_START_DATE=${start_date}"
    meta+=",VM_END_DATE=${end_date}"
    meta+=",VM_DATA_TYPES=${DATA_TYPES}"
    meta+=",VM_INSTRUMENT_IDS=${SYMBOLS}"
    meta+=",VM_FORCE=${FORCE}"
    meta+=",DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    meta+=",VM_SHUTDOWN_ON_COMPLETION=true"
    meta+=",MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
    meta+=",MANIFEST_FAIL_ON_STALE_FALLBACK=true"
    meta+=",MANIFEST_PER_VM_SHARDS=true"

    echo "[LAUNCH] ${vm_name}  venue=${venue}  ${start_date}→${end_date}  types=${DATA_TYPES}"

    if [[ "$DRY_RUN" == "1" ]]; then
        echo "[DRY-RUN] would launch: gcloud compute instances create ${vm_name} --zone=${ZONE} --machine-type=${machine}"
        return
    fi

    # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
    PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
    if [[ "${ON_DEMAND:-false}" == "true" ]]; then PROVISIONING_FLAGS=""; fi

    wait_for_slot
    # shellcheck disable=SC2086
    gcloud compute instances create "${vm_name}" \
        --zone="${ZONE}" \
        --machine-type="${machine}" \
        ${PROVISIONING_FLAGS} \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
        --scopes=cloud-platform \
        --metadata="${meta}" \
        --labels=env="${DEPLOYMENT_ENV}" \
        --project="${PROJECT}" \
        --async 2>&1 | tail -1 &
    running_jobs=$((running_jobs + 1))
}

# ── Main: launch all year shards ─────────────────────────────────────────────
echo "=== CeFi on-chain-perp historical backfill launcher (HL/ASTER/LIGHTER/EXTENDED) ==="
echo "    DRY_RUN=${DRY_RUN}  FORCE=${FORCE}  MAX_CONCURRENT=${MAX_CONCURRENT}  VENUES=${VENUES}"
echo "    RUN_TS=${RUN_TS}"
echo ""

for venue in ${VENUES}; do
    start_year="${VENUE_START_YEAR[$venue]}"
    echo "--- ${venue} (${VENUE_START_DATE[$venue]}→${CURRENT_YEAR}) ---"
    for year in $(seq "$start_year" "$CURRENT_YEAR"); do
        if [[ -n "$YEARS" ]] && [[ " ${YEARS} " != *" ${year} "* ]]; then
            echo "[SKIP] ${venue} ${year} (not in YEARS=${YEARS})"
            continue
        fi
        launch_shard "$venue" "$year"
    done
    echo ""
done

# Wait for all background gcloud calls to finish
wait
echo ""
echo "=== All shards dispatched. Verify at T+10min: ==="
echo "    gcloud compute instances list --project=${PROJECT} --filter='name~^cefi-hyperliquid\\|name~^cefi-aster\\|name~^cefi-lighter\\|name~^cefi-extended' --format='table(name,status,zone)'"
