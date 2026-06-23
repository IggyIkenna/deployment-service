#!/usr/bin/env bash
# Epic: mtds_mdps_master
# Lifecycle: campaign
# Delete-when: HL/ASTER historical backfill complete (48.5k attempted_failed cells resolved)
#
# Launch year-sharded backfill VMs for HYPERLIQUID (S3 requester-pays) and ASTER (REST)
# historical data. These venues were previously blocked by an orchestrator defi-strip that
# misclassified them as DeFi despite their failed cells living in the cefi manifest.
#
# Root cause fix (UAC@defi_venues.py): HYPERLIQUID and ASTER removed from ALL_DEFI_VENUES +
# DEFI_VENUE_PHASE → VENUE_TO_ASSET_GROUP now maps them to "cefi" → both orchestrator
# defi-strip and umi_tick_provider defi guard now pass → _fetch_hyperliquid_s3 and
# _fetch_aster_rest routes in umi_tick_provider.py are reachable.
#
# Data sources:
#   HYPERLIQUID: requester-pays S3 via HyperliquidS3Downloader
#                auth: aws-hyperliquid-s3 Secret Manager key (EXISTS)
#   ASTER:       REST API via _fetch_aster_rest (Binance Futures-compatible)
#
# Coverage ranges (from cefi manifest attempted_failed audit 2026-06-21):
#   HYPERLIQUID: 2023-01-01 → 2026-06-20 (30,835 cells)
#   ASTER:       2024-01-01 → 2026-06-20 (17,675 cells)
#
# Data types: trades, derivative_ticker (+ book_snapshot_5 for HL only — the
#   handler EXCLUDES per-venue live-only/dropped data_types automatically):
#   - ASTER book_snapshot_5 + liquidations: LIVE-ONLY (Binance-compat WS captures
#     them going forward) → excluded from the batch universe by the handler.
#   - HYPERLIQUID liquidations: DROPPED entirely (HL publishes no liq feed anywhere).
#   SSOT: cefi_hl_aster_batch_data_gaps_2026_06_22.md BUG #4.
#
# VM prefixes (already registered in vm_zombie_watchdog.py VM_PREFIX_TO_BUCKET):
#   cefi-hyperliquid-  → EPHEMERAL_BATCH → tick-cefi bucket
#   cefi-aster-        → EPHEMERAL_BATCH → tick-cefi bucket
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

DRY_RUN="${DRY_RUN:-0}"
FORCE="${FORCE:-0}"
MAX_CONCURRENT="${MAX_CONCURRENT:-8}"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

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
# HL:    2023-01-01 → 2026-06-20
# ASTER: 2024-01-01 → 2026-06-20
CURRENT_YEAR=$(date +%Y)
CUTOFF_DATE="2026-06-20"

declare -A VENUE_START_YEAR
VENUE_START_YEAR["HYPERLIQUID"]=2023
VENUE_START_YEAR["ASTER"]=2024

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

    if [[ "$year" -ge "$CURRENT_YEAR" ]]; then
        end_date="$CUTOFF_DATE"
    else
        end_date="${year}-12-31"
    fi

    local vm_name="${venue_lower//_/-}-${year}-${RUN_TS}"
    # Prepend cefi- prefix to match registered vm_zombie_watchdog.py prefixes:
    #   cefi-hyperliquid-  / cefi-aster-
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

    wait_for_slot
    gcloud compute instances create "${vm_name}" \
        --zone="${ZONE}" \
        --machine-type="${machine}" \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size=50GB \
        --scopes=cloud-platform \
        --metadata="${meta}" \
        --labels=env="${DEPLOYMENT_ENV}" \
        --project="${PROJECT}" \
        --async 2>&1 | tail -1 &
    running_jobs=$((running_jobs + 1))
}

# ── Main: launch all year shards ─────────────────────────────────────────────
echo "=== CeFi HL/ASTER historical backfill launcher ==="
echo "    DRY_RUN=${DRY_RUN}  FORCE=${FORCE}  MAX_CONCURRENT=${MAX_CONCURRENT}"
echo "    RUN_TS=${RUN_TS}"
echo ""

for venue in HYPERLIQUID ASTER; do
    start_year="${VENUE_START_YEAR[$venue]}"
    echo "--- ${venue} (${start_year}→${CURRENT_YEAR}) ---"
    for year in $(seq "$start_year" "$CURRENT_YEAR"); do
        launch_shard "$venue" "$year"
    done
    echo ""
done

# Wait for all background gcloud calls to finish
wait
echo ""
echo "=== All shards dispatched. Verify at T+10min: ==="
echo "    gcloud compute instances list --project=${PROJECT} --filter='name~^cefi-hyperliquid\\|name~^cefi-aster' --format='table(name,status,zone)'"
