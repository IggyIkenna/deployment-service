#!/usr/bin/env bash
# Launch sharded TRADFI backfill VMs — Databento CME ES futures (+ adjacent
# TRADFI instruments) for the CME S&P 500 ML directional signal (Tier 1 MVP).
#
# Purpose: ingest CME ES ohlcv_1m + trades across 2022-2026 expiries via the
# unified MTDS download CLI — same code path as the Tardis-backed CeFi /
# TradFi fleet, routed to the Databento adapter for proper CME tick data.
# Writes to:
#   gs://<tick-data-bucket>/raw_tick_data/
#     by_date/day={D}/category=tradfi/venue=CME/instrument_type=future/
#     data_type={ohlcv_1m,trades}/ES-<EXPIRY>.parquet
#
# Manifest:
#   gs://<tick-data-bucket>/_index/availability_index.parquet
#
# Runs the unified MTDS CLI via setup-data-pipeline-vm.sh metadata routing —
# same code path as the production CeFi/TradFi backfill fleet:
#   python -m market_tick_data_service \
#     --operation download --mode batch --category TRADFI \
#     --venues DATABENTO --data-types ohlcv_1m trades \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE \
#     --instrument-ids CME:FUTURE:ES-YYYYMMDD ...
#
# Sharding strategy: one VM per expiry-year shard (2022-2026). Each ES
# quarterly contract lives ~3 months; five years = ~20 expiries. A single-VM
# backfill for all 20 is slow + prone to single-shard failure taking the
# whole run down. Year-shard keeps failure isolation + wall-clock tight.
#
# Prerequisites:
#   - Tarballs uploaded to gs://deployment-scripts-central-element-323112/code/
#     (run `bash create-code-tarballs.sh --category TRADFI` first).
#   - databento-api-key in Secret Manager.
#
# Usage:
#   bash launch-tradfi-backfill-vm.sh --dry-run                    # preview all shards
#   bash launch-tradfi-backfill-vm.sh                              # all expiry-year shards 2022-2026
#   bash launch-tradfi-backfill-vm.sh --year 2026                  # single year
#   bash launch-tradfi-backfill-vm.sh --force --year 2026          # bypass singleton lock
#   bash launch-tradfi-backfill-vm.sh --year 2026 --instrument-ids 'CME:FUTURE:ES-20260620'
#                                                                  # explicit contract list
#
# Cost: e2-standard-4 per shard (Databento OHLCV_1M is lighter than Tardis
# L2 book; a year-shard completes in ~30-90 min depending on contract count).
#
# Singleton lock: refuses to launch if any tradfi-bf-* VM is RUNNING in the
# zone. Databento per-IP rate-limits are looser than RapidAPI but the account
# is shared across the team; concurrent backfills risk contract-exceeded
# errors on aggressive windows. --force bypasses for legitimate parallel
# investigations. Mirrors the launch-mtds-prediction-backfill-vm.sh +
# launch-sfi-forward-poll.sh pattern (2026-04-20 SSOT).
#
# Observability: inherits the STALL_TIMEOUT_SEC=600 log-mtime watchdog +
# `timeout 30s` run_heartbeat + py-spy stack dump + pkill-by-name fallback
# from vm-exec-with-gcs-tee.sh (deployed 2026-04-19 after 6 TradFi VMs
# silently wedged for 5-6h — memory project_vm_hang_class_bug_and_mdps...).
set -euo pipefail

# ── Defaults + arg parsing ──
FORCE=false
DRY_RUN=false
YEAR=""
INSTRUMENT_IDS_OVERRIDE=""
DATA_TYPES="ohlcv_1m;trades"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force)           FORCE=true; shift ;;
        --dry-run)         DRY_RUN=true; shift ;;
        --year)            YEAR="$2"; shift 2 ;;
        --instrument-ids)  INSTRUMENT_IDS_OVERRIDE="$2"; shift 2 ;;
        --data-types)      DATA_TYPES="$2"; shift 2 ;;
        --help|-h)
            grep '^#' "$0" | head -60
            exit 0
            ;;
        *)
            echo "Unknown arg: $1" >&2
            echo "Usage: $0 [--dry-run] [--force] [--year YYYY] [--instrument-ids 'ID;ID;ID'] [--data-types 'ohlcv_1m;trades']" >&2
            exit 1
            ;;
    esac
done

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"
STARTUP="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
MACHINE_TYPE="e2-standard-4"
BOOT_DISK_GB="50"

# ── Singleton lock: databento account shared, concurrent backfills risk quota
#    exhaustion on big windows. Mirrors sfi-fwd / mtds-prediction pattern ──
if ! $FORCE && ! $DRY_RUN; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^tradfi-bf-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: TRADFI backfill VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — the Databento account is shared; concurrent
VMs on wide windows risk contract-exceeded errors. Adding more shards in
parallel does not speed up a bounded quota.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force${YEAR:+ --year $YEAR}
EOF
        exit 1
    fi
fi

# ── ES expiry calendar — quarterly contracts (H=Mar, M=Jun, U=Sep, Z=Dec).
#    Canonical-id scheme per UAC: CME:FUTURE:ES-YYYYMMDD where YYYYMMDD is
#    the last trading date. Dates below are 3rd-Friday of contract month
#    per CME spec (Tier 1 MVP uses these single-expiry contracts; Phase B
#    stitches continuous series from them). ──
declare -A ES_EXPIRIES_BY_YEAR=(
    ["2022"]="CME:FUTURE:ES-20220318;CME:FUTURE:ES-20220617;CME:FUTURE:ES-20220916;CME:FUTURE:ES-20221216"
    ["2023"]="CME:FUTURE:ES-20230317;CME:FUTURE:ES-20230616;CME:FUTURE:ES-20230915;CME:FUTURE:ES-20231215"
    ["2024"]="CME:FUTURE:ES-20240315;CME:FUTURE:ES-20240621;CME:FUTURE:ES-20240920;CME:FUTURE:ES-20241220"
    ["2025"]="CME:FUTURE:ES-20250321;CME:FUTURE:ES-20250620;CME:FUTURE:ES-20250919;CME:FUTURE:ES-20251219"
    ["2026"]="CME:FUTURE:ES-20260320;CME:FUTURE:ES-20260619"
)

# Choose which years to launch
if [[ -n "$YEAR" ]]; then
    YEARS_TO_RUN=("$YEAR")
    if [[ -z "${ES_EXPIRIES_BY_YEAR[$YEAR]:-}" ]]; then
        echo "ERROR: No ES expiries declared for --year $YEAR. Known: ${!ES_EXPIRIES_BY_YEAR[*]}" >&2
        exit 1
    fi
else
    YEARS_TO_RUN=(2022 2023 2024 2025 2026)
fi

_launched=0
for year in "${YEARS_TO_RUN[@]}"; do
    if [[ "$year" == "2026" ]]; then
        START_DATE="2026-01-01"
        END_DATE="$(date +%Y-%m-%d)"
    else
        START_DATE="${year}-01-01"
        END_DATE="${year}-12-31"
    fi

    if [[ -n "$INSTRUMENT_IDS_OVERRIDE" ]]; then
        INSTRUMENT_IDS="$INSTRUMENT_IDS_OVERRIDE"
    else
        INSTRUMENT_IDS="${ES_EXPIRIES_BY_YEAR[$year]}"
    fi

    RUN_TS="$(date +%Y%m%d-%H%M%S)"
    VM_NAME="tradfi-bf-es-${year}-${RUN_TS}"

    # Metadata follows the cefi-backfill convention — setup-data-pipeline-vm.sh
    # routes VM_TASK=cefi-backfill through the generic MTDS CLI assembly
    # (task name is misleading; routing is category-agnostic).
    METADATA="VM_TASK=cefi-backfill"
    METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
    METADATA="${METADATA},VM_OPERATION=download"
    METADATA="${METADATA},VM_CATEGORY=TRADFI"
    METADATA="${METADATA},VM_VENUE=DATABENTO"
    METADATA="${METADATA},VM_START_DATE=${START_DATE}"
    METADATA="${METADATA},VM_END_DATE=${END_DATE}"
    METADATA="${METADATA},VM_DATA_TYPES=${DATA_TYPES}"
    METADATA="${METADATA},VM_INSTRUMENT_IDS=${INSTRUMENT_IDS}"
    METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

    if $DRY_RUN; then
        echo "[DRY-RUN] $VM_NAME"
        echo "          year=$year ${START_DATE}..${END_DATE}"
        echo "          venue=DATABENTO data_types=${DATA_TYPES}"
        echo "          instruments=${INSTRUMENT_IDS}"
        echo "          machine=${MACHINE_TYPE} zone=${ZONE}"
    else
        echo "Launching $VM_NAME (ES ${year} ${START_DATE}..${END_DATE})"
        gcloud compute instances create "$VM_NAME" \
            --project="$PROJECT" \
            --zone="$ZONE" \
            --machine-type="$MACHINE_TYPE" \
            --image-family=ubuntu-2404-lts-amd64 \
            --image-project=ubuntu-os-cloud \
            --boot-disk-size="${BOOT_DISK_GB}GB" \
            --scopes=cloud-platform \
            --metadata="startup-script-url=${STARTUP},${METADATA}" \
            --labels=purpose=tradfi-backfill,run-ts="${RUN_TS}",year="${year}"
        echo "  VM launched."
        # Brief stagger between shards so RUN_TS timestamps stay unique +
        # gcloud create calls don't race the quota prechecks.
        sleep 3
    fi
    _launched=$((_launched + 1))
done

echo ""
if $DRY_RUN; then
    echo "=========================================="
    echo "DRY-RUN: ${_launched} VM(s) would be launched"
    echo "=========================================="
else
    echo "${_launched} TRADFI backfill VM(s) launched in ${ZONE}"
    echo ""
    echo "Manifest check:"
    echo "  gsutil cp gs://market-data-tick-tradfi-${PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
    echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[df.venue=='CME'].capture_status.value_counts())\""
fi
