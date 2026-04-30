#!/usr/bin/env bash
# Launch a TradFi ETF backfill VM via Yahoo Finance daily OHLCV.
#
# Backfill scope: institutional ETF tickers — SPY/IVV/VOO (S&P), QQQ (Nasdaq),
# IWM (Russell 2K), DIA (Dow), GLD/SLV (precious metals), USO (oil),
# TLT/IEF (treasuries), HYG/LQD (credit), XLE/XLF/XLK/... (sector SPDRs),
# IBIT/FBTC/ARKB (BTC ETFs), ETHA/FETH (ETH ETFs). Daily OHLCV is the
# Yahoo Finance free-tier sweet spot — no per-key rate-limit concerns.
#
# Writes to:
#   gs://market-data-tick-tradfi-central-element-323112/raw_tick_data/
#     by_date/day={D}/asset_group=tradfi/venue=YAHOO_FINANCE/
#     instrument_type=etf/data_type=ohlcv_1d/{SYMBOL}.parquet
#
# Manifest:
#   gs://market-data-tick-tradfi-central-element-323112/_index/availability_index.parquet
#
# Default window: 2010-01-01 to today (covers the full Yahoo Finance daily
# history for major ETFs). Pass two YYYY-MM-DD dates for an explicit window.
#
# Usage:
#   bash launch-tradfi-etf-backfill-vm.sh                          # 2010-01-01..today
#   bash launch-tradfi-etf-backfill-vm.sh 2024-01-01 2026-04-30    # explicit window
#   bash launch-tradfi-etf-backfill-vm.sh --force                  # bypass singleton lock
#
# Singleton lock: refuses to launch if another tradfi-etf-* VM is RUNNING in
# the zone. Yahoo Finance is gentler than Polymarket gamma but the per-IP
# limits still bite when multiple VMs share the project NAT. --force bypasses.
#
# Cost: e2-standard-2 + 30GB. Daily OHLCV for ~30 ETFs × 16 years
# ≈ 5800 days × 30 symbols = 174K records. Single-VM wall time ~15-30 min.
#
# Catalogue follow-up: the launcher closes the empty TARDIS etf_flows gap
# noted in instrument-catalogue.md by adding a YAHOO_FINANCE etf
# capability to DataTypeCapability registry. The catalogue auto-detects it
# on the next nightly regen.
set -euo pipefail

FORCE=false
if [[ "${1:-}" == "--force" ]]; then
    FORCE=true
    shift
fi

if [[ $# -eq 2 ]]; then
    START_DATE="$1"
    END_DATE="$2"
elif [[ $# -eq 0 ]]; then
    START_DATE="2010-01-01"
    END_DATE="$(date +%Y-%m-%d)"
else
    cat >&2 <<EOF
Usage: $0 [--force] [START_DATE END_DATE]

Defaults to 2010-01-01..today (full Yahoo Finance ETF history).
Pass two YYYY-MM-DD dates for an explicit window.
Pass --force as the first arg to bypass the singleton lock.
EOF
    exit 1
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"
MACHINE_TYPE="e2-standard-2"
BOOT_DISK_GB="30"

# ETF universe — institutional core. Semicolon-separated for
# VM_INSTRUMENT_IDS routing (gcloud's comma is the metadata key separator).
SYMBOLS="SPY;IVV;VOO;QQQ;IWM;DIA;GLD;SLV;USO;TLT;IEF;SHY;HYG;LQD;EEM;EFA"
SYMBOLS="${SYMBOLS};XLE;XLF;XLK;XLV;XLY;XLI;XLP;XLU;XLB;XLRE"
SYMBOLS="${SYMBOLS};IBIT;FBTC;ARKB;ETHA;FETH"

# ── Singleton lock ──
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^tradfi-etf-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: tradfi-etf VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — Yahoo Finance per-IP rate-limits via
the shared project NAT. --force bypasses for legitimate parallel investigations.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force ${START_DATE} ${END_DATE}
EOF
        exit 1
    fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="tradfi-etf-${RUN_TS}"

echo "Launching $VM_NAME: YAHOO_FINANCE ETF ohlcv_1d ${START_DATE}..${END_DATE}"
echo "Symbols: ${SYMBOLS//;/, }"

# Metadata follows the cefi-backfill convention — setup-data-pipeline-vm.sh
# routes VM_TASK=cefi-backfill through the generic MTDS CLI assembly (the
# task name is misleadingly category-specific; the routing is asset-group-
# agnostic).
METADATA="VM_TASK=cefi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=download"
METADATA="${METADATA},VM_ASSET_GROUP=TRADFI"
METADATA="${METADATA},VM_VENUE=YAHOO_FINANCE"
METADATA="${METADATA},VM_DATA_TYPES=ohlcv_1d"
METADATA="${METADATA},VM_INSTRUMENT_IDS=${SYMBOLS}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=tradfi-etf-backfill,run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Manifest:    gsutil cp gs://market-data-tick-tradfi-central-element-323112/_index/availability_index.parquet /tmp/t.parquet"
echo "Inspect:     python -c \"import pandas as pd; df = pd.read_parquet('/tmp/t.parquet'); print(df[df.venue=='YAHOO_FINANCE'].capture_status.value_counts())\""
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
