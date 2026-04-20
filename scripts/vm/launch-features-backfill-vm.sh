#!/usr/bin/env bash
# Phase 5c.3 features backfill VM launcher — spawn one VM per
# (feature_service × category) cell. The service CLIs follow the standardised
# axes defined in codex/06-coding-standards/cli-convention.md:
#     --operation backfill --mode batch --category {CEFI|TRADFI|DEFI|SPORTS|PREDICTION}
#     --start-date YYYY-MM-DD --end-date YYYY-MM-DD
#
# Input reads: features read from
#   * gs://market-data-tick-{category}-*/{raw_tick_data,processed_candles}/...
#   * gs://instruments-store-{category}-*/...
# Output writes: per feature_group + category bucket layout, e.g.
#   * gs://features-{group}-{category}-*/features/by_date/day=/feature_group=/timeframe=/...
# Manifest: availability_index.parquet in each features bucket.
#
# Viable (feature_service × category) cells (Phase 5c.3 matrix — ≈29 cells):
#     features-delta-one      : CEFI, DEFI, TRADFI, PREDICTION
#     features-volatility     : CEFI, TRADFI
#     features-onchain        : DEFI only
#     features-sports         : SPORTS only
#     features-calendar       : CEFI, TRADFI
#     features-multi-timeframe: CEFI, DEFI, TRADFI
#     features-cross-instrument: CEFI, TRADFI, PREDICTION
#     features-commodity      : TRADFI only
#
# Scope per category (mirrors Phase 5b.3):
#   CeFi/TradFi/DeFi 2020-01-01 → 2026-04-18
#   Sports           2019-01-01 → 2026-04-18
#   Prediction       2025-03-14 → 2026-04-18
#
# Operational safety:
#   * 50 GB boot disk.
#   * DEPLOYMENT_STARTED/PROGRESS/COMPLETED events via deployment_heartbeat.py.
#   * stdout/stderr tee'd via vm-exec-with-gcs-tee.sh.
#   * asia-northeast1-c.
#   * Launch in waves: 4 concurrent VMs per wave (deployment-service budget).
#
# Usage:
#   bash launch-features-backfill-vm.sh <feature_service> <category> <start> <end> [dry|full]
#
# Examples:
#   bash launch-features-backfill-vm.sh delta-one CEFI       2020-01-01 2026-04-18 dry
#   bash launch-features-backfill-vm.sh onchain   DEFI       2020-01-01 2026-04-18 full
#   bash launch-features-backfill-vm.sh sports    SPORTS     2019-01-01 2026-04-18 full
#   bash launch-features-backfill-vm.sh commodity TRADFI     2020-01-01 2026-04-18 full

set -euo pipefail

SERVICE_SHORT="${1:-}"
CATEGORY="${2:-}"
START_DATE="${3:-}"
END_DATE="${4:-}"
MODE="${5:-dry}"  # dry | full

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"
MACHINE_TYPE="e2-standard-8"
BOOT_DISK_GB="50"

if [[ -z "$SERVICE_SHORT" || -z "$CATEGORY" || -z "$START_DATE" || -z "$END_DATE" ]]; then
    cat <<EOF
Usage: $0 <feature_service_short> <CATEGORY> <start-date> <end-date> [dry|full]

feature_service_short ∈ {
  delta-one, volatility, onchain, sports,
  calendar, multi-timeframe, cross-instrument, commodity
}
CATEGORY ∈ { CEFI, DEFI, TRADFI, SPORTS, PREDICTION }
EOF
    exit 2
fi

# (feature_service_short, CATEGORY) validity matrix.
_is_viable_cell() {
    local svc="$1" cat="$2"
    case "$svc/$cat" in
        delta-one/CEFI|delta-one/DEFI|delta-one/TRADFI|delta-one/PREDICTION) return 0 ;;
        volatility/CEFI|volatility/TRADFI) return 0 ;;
        onchain/DEFI) return 0 ;;
        sports/SPORTS) return 0 ;;
        calendar/CEFI|calendar/TRADFI) return 0 ;;
        multi-timeframe/CEFI|multi-timeframe/DEFI|multi-timeframe/TRADFI) return 0 ;;
        cross-instrument/CEFI|cross-instrument/TRADFI|cross-instrument/PREDICTION) return 0 ;;
        commodity/TRADFI) return 0 ;;
        *) return 1 ;;
    esac
}

if ! _is_viable_cell "$SERVICE_SHORT" "$CATEGORY"; then
    echo "Not a viable (service × category) cell: $SERVICE_SHORT × $CATEGORY"
    echo "See header for the viable matrix."
    exit 2
fi

# Python module name for each service — matches the dash→underscore rule.
_python_module_for() {
    case "$1" in
        delta-one)        echo "features_delta_one_service" ;;
        volatility)       echo "features_volatility_service" ;;
        onchain)          echo "features_onchain_service" ;;
        sports)           echo "features_sports_service" ;;
        calendar)         echo "features_calendar_service" ;;
        multi-timeframe)  echo "features_multi_timeframe_service" ;;
        cross-instrument) echo "features_cross_instrument_service" ;;
        commodity)        echo "features_commodity_service" ;;
        *) echo ""; return 1 ;;
    esac
}

PY_MODULE="$(_python_module_for "$SERVICE_SHORT")"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
CATEGORY_LOWER="$(echo "$CATEGORY" | tr '[:upper:]' '[:lower:]')"  # bash-3-compat (${var,,} is bash 4+)
VM_NAME="features-${SERVICE_SHORT}-${CATEGORY_LOWER}-backfill-${RUN_TS}"

# Canonical service CLI convention (codex/06-coding-standards/cli-convention.md).
CMD="python -m ${PY_MODULE} --operation backfill --mode batch"
CMD="$CMD --category ${CATEGORY} --start-date ${START_DATE} --end-date ${END_DATE}"
if [[ "$MODE" == "dry" ]]; then
    CMD="$CMD --dry-run"
fi

echo "Launching $VM_NAME"
echo "  cmd: $CMD"
echo "  zone: $ZONE, machine: $MACHINE_TYPE, boot: ${BOOT_DISK_GB}G"

MD="VM_TASK=features-backfill"
MD="${MD},VM_SERVICE=${PY_MODULE}"
MD="${MD},VM_OPERATION=backfill-${SERVICE_SHORT}-${CATEGORY,,}"
MD="${MD},VM_CATEGORY=${CATEGORY}"
MD="${MD},VM_START_DATE=${START_DATE}"
MD="${MD},VM_END_DATE=${END_DATE}"
MD="${MD},VM_BACKFILL_CMD=${CMD}"
MD="${MD},VM_BACKFILL_MODE=${MODE}"

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --boot-disk-size="${BOOT_DISK_GB}GB" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${MD}" \
    --labels=purpose=features-backfill,service="${SERVICE_SHORT}",category="${CATEGORY,,}",mode="${MODE}",run-ts="${RUN_TS}"
echo "  SSH: gcloud compute ssh $VM_NAME --zone=$ZONE"
echo "  Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Post-backfill manifest rebuild (one per features bucket):"
echo "  python -c \"from unified_trading_library.manifest_writer import rebuild_manifest_from_canonical_paths; \\"
echo "    rebuild_manifest_from_canonical_paths('features-${SERVICE_SHORT}-${CATEGORY,,}-central-element-323112', \\"
echo "      service_name='features-${SERVICE_SHORT}-service', prefix='features/by_date')\""
