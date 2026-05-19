#!/usr/bin/env bash
# Launch extended SKU-matrix v2 benchmark for the 3 slowest stages.
#
# Phase 5 of compute_optimization_mock_data_2026_05_13.md:
#   "Run the extended matrix on the 3 slowest stages identified in Phase 0."
#
# Phase 0 top-3 (by cutover-window contribution):
#   1. features_compute  (~64,900 hrs serial — deferred talib benchmark)
#   2. mtds_read         (~10,360 hrs — I/O-bound)
#   3. mdps_compute      (~9,270 hrs — deferred exchange_calendars benchmark)
#
# This script fans out launch-synthetic-benchmark-vm.sh across the top-3 archetypes
# (carry_staked_basis + arbitrage_price_dispersion) on all v2 shapes.
# Each VM runs a 7-day synthetic window (--date-start / --date-end configurable).
#
# Outputs land in gs://central-element-323112-benchmark-reports/sku_matrix_v2/.
# After all VMs stop, aggregate with:
#   python -m unified_trading_library.synthetic --aggregate-report \
#     --input-prefix gs://central-element-323112-benchmark-reports/sku_matrix_v2/ \
#     --output-table codex/05-infrastructure/runtime-tiers-and-deployment.md
#
# No fire-and-forget (CLAUDE.md): verify each VM emits STARTED within 90s and STOPPED at exit.
#
# VM naming: sku2-<arch_short>-<shape_short>-<ts> — prefix "sku2-" registered in
# vm_zombie_watchdog.py:VM_PREFIX_TO_BUCKET (add it if absent and relaunch watchdog).
set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"
DATE_START="${DATE_START:-2024-01-01}"
DATE_END="${DATE_END:-2024-01-07}"
MODE="${BENCHMARK_MODE:-subprocess}"
ROW_COUNT_SCALE="${ROW_COUNT_SCALE:-1.0}"
ENV="${DEPLOYMENT_ENV:-prod}"

# Extended v2 shape list (Phase 5)
V2_SHAPES="c2-standard-8 c2-standard-16 c2-standard-30 c3-highcpu-44 c3-highcpu-88 c3-highcpu-176 m3-megamem-128 m3-ultramem-160"

# Top-3 archetypes from Phase 0 analysis (proxy for features_compute + mdps_compute)
ARCHETYPES="carry_staked_basis arbitrage_price_dispersion"

echo "=== SKU Matrix v2 Benchmark ==="
echo "Shapes:    $V2_SHAPES"
echo "Archetypes: $ARCHETYPES"
echo "Window:    ${DATE_START} → ${DATE_END}"
echo "Mode:      $MODE"
echo ""

for ARCHETYPE in $ARCHETYPES; do
  echo "--- Launching $ARCHETYPE ---"
  bash "${SCRIPT_DIR}/launch-synthetic-benchmark-vm.sh" \
    --archetype "$ARCHETYPE" \
    --shapes "$V2_SHAPES" \
    --date-start "$DATE_START" \
    --date-end "$DATE_END" \
    --mode "$MODE" \
    --row-count-scale "$ROW_COUNT_SCALE" \
    --env "$ENV"
  echo ""
done

echo "All VMs launched. Post-launch checklist:"
echo "  T+10min: verify STARTED events in gs://central-element-323112-events/"
echo "  T+30min: spot-check progress events (>= 1/hr per VM)"
echo "  On STOPPED: aggregate reports with unified_trading_library.synthetic --aggregate-report"
echo "  Write SKU recommendation table to: codex/05-infrastructure/runtime-tiers-and-deployment.md"
