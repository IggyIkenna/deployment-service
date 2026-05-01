#!/usr/bin/env bash
# Master orchestrator for the full features-pipeline backfill.
#
# Wiring:
#   Phase 0 (manual / pre-req): refresh code tarball — pulls latest fixes.
#       bash deployment-service/scripts/vm/create-code-tarballs.sh --all
#
#   Phase 1 (parallel): MTDS RAW capture for the dense window.
#       - DERIBIT options_chain (CEFI)            → launch-cefi-sharded-backfill.sh DERIBIT
#       - CME-OPTIONS / CBOE options_chain (TRADFI) → launch-cefi-sharded-backfill.sh CME-OPTIONS / CBOE-VIX-OPTIONS
#       - POLYMARKET trade fills (PREDICTION gap)  → launch-mtds-prediction-backfill-vm.sh
#
#   Phase 2 (after Phase 1 lands): MDPS processed_candles
#       - MDPS DEFI (wide window — only 2 days exist now)
#       - MDPS PREDICTION (gap fill)
#       - MDPS CEFI/TRADFI for new options data
#
#   Phase 3 (after Phase 2 lands): features-delta-one × all 4 asset_groups
#       - launch-features-backfill-vm.sh delta-one CEFI ...
#       - launch-features-backfill-vm.sh delta-one TRADFI ...
#       - launch-features-backfill-vm.sh delta-one DEFI ...
#       - launch-features-backfill-vm.sh delta-one PREDICTION ...
#
#   Phase 4 (parallel, after Phase 3): downstream features
#       - features-volatility CEFI/TRADFI
#       - features-multi-timeframe CEFI/TRADFI/DEFI
#       - features-cross-instrument CEFI/TRADFI/PREDICTION
#       - features-onchain DEFI
#       - features-commodity TRADFI
#       - features-calendar CEFI/TRADFI (independent — could run anytime)
#
# This script is a launcher with explicit phase gates. Each phase can be run
# independently via ``--phase N``; ``--all`` runs every phase, blocking on
# manifest sentinels between phases (poll loop).
#
# Default dense window: 2024-01-01 → 2026-04-14 (TradFi/CeFi/DeFi).
# PREDICTION uses 2025-03-14 onwards (instruments-store start).
#
# Usage:
#   bash run-features-pipeline-backfill.sh --phase 0       # tarball refresh
#   bash run-features-pipeline-backfill.sh --phase 1 --dry # dry-run Phase 1
#   bash run-features-pipeline-backfill.sh --phase 1       # launch Phase 1 VMs
#   bash run-features-pipeline-backfill.sh --phase 2 --start 2024-01-01 --end 2026-04-14
#   bash run-features-pipeline-backfill.sh --all --start 2024-01-01 --end 2026-04-14
#
# Operational safety:
#   - DRY by default (no VM launches without --commit)
#   - Each phase prints the EXACT VM commands it would issue first
#   - Phase gates poll manifests via ``check-phase-complete.sh`` (TODO: stub here)
#   - VMs use the singleton-lock pattern; re-running this script is idempotent

set -euo pipefail

WORKSPACE_ROOT="${WORKSPACE_ROOT:-/Users/ikennaigboaka/Code/unified-trading-system-repos}"
DEPLOY_VM_DIR="${WORKSPACE_ROOT}/deployment-service/scripts/vm"

PHASE=""
START_DATE="2024-01-01"
END_DATE="2026-04-14"
COMMIT=0
DRY=1

usage() {
    cat <<EOF
Usage: $0 --phase N [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--commit | --dry]

  --phase 0   Refresh code tarball (no VMs).
  --phase 1   Phase 1: MTDS raw capture (options + POLYMARKET).
  --phase 2   Phase 2: MDPS processed_candles.
  --phase 3   Phase 3: features-delta-one × all 4 asset_groups.
  --phase 4   Phase 4: downstream features (volatility / mtf / cross-instr / onchain / commodity / calendar).
  --all       Run phases 0..4 with sentinel polling between phases.

  --start     Backfill window start (default: 2024-01-01).
  --end       Backfill window end   (default: 2026-04-14).
  --commit    Apply VM launches (default: dry-run).
  --dry       Print commands only (default).
EOF
    exit 2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --phase)  PHASE="$2"; shift 2 ;;
        --all)    PHASE="all"; shift ;;
        --start)  START_DATE="$2"; shift 2 ;;
        --end)    END_DATE="$2"; shift 2 ;;
        --commit) COMMIT=1; DRY=0; shift ;;
        --dry)    DRY=1; COMMIT=0; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown arg: $1"; usage ;;
    esac
done

[[ -z "$PHASE" ]] && usage

_run() {
    # Print the command; only execute when --commit is set.
    echo "  → $*"
    if [[ "$COMMIT" -eq 1 ]]; then
        eval "$@"
    fi
}

phase_0_tarball() {
    echo "=== Phase 0: refresh code tarball ==="
    _run "/opt/homebrew/bin/bash ${DEPLOY_VM_DIR}/create-code-tarballs.sh --all"
}

phase_1_raw_capture() {
    echo "=== Phase 1: MTDS RAW capture (options_chain + POLYMARKET) ==="
    echo "Window: ${START_DATE} → ${END_DATE}"

    # 1a. DERIBIT options_chain (CEFI)
    echo "-- DERIBIT options_chain CEFI --"
    _run "bash ${DEPLOY_VM_DIR}/launch-cefi-sharded-backfill.sh DERIBIT options_chain ${START_DATE} ${END_DATE}"

    # 1b. CME-OPTIONS (ES/NQ futures options) and CBOE-VIX-OPTIONS (TRADFI)
    echo "-- CME-OPTIONS TRADFI --"
    _run "bash ${DEPLOY_VM_DIR}/launch-cefi-sharded-backfill.sh CME-OPTIONS options_chain ${START_DATE} ${END_DATE}"
    echo "-- CBOE-VIX-OPTIONS TRADFI --"
    _run "bash ${DEPLOY_VM_DIR}/launch-cefi-sharded-backfill.sh CBOE-VIX-OPTIONS options_chain ${START_DATE} ${END_DATE}"

    # 1c. POLYMARKET historical trade-fills (PREDICTION) — gap fill 2026-04-01..04-13
    echo "-- POLYMARKET trade-fills PREDICTION --"
    _run "bash ${DEPLOY_VM_DIR}/launch-mtds-prediction-backfill-vm.sh ${START_DATE} ${END_DATE}"

    echo
    echo "Phase 1 launched. Wait for VMs to complete before Phase 2."
    echo "Monitor: gcloud compute instances list --filter='name~deribit OR name~cme OR name~cboe OR name~prediction'"
}

phase_2_processed_candles() {
    echo "=== Phase 2: MDPS processed_candles for all asset_groups ==="
    echo "Window: ${START_DATE} → ${END_DATE}"

    for ag in CEFI TRADFI DEFI PREDICTION; do
        echo "-- MDPS ${ag} --"
        _run "bash ${DEPLOY_VM_DIR}/launch-mdps-sharded-backfill.sh ${ag} ${START_DATE} ${END_DATE}"
    done

    echo
    echo "Phase 2 launched. Wait for VMs to land processed_candles before Phase 3."
}

phase_3_delta_one() {
    echo "=== Phase 3: features-delta-one × {CEFI, TRADFI, DEFI, PREDICTION} ==="
    echo "Window: ${START_DATE} → ${END_DATE}"

    for ag in CEFI TRADFI DEFI PREDICTION; do
        local pred_start="${START_DATE}"
        if [[ "$ag" == "PREDICTION" ]]; then
            pred_start="2025-03-14"  # instruments-store start
        fi
        echo "-- delta-one ${ag} (${pred_start} → ${END_DATE}) --"
        _run "bash ${DEPLOY_VM_DIR}/launch-features-backfill-vm.sh delta-one ${ag} ${pred_start} ${END_DATE} full"
    done

    echo
    echo "Phase 3 launched. Wait for delta-one features to land before Phase 4."
}

phase_4_downstream() {
    echo "=== Phase 4: downstream features (parallel) ==="
    echo "Window: ${START_DATE} → ${END_DATE}"

    # 4a. volatility — needs options_chain processed by MDPS
    for ag in CEFI TRADFI; do
        echo "-- volatility ${ag} --"
        _run "bash ${DEPLOY_VM_DIR}/launch-features-backfill-vm.sh volatility ${ag} ${START_DATE} ${END_DATE} full"
    done

    # 4b. multi-timeframe — reads delta-one features
    for ag in CEFI TRADFI DEFI; do
        echo "-- multi-timeframe ${ag} --"
        _run "bash ${DEPLOY_VM_DIR}/launch-features-backfill-vm.sh multi-timeframe ${ag} ${START_DATE} ${END_DATE} full"
    done

    # 4c. cross-instrument — reads delta-one (CEFI/TRADFI) or POLYMARKET ticks (PREDICTION)
    for ag in CEFI TRADFI PREDICTION; do
        local cx_start="${START_DATE}"
        if [[ "$ag" == "PREDICTION" ]]; then
            cx_start="2025-03-14"
        fi
        echo "-- cross-instrument ${ag} --"
        _run "bash ${DEPLOY_VM_DIR}/launch-features-backfill-vm.sh cross-instrument ${ag} ${cx_start} ${END_DATE} full"
    done

    # 4d. onchain — DEFI only
    echo "-- onchain DEFI --"
    _run "bash ${DEPLOY_VM_DIR}/launch-features-backfill-vm.sh onchain DEFI ${START_DATE} ${END_DATE} full"

    # 4e. commodity — TRADFI only
    echo "-- commodity TRADFI --"
    _run "bash ${DEPLOY_VM_DIR}/launch-features-backfill-vm.sh commodity TRADFI ${START_DATE} ${END_DATE} full"

    # 4f. calendar — CEFI/TRADFI (upstream-INDEPENDENT, can run anytime)
    for ag in CEFI TRADFI; do
        echo "-- calendar ${ag} --"
        _run "bash ${DEPLOY_VM_DIR}/launch-features-backfill-vm.sh calendar ${ag} ${START_DATE} ${END_DATE} full"
    done

    echo
    echo "Phase 4 launched. ALL features are running."
}

case "$PHASE" in
    0)   phase_0_tarball ;;
    1)   phase_1_raw_capture ;;
    2)   phase_2_processed_candles ;;
    3)   phase_3_delta_one ;;
    4)   phase_4_downstream ;;
    all)
        phase_0_tarball
        echo
        echo "WARNING: --all blocks between phases by polling MDPS / features manifests."
        echo "         For now this orchestrator is dry-only when --all is used; run phases"
        echo "         individually with --commit and verify manifests in the deployment-ui."
        DRY=1; COMMIT=0
        phase_1_raw_capture
        phase_2_processed_candles
        phase_3_delta_one
        phase_4_downstream
        ;;
    *) usage ;;
esac

if [[ "$DRY" -eq 1 ]]; then
    echo
    echo "(dry-run — no VMs launched. Pass --commit to apply.)"
fi
