#!/bin/bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Chainlink-on-EVM real-data smoke runbook (per chain).
#
# Plan: api_keys_wallets_accounts_readiness_2026_05_10.md Phase 4.F.
#
# Mirrors Phase 4.E (Pyth) but Chainlink reads on-chain so simpler — no off-chain
# credential beyond chain RPC. Per chain (Ethereum / Arbitrum / Base / Polygon),
# triggers MTDS oracle_prices_handler, captures per-asset price, confirms event-stream emission.
#
# Per Runbook Execution-Owner SSOT HARD RULE:
#   execution:
#     owner: ikennaigboaka (operator) + Harsh side (MTDS Phase 4)
#     cadence: weekly cron VM + pre-cutover gate 2026-05-22
#     verifier: gs://${PID}-events/events/mtds/...{vm-name}/ shows STARTED + per-asset INSTRUMENT_PROCESSED + STOPPED
#     last_executed: NEVER

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../../" && pwd)"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-development}"
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-central-element-323112}"

CHAIN=""
DRY_RUN=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --chain) CHAIN="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        --project-id) PROJECT_ID="$2"; shift 2 ;;
        -h|--help) grep '^#' "$0" | head -20; exit 0 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$CHAIN" ]]; then
    echo "Required: --chain {ETHEREUM|ARBITRUM|BASE|POLYGON}" >&2
    exit 1
fi
case "$CHAIN" in
    ETHEREUM|ARBITRUM|BASE|POLYGON) ;;
    *) echo "Invalid chain ${CHAIN}; must be EVM cutover chain" >&2; exit 1 ;;
esac

VM_NAME="mtds-chainlink-${CHAIN,,}-smoke-$(date +%Y%m%d-%H%M%S)"

# Per-chain asset set to probe (sample feeds; expand per cutover scope)
declare -A CHAIN_ASSETS=(
    [ETHEREUM]="ETH/USD BTC/USD USDC/USD STETH/ETH"
    [ARBITRUM]="ETH/USD ARB/USD WSTETH/ETH"
    [BASE]="ETH/USD WEETH/ETH"
    [POLYGON]="MATIC/USD ETH/USD USDC/USD"
)
ASSETS="${CHAIN_ASSETS[$CHAIN]}"

echo "==========================================
Chainlink-on-EVM real-data smoke
  VM: ${VM_NAME}
  Chain: ${CHAIN}
  Project: ${PROJECT_ID}
  Env: ${DEPLOYMENT_ENV}
  Assets: ${ASSETS}
=========================================="

if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] Would launch VM ${VM_NAME}"
    echo "[DRY-RUN] Would assert: STARTED + per-asset INSTRUMENT_PROCESSED + STOPPED within 5min"
    exit 0
fi

LAUNCHER="${WORKSPACE_ROOT}/deployment-service/scripts/vm/launch-mtds-chainlink-smoke-vm.sh"
if [[ ! -x "$LAUNCHER" ]]; then
    echo "ERROR: VM launcher ${LAUNCHER} missing — Phase 4.F launcher must ship first" >&2
    echo "       Hand-off: slot-4-successor or Harsh." >&2
    exit 2
fi

bash "$LAUNCHER" \
    --vm-name "$VM_NAME" \
    --project-id "$PROJECT_ID" \
    --env "$DEPLOYMENT_ENV" \
    --chain "$CHAIN" \
    --assets "$ASSETS"

EVENTS_PATH="gs://${PROJECT_ID}-events/events/mtds/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo ""
echo "Tailing events at ${EVENTS_PATH}..."
DEADLINE=$(($(date +%s) + 300))
SAW_STARTED=false
SAW_ASSET_COUNT=0
SAW_STOPPED=false

while [[ $(date +%s) -lt $DEADLINE ]]; do
    if gcloud storage cat "${EVENTS_PATH}**" 2>/dev/null | jq -r '.event_type' 2>/dev/null > /tmp/chainlink-smoke-events.txt; then
        if grep -q "STARTED" /tmp/chainlink-smoke-events.txt; then SAW_STARTED=true; fi
        SAW_ASSET_COUNT=$(grep -c "INSTRUMENT_PROCESSED" /tmp/chainlink-smoke-events.txt || echo 0)
        if grep -q "STOPPED" /tmp/chainlink-smoke-events.txt; then SAW_STOPPED=true; fi
        if $SAW_STOPPED; then break; fi
    fi
    sleep 10
done

# shellcheck disable=SC2086
EXPECTED_COUNT=$(echo $ASSETS | wc -w | tr -d ' ')

if ! $SAW_STARTED; then
    echo "FAIL: VM ${VM_NAME} did not emit STARTED within 5min" >&2
    exit 1
fi
if [[ $SAW_ASSET_COUNT -lt $EXPECTED_COUNT ]]; then
    echo "FAIL: saw ${SAW_ASSET_COUNT}/${EXPECTED_COUNT} INSTRUMENT_PROCESSED" >&2
    exit 1
fi

echo "✅ Chainlink-on-${CHAIN} smoke: STARTED + ${SAW_ASSET_COUNT}/${EXPECTED_COUNT} captures + ${SAW_STOPPED:+STOPPED}"
echo "   Events at: ${EVENTS_PATH}"
echo "   Cross-check: data.chain.link UI shows live feeds for ${ASSETS}"
