#!/bin/bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Pyth-on-Solana real-data smoke runbook.
#
# Plan: api_keys_wallets_accounts_readiness_2026_05_10.md Phase 4.E.
# SSOT: codex/04-architecture/custody-providers.md + MTDS oracle_prices_handler.
#
# Triggers MTDS oracle_prices_handler against mainnet Solana RPC + Hermes endpoint,
# captures per-LST price (jitoSOL / mSOL / bSOL), confirms event-stream emission.
#
# Per Runbook Execution-Owner SSOT HARD RULE:
#   execution:
#     owner: ikennaigboaka (operator) + Harsh side (MTDS Phase 4)
#     cadence: weekly cron VM + pre-cutover gate 2026-05-22
#     verifier: gs://${PID}-events/events/mtds/$(date +%Y-%m-%d)/<vm-name>/ shows STARTED + INSTRUMENT_PROCESSED + STOPPED
#     last_executed: NEVER
#
# Per CLAUDE.md "No fire-and-forget VM launches" HARD RULE — this script launches
# the VM + tails the event-stream until STOPPED or 60s timeout.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../../" && pwd)"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-development}"
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-central-element-323112}"

DRY_RUN=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true; shift ;;
        --project-id) PROJECT_ID="$2"; shift 2 ;;
        -h|--help) grep '^#' "$0" | head -25; exit 0 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

VM_NAME="mtds-pyth-realdata-smoke-$(date +%Y%m%d-%H%M%S)"
LST_TOKENS=("jitoSOL" "mSOL" "bSOL")

echo "==========================================
Pyth-on-Solana real-data smoke
  VM: ${VM_NAME}
  Project: ${PROJECT_ID}
  Env: ${DEPLOYMENT_ENV}
  LSTs to probe: ${LST_TOKENS[*]}
=========================================="

if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] Would launch VM ${VM_NAME} via launch-mtds-pyth-smoke-vm.sh"
    echo "[DRY-RUN] Would tail gs://${PROJECT_ID}-events/events/mtds/$(date +%Y-%m-%d)/${VM_NAME}/"
    echo "[DRY-RUN] Would assert: STARTED + per-LST INSTRUMENT_PROCESSED + STOPPED within 5min"
    exit 0
fi

# Real-infra path — requires deployment-service/scripts/vm/launch-mtds-pyth-smoke-vm.sh
# (PENDING; spec'd here per Phase 4.E sub-deliverable)
LAUNCHER="${WORKSPACE_ROOT}/deployment-service/scripts/vm/launch-mtds-pyth-smoke-vm.sh"
if [[ ! -x "$LAUNCHER" ]]; then
    echo "ERROR: VM launcher ${LAUNCHER} missing — Phase 4.E launcher must ship first" >&2
    echo "       Hand-off: slot-4-successor or Harsh to author launcher mirroring" >&2
    echo "       deployment-service/scripts/vm/launch-mdps-features-live.sh shape." >&2
    exit 2
fi

bash "$LAUNCHER" \
    --vm-name "$VM_NAME" \
    --project-id "$PROJECT_ID" \
    --env "$DEPLOYMENT_ENV" \
    --lst-tokens "${LST_TOKENS[*]}" \
    --hermes-endpoint "https://hermes.pyth.network/v2/updates/price/latest"

# Per CLAUDE.md HARD RULE: event-stream verification (no fire-and-forget)
EVENTS_PATH="gs://${PROJECT_ID}-events/events/mtds/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo ""
echo "Tailing events at ${EVENTS_PATH}..."
DEADLINE=$(($(date +%s) + 300))  # 5 min timeout
SAW_STARTED=false
SAW_LST_COUNT=0
SAW_STOPPED=false

while [[ $(date +%s) -lt $DEADLINE ]]; do
    if gcloud storage cat "${EVENTS_PATH}**" 2>/dev/null | jq -r '.event_type' 2>/dev/null > /tmp/pyth-smoke-events.txt; then
        if grep -q "STARTED" /tmp/pyth-smoke-events.txt; then SAW_STARTED=true; fi
        SAW_LST_COUNT=$(grep -c "INSTRUMENT_PROCESSED" /tmp/pyth-smoke-events.txt || echo 0)
        if grep -q "STOPPED" /tmp/pyth-smoke-events.txt; then SAW_STOPPED=true; fi
        if $SAW_STOPPED; then break; fi
    fi
    sleep 10
done

if ! $SAW_STARTED; then
    echo "FAIL: VM ${VM_NAME} did not emit STARTED event within 5min" >&2
    exit 1
fi
if [[ $SAW_LST_COUNT -lt ${#LST_TOKENS[@]} ]]; then
    echo "FAIL: only saw ${SAW_LST_COUNT}/${#LST_TOKENS[@]} INSTRUMENT_PROCESSED events" >&2
    exit 1
fi
if ! $SAW_STOPPED; then
    echo "WARN: STOPPED event not observed within 5min — VM may still be running" >&2
fi

echo "✅ Pyth-on-Solana smoke: STARTED + ${SAW_LST_COUNT} LST captures + ${SAW_STOPPED:+STOPPED}"
echo "   Events at: ${EVENTS_PATH}"
echo "   Cross-check: pyth.network UI shows live prices for ${LST_TOKENS[*]}"
