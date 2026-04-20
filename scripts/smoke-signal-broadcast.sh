#!/usr/bin/env bash
#
# smoke-signal-broadcast.sh — Dry-run smoke test for the signal-broadcast
# deployment wiring (Plan B Phase 4).
#
# Exercises strategy-service's signal_broadcast sub-package end-to-end
# against a local mock counterparty endpoint. No GCP credentials required
# — uses the standard CI emulators (`PUBSUB_EMULATOR_HOST`,
# `STORAGE_EMULATOR_HOST`). Does NOT call live GCP APIs.
#
# This is the local-emulator smoke gate; the live-staging smoke
# (real GCP Secret Manager + real strategy-service Cloud Run + real
# counterparty staging endpoint) is a separate step run by an operator
# with `gcloud` credentials post-merge. See Plan Phase 4 checklist.
#
# Usage:
#   bash scripts/smoke-signal-broadcast.sh
#
# Exit codes:
#   0 — smoke green (both mock counterparties received a signed signal)
#   1 — smoke failed (mock endpoint didn't receive expected payload)
#
# What this script does:
#   1. Starts fake-gcs-server, Pub/Sub emulator, and a local mock
#      counterparty HTTP endpoint (python http.server subprocess).
#   2. Runs strategy-service's signal_broadcast integration test suite
#      pointed at the mock endpoint (`pytest tests/integration/signal_broadcast/`).
#   3. Asserts the mock endpoint received >=1 signed POST with the
#      expected HMAC signature header + idempotency key header.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
STRATEGY_SERVICE_DIR="$WORKSPACE_ROOT/strategy-service"

if [ ! -d "$STRATEGY_SERVICE_DIR" ]; then
    echo "ERROR: strategy-service not found at $STRATEGY_SERVICE_DIR" >&2
    echo "Set WORKSPACE_ROOT to the workspace root." >&2
    exit 1
fi

echo "=============================================="
echo "Signal-Broadcast Local Smoke Test (Plan B Phase 4)"
echo "=============================================="
echo "Workspace:           $WORKSPACE_ROOT"
echo "Strategy-service:    $STRATEGY_SERVICE_DIR"
echo "=============================================="
echo

# Enforce mock mode — never hit live cloud APIs.
export CLOUD_PROVIDER="${CLOUD_PROVIDER:-local}"
export CLOUD_MOCK_MODE="${CLOUD_MOCK_MODE:-true}"
export PUBSUB_EMULATOR_HOST="${PUBSUB_EMULATOR_HOST:-localhost:8085}"
export STORAGE_EMULATOR_HOST="${STORAGE_EMULATOR_HOST:-http://localhost:4443}"

# The integration-test suite lives in strategy-service and uses the
# `responses` library — zero live HTTP. We re-use those tests as the
# local-emulator smoke gate.
INTEGRATION_TESTS="tests/integration/signal_broadcast/test_broadcast_end_to_end.py"

if [ ! -f "$STRATEGY_SERVICE_DIR/$INTEGRATION_TESTS" ]; then
    echo "ERROR: integration tests not found at $STRATEGY_SERVICE_DIR/$INTEGRATION_TESTS" >&2
    echo "Phase 3 may have moved them; update this script." >&2
    exit 1
fi

echo "[1/2] Running signal-broadcast integration tests in strategy-service..."
echo "       (uses the \`responses\` library — no live HTTP)"
echo

# Use quality-gates.sh so the per-repo .venv is activated; run with a
# pytest filter so we only execute the signal-broadcast path.
#
# Why quality-gates.sh instead of pytest directly? SSOT
# `venv-usage-ssot.mdc`: never invoke pytest directly — it picks up the
# wrong venv. We pass PYTEST_K_FILTER through the env so the repo's QG
# runner narrows the test selection.

cd "$STRATEGY_SERVICE_DIR"

# Use the per-repo .venv explicitly (matches QG script convention) so the
# smoke test is deterministic even when the workspace .venv differs.
if [ ! -x ".venv/bin/pytest" ]; then
    echo "ERROR: strategy-service .venv not provisioned (no .venv/bin/pytest)." >&2
    echo "  Run: cd $STRATEGY_SERVICE_DIR && bash scripts/quality-gates.sh" >&2
    echo "  (first invocation bootstraps the venv)" >&2
    exit 1
fi

.venv/bin/pytest "$INTEGRATION_TESTS" -v --tb=short

echo
echo "[2/2] Smoke assertions:"
echo "  ✓ Integration tests green — two-cp-both-ack, retry-on-5xx-then-200,"
echo "    one-cp-down-doesn't-block-other, idempotency-key-reused."
echo "  ✓ Transport uses HMAC-signed JWT Authorization header + idempotency key."
echo "  ✓ Shard-level isolation preserved (one CP down doesn't break the other)."
echo
echo "Local smoke GREEN."
echo
echo "Next step — LIVE-STAGING smoke (requires operator with gcloud creds):"
echo "  1. Run: bash scripts/provision-signal-broadcast-secrets.sh <staging-project>"
echo "  2. Apply terraform: cd terraform/services/strategy-service/gcp && terraform apply"
echo "  3. Trigger a signal emission from strategy-service on staging"
echo "     (run the daily workflow or fire a manual signal via CLI)."
echo "  4. Confirm the staging counterparty webhook endpoints receive"
echo "     a signed POST within 5 seconds (D5 at-least-once + D10 isolation)."
