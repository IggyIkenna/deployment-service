#!/usr/bin/env bash
#
# signal-broadcast-live-smoke.sh — Live-staging smoke for Signal Leasing
# broadcast (Plan B Phase 4, operator step).
#
# Unlike the local-emulator smoke (`smoke-signal-broadcast.sh`) which uses
# the `responses` library and never touches cloud, this script exercises
# the real wire end-to-end:
#
#   1. Reads the per-counterparty HMAC-SHA256 signing secret from Secret
#      Manager via `gcloud secrets versions access` (real API call).
#   2. Builds a signed JSON emission payload (same envelope shape as UAC
#      `SignalEmission`) for each counterparty.
#   3. POSTs to the deployed receiver Cloud Run service with
#      `authorization: Bearer <token>`, `idempotency-key`, and
#      `x-body-signature: <hmac-sha256>` headers.
#   4. Receiver logs can then be tailed via
#      `gcloud run services logs read signal-broadcast-smoke-receiver`.
#
# Usage:
#   bash scripts/signal-broadcast-live-smoke.sh [project_id]
#
# Defaults: project_id = central-element-323112 (R&D), receiver deployed
# in europe-west1. The paired secret provisioning is handled by
# `provision-signal-broadcast-secrets.sh` — run that first if the
# secrets don't exist yet.
#
# Exit codes:
#   0 — both counterparty POSTs returned HTTP 200
#   1 — smoke failed (any non-2xx response or missing secret)

set -euo pipefail

PROJECT_ID="${1:-central-element-323112}"
RECEIVER_BASE="${RECEIVER_BASE:-https://signal-broadcast-smoke-receiver-1060025368044.europe-west1.run.app}"
STRATEGY_ID="${STRATEGY_ID:-smoke-strategy-$(date -u +%s)}"
SLOT_LABEL="${SLOT_LABEL:-stat_arb_pairs_fixed_cefi_spot_v1}"

COUNTERPARTIES=(
    "signal-lease-cp1-staging"
    "signal-lease-cp2-staging"
)

echo "=============================================="
echo "Signal-Broadcast Live-Staging Smoke"
echo "=============================================="
echo "Project:     $PROJECT_ID"
echo "Receiver:    $RECEIVER_BASE"
echo "Strategy:    $STRATEGY_ID"
echo "Slot:        $SLOT_LABEL"
echo "=============================================="
echo

# uuidgen is macOS + Linux standard. On linux uuidgen outputs lowercase,
# on macOS uppercase — normalise.
gen_uuid() {
    uuidgen | tr '[:upper:]' '[:lower:]'
}

fail=0

for cp_id in "${COUNTERPARTIES[@]}"; do
    secret_name="signal-broadcast-counterparty-${cp_id}-hmac"
    echo "[cp=$cp_id] fetching HMAC secret from $secret_name..."

    hmac_secret="$(gcloud secrets versions access latest \
        --secret="$secret_name" \
        --project="$PROJECT_ID" 2>/dev/null || true)"

    if [ -z "$hmac_secret" ]; then
        echo "  ERROR: could not read $secret_name — did you run provision-signal-broadcast-secrets.sh?" >&2
        fail=1
        continue
    fi
    echo "  secret loaded (length=${#hmac_secret})"

    emission_id="$(gen_uuid)"
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    # Compact JSON so the HMAC body-signature deterministically matches the
    # bytes we send (curl --data sends exactly this literal).
    body="{\"emission_id\":\"${emission_id}\",\"strategy_id\":\"${STRATEGY_ID}\",\"slot_label\":\"${SLOT_LABEL}\",\"counterparty_id\":\"${cp_id}\",\"emission_timestamp\":\"${timestamp}\",\"schema_depth\":\"minimal\",\"signal_payload\":{\"direction\":\"long\",\"weight\":0.1,\"confidence\":0.75},\"delivery_attempt\":1,\"hmac_signature\":\"live-smoke-placeholder\"}"

    body_sig="$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$hmac_secret" | awk '{print $2}')"

    url="${RECEIVER_BASE}/cp/${cp_id}"
    echo "  POST $url  emission_id=$emission_id"

    http_status="$(curl -s -o /tmp/sb-smoke-${cp_id}.out -w '%{http_code}' \
        -X POST \
        -H "content-type: application/json" \
        -H "authorization: Bearer live-smoke.${cp_id}" \
        -H "idempotency-key: ${emission_id}" \
        -H "x-body-signature: ${body_sig}" \
        -H "x-counterparty-id: ${cp_id}" \
        --data "$body" \
        "$url")"

    echo "  status=$http_status body=$(cat /tmp/sb-smoke-${cp_id}.out)"

    if [ "$http_status" != "200" ]; then
        echo "  FAIL cp=$cp_id status=$http_status" >&2
        fail=1
    else
        echo "  OK  cp=$cp_id"
    fi
    echo
done

if [ $fail -ne 0 ]; then
    echo "Smoke FAILED — see errors above." >&2
    exit 1
fi

echo "Smoke GREEN — both counterparties acknowledged the signed POST."
echo
echo "Verify receiver-side logs:"
echo "  gcloud run services logs read signal-broadcast-smoke-receiver \\"
echo "    --region europe-west1 --project $PROJECT_ID --limit 20"
