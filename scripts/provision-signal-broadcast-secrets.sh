#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
#
# provision-signal-broadcast-secrets.sh — Provision per-counterparty HMAC
# signing secrets for the signal-leasing broadcast channel (Plan B Phase 4).
#
# Each signal-leasing counterparty owns a GCP Secret Manager entry holding
# the HMAC-SHA256 signing key that strategy-service' signal_broadcast
# sub-package uses to sign outbound webhook payloads (D3 auth model, plan
# `signal_leasing_broadcast_architecture_2026_04_20`). The secret is read
# at runtime by `CounterpartyCredentialsManager` (wraps UTL `ApiKeyReloader`)
# — credentials hot-reload on a 5-minute timer, so rotated secrets propagate
# without a service restart.
#
# Secret naming convention (Secret Manager resource names):
#   signal-broadcast-counterparty-{counterparty_id}-hmac
#
# The resource name is stored in UAC `Counterparty.hmac_secret_ref` and is
# the key passed to `ApiKeyReloader`.
#
# Usage:
#   ./provision-signal-broadcast-secrets.sh [project_id]
#
#   # Provision with a pre-generated key (for rotation / restore):
#   HMAC_KEY_CP1=... HMAC_KEY_CP2=... ./provision-signal-broadcast-secrets.sh my-project
#
# Notes:
#   - Staging fixtures: `signal-lease-cp1-staging`, `signal-lease-cp2-staging`.
#   - If no HMAC_KEY_* env var is supplied, this script generates a
#     cryptographically-random 32-byte secret via `openssl rand -hex 32`.
#   - Follows the same create-or-rotate pattern as `setup-oauth.sh` §[4/6].
#   - D3 decision locked 2026-04-20: HMAC-SHA256 webhook signing with
#     rotation via Secret Manager versions.
#

set -euo pipefail

PROJECT_ID="${1:-$(gcloud config get-value project 2>/dev/null || true)}"

if [ -z "$PROJECT_ID" ]; then
    echo "ERROR: project_id not supplied and gcloud has no default project." >&2
    echo "Usage: $0 [project_id]" >&2
    exit 1
fi

# Staging counterparty fixtures for the Sept-2026 go-live anchor (2 CPs).
# Production counterparty IDs are provisioned via the same script with
# the `-staging` suffix removed.
readonly COUNTERPARTIES=(
    "signal-lease-cp1-staging"
    "signal-lease-cp2-staging"
)

echo "=============================================="
echo "Signal-Broadcast Secret Manager Provisioning"
echo "=============================================="
echo "Project: $PROJECT_ID"
echo "Counterparties: ${COUNTERPARTIES[*]}"
echo "=============================================="
echo

gcloud config set project "$PROJECT_ID" >/dev/null

# Ensure Secret Manager API is enabled — idempotent, matches setup-oauth.sh.
echo "[1/3] Ensuring Secret Manager API is enabled..."
gcloud services enable secretmanager.googleapis.com --project="$PROJECT_ID"
echo "API enabled."
echo

# create-or-rotate — mirrors setup-oauth.sh's `create_or_update_secret` pattern.
create_or_rotate_secret() {
    local secret_name="$1"
    local secret_value="$2"

    if gcloud secrets describe "$secret_name" --project="$PROJECT_ID" >/dev/null 2>&1; then
        echo "  Rotating secret: $secret_name"
        echo -n "$secret_value" | gcloud secrets versions add "$secret_name" \
            --project="$PROJECT_ID" \
            --data-file=-
    else
        echo "  Creating secret: $secret_name"
        echo -n "$secret_value" | gcloud secrets create "$secret_name" \
            --project="$PROJECT_ID" \
            --replication-policy="automatic" \
            --data-file=- \
            --labels="managed_by=signal-broadcast,purpose=hmac-webhook-signing"
    fi
}

echo "[2/3] Provisioning per-counterparty HMAC secrets..."
for cp_id in "${COUNTERPARTIES[@]}"; do
    secret_name="signal-broadcast-counterparty-${cp_id}-hmac"

    # Accept an override env var so operators can restore a known key.
    # Env-var name: HMAC_KEY_<uppercased-counterparty-id-with-hyphens-as-underscores>
    override_var="HMAC_KEY_$(echo "$cp_id" | tr '[:lower:]-' '[:upper:]_')"
    if [ -n "${!override_var:-}" ]; then
        echo "  Using pre-generated key from \$$override_var for $cp_id"
        hmac_value="${!override_var}"
    else
        echo "  Generating 32-byte HMAC key for $cp_id"
        hmac_value="$(openssl rand -hex 32)"
    fi

    create_or_rotate_secret "$secret_name" "$hmac_value"
done

echo
echo "[3/3] Summary — created/rotated secrets:"
for cp_id in "${COUNTERPARTIES[@]}"; do
    secret_name="signal-broadcast-counterparty-${cp_id}-hmac"
    echo "  - $secret_name (hmac_secret_ref for Counterparty($cp_id))"
done
echo
echo "Next steps:"
echo "  1. Register each Counterparty in UAC with hmac_secret_ref set to the"
echo "     full resource name: projects/$PROJECT_ID/secrets/<secret_name>"
echo "  2. Apply terraform for strategy-service so Cloud Run mounts the"
echo "     secrets as env vars: terraform/services/strategy-service/gcp/"
echo "  3. Refresh code tarballs so VMs pick up the latest strategy-service"
echo "     (includes signal_broadcast sub-package):"
echo "       bash scripts/vm/create-code-tarballs.sh --all"
echo
echo "Provisioning complete."
