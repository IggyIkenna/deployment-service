#!/usr/bin/env bash
#
# setup-cloudsql.sh — Create GCP Cloud SQL (PostgreSQL) for execution-service order state
#
# Provisions a Cloud SQL PostgreSQL 15 instance for live order + position state persistence.
# The execution-service optionally persists order/position state to PostgreSQL when
# `use_database=true` is set in its config (database_url field).
#
# Idempotent — safe to re-run. Skips already-existing instances/databases/users.
#
# Usage:
#   GCP_PROJECT_ID=central-element-323112 ./setup-cloudsql.sh [--dry-run]
#
# Required env vars (or positional arg):
#   GCP_PROJECT_ID    — GCP project ID
#
# Optional env vars:
#   CLOUDSQL_INSTANCE_NAME  — defaults to "trading-order-state"
#   CLOUDSQL_REGION         — defaults to "asia-northeast1"
#   CLOUDSQL_TIER           — defaults to "db-g1-small" (1 vCPU, 1.7 GB RAM)
#   CLOUDSQL_DB_NAME        — defaults to "order_state"
#   CLOUDSQL_USER           — defaults to "execution_svc"
#   CLOUDSQL_PASSWORD       — auto-generated if not set; stored in Secret Manager
#
# AWS RDS equivalent: see setup-aws-rds.sh (blocked — no AWS creds)
#

set -euo pipefail

PROJECT_ID="${1:-${GCP_PROJECT_ID:-$(gcloud config get-value project)}}"
DRY_RUN=false

for arg in "$@"; do
    [[ "$arg" == "--dry-run" ]] && DRY_RUN=true
done

INSTANCE_NAME="${CLOUDSQL_INSTANCE_NAME:-trading-order-state}"
REGION="${CLOUDSQL_REGION:-${GCS_REGION:-asia-northeast1}}"
TIER="${CLOUDSQL_TIER:-db-g1-small}"
DB_NAME="${CLOUDSQL_DB_NAME:-order_state}"
DB_USER="${CLOUDSQL_USER:-execution_svc}"

echo "================================================="
echo "Unified Trading System — Cloud SQL Setup"
echo "================================================="
echo "Project:   $PROJECT_ID"
echo "Instance:  $INSTANCE_NAME"
echo "Region:    $REGION"
echo "Tier:      $TIER"
echo "Database:  $DB_NAME"
echo "User:      $DB_USER"
echo "Dry-run:   $DRY_RUN"
echo "================================================="
echo

if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] Would create:"
    echo "  gcloud sql instances create $INSTANCE_NAME --tier=$TIER --region=$REGION ..."
    echo "  gcloud sql databases create $DB_NAME --instance=$INSTANCE_NAME"
    echo "  gcloud sql users create $DB_USER --instance=$INSTANCE_NAME"
    echo "  Secret Manager: cloudsql-execution-db-url"
    echo "  IAM: Cloud SQL Client role for github-actions-deploy SA"
    echo ""
    echo "Connection string format:"
    echo "  postgresql+asyncpg://$DB_USER:<password>@/<db_name>?host=/cloudsql/$PROJECT_ID:$REGION:$INSTANCE_NAME"
    exit 0
fi

gcloud config set project "$PROJECT_ID" --quiet

# Enable Cloud SQL Admin API
echo "Enabling Cloud SQL API..."
gcloud services enable sqladmin.googleapis.com --project="$PROJECT_ID" --quiet || true

# ---------------------------------------------------------------------------
# 1. Create Cloud SQL instance (if not exists)
# ---------------------------------------------------------------------------
if gcloud sql instances describe "$INSTANCE_NAME" --project="$PROJECT_ID" &>/dev/null; then
    echo "[SKIP] Cloud SQL instance '$INSTANCE_NAME' already exists"
else
    echo "[CREATE] Cloud SQL instance '$INSTANCE_NAME' (this takes 3-5 minutes)..."
    gcloud sql instances create "$INSTANCE_NAME" \
        --project="$PROJECT_ID" \
        --database-version=POSTGRES_15 \
        --tier="$TIER" \
        --region="$REGION" \
        --storage-auto-increase \
        --storage-size=10GB \
        --backup-start-time=03:00 \
        --deletion-protection \
        --database-flags=max_connections=100,log_min_duration_statement=1000 \
        --insights-config-query-insights-enabled \
        --quiet
    echo "[OK] Cloud SQL instance created"
fi

# ---------------------------------------------------------------------------
# 2. Create database (if not exists)
# ---------------------------------------------------------------------------
if gcloud sql databases describe "$DB_NAME" --instance="$INSTANCE_NAME" --project="$PROJECT_ID" &>/dev/null; then
    echo "[SKIP] Database '$DB_NAME' already exists"
else
    echo "[CREATE] Database '$DB_NAME'..."
    gcloud sql databases create "$DB_NAME" \
        --instance="$INSTANCE_NAME" \
        --project="$PROJECT_ID" \
        --quiet
    echo "[OK] Database '$DB_NAME' created"
fi

# ---------------------------------------------------------------------------
# 3. Create user + store password in Secret Manager
# ---------------------------------------------------------------------------
PASSWORD_SECRET="cloudsql-execution-db-password"
DB_URL_SECRET="cloudsql-execution-db-url"

# Generate password if not already in Secret Manager
if gcloud secrets describe "$PASSWORD_SECRET" --project="$PROJECT_ID" &>/dev/null; then
    echo "[SKIP] Secret '$PASSWORD_SECRET' already exists"
    DB_PASSWORD=$(gcloud secrets versions access latest --secret="$PASSWORD_SECRET" --project="$PROJECT_ID")
else
    echo "[CREATE] Generating password and storing in Secret Manager..."
    DB_PASSWORD=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    echo -n "$DB_PASSWORD" | gcloud secrets create "$PASSWORD_SECRET" \
        --project="$PROJECT_ID" \
        --data-file=- \
        --quiet
    echo "[OK] Password stored in Secret Manager as '$PASSWORD_SECRET'"
fi

# Create DB user
if gcloud sql users list --instance="$INSTANCE_NAME" --project="$PROJECT_ID" 2>/dev/null | grep -q "^$DB_USER "; then
    echo "[SKIP] DB user '$DB_USER' already exists"
else
    echo "[CREATE] DB user '$DB_USER'..."
    gcloud sql users create "$DB_USER" \
        --instance="$INSTANCE_NAME" \
        --project="$PROJECT_ID" \
        --password="$DB_PASSWORD" \
        --quiet
    echo "[OK] DB user '$DB_USER' created"
fi

# Store full connection URL in Secret Manager
INSTANCE_CONNECTION_NAME="$PROJECT_ID:$REGION:$INSTANCE_NAME"
DB_URL="postgresql+asyncpg://$DB_USER:$DB_PASSWORD@/$DB_NAME?host=/cloudsql/$INSTANCE_CONNECTION_NAME"

if gcloud secrets describe "$DB_URL_SECRET" --project="$PROJECT_ID" &>/dev/null; then
    echo "[UPDATE] Updating '$DB_URL_SECRET' in Secret Manager..."
    echo -n "$DB_URL" | gcloud secrets versions add "$DB_URL_SECRET" \
        --project="$PROJECT_ID" \
        --data-file=- \
        --quiet
else
    echo "[CREATE] Storing connection URL in Secret Manager as '$DB_URL_SECRET'..."
    echo -n "$DB_URL" | gcloud secrets create "$DB_URL_SECRET" \
        --project="$PROJECT_ID" \
        --data-file=- \
        --quiet
fi
echo "[OK] DB URL stored in Secret Manager as '$DB_URL_SECRET'"

# ---------------------------------------------------------------------------
# 4. Grant Cloud SQL Client role to github-actions-deploy SA
# ---------------------------------------------------------------------------
SA_EMAIL="github-actions-deploy@${PROJECT_ID}.iam.gserviceaccount.com"
echo "[IAM] Granting roles/cloudsql.client to $SA_EMAIL..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/cloudsql.client" \
    --quiet 2>&1 | grep -E "bindings|serviceAccount" || true
echo "[OK] IAM policy updated"

echo
echo "================================================="
echo "Cloud SQL setup complete"
echo "================================================="
echo "Instance: $INSTANCE_NAME"
echo "Connection: $INSTANCE_CONNECTION_NAME"
echo ""
echo "Secrets stored in GCP Secret Manager:"
echo "  $PASSWORD_SECRET   — DB password"
echo "  $DB_URL_SECRET     — Full asyncpg connection URL"
echo ""
echo "To connect via Cloud SQL Auth Proxy:"
echo "  cloud-sql-proxy $INSTANCE_CONNECTION_NAME &"
echo "  psql -h 127.0.0.1 -U $DB_USER -d $DB_NAME"
echo ""
echo "execution-service config:"
echo "  DATABASE_URL=\$(gcloud secrets versions access latest --secret=$DB_URL_SECRET)"
echo "  USE_DATABASE=true"
