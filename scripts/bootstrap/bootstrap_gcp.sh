#!/usr/bin/env bash
# Idempotent GCP bootstrap — enables APIs, creates Terraform state bucket, applies shared infra.
#
# Usage:
#   bash scripts/bootstrap/bootstrap_gcp.sh \
#     --project-id PROJECT \
#     --region     REGION  \
#     --env        ENV     \
#     --bucket-prefix PREFIX
#
# Prerequisites:
#   - gcloud CLI authenticated (gcloud auth application-default login)
#   - terraform >= 1.6 on PATH
#   - ENV must be one of: dev, staging, prod

set -euo pipefail

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
PROJECT_ID=""
REGION=""
ENV=""
PREFIX=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-id)    PROJECT_ID="$2"; shift 2 ;;
    --region)        REGION="$2";     shift 2 ;;
    --env)           ENV="$2";        shift 2 ;;
    --bucket-prefix) PREFIX="$2";     shift 2 ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 --project-id PROJECT --region REGION --env ENV --bucket-prefix PREFIX" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$PROJECT_ID" || -z "$REGION" || -z "$ENV" || -z "$PREFIX" ]]; then
  echo "Error: all of --project-id, --region, --env, --bucket-prefix are required." >&2
  exit 1
fi

if [[ ! "$ENV" =~ ^(dev|staging|prod)$ ]]; then
  echo "Error: --env must be one of: dev, staging, prod (got: $ENV)" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Enable required GCP APIs (idempotent)
# ---------------------------------------------------------------------------
REQUIRED_APIS=(
  storage.googleapis.com
  bigquery.googleapis.com
  secretmanager.googleapis.com
  run.googleapis.com
  iam.googleapis.com
  cloudbuild.googleapis.com
)

echo "==> Enabling GCP APIs for project: $PROJECT_ID"
for api in "${REQUIRED_APIS[@]}"; do
  echo "    enabling $api ..."
  gcloud services enable "$api" --project="$PROJECT_ID" --quiet
done
echo "    All APIs enabled."

# ---------------------------------------------------------------------------
# Create Terraform state bucket (idempotent)
# ---------------------------------------------------------------------------
STATE_BUCKET="${PREFIX}-terraform-state-${PROJECT_ID}"
echo "==> Ensuring Terraform state bucket: gs://${STATE_BUCKET}"

if ! gsutil ls -b "gs://${STATE_BUCKET}" &>/dev/null; then
  gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://${STATE_BUCKET}"
  gsutil versioning set on "gs://${STATE_BUCKET}"
  echo "    Created: gs://${STATE_BUCKET}"
else
  echo "    Already exists: gs://${STATE_BUCKET}"
fi

# ---------------------------------------------------------------------------
# Terraform init + apply
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TF_DIR="$REPO_ROOT/terraform/gcp"

if [[ ! -d "$TF_DIR" ]]; then
  echo "Error: Terraform directory not found: $TF_DIR" >&2
  exit 1
fi

echo "==> terraform init ($TF_DIR)"
terraform -chdir="$TF_DIR" init \
  -backend-config="bucket=${STATE_BUCKET}" \
  -backend-config="prefix=shared-infrastructure" \
  -reconfigure

echo "==> terraform apply"
terraform -chdir="$TF_DIR" apply -auto-approve \
  -var="project_id=${PROJECT_ID}" \
  -var="region=${REGION}" \
  -var="environment=${ENV}" \
  -var="bucket_prefix=${PREFIX}"

echo ""
echo "==> GCP bootstrap complete."
echo "    Project:  $PROJECT_ID"
echo "    Region:   $REGION"
echo "    Env:      $ENV"
echo "    State:    gs://${STATE_BUCKET}/shared-infrastructure"
