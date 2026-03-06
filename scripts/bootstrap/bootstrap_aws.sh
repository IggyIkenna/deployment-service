#!/usr/bin/env bash
# Idempotent AWS bootstrap — creates S3 state bucket, applies UCI shared infra.
#
# Usage:
#   bash scripts/bootstrap/bootstrap_aws.sh \
#     --account-id  ACCOUNT \
#     --region      REGION  \
#     --env         ENV     \
#     --bucket-prefix PREFIX
#
# Prerequisites:
#   - AWS CLI configured (aws configure or IAM role via instance profile)
#   - terraform >= 1.6 on PATH
#   - ENV must be one of: dev, staging, prod

set -euo pipefail

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
ACCOUNT_ID=""
REGION=""
ENV=""
PREFIX=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --account-id)    ACCOUNT_ID="$2"; shift 2 ;;
    --region)        REGION="$2";     shift 2 ;;
    --env)           ENV="$2";        shift 2 ;;
    --bucket-prefix) PREFIX="$2";     shift 2 ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 --account-id ACCOUNT --region REGION --env ENV --bucket-prefix PREFIX" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$ACCOUNT_ID" || -z "$REGION" || -z "$ENV" || -z "$PREFIX" ]]; then
  echo "Error: all of --account-id, --region, --env, --bucket-prefix are required." >&2
  exit 1
fi

if [[ ! "$ENV" =~ ^(dev|staging|prod)$ ]]; then
  echo "Error: --env must be one of: dev, staging, prod (got: $ENV)" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Create S3 Terraform state bucket (idempotent)
# ---------------------------------------------------------------------------
STATE_BUCKET="${PREFIX}-terraform-state-${ACCOUNT_ID}"
echo "==> Ensuring S3 Terraform state bucket: s3://${STATE_BUCKET}"

if aws s3api head-bucket --bucket "$STATE_BUCKET" --region "$REGION" 2>/dev/null; then
  echo "    Already exists: s3://${STATE_BUCKET}"
else
  echo "    Creating: s3://${STATE_BUCKET}"

  # us-east-1 requires no LocationConstraint; all other regions require it.
  if [[ "$REGION" == "us-east-1" ]]; then
    aws s3api create-bucket \
      --bucket "$STATE_BUCKET" \
      --region "$REGION"
  else
    aws s3api create-bucket \
      --bucket "$STATE_BUCKET" \
      --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION"
  fi

  # Enable versioning for state safety
  aws s3api put-bucket-versioning \
    --bucket "$STATE_BUCKET" \
    --versioning-configuration Status=Enabled

  # Block public access
  aws s3api put-public-access-block \
    --bucket "$STATE_BUCKET" \
    --public-access-block-configuration \
      BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

  # Enable server-side encryption
  aws s3api put-bucket-encryption \
    --bucket "$STATE_BUCKET" \
    --server-side-encryption-configuration '{
      "Rules": [{
        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}
      }]
    }'

  echo "    Created and secured: s3://${STATE_BUCKET}"
fi

# ---------------------------------------------------------------------------
# Terraform init + apply
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TF_DIR="$REPO_ROOT/terraform/aws"

if [[ ! -d "$TF_DIR" ]]; then
  echo "Error: Terraform directory not found: $TF_DIR" >&2
  exit 1
fi

echo "==> terraform init ($TF_DIR)"
terraform -chdir="$TF_DIR" init \
  -backend-config="bucket=${STATE_BUCKET}" \
  -backend-config="key=terraform/state" \
  -backend-config="region=${REGION}" \
  -reconfigure

echo "==> terraform apply"
terraform -chdir="$TF_DIR" apply -auto-approve \
  -var="aws_account_id=${ACCOUNT_ID}" \
  -var="aws_region=${REGION}" \
  -var="environment=${ENV}" \
  -var="bucket_prefix=${PREFIX}"

echo ""
echo "==> AWS bootstrap complete."
echo "    Account:  $ACCOUNT_ID"
echo "    Region:   $REGION"
echo "    Env:      $ENV"
echo "    State:    s3://${STATE_BUCKET}/terraform/state"
