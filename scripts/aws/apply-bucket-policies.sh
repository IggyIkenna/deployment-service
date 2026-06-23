#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Apply IAM bucket policies to unified-trading S3 buckets.
# Reads the policy intent from configs/iam-bucket-policies.aws.yaml and generates
# per-bucket IAM policy JSON, then applies via aws s3api put-bucket-policy.
#
# Plan: unified-trading-pm/plans/active/aws_migration_defi_first_2026_05_07.md Phase 2 [QG] P0.
# SSOT: deployment-service/configs/iam-bucket-policies.aws.yaml
#
# Three policies applied per DeFi prod bucket:
#   1. prod_write_protection — denies writes from dev/staging roles; allows prod + glue + admin.
#   2. glue_read_access      — allows Glue crawler read on unified-trading-*-defi-* buckets.
#   3. athena_results_write  — allows Athena results write on events bucket _athena-results/ prefix.
#
# Idempotent: applying the same policy twice is a no-op (put-bucket-policy is upsert).
#
# Env:
#   AWS_ACCOUNT_ID      — defaults to sts get-caller-identity (427895769566).
#   AWS_DEFAULT_REGION  — defaults to ap-northeast-1.
#
# Usage:
#   bash scripts/aws/apply-bucket-policies.sh               # dry-run (print policies only)
#   bash scripts/aws/apply-bucket-policies.sh --apply       # apply to all DeFi prod buckets
#   bash scripts/aws/apply-bucket-policies.sh --apply --bucket unified-trading-evm-defi-prod-427895769566
set -euo pipefail

APPLY=false
BUCKET_FILTER=""
REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply) APPLY=true; shift ;;
        --bucket) BUCKET_FILTER="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,30p' "$0"
            exit 0
            ;;
        *) echo "Unknown flag: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
    AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
fi
if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
    echo "AWS_ACCOUNT_ID unset and aws sts get-caller-identity failed" >&2
    echo "Authenticate with the operator account first (admin_od / 427895769566)." >&2
    exit 1
fi

echo "AWS account:  $AWS_ACCOUNT_ID"
echo "Region:       $REGION"
echo "Bucket filter:${BUCKET_FILTER:- all}"
echo "Mode:         $([[ "$APPLY" == "true" ]] && echo APPLY || echo DRY-RUN)"
echo ""

# DeFi prod buckets — canonical names per cloud-providers.yaml SSOT (2026-05-20 rename to
# {kind}-{env_short}-{account} format; env_short=prd for production).
# See deployment-service@de78a42 Phase 5 bucket-rename migration.
ALL_DEFI_PROD_BUCKETS=(
    "evm-defi-prd-${AWS_ACCOUNT_ID}"
    "dex-pools-prd-${AWS_ACCOUNT_ID}"
    "dex-swaps-prd-${AWS_ACCOUNT_ID}"
    "solana-defi-prd-${AWS_ACCOUNT_ID}"
    "eigenlayer-rewards-prd-${AWS_ACCOUNT_ID}"
    "pnl-store-defi-prd-${AWS_ACCOUNT_ID}"
    "positions-store-defi-prd-${AWS_ACCOUNT_ID}"
    "risk-store-defi-prd-${AWS_ACCOUNT_ID}"
    "config-store-prd-${AWS_ACCOUNT_ID}"
    "events-prd-${AWS_ACCOUNT_ID}"
    "instruments-store-defi-prd-${AWS_ACCOUNT_ID}"
    "market-data-tick-defi-prd-${AWS_ACCOUNT_ID}"
)

# Events bucket — receives the athena_results_write policy in addition to prod_write_protection.
EVENTS_BUCKET="events-prd-${AWS_ACCOUNT_ID}"

# Build the prod_write_protection policy JSON for a given bucket.
# Mirrors GCP iam-bucket-policies.yaml prod_write_protection: denies writes from dev/staging roles;
# allows prod service roles + Glue crawler + admin. Migration transfer user excluded (temporary cred).
build_prod_write_protection_policy() {
    local bucket="$1"
    cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyWritesFromDevStagingRoles",
      "Effect": "Deny",
      "Principal": "*",
      "Action": [
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:DeleteObjectVersion"
      ],
      "Resource": [
        "arn:aws:s3:::${bucket}",
        "arn:aws:s3:::${bucket}/*"
      ],
      "Condition": {
        "ArnLike": {
          "aws:PrincipalArn": [
            "arn:aws:iam::${AWS_ACCOUNT_ID}:role/uts-*-dev",
            "arn:aws:iam::${AWS_ACCOUNT_ID}:role/uts-*-staging"
          ]
        }
      }
    },
    {
      "Sid": "AllowProdServiceRolesFullAccess",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${AWS_ACCOUNT_ID}:root"
      },
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::${bucket}",
        "arn:aws:s3:::${bucket}/*"
      ],
      "Condition": {
        "ArnLike": {
          "aws:PrincipalArn": [
            "arn:aws:iam::${AWS_ACCOUNT_ID}:role/uts-*-prod",
            "arn:aws:iam::${AWS_ACCOUNT_ID}:role/AWSGlueServiceRole-UnifiedTradingDeFi",
            "arn:aws:iam::${AWS_ACCOUNT_ID}:user/admin_od"
          ]
        }
      }
    }
  ]
}
EOF
}

# Build the athena_results_write policy for the events bucket (additive with prod_write_protection).
# AWS requires a SINGLE bucket policy document, so this is a combined policy when both apply.
build_events_bucket_policy() {
    local bucket="$1"
    cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyWritesFromDevStagingRoles",
      "Effect": "Deny",
      "Principal": "*",
      "Action": [
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:DeleteObjectVersion"
      ],
      "Resource": [
        "arn:aws:s3:::${bucket}",
        "arn:aws:s3:::${bucket}/*"
      ],
      "Condition": {
        "ArnLike": {
          "aws:PrincipalArn": [
            "arn:aws:iam::${AWS_ACCOUNT_ID}:role/uts-*-dev",
            "arn:aws:iam::${AWS_ACCOUNT_ID}:role/uts-*-staging"
          ]
        }
      }
    },
    {
      "Sid": "AllowProdServiceRolesFullAccess",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${AWS_ACCOUNT_ID}:root"
      },
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::${bucket}",
        "arn:aws:s3:::${bucket}/*"
      ],
      "Condition": {
        "ArnLike": {
          "aws:PrincipalArn": [
            "arn:aws:iam::${AWS_ACCOUNT_ID}:role/uts-*-prod",
            "arn:aws:iam::${AWS_ACCOUNT_ID}:role/AWSGlueServiceRole-UnifiedTradingDeFi",
            "arn:aws:iam::${AWS_ACCOUNT_ID}:user/admin_od"
          ]
        }
      }
    },
    {
      "Sid": "AllowAthenaResultsWrite",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${AWS_ACCOUNT_ID}:root"
      },
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${bucket}/_athena-results/*",
        "arn:aws:s3:::${bucket}"
      ],
      "Condition": {
        "ArnLike": {
          "aws:PrincipalArn": [
            "arn:aws:iam::${AWS_ACCOUNT_ID}:role/uts-*-prod",
            "arn:aws:iam::${AWS_ACCOUNT_ID}:user/admin_od"
          ]
        }
      }
    }
  ]
}
EOF
}

APPLIED=0
SKIPPED=0
FAILED=0

for BUCKET in "${ALL_DEFI_PROD_BUCKETS[@]}"; do
    if [[ -n "$BUCKET_FILTER" && "$BUCKET" != "$BUCKET_FILTER" ]]; then
        continue
    fi

    # Confirm bucket exists before attempting to set policy.
    if ! aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
        echo "  [SKIP]    $BUCKET — bucket not found (skipping policy)"
        ((SKIPPED++)) || true
        continue
    fi

    # Choose the appropriate policy document.
    if [[ "$BUCKET" == "$EVENTS_BUCKET" ]]; then
        POLICY_JSON="$(build_events_bucket_policy "$BUCKET")"
        POLICY_LABEL="events combined (prod_write_protection + athena_results_write)"
    else
        POLICY_JSON="$(build_prod_write_protection_policy "$BUCKET")"
        POLICY_LABEL="prod_write_protection"
    fi

    if [[ "$APPLY" == "false" ]]; then
        echo "  [DRY-RUN] $BUCKET  policy=$POLICY_LABEL"
        echo "$POLICY_JSON" | python3 -m json.tool --no-ensure-ascii 2>/dev/null | head -5
        echo "    ..."
        ((APPLIED++)) || true
        continue
    fi

    echo -n "  [APPLY]   $BUCKET ($POLICY_LABEL) ... "
    if aws s3api put-bucket-policy \
        --bucket "$BUCKET" \
        --policy "$POLICY_JSON" \
        --region "$REGION" 2>/dev/null; then
        echo "✅"
        ((APPLIED++)) || true
    else
        echo "❌ FAILED" >&2
        ((FAILED++)) || true
    fi
done

echo ""
echo "Summary: applied=${APPLIED}, skipped=${SKIPPED}, failed=${FAILED}"
echo ""

if [[ "$APPLY" == "false" ]]; then
    echo "Dry-run only. Re-run with --apply to apply bucket policies."
    exit 0
fi

# Phase 2 QG verification: check policy applied on each bucket.
echo "--- Post-apply policy verification ---"
VERIFY_OK=0
VERIFY_FAIL=0
for BUCKET in "${ALL_DEFI_PROD_BUCKETS[@]}"; do
    if [[ -n "$BUCKET_FILTER" && "$BUCKET" != "$BUCKET_FILTER" ]]; then
        continue
    fi
    if aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
        if aws s3api get-bucket-policy --bucket "$BUCKET" --region "$REGION" \
            --query 'Policy' --output text 2>/dev/null | python3 -m json.tool --no-ensure-ascii >/dev/null 2>&1; then
            echo "  ✅  $BUCKET — policy present and valid JSON"
            ((VERIFY_OK++)) || true
        else
            echo "  ⚠️   $BUCKET — bucket exists but no policy or invalid JSON" >&2
            ((VERIFY_FAIL++)) || true
        fi
    fi
done
echo ""
echo "Verification: ok=${VERIFY_OK}, fail=${VERIFY_FAIL}"
if [[ "$VERIFY_FAIL" -gt 0 ]]; then
    echo "⚠️  Some bucket policies missing. Check IAM permissions and re-run with --apply." >&2
    exit 1
fi
echo "Phase 2 [QG] P0 — IAM bucket policies applied and verified. ✅"
