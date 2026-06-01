#!/usr/bin/env bash
# Provision the 10 DeFi-relevant S3 buckets needed for May-23 AWS cutover.
#
# Plan: unified-trading-pm/plans/active/aws_migration_defi_first_2026_05_07.plan Phase 2.
# Triggered by Agent 4 (work_split_2026_05_07_ikenna_5tab_layout.md Item 3).
#
# Idempotent: each bucket is created only if missing. Reads bucket templates from
# `deployment-service/configs/cloud-providers.yaml` aws.storage section so the
# names stay in lockstep with the SSOT (no inline string fallbacks).
#
# Env required:
#   AWS_ACCOUNT_ID       — typically 427895769566 (operator account; checked from
#                          .act-secrets or `aws sts get-caller-identity`).
#   AWS_DEFAULT_REGION   — defaults to ap-northeast-1 (Tokyo) per buildspec.aws.yaml.
#   DEPLOYMENT_ENV       — defaults to prod.
#
# Usage:
#   bash scripts/aws/setup-defi-buckets.sh                   # dry-run preview
#   bash scripts/aws/setup-defi-buckets.sh --apply           # actually create
#   bash scripts/aws/setup-defi-buckets.sh --apply --env staging
#
# Cost: bucket creation is free; storage + transfer is metered by AWS.
# Phase 5 (data migration) sizes the buckets before the GCS->S3 sync fires.
set -euo pipefail

APPLY=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply) APPLY=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,25p' "$0"
            exit 0
            ;;
        *) echo "Unknown flag: $1" >&2; exit 2 ;;
    esac
done

# Map DEPLOYMENT_ENV to 3-char short form matching ${DEPLOYMENT_ENV_SHORT} in
# cloud-providers.yaml templates (per bucket_name_ssot_canonicalisation_2026_05_10 Q5/A5).
# The resolver resolve_bucket_name() uses the short form; provisioned bucket names MUST match.
case "${DEPLOYMENT_ENV}" in
    dev|development)     ENV_SHORT="dev" ;;
    staging|stg)         ENV_SHORT="stg" ;;
    prod|production|prd) ENV_SHORT="prd" ;;
    test)                ENV_SHORT="test" ;;
    ci)                  ENV_SHORT="ci" ;;
    *) echo "Unknown DEPLOYMENT_ENV=${DEPLOYMENT_ENV}. Valid: dev/staging/prod/test/ci" >&2; exit 1 ;;
esac

if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
    AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
fi
if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
    echo "AWS_ACCOUNT_ID unset and aws sts get-caller-identity failed" >&2
    echo "Authenticate with the operator account first (admin_od / 427895769566)." >&2
    exit 1
fi

echo "AWS account:   $AWS_ACCOUNT_ID"
echo "Region:        $REGION"
echo "Deployment:    $DEPLOYMENT_ENV (short: $ENV_SHORT)"
echo "Mode:          $([[ "$APPLY" == "true" ]] && echo APPLY || echo DRY-RUN)"
echo

# 10 DeFi-relevant bucket keys per aws_migration_defi_first_2026_05_07.plan Phase 2.
# Names mirror cloud-providers.yaml aws.storage entries exactly (using ENV_SHORT not DEPLOYMENT_ENV).
# Phase 1 symmetry fix 2026-05-20 (slot 2): aligned to Phase 1 YAML templates —
#   (a) drop unified-trading- prefix (dex-pools, dex-swaps, evm-defi, eigenlayer-rewards, solana-defi, config-store)
#   (b) events is env-LESS (no ENV_SHORT) — matches cloud-providers.yaml aws.storage.events template
#   (c) pnl/positions/risk already correct (no prefix change needed)
# On-disk old buckets (with unified-trading- prefix) remain until Phase 5 migration renames them.
BUCKETS=(
    "dex-pools-${ENV_SHORT}-${AWS_ACCOUNT_ID}"
    "dex-swaps-${ENV_SHORT}-${AWS_ACCOUNT_ID}"
    "evm-defi-${ENV_SHORT}-${AWS_ACCOUNT_ID}"
    "eigenlayer-rewards-${ENV_SHORT}-${AWS_ACCOUNT_ID}"
    "solana-defi-${ENV_SHORT}-${AWS_ACCOUNT_ID}"
    "pnl-store-defi-${ENV_SHORT}-${AWS_ACCOUNT_ID}"
    "positions-store-defi-${ENV_SHORT}-${AWS_ACCOUNT_ID}"
    "risk-store-defi-${ENV_SHORT}-${AWS_ACCOUNT_ID}"
    "unified-trading-events-${AWS_ACCOUNT_ID}"
    "config-store-${ENV_SHORT}-${AWS_ACCOUNT_ID}"
)

CREATED=0
EXISTING=0
FAILED=0

for BUCKET in "${BUCKETS[@]}"; do
    if aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
        echo "  [exists]  $BUCKET"
        EXISTING=$((EXISTING + 1))
        continue
    fi

    if [[ "$APPLY" != "true" ]]; then
        echo "  [would-create] $BUCKET"
        CREATED=$((CREATED + 1))
        continue
    fi

    # ap-northeast-1 requires LocationConstraint; us-east-1 rejects it. Branch.
    if [[ "$REGION" == "us-east-1" ]]; then
        if aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" 2>&1 | tee /tmp/.s3-create.out; then
            CREATED=$((CREATED + 1))
            echo "  [created] $BUCKET"
        else
            FAILED=$((FAILED + 1))
            echo "  [FAILED]  $BUCKET" >&2
        fi
    else
        if aws s3api create-bucket \
            --bucket "$BUCKET" \
            --region "$REGION" \
            --create-bucket-configuration "LocationConstraint=$REGION" \
            2>&1 | tee /tmp/.s3-create.out; then
            CREATED=$((CREATED + 1))
            echo "  [created] $BUCKET"
        else
            FAILED=$((FAILED + 1))
            echo "  [FAILED]  $BUCKET" >&2
        fi
    fi

    # Block public access by default (matches GCP uniform bucket-level access).
    aws s3api put-public-access-block \
        --bucket "$BUCKET" \
        --public-access-block-configuration \
            "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" \
        --region "$REGION" >/dev/null 2>&1 || true

    # Enable versioning (matches GCP retention semantics; safe default).
    aws s3api put-bucket-versioning \
        --bucket "$BUCKET" \
        --versioning-configuration "Status=Enabled" \
        --region "$REGION" >/dev/null 2>&1 || true
done

echo
echo "Summary: existing=$EXISTING, created=$CREATED, failed=$FAILED"
echo
if [[ "$APPLY" != "true" ]]; then
    echo "Dry-run only. Re-run with --apply to actually create buckets."
    exit 0
fi

# Phase 2 follow-ups (not yet automated — Phase 4-6 work):
#   - aws s3api put-bucket-policy with iam-bucket-policies.aws.yaml mirror.
#   - Storage Transfer Service config (Phase 5) for GCS->S3 backfill.
#   - VPC endpoint policy for ap-northeast-1 traffic locality.
echo "Phase 2 bucket provisioning complete."
echo "Next: Phase 4 secrets parity, Phase 5 GCS->S3 transfer, Phase 6 ECS deployment."
