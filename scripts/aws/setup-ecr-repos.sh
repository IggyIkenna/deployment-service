#!/usr/bin/env bash
# Provision the 8 missing ECR repositories needed for DeFi cutover.
#
# Plan: unified-trading-pm/plans/active/aws_migration_defi_first_2026_05_07.md Phase 3.
# Context: 4 repos already exist (instruments-service, unified-trading-library,
#          unified-trading-system, market-tick-data-service). This script adds the 8
#          that are missing for DeFi live services.
#
# Idempotent: each repo is created only if missing (describe-repositories check before create).
#
# Env required:
#   AWS_ACCOUNT_ID       — defaults to 427895769566 via sts get-caller-identity.
#   AWS_DEFAULT_REGION   — defaults to ap-northeast-1 (Tokyo).
#
# Usage:
#   bash scripts/aws/setup-ecr-repos.sh                   # dry-run preview
#   bash scripts/aws/setup-ecr-repos.sh --apply           # actually create repos
#   bash scripts/aws/setup-ecr-repos.sh --apply --region us-east-1
set -euo pipefail

APPLY=false
REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply) APPLY=true; shift ;;
        --region) REGION="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,22p' "$0"
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
    exit 1
fi

echo "AWS account: $AWS_ACCOUNT_ID  region: $REGION  apply: $APPLY"
echo ""

# The 8 missing ECR repos for DeFi cutover (Phase 3).
# Already exist: instruments-service, unified-trading-library, unified-trading-system,
#               market-tick-data-service
REPOS=(
    "features-service"
    "strategy-service"
    "execution-service"
    "risk-and-exposure-service"
    "position-balance-monitor-service"
    "alerting-service"
    "deployment-api"
    "deployment-service"
)

CREATED=0
EXISTING=0
ERRORS=0

for REPO_NAME in "${REPOS[@]}"; do
    # Check if repo already exists
    if aws ecr describe-repositories \
        --repository-names "$REPO_NAME" \
        --region "$REGION" \
        --output text \
        --query 'repositories[0].repositoryName' 2>/dev/null | grep -q "^${REPO_NAME}$"; then
        echo "  [EXISTS]  $REPO_NAME"
        ((EXISTING++)) || true
        continue
    fi

    if [[ "$APPLY" == "false" ]]; then
        echo "  [DRY-RUN] Would create ECR repo: $REPO_NAME (region=$REGION)"
    else
        echo -n "  [CREATE]  $REPO_NAME ... "
        if aws ecr create-repository \
            --repository-name "$REPO_NAME" \
            --region "$REGION" \
            --image-scanning-configuration scanOnPush=true \
            --encryption-configuration encryptionType=AES256 \
            --output json \
            --query 'repository.repositoryUri' 2>/dev/null | tr -d '"'; then
            echo " ✅"
            ((CREATED++)) || true
        else
            echo " ❌ FAILED"
            ((ERRORS++)) || true
        fi
    fi
done

echo ""
echo "Summary: ${EXISTING} existing, ${CREATED} created, ${ERRORS} errors"
if [[ "$APPLY" == "false" && "${#REPOS[@]}" -gt $EXISTING ]]; then
    echo ""
    echo "Re-run with --apply to create the missing repos."
fi

# Verify final state
echo ""
echo "--- ECR repo state (post-run) ---"
aws ecr describe-repositories \
    --region "$REGION" \
    --query 'repositories[*].repositoryName' \
    --output table 2>&1
