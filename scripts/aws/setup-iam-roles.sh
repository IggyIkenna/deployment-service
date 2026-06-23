#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Create per-service IAM roles for the unified-trading system on AWS.
# Mirrors the GCP SA matrix (per-service-per-env service accounts) as IAM roles.
#
# Plan: unified-trading-pm/plans/active/aws_migration_defi_first_2026_05_07.md Phase 1.B.
# Related: iam-bucket-policies.aws.yaml (bucket-level policies applied separately via
#           scripts/aws/apply-bucket-policies.sh).
#
# Role naming: uts-{service}-{env}  (e.g. uts-features-service-prod)
# Tiers: prod | staging | dev
#
# Trust policy: ecs-tasks.amazonaws.com (Fargate) + ec2.amazonaws.com (VM / EC2).
# Permissions:  S3 full access on unified-trading-* buckets; Secrets Manager read-only.
# Enforcement:  prod_write_protection bucket policies (applied by apply-bucket-policies.sh)
#               deny dev/staging role writes to prod-tier buckets — same model as GCP
#               iam-bucket-policies.yaml prod_write_protection rule.
#
# Idempotent: each role is created only if it does not already exist.
#
# Env:
#   AWS_ACCOUNT_ID      — defaults to sts get-caller-identity (427895769566).
#   AWS_DEFAULT_REGION  — defaults to ap-northeast-1.
#
# Usage:
#   bash scripts/aws/setup-iam-roles.sh                     # dry-run (no changes)
#   bash scripts/aws/setup-iam-roles.sh --apply             # create missing roles
#   bash scripts/aws/setup-iam-roles.sh --apply --tier prod # prod tier only
set -euo pipefail

APPLY=false
TIER_FILTER=""
REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply) APPLY=true; shift ;;
        --tier) TIER_FILTER="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,32p' "$0"
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
echo "Tier filter:  ${TIER_FILTER:-all}"
echo "Mode:         $([[ "$APPLY" == "true" ]] && echo APPLY || echo DRY-RUN)"
echo ""

# Services that need IAM roles for DeFi cutover.
# Maps to ECR repos (Phase 3) + ECS Fargate deployment targets (Phase 6).
SERVICES=(
    "features-service"
    "strategy-service"
    "execution-service"
    "risk-and-exposure-service"
    "position-balance-monitor-service"
    "alerting-service"
    "deployment-api"
    "deployment-service"
    "instruments-service"
    "market-tick-data-service"
)

TIERS=("prod" "staging" "dev")

# Trust policy document allowing ECS tasks and EC2 instances to assume the role.
# ECS Fargate uses ecs-tasks.amazonaws.com; EC2 VMs use ec2.amazonaws.com.
TRUST_POLICY=$(cat <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "ecs-tasks.amazonaws.com",
          "ec2.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
)

# Inline permission policy: S3 access on unified-trading-* buckets + SM read.
# Bucket-level prod_write_protection policies (apply-bucket-policies.sh) enforce
# write isolation; roles themselves get s3:* on the namespace to avoid over-provisioning
# per-service bucket lists (GCP SA matrix uses same broad-then-restrict model).
build_inline_policy() {
    local tier="$1"
    cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3UnifiedTradingAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:GetObjectVersion",
        "s3:ListBucketVersions"
      ],
      "Resource": [
        "arn:aws:s3:::unified-trading-*",
        "arn:aws:s3:::unified-trading-*/*"
      ]
    },
    {
      "Sid": "SecretsManagerReadOnly",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ],
      "Resource": "arn:aws:secretsmanager:${REGION}:${AWS_ACCOUNT_ID}:secret:unified-trading/*"
    },
    {
      "Sid": "ECRReadAccess",
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage"
      ],
      "Resource": "*"
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams"
      ],
      "Resource": "arn:aws:logs:${REGION}:${AWS_ACCOUNT_ID}:log-group:/unified-trading/*"
    }
  ]
}
EOF
}

CREATED=0
EXISTING=0
FAILED=0

for SERVICE in "${SERVICES[@]}"; do
    for TIER in "${TIERS[@]}"; do
        if [[ -n "$TIER_FILTER" && "$TIER" != "$TIER_FILTER" ]]; then
            continue
        fi

        ROLE_NAME="uts-${SERVICE}-${TIER}"

        # Check existence
        if aws iam get-role --role-name "$ROLE_NAME" \
            --query 'Role.RoleName' --output text 2>/dev/null | grep -q "^${ROLE_NAME}$"; then
            echo "  [EXISTS]  $ROLE_NAME"
            ((EXISTING++)) || true
            continue
        fi

        if [[ "$APPLY" == "false" ]]; then
            echo "  [DRY-RUN] Would create: $ROLE_NAME"
            ((CREATED++)) || true
            continue
        fi

        echo -n "  [CREATE]  $ROLE_NAME ... "
        POLICY_DOC="$(build_inline_policy "$TIER")"

        if aws iam create-role \
            --role-name "$ROLE_NAME" \
            --assume-role-policy-document "$TRUST_POLICY" \
            --description "UTS ${SERVICE} ${TIER} service role (DeFi cutover -- mirrors GCP SA uts-${SERVICE}-${TIER}@central-element-323112.iam.gserviceaccount.com)" \
            --tags "Key=ManagedBy,Value=uts-iam-setup" "Key=Service,Value=${SERVICE}" "Key=Tier,Value=${TIER}" \
            --output text --query 'Role.RoleName' 2>/dev/null; then
            echo " ✅ role created"

            # Attach inline policy
            if aws iam put-role-policy \
                --role-name "$ROLE_NAME" \
                --policy-name "uts-${SERVICE}-${TIER}-s3-sm-ecr-logs" \
                --policy-document "$POLICY_DOC" 2>/dev/null; then
                echo "           └─ inline policy attached"
            else
                echo "           └─ ⚠️  inline policy FAILED (role exists, retry manually)" >&2
            fi

            ((CREATED++)) || true
        else
            echo " ❌ FAILED"
            ((FAILED++)) || true
        fi
    done
done

echo ""
echo "Summary: existing=${EXISTING}, created=${CREATED}, failed=${FAILED}"
echo ""

if [[ "$APPLY" == "false" ]]; then
    echo "Dry-run only. Re-run with --apply to create the missing roles."
    echo ""
fi

# Verify: list all uts-* roles
echo "--- uts-* IAM roles in account ---"
aws iam list-roles \
    --query 'Roles[?starts_with(RoleName, `uts-`)].RoleName' \
    --output table 2>&1 || true
