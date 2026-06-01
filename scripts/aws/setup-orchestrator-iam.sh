#!/usr/bin/env bash
# setup-orchestrator-iam.sh — idempotent IAM setup for the orchestrator epic VM fleet.
#
# Creates (or verifies existing):
#   IAM role:            uts-orchestrator-epic-role
#   IAM policy:          uts-orchestrator-epic-policy  (secretsmanager + s3 permissions)
#   Instance profile:    uts-orchestrator-epic          (wraps the role; attached to EC2 instances)
#
# Permissions granted:
#   secretsmanager:GetSecretValue  on secrets: GH_PAT, ORCHESTRATOR_ENV_LOCAL
#   s3:GetObject                   on uts-orchestrator-creds-{account}/config/*
#   s3:PutObject                   on uts-orchestrator-events-{account}/orchestrator/*
#
# Usage:
#   bash setup-orchestrator-iam.sh                 # apply (idempotent)
#   bash setup-orchestrator-iam.sh --dry-run       # print what would be created
#   bash setup-orchestrator-iam.sh --check         # verify existing resources, exit 0 if OK
#
# Prereqs:
#   aws CLI v2+, authenticated (aws sso login --profile operator)
#   iam:CreateRole, iam:CreatePolicy, iam:AttachRolePolicy,
#   iam:CreateInstanceProfile, iam:AddRoleToInstanceProfile
#
# Plan: plans/active/aws_epic_vm_fleet_2026_05_22.md Phase 2 + Phase 3
set -euo pipefail

AWS_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
DRY_RUN=false
CHECK_ONLY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)  DRY_RUN=true;    shift ;;
    --check)    CHECK_ONLY=true; shift ;;
    --region)   AWS_REGION="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,35p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "ERROR: unknown arg: $1" >&2; exit 2 ;;
  esac
done

# Resolve account
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-}"
if [[ -z "${AWS_ACCOUNT_ID}" ]]; then
  AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)" || {
    echo "ERROR: Cannot resolve AWS_ACCOUNT_ID. Authenticate: aws sso login --profile operator" >&2
    exit 1
  }
fi

ROLE_NAME="uts-orchestrator-epic-role"
POLICY_NAME="uts-orchestrator-epic-policy"
PROFILE_NAME="uts-orchestrator-epic"
CREDS_BUCKET="uts-orchestrator-creds-${AWS_ACCOUNT_ID}"
EVENTS_BUCKET="uts-orchestrator-events-${AWS_ACCOUNT_ID}"

log() { echo "[iam-setup] $*"; }

log "=== Orchestrator Epic IAM Setup ==="
log "  account:       ${AWS_ACCOUNT_ID}"
log "  region:        ${AWS_REGION}"
log "  role:          ${ROLE_NAME}"
log "  policy:        ${POLICY_NAME}"
log "  profile:       ${PROFILE_NAME}"
log "  creds bucket:  ${CREDS_BUCKET}"
log "  events bucket: ${EVENTS_BUCKET}"
log ""

# ── Trust policy: EC2 instances only ──
TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "ec2.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}'

# ── Permission policy ──
PERMISSION_POLICY="{
  \"Version\": \"2012-10-17\",
  \"Statement\": [
    {
      \"Sid\": \"SecretsReadOnly\",
      \"Effect\": \"Allow\",
      \"Action\": \"secretsmanager:GetSecretValue\",
      \"Resource\": [
        \"arn:aws:secretsmanager:${AWS_REGION}:${AWS_ACCOUNT_ID}:secret:GH_PAT*\",
        \"arn:aws:secretsmanager:${AWS_REGION}:${AWS_ACCOUNT_ID}:secret:ORCHESTRATOR_ENV_LOCAL*\"
      ]
    },
    {
      \"Sid\": \"CredsS3Read\",
      \"Effect\": \"Allow\",
      \"Action\": \"s3:GetObject\",
      \"Resource\": \"arn:aws:s3:::${CREDS_BUCKET}/config/*\"
    },
    {
      \"Sid\": \"CredsS3ListConfig\",
      \"Effect\": \"Allow\",
      \"Action\": \"s3:ListBucket\",
      \"Resource\": \"arn:aws:s3:::${CREDS_BUCKET}\",
      \"Condition\": {\"StringLike\": {\"s3:prefix\": [\"config/*\", \"accounts/*\"]}}
    },
    {
      \"Sid\": \"AccountsS3Read\",
      \"Effect\": \"Allow\",
      \"Action\": \"s3:GetObject\",
      \"Resource\": \"arn:aws:s3:::${CREDS_BUCKET}/accounts/*\"
    },
    {
      \"Sid\": \"EventsS3Write\",
      \"Effect\": \"Allow\",
      \"Action\": [\"s3:PutObject\", \"s3:GetObject\"],
      \"Resource\": \"arn:aws:s3:::${EVENTS_BUCKET}/orchestrator/*\"
    },
    {
      \"Sid\": \"SSMSessionManager\",
      \"Effect\": \"Allow\",
      \"Action\": [
        \"ssm:UpdateInstanceInformation\",
        \"ssmmessages:CreateControlChannel\",
        \"ssmmessages:CreateDataChannel\",
        \"ssmmessages:OpenControlChannel\",
        \"ssmmessages:OpenDataChannel\"
      ],
      \"Resource\": \"*\"
    }
  ]
}"

# ── Check mode ──
_check() {
  local all_ok=true

  log "Checking IAM role ${ROLE_NAME}..."
  if aws iam get-role --role-name "${ROLE_NAME}" &>/dev/null 2>&1; then
    log "  ✓ Role exists"
  else
    log "  ✗ Role MISSING"
    all_ok=false
  fi

  log "Checking IAM policy ${POLICY_NAME}..."
  POLICY_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME}"
  if aws iam get-policy --policy-arn "${POLICY_ARN}" &>/dev/null 2>&1; then
    log "  ✓ Policy exists"
  else
    log "  ✗ Policy MISSING"
    all_ok=false
  fi

  log "Checking instance profile ${PROFILE_NAME}..."
  if aws iam get-instance-profile --instance-profile-name "${PROFILE_NAME}" &>/dev/null 2>&1; then
    log "  ✓ Instance profile exists"
  else
    log "  ✗ Instance profile MISSING"
    all_ok=false
  fi

  if $all_ok; then
    log ""
    log "All IAM resources present."
    exit 0
  else
    log ""
    log "Some resources missing — run without --check to create them."
    exit 1
  fi
}

if $CHECK_ONLY; then
  _check
fi

if $DRY_RUN; then
  log "DRY RUN — would create:"
  log "  aws iam create-role --role-name ${ROLE_NAME}"
  log "  aws iam create-policy --policy-name ${POLICY_NAME}"
  log "  aws iam attach-role-policy --role-name ${ROLE_NAME} --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME}"
  log "  aws iam create-instance-profile --instance-profile-name ${PROFILE_NAME}"
  log "  aws iam add-role-to-instance-profile --instance-profile-name ${PROFILE_NAME} --role-name ${ROLE_NAME}"
  exit 0
fi

# ── Step 1: Create role (idempotent) ──
log "STEP 1: Creating IAM role ${ROLE_NAME}..."
if aws iam get-role --role-name "${ROLE_NAME}" &>/dev/null 2>&1; then
  log "  Role already exists — skipping create"
else
  aws iam create-role \
    --role-name "${ROLE_NAME}" \
    --assume-role-policy-document "${TRUST_POLICY}" \
    --description "Orchestrator epic VM fleet — secretsmanager + S3 read/write" \
    --tags Key=managed-by,Value=setup-orchestrator-iam Key=purpose,Value=orchestrator-epic \
    --output text --query 'Role.RoleName' &>/dev/null
  log "  Role created: ${ROLE_NAME}"
fi

# ── Step 2: Create / update policy ──
log "STEP 2: Creating IAM policy ${POLICY_NAME}..."
POLICY_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME}"
if aws iam get-policy --policy-arn "${POLICY_ARN}" &>/dev/null 2>&1; then
  log "  Policy already exists — creating new version"
  # Delete all non-default versions first (max 5 versions allowed)
  VERSIONS="$(aws iam list-policy-versions --policy-arn "${POLICY_ARN}" \
    --query 'Versions[?!IsDefaultVersion].VersionId' --output text 2>/dev/null || echo '')"
  for ver in ${VERSIONS}; do
    aws iam delete-policy-version --policy-arn "${POLICY_ARN}" --version-id "${ver}" 2>/dev/null || true
  done
  aws iam create-policy-version \
    --policy-arn "${POLICY_ARN}" \
    --policy-document "${PERMISSION_POLICY}" \
    --set-as-default \
    --output text --query 'PolicyVersion.VersionId' &>/dev/null
  log "  Policy updated to new version"
else
  POLICY_ARN="$(aws iam create-policy \
    --policy-name "${POLICY_NAME}" \
    --policy-document "${PERMISSION_POLICY}" \
    --description "Orchestrator epic VM fleet — secretsmanager + S3 read/write" \
    --tags Key=managed-by,Value=setup-orchestrator-iam Key=purpose,Value=orchestrator-epic \
    --query 'Policy.Arn' --output text)"
  log "  Policy created: ${POLICY_ARN}"
fi

# ── Step 3: Attach policy to role ──
log "STEP 3: Attaching policy to role..."
ATTACHED="$(aws iam list-attached-role-policies --role-name "${ROLE_NAME}" \
  --query "AttachedPolicies[?PolicyArn=='${POLICY_ARN}'].PolicyArn" --output text 2>/dev/null || echo '')"
if [[ -n "${ATTACHED}" ]]; then
  log "  Policy already attached — skipping"
else
  aws iam attach-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-arn "${POLICY_ARN}"
  log "  Policy attached"
fi

# ── Step 4: Create instance profile ──
log "STEP 4: Creating instance profile ${PROFILE_NAME}..."
if aws iam get-instance-profile --instance-profile-name "${PROFILE_NAME}" &>/dev/null 2>&1; then
  log "  Instance profile already exists — skipping"
else
  aws iam create-instance-profile \
    --instance-profile-name "${PROFILE_NAME}" \
    --tags Key=managed-by,Value=setup-orchestrator-iam Key=purpose,Value=orchestrator-epic \
    --output text --query 'InstanceProfile.InstanceProfileName' &>/dev/null
  log "  Instance profile created: ${PROFILE_NAME}"
fi

# ── Step 5: Add role to profile ──
log "STEP 5: Adding role to instance profile..."
PROFILE_ROLES="$(aws iam get-instance-profile \
  --instance-profile-name "${PROFILE_NAME}" \
  --query 'InstanceProfile.Roles[].RoleName' --output text 2>/dev/null || echo '')"
if echo "${PROFILE_ROLES}" | grep -qF "${ROLE_NAME}"; then
  log "  Role already in profile — skipping"
else
  aws iam add-role-to-instance-profile \
    --instance-profile-name "${PROFILE_NAME}" \
    --role-name "${ROLE_NAME}"
  log "  Role added to profile"
fi

log ""
log "=== IAM setup complete ==="
log ""
log "  Role ARN:    arn:aws:iam::${AWS_ACCOUNT_ID}:role/${ROLE_NAME}"
log "  Policy ARN:  ${POLICY_ARN}"
log "  Profile:     ${PROFILE_NAME}"
log ""
log "Verify:"
log "  bash setup-orchestrator-iam.sh --check"
log ""
log "Next: create S3 buckets + upload secrets (Phase 2 OPERATOR tasks)"
log "  aws s3 mb s3://${CREDS_BUCKET} --region ${AWS_REGION}"
log "  aws s3 mb s3://${EVENTS_BUCKET} --region ${AWS_REGION}"
log "  aws secretsmanager create-secret --name GH_PAT --region ${AWS_REGION} --secret-string '<value>'"
log "  aws secretsmanager create-secret --name ORCHESTRATOR_ENV_LOCAL --region ${AWS_REGION} --secret-string '<value>'"
log "  aws s3 cp accounts.json s3://${CREDS_BUCKET}/config/accounts.json"
log "  aws s3 cp backlog.yaml  s3://${CREDS_BUCKET}/config/backlog.yaml"
