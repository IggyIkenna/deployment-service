#!/usr/bin/env bash
# Replicate GCP Secret Manager secrets to AWS Secrets Manager.
#
# Plan: api_keys_wallets_accounts_readiness_2026_05_10.md Phase 1.E.
# Codex: codex/05-infrastructure/secret-manager-naming.md (naming convention SSOT).
#
# For each secret in GCP Secret Manager (project=central-element-323112), creates an
# equivalent secret in AWS Secrets Manager (region=ap-northeast-1, account=427895769566).
#
# Naming convention (from secret-manager-naming.md):
#   GCP: <class>-<surface>-<role>-<version>  e.g. bybit-trade-api-key-v1
#   AWS: unified-trading/<class>/<surface>/<role>/<version>  (hierarchical path)
#   For simple names (no version suffix): unified-trading/<name>
#
# Exclusions (not replicated):
#   - firebase-sa-json  (GCP Cloud Run only; no AWS consumer)
#   - gcp-sa-key-*      (GCP service account keys; never leave GCP)
#   - github-pat        (GitHub token; not needed on AWS side)
#
# Prerequisites:
#   1. gcloud auth application-default login (or ADC via SA key)
#   2. aws sso login --profile operator  (or AWS_PROFILE + aws configure sso)
#   3. aws CLI v2.x installed
#
# Usage:
#   bash scripts/aws/replicate-secrets-to-aws.sh                    # dry-run
#   bash scripts/aws/replicate-secrets-to-aws.sh --apply            # create/update secrets
#   bash scripts/aws/replicate-secrets-to-aws.sh --apply --filter bybit  # bybit-* only
#   bash scripts/aws/replicate-secrets-to-aws.sh --verify           # check AWS has GCP set
set -euo pipefail

APPLY=false
VERIFY=false
FILTER=""
GCP_PROJECT="central-element-323112"
AWS_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
AWS_PREFIX="unified-trading"

# Secrets excluded from replication (GCP-only secrets)
EXCLUSION_PATTERNS=(
    "firebase-sa-json"
    "gcp-sa-key"
    "github-pat"
    "WORKLOAD_IDENTITY"
)

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply)   APPLY=true;  shift ;;
        --verify)  VERIFY=true; shift ;;
        --filter)  FILTER="$2"; shift 2 ;;
        --project) GCP_PROJECT="$2"; shift 2 ;;
        --region)  AWS_REGION="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,25p' "$0"
            exit 0
            ;;
        *) echo "Unknown flag: $1" >&2; exit 2 ;;
    esac
done

echo "GCP project:   $GCP_PROJECT"
echo "AWS region:    $AWS_REGION"
echo "Filter:        ${FILTER:-none}"
echo "Mode:          $([[ "$APPLY" == "true" ]] && echo APPLY || { [[ "$VERIFY" == "true" ]] && echo VERIFY; } || echo DRY-RUN)"
echo ""

# Verify CLI availability
if ! command -v gcloud &>/dev/null; then
    echo "ERROR: gcloud CLI not found. Install Google Cloud SDK." >&2
    exit 1
fi
if ! command -v aws &>/dev/null; then
    echo "ERROR: aws CLI not found. Install: brew install awscli (or apt install awscli)." >&2
    echo "Then authenticate: aws sso login --profile operator" >&2
    exit 1
fi

# Get AWS account ID
if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
    AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
fi
if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
    echo "ERROR: AWS_ACCOUNT_ID unset and aws sts get-caller-identity failed." >&2
    echo "Authenticate first: aws sso login --profile operator" >&2
    exit 1
fi

echo "AWS account: $AWS_ACCOUNT_ID"
echo ""

# Build exclusion grep pattern
EXCLUSION_GREP=$(printf "|%s" "${EXCLUSION_PATTERNS[@]}")
EXCLUSION_GREP="${EXCLUSION_GREP:1}"  # strip leading |

# List all GCP secrets
echo "Listing GCP Secret Manager secrets in project $GCP_PROJECT..."
GCP_SECRETS=$(gcloud secrets list \
    --project="$GCP_PROJECT" \
    --format="value(name)" \
    --filter="state=ENABLED" 2>/dev/null | \
    grep -Ev "$EXCLUSION_GREP" | \
    { [[ -n "$FILTER" ]] && grep "$FILTER" || cat; } | \
    sort)

if [[ -z "$GCP_SECRETS" ]]; then
    echo "No GCP secrets found (check gcloud auth + project access)."
    exit 0
fi

TOTAL=$(echo "$GCP_SECRETS" | wc -l | tr -d ' ')
echo "Found $TOTAL GCP secrets to replicate (exclusions applied)."
echo ""

CREATED=0
UPDATED=0
EXISTS=0
FAILED=0
MISSING_AWS=()

while IFS= read -r SECRET_NAME; do
    # AWS SM uses hierarchical path
    AWS_SECRET_NAME="${AWS_PREFIX}/${SECRET_NAME}"

    # Dry-run / verify mode: just check existence
    if [[ "$VERIFY" == "true" ]]; then
        if aws secretsmanager describe-secret \
            --secret-id "$AWS_SECRET_NAME" \
            --region "$AWS_REGION" \
            --query 'Name' --output text 2>/dev/null | grep -q "^${AWS_SECRET_NAME}$"; then
            echo "  [OK]      $SECRET_NAME → $AWS_SECRET_NAME"
            ((EXISTS++)) || true
        else
            echo "  [MISSING] $SECRET_NAME → $AWS_SECRET_NAME"
            MISSING_AWS+=("$SECRET_NAME")
        fi
        continue
    fi

    if [[ "$APPLY" == "false" ]]; then
        echo "  [DRY-RUN] Would replicate: $SECRET_NAME → $AWS_SECRET_NAME"
        ((CREATED++)) || true
        continue
    fi

    # Fetch latest GCP secret value
    SECRET_VALUE=$(gcloud secrets versions access latest \
        --secret="$SECRET_NAME" \
        --project="$GCP_PROJECT" 2>/dev/null || true)

    if [[ -z "$SECRET_VALUE" ]]; then
        echo "  [SKIP]    $SECRET_NAME — no latest version or no access" >&2
        ((FAILED++)) || true
        continue
    fi

    # Check if AWS secret already exists
    if aws secretsmanager describe-secret \
        --secret-id "$AWS_SECRET_NAME" \
        --region "$AWS_REGION" \
        --query 'Name' --output text 2>/dev/null | grep -q "^${AWS_SECRET_NAME}$"; then
        # Update existing secret
        echo -n "  [UPDATE]  $SECRET_NAME → $AWS_SECRET_NAME ... "
        if aws secretsmanager put-secret-value \
            --secret-id "$AWS_SECRET_NAME" \
            --secret-string "$SECRET_VALUE" \
            --region "$AWS_REGION" \
            --output text --query 'VersionId' 2>/dev/null; then
            echo " ✅ updated"
            ((UPDATED++)) || true
        else
            echo " ❌ FAILED" >&2
            ((FAILED++)) || true
        fi
    else
        # Create new secret
        echo -n "  [CREATE]  $SECRET_NAME → $AWS_SECRET_NAME ... "
        if aws secretsmanager create-secret \
            --name "$AWS_SECRET_NAME" \
            --secret-string "$SECRET_VALUE" \
            --region "$AWS_REGION" \
            --description "Replicated from GCP Secret Manager: $SECRET_NAME (project: $GCP_PROJECT)" \
            --tags "Key=ManagedBy,Value=uts-sm-replication" \
                   "Key=GCPSource,Value=${GCP_PROJECT}/${SECRET_NAME}" \
            --output text --query 'Name' 2>/dev/null; then
            echo " ✅ created"
            ((CREATED++)) || true
        else
            echo " ❌ FAILED" >&2
            ((FAILED++)) || true
        fi
    fi
done <<< "$GCP_SECRETS"

echo ""

if [[ "$VERIFY" == "true" ]]; then
    echo "Verify summary: present=${EXISTS}, missing=${#MISSING_AWS[@]}"
    if [[ ${#MISSING_AWS[@]} -gt 0 ]]; then
        echo ""
        echo "Missing in AWS SM (run --apply to create):"
        printf "  %s\n" "${MISSING_AWS[@]}"
        exit 1
    fi
    echo "✅ All GCP secrets present in AWS SM"
    exit 0
fi

echo "Summary: created=${CREATED}, updated=${UPDATED}, failed=${FAILED}"
echo ""

if [[ "$APPLY" == "false" ]]; then
    echo "Dry-run only. Re-run with --apply to create/update secrets."
fi

if [[ "$FAILED" -gt 0 ]]; then
    exit 1
fi

# Per Runbook Execution-Owner SSOT HARD RULE
# owner: deployment-service maintainer
# cadence: per-credential-rotation + monthly full sync
# verifier: bash scripts/aws/replicate-secrets-to-aws.sh --verify (exit 0 = all present)
# last_executed: NEVER (pending aws CLI install + aws sso login)
