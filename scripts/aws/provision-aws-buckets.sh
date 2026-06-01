#!/usr/bin/env bash
# Provision env-tiered AWS S3 buckets for all three deployment environments.
#
# Scope: bucket_name_ssot_canonicalisation Phase 0c (AWS leg) + aws_migration_defi_first Phase 2.
# Creates prd, stg, and dev variants of the 10 DeFi-relevant buckets so that
# resolve_bucket_name() calls return buckets that physically exist.
#
# The existing "prod"-named buckets (provisioned 2026-05-08 before naming canonicalisation)
# are NOT renamed here — that is Phase 0d / GAP-2.4.C (data migration).
# This script creates the CORRECT "prd"-named counterparts that the resolver now returns.
#
# Pre-reqs:
#   aws CLI authenticated as admin_od (IAM role trading-vm-role / AWS account 427895769566).
#   On macOS: PATH=/opt/homebrew/bin:$PATH to route around broken /usr/local/bin/aws.
#
# Usage:
#   bash scripts/aws/provision-aws-buckets.sh                        # dry-run all 3 envs
#   bash scripts/aws/provision-aws-buckets.sh --apply                # create all 3 envs
#   bash scripts/aws/provision-aws-buckets.sh --apply --envs prd     # prod only
#   bash scripts/aws/provision-aws-buckets.sh --apply --envs prd,stg # prod + staging
#
# Cost: bucket creation is free; storage is metered by AWS. prd has data migrated in
# GAP-2.4.C; stg/dev will be seeded with a truncated window per bucket_name_ssot Phase 0c.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APPLY=false
ENVS="prd,stg,dev"  # comma-separated list of short-form envs to provision
REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply) APPLY=true; shift ;;
        --envs) ENVS="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,32p' "$0"
            exit 0
            ;;
        *) echo "Unknown flag: $1" >&2; exit 2 ;;
    esac
done

# Map prd→prod, stg→staging, dev→development for the inner script's --env flag
# (setup-defi-buckets.sh takes long-form DEPLOYMENT_ENV and converts internally)
map_env_long() {
    case "$1" in
        prd) echo "prod" ;;
        stg) echo "staging" ;;
        dev) echo "development" ;;
        *) echo "$1" ;;   # pass-through for test/ci
    esac
}

if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
    AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
fi
if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
    echo "AWS_ACCOUNT_ID unset and aws sts get-caller-identity failed" >&2
    echo "Authenticate with the operator account first (admin_od / 427895769566)." >&2
    exit 1
fi

echo "=== provision-aws-buckets.sh ==="
echo "AWS account:  $AWS_ACCOUNT_ID"
echo "Region:       $REGION"
echo "Environments: $ENVS"
echo "Mode:         $([[ "$APPLY" == "true" ]] && echo APPLY || echo DRY-RUN)"
echo

IFS=',' read -ra ENV_LIST <<< "$ENVS"
TOTAL_CREATED=0
TOTAL_EXISTING=0
TOTAL_FAILED=0

for SHORT_ENV in "${ENV_LIST[@]}"; do
    LONG_ENV="$(map_env_long "$SHORT_ENV")"
    echo "--- Env: $SHORT_ENV (--env $LONG_ENV) ---"

    APPLY_FLAG=""
    [[ "$APPLY" == "true" ]] && APPLY_FLAG="--apply"

    # Delegate to setup-defi-buckets.sh which handles idempotency + public-access block + versioning
    output="$(AWS_ACCOUNT_ID="$AWS_ACCOUNT_ID" \
        bash "${SCRIPT_DIR}/setup-defi-buckets.sh" \
        --env "$LONG_ENV" \
        --region "$REGION" \
        ${APPLY_FLAG} 2>&1)"
    echo "$output"

    TOTAL_CREATED=$(( TOTAL_CREATED + $(echo "$output" | grep -c '\[created\]' || true) ))
    TOTAL_EXISTING=$(( TOTAL_EXISTING + $(echo "$output" | grep -c '\[exists\]' || true) ))
    TOTAL_FAILED=$(( TOTAL_FAILED + $(echo "$output" | grep -c '\[FAILED\]' || true) ))
    echo
done

echo "=== Grand total: existing=$TOTAL_EXISTING, created=$TOTAL_CREATED, failed=$TOTAL_FAILED ==="
if [[ "$APPLY" != "true" ]]; then
    echo "Dry-run only. Re-run with --apply to actually create buckets."
    exit 0
fi

if [[ "$TOTAL_FAILED" -gt 0 ]]; then
    echo "ERROR: $TOTAL_FAILED bucket(s) failed — check output above." >&2
    exit 1
fi

echo "Phase 0c AWS bucket provisioning complete."
echo "Next: GAP-2.4.C — migrate data from prod-named buckets into prd-named buckets."
echo "  Reference: bucket_name_ssot_canonicalisation_2026_05_10.md Phase 0d."
