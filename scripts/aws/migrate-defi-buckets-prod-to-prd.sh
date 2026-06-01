#!/usr/bin/env bash
# Migrate data from wrong-named "prod" DeFi buckets into correctly-named "prd" buckets.
#
# Context: Phase 2 of aws_migration_defi_first (2026-05-08) created 10 DeFi buckets using
# ${DEPLOYMENT_ENV} (long form: "prod") as the env token, but cloud-providers.yaml SSOT
# uses ${DEPLOYMENT_ENV_SHORT} ("prd"). All services with CLOUD_PROVIDER=aws call
# resolve_bucket_name() which returns "prd" names; the old "prod" buckets are invisible.
#
# This script syncs data from the 10 old "prod" buckets into the 10 correct "prd" buckets.
# It does NOT delete the source buckets — operator archives them manually after 30-day
# observation window (per bucket_name_ssot_canonicalisation Phase 0d).
#
# Preconditions:
#   1. GAP-2.4.B has been run: prd-named buckets exist.
#      Run: bash scripts/aws/provision-aws-buckets.sh --apply --envs prd
#   2. aws CLI authenticated as admin_od (account 427895769566).
#      On macOS: PATH=/opt/homebrew/bin:$PATH
#
# Usage:
#   bash scripts/aws/migrate-defi-buckets-prod-to-prd.sh              # dry-run (aws s3 sync --dryrun)
#   bash scripts/aws/migrate-defi-buckets-prod-to-prd.sh --apply      # real sync
#   bash scripts/aws/migrate-defi-buckets-prod-to-prd.sh --verify     # count verification only
#
# After running: verify object counts match, then tag old buckets:
#   aws s3api put-bucket-tagging --bucket <old> \
#     --tagging 'TagSet=[{Key=archived-by,Value=prod-to-prd-migration-2026-05},{Key=safe-to-delete-after,Value=2026-06-19}]'
set -euo pipefail

APPLY=false
VERIFY_ONLY=false
REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply) APPLY=true; shift ;;
        --verify) VERIFY_ONLY=true; shift ;;
        --region) REGION="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,38p' "$0"
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
echo "Mode:         $([[ "$APPLY" == "true" ]] && echo APPLY || ([[ "$VERIFY_ONLY" == "true" ]] && echo VERIFY || echo DRY-RUN))"
echo

# Mapping: OLD_NAME → NEW_NAME
# Old (prod): created 2026-05-08 using ${DEPLOYMENT_ENV}=prod (long form)
# New (prd): correct names from cloud-providers.yaml SSOT using ${DEPLOYMENT_ENV_SHORT}=prd
#
# Note: pnl/positions/risk-store-defi DROP the unified-trading- prefix in new names
# (GAP-2.4.A finding: yaml SSOT matches GCP naming, no unified-trading- prefix there)
declare -A BUCKET_MAP=(
    ["unified-trading-dex-pools-prod-${AWS_ACCOUNT_ID}"]="unified-trading-dex-pools-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-dex-swaps-prod-${AWS_ACCOUNT_ID}"]="unified-trading-dex-swaps-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-evm-defi-prod-${AWS_ACCOUNT_ID}"]="unified-trading-evm-defi-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-eigenlayer-rewards-prod-${AWS_ACCOUNT_ID}"]="unified-trading-eigenlayer-rewards-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-solana-defi-prod-${AWS_ACCOUNT_ID}"]="unified-trading-solana-defi-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-events-prod-${AWS_ACCOUNT_ID}"]="unified-trading-events-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-config-store-prod-${AWS_ACCOUNT_ID}"]="unified-trading-config-store-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-pnl-store-defi-prod-${AWS_ACCOUNT_ID}"]="pnl-store-defi-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-positions-store-defi-prod-${AWS_ACCOUNT_ID}"]="positions-store-defi-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-risk-store-defi-prod-${AWS_ACCOUNT_ID}"]="risk-store-defi-prd-${AWS_ACCOUNT_ID}"
)

SYNCED=0
SKIPPED=0
FAILED=0

# Robust bucket existence check. `aws s3api head-bucket` exits non-zero for both
# "does not exist" (NoSuchBucket) and "you can't see it" (PermanentRedirect /
# AccessDenied / region-mismatch). The original script collapsed all of these
# into "does not exist", false-flagging buckets that actually exist when the
# caller's IAM policy quirks or the bucket's home-region differed from
# $REGION. We now use `aws s3api list-buckets` (account-scoped, region-agnostic)
# to check existence, and surface real head-bucket failures separately.
# `--output text` returns names tab-delimited on a single line; normalize tabs
# to newlines so the per-name framing test below is unambiguous.
ACCOUNT_BUCKETS="$(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null | tr '\t' '\n' || true)"
_bucket_exists() {
    local bucket="$1"
    # newline-delimited list; match the bucket as a whole line with grep -Fx.
    printf '%s\n' "$ACCOUNT_BUCKETS" | grep -qFx -- "$bucket"
}

# Robust object-counter. The previous `aws s3 ls --recursive | wc -l || echo 0`
# pipeline ran under `set -o pipefail`: any transient retry / SlowDown / Throttle
# from S3 mid-pagination caused the whole pipeline to exit non-zero, the
# `|| echo 0` fallback fired AFTER wc -l had already printed the partial count,
# and OLD_COUNT/NEW_COUNT became the two-line string "N\n0" — which broke the
# `[[ ... -eq ... ]]` arithmetic compare in verify-mode and `--summarize` line
# wrap in display-mode (the observed `dst: 20000\n0 objs` for dex-swaps).
#
# Switch to `aws s3 ls --recursive --summarize` and parse the canonical
# "Total Objects: N" trailer line. Falls back to 0 only on full pipeline
# failure, never on partial-success retries. No pipe-to-wc, no pipefail trap.
_count_objects() {
    local bucket="$1"
    local out
    out="$(aws s3 ls --recursive --summarize "s3://${bucket}" --region "$REGION" 2>/dev/null)" || true
    local n
    n="$(printf '%s\n' "$out" | awk '/^Total Objects:/ {print $3; exit}')"
    [[ -z "$n" ]] && n=0
    printf '%s' "$n"
}

for OLD_BUCKET in "${!BUCKET_MAP[@]}"; do
    NEW_BUCKET="${BUCKET_MAP[$OLD_BUCKET]}"

    # Check old bucket exists (account-scoped, region-agnostic — fixes the
    # false-negative observed for evm-defi-prod / risk-store-defi-prod /
    # solana-defi-prod where head-bucket transient-errored and the script
    # mis-reported them as already-migrated).
    if ! _bucket_exists "$OLD_BUCKET"; then
        echo "  [skip-no-source] $OLD_BUCKET (does not exist in account — already migrated or never created)"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Check new bucket exists (precondition: GAP-2.4.B must have run)
    if ! _bucket_exists "$NEW_BUCKET"; then
        echo "  [BLOCKED] $NEW_BUCKET does not exist — run provision-aws-buckets.sh --apply first" >&2
        FAILED=$((FAILED + 1))
        continue
    fi

    OLD_COUNT="$(_count_objects "$OLD_BUCKET")"
    NEW_COUNT="$(_count_objects "$NEW_BUCKET")"
    echo "  $OLD_BUCKET → $NEW_BUCKET  (src: ${OLD_COUNT} objs, dst: ${NEW_COUNT} objs)"

    if [[ "$VERIFY_ONLY" == "true" ]]; then
        if [[ "$OLD_COUNT" -eq "$NEW_COUNT" ]]; then
            echo "    [VERIFIED] counts match ($OLD_COUNT)"
        else
            echo "    [MISMATCH] src=$OLD_COUNT dst=$NEW_COUNT — re-run sync"
        fi
        continue
    fi

    if [[ "$OLD_COUNT" -eq 0 ]]; then
        echo "    [skip-empty] source has 0 objects — nothing to migrate"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Dry-run or real sync
    if [[ "$APPLY" != "true" ]]; then
        aws s3 sync "s3://${OLD_BUCKET}/" "s3://${NEW_BUCKET}/" \
            --region "$REGION" \
            --source-region "$REGION" \
            --dryrun 2>&1 | tail -5
        echo "    [would-sync] $OLD_COUNT objects → $NEW_BUCKET"
        SYNCED=$((SYNCED + 1))
    else
        echo "    Syncing..."
        if aws s3 sync "s3://${OLD_BUCKET}/" "s3://${NEW_BUCKET}/" \
            --region "$REGION" \
            --source-region "$REGION" \
            --only-show-errors 2>&1; then
            # Post-sync verification (use the same robust counter as pre-sync).
            NEW_COUNT_AFTER="$(_count_objects "$NEW_BUCKET")"
            if [[ "$NEW_COUNT_AFTER" -ge "$OLD_COUNT" ]]; then
                echo "    [synced] ${OLD_COUNT} → ${NEW_COUNT_AFTER} objects in $NEW_BUCKET"
                SYNCED=$((SYNCED + 1))
            else
                echo "    [WARN] count mismatch post-sync: src=$OLD_COUNT dst=${NEW_COUNT_AFTER}" >&2
                FAILED=$((FAILED + 1))
            fi
        else
            echo "    [FAILED] sync failed for $OLD_BUCKET → $NEW_BUCKET" >&2
            FAILED=$((FAILED + 1))
        fi
    fi
done

echo
echo "Summary: synced=$SYNCED, skipped=$SKIPPED, failed=$FAILED"
echo

if [[ "$APPLY" == "true" && "$FAILED" -eq 0 ]]; then
    echo "Migration complete. Old prod-named buckets still exist (do NOT delete yet)."
    echo "After 30-day observation window, tag for archival:"
    echo "  for B in \$(echo '${!BUCKET_MAP[@]}'); do"
    echo "    aws s3api put-bucket-tagging --bucket \"\$B\" --region $REGION \\"
    echo "      --tagging 'TagSet=[{Key=archived-by,Value=prod-to-prd-migration-2026-05},{Key=safe-to-delete-after,Value=2026-06-19}]'"
    echo "  done"
elif [[ "$APPLY" != "true" && "$VERIFY_ONLY" != "true" ]]; then
    echo "Dry-run only. Re-run with --apply to sync data."
fi

if [[ "$FAILED" -gt 0 ]]; then
    exit 1
fi
