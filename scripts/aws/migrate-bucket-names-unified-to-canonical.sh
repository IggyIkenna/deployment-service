#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run verified + GCS orphan-sweep=0
# STATUS (re-confirmed 2026-07-25, bucket_estate_consolidation_closeout_2026_07_24.md, asset-group parity
# drift cleanup): Phase 5 has EXECUTED — the ml-*/features-*/execution-* legacy `unified-trading-*` names
# this script renames are confirmed either already deleted or already superseded by later folds (env-tiered
# ml-store / features / execution-store). This file is a HISTORICAL RECORD of an executed rename migration,
# not a live inventory — do not edit the old/new name pairs in place to "fix" them to current canonical
# shapes. Decide delete-vs-keep against the Delete-when condition above (a dedicated GCS orphan-sweep, not
# done in this session) rather than re-running it against the current estate.
# Phase 5 — AWS bucket rename: unified-trading-* → canonical symmetric names
#
# Context: coordinator data_pipeline_master_coordination_2026_05_20.md Phase 5.
#   Phase 1 audit (plans/audit/results/aws_gcp_bucket_symmetry_2026_05_20.csv) identified
#   61 old-name → canonical-name pairs to migrate. New buckets are already provisioned
#   (deployed in Phase 2 via setup-defi-buckets.sh). This script syncs object data from
#   old → new, then optionally tags old buckets for 30-day archival window.
#
# GATE REQUIREMENT (HARD RULE — DO NOT REMOVE):
#   This script MUST NOT run until coordinator Phase 4 (GCS migration) is GREEN.
#   Phase 4 requires Phase 3 (drain) and Phase 2 (code freeze) first.
#   Pass --phase4-green to unlock; default is dry-run mode only.
#
# Usage:
#   bash scripts/aws/migrate-bucket-names-unified-to-canonical.sh                  # dry-run all
#   bash scripts/aws/migrate-bucket-names-unified-to-canonical.sh --verify          # count check only
#   bash scripts/aws/migrate-bucket-names-unified-to-canonical.sh --phase4-green --apply  # LIVE sync
#   bash scripts/aws/migrate-bucket-names-unified-to-canonical.sh --tag-old         # tag after verify
#
# After running with --apply:
#   1. Run --verify to confirm object counts match
#   2. Run --tag-old to mark old buckets safe-to-delete-after 30 days
#   3. Wait 30 days, then run deletion (separate script: delete-old-unified-trading-buckets.sh)
#
# Inventory source: plans/audit/results/aws_gcp_bucket_symmetry_2026_05_20.csv
# Operator contact for Phase 4 gate: slot-1 main (plans/active/_agent_pings.md)
set -euo pipefail

APPLY=false
VERIFY_ONLY=false
TAG_OLD=false
PHASE4_GREEN=false
REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
TAG_DATE=$(date -u +%Y-%m-%d)
DELETE_SAFE_AFTER=$(date -u -d "+30 days" +%Y-%m-%d 2>/dev/null || date -u -v+30d +%Y-%m-%d 2>/dev/null || echo "2026-06-21")

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply) APPLY=true; shift ;;
        --verify) VERIFY_ONLY=true; shift ;;
        --tag-old) TAG_OLD=true; shift ;;
        --phase4-green) PHASE4_GREEN=true; shift ;;
        --region) REGION="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,35p' "$0"
            exit 0
            ;;
        *) echo "Unknown flag: $1" >&2; exit 2 ;;
    esac
done

# Gate: prevent live sync without Phase 4 GREEN confirmation
if [[ "$APPLY" == "true" && "$PHASE4_GREEN" != "true" ]]; then
    echo "ERROR: --apply requires --phase4-green flag." >&2
    echo "" >&2
    echo "  Phase 5 (AWS bucket rename) gates on Phase 4 GREEN (GCS migration)." >&2
    echo "  Phase 4 requires Phase 3 (drain) + Phase 2 (code freeze) first." >&2
    echo "  Confirm Phase 4 is GREEN in _agent_pings.md, then re-run with:" >&2
    echo "    bash $0 --phase4-green --apply" >&2
    exit 1
fi

if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
    AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
fi
if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
    echo "AWS_ACCOUNT_ID unset and aws sts get-caller-identity failed" >&2
    exit 1
fi

echo "AWS account:  $AWS_ACCOUNT_ID"
echo "Region:       $REGION"
echo "Mode:         $([[ "$APPLY" == "true" ]] && echo APPLY || ([[ "$VERIFY_ONLY" == "true" ]] && echo VERIFY || ([[ "$TAG_OLD" == "true" ]] && echo TAG-OLD || echo DRY-RUN)))"
echo "Phase4 gate:  $([[ "$PHASE4_GREEN" == "true" ]] && echo CONFIRMED || echo DRY-RUN-ONLY)"
echo

# Mapping: OLD_NAME → NEW_NAME (from aws_gcp_bucket_symmetry_2026_05_20.csv)
# Format: old_template_resolved(env=prd) → target_template_resolved(env=prd)
# Group B kinds (no env-tier): both old and new have no env tier.
# Pairs where old == new (already_symmetric) are excluded — see CSV rows with issue_type=already_symmetric.
declare -A BUCKET_MAP=(
    # Standard prefix-drop (drop unified-trading-) — env-tiered
    ["unified-trading-config-store-prd-${AWS_ACCOUNT_ID}"]="config-store-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-dex-pools-prd-${AWS_ACCOUNT_ID}"]="dex-pools-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-dex-swaps-prd-${AWS_ACCOUNT_ID}"]="dex-swaps-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-eigenlayer-rewards-prd-${AWS_ACCOUNT_ID}"]="eigenlayer-rewards-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-events-prd-${AWS_ACCOUNT_ID}"]="events-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-evm-defi-prd-${AWS_ACCOUNT_ID}"]="evm-defi-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-calendar-prd-${AWS_ACCOUNT_ID}"]="features-calendar-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-pred-prd-${AWS_ACCOUNT_ID}"]="features-pred-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-sports-prd-${AWS_ACCOUNT_ID}"]="features-sports-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-manual-audit-prd-${AWS_ACCOUNT_ID}"]="manual-audit-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-solana-defi-prd-${AWS_ACCOUNT_ID}"]="solana-defi-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-archetype-state-prd-${AWS_ACCOUNT_ID}"]="archetype-state-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-client-reports-prd-${AWS_ACCOUNT_ID}"]="client-reports-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-client-statements-prd-${AWS_ACCOUNT_ID}"]="client-statements-prd-${AWS_ACCOUNT_ID}"

    # audit-records: drop unified- component (keep trading-audit-records- prefix)
    ["unified-trading-audit-records-prd-${AWS_ACCOUNT_ID}"]="trading-audit-records-prd-${AWS_ACCOUNT_ID}"

    # instruments-store: drop prefix + add -store- infix (env-tiered)
    ["unified-trading-instruments-cefi-prd-${AWS_ACCOUNT_ID}"]="instruments-store-cefi-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-instruments-defi-prd-${AWS_ACCOUNT_ID}"]="instruments-store-defi-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-instruments-tradfi-prd-${AWS_ACCOUNT_ID}"]="instruments-store-tradfi-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-instruments-sports-prd-${AWS_ACCOUNT_ID}"]="instruments-store-sports-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-instruments-pred-prd-${AWS_ACCOUNT_ID}"]="instruments-store-pred-prd-${AWS_ACCOUNT_ID}"

    # market-data: drop prefix + add tick- infix (env-tiered)
    ["unified-trading-market-data-cefi-prd-${AWS_ACCOUNT_ID}"]="market-data-tick-cefi-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-market-data-defi-prd-${AWS_ACCOUNT_ID}"]="market-data-tick-defi-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-market-data-tradfi-prd-${AWS_ACCOUNT_ID}"]="market-data-tick-tradfi-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-market-data-sports-prd-${AWS_ACCOUNT_ID}"]="market-data-tick-sports-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-market-data-pred-prd-${AWS_ACCOUNT_ID}"]="market-data-tick-pred-prd-${AWS_ACCOUNT_ID}"

    # ML kinds: drop prefix + add -store suffix (env-tiered)
    ["unified-trading-ml-configs-prd-${AWS_ACCOUNT_ID}"]="ml-configs-store-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-ml-models-prd-${AWS_ACCOUNT_ID}"]="ml-models-store-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-ml-predictions-prd-${AWS_ACCOUNT_ID}"]="ml-predictions-store-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-execution-pred-prd-${AWS_ACCOUNT_ID}"]="execution-store-pred-prd-${AWS_ACCOUNT_ID}"
    ["unified-trading-strategy-pred-prd-${AWS_ACCOUNT_ID}"]="strategy-store-pred-prd-${AWS_ACCOUNT_ID}"

    # Group B — no env-tier (env-split rolled back); drop prefix + add -store- infix
    ["unified-trading-execution-cefi-${AWS_ACCOUNT_ID}"]="execution-store-cefi-${AWS_ACCOUNT_ID}"
    ["unified-trading-execution-defi-${AWS_ACCOUNT_ID}"]="execution-store-defi-${AWS_ACCOUNT_ID}"
    ["unified-trading-execution-tradfi-${AWS_ACCOUNT_ID}"]="execution-store-tradfi-${AWS_ACCOUNT_ID}"
    ["unified-trading-strategy-cefi-${AWS_ACCOUNT_ID}"]="strategy-store-cefi-${AWS_ACCOUNT_ID}"
    ["unified-trading-strategy-defi-${AWS_ACCOUNT_ID}"]="strategy-store-defi-${AWS_ACCOUNT_ID}"
    ["unified-trading-strategy-tradfi-${AWS_ACCOUNT_ID}"]="strategy-store-tradfi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-delta-one-cefi-${AWS_ACCOUNT_ID}"]="features-delta-one-cefi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-delta-one-defi-${AWS_ACCOUNT_ID}"]="features-delta-one-defi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-delta-one-tradfi-${AWS_ACCOUNT_ID}"]="features-delta-one-tradfi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-delta-one-sports-${AWS_ACCOUNT_ID}"]="features-delta-one-sports-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-delta-one-pred-${AWS_ACCOUNT_ID}"]="features-delta-one-pred-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-volatility-cefi-${AWS_ACCOUNT_ID}"]="features-volatility-cefi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-volatility-defi-${AWS_ACCOUNT_ID}"]="features-volatility-defi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-volatility-tradfi-${AWS_ACCOUNT_ID}"]="features-volatility-tradfi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-volatility-sports-${AWS_ACCOUNT_ID}"]="features-volatility-sports-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-volatility-pred-${AWS_ACCOUNT_ID}"]="features-volatility-pred-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-onchain-cefi-${AWS_ACCOUNT_ID}"]="features-onchain-cefi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-onchain-defi-${AWS_ACCOUNT_ID}"]="features-onchain-defi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-xinstrument-cefi-${AWS_ACCOUNT_ID}"]="features-xinstrument-cefi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-xinstrument-defi-${AWS_ACCOUNT_ID}"]="features-xinstrument-defi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-xinstrument-tradfi-${AWS_ACCOUNT_ID}"]="features-xinstrument-tradfi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-xinstrument-sports-${AWS_ACCOUNT_ID}"]="features-xinstrument-sports-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-xinstrument-pred-${AWS_ACCOUNT_ID}"]="features-xinstrument-pred-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-mtf-cefi-${AWS_ACCOUNT_ID}"]="features-mtf-cefi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-mtf-defi-${AWS_ACCOUNT_ID}"]="features-mtf-defi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-mtf-tradfi-${AWS_ACCOUNT_ID}"]="features-mtf-tradfi-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-mtf-sports-${AWS_ACCOUNT_ID}"]="features-mtf-sports-${AWS_ACCOUNT_ID}"
    ["unified-trading-features-mtf-pred-${AWS_ACCOUNT_ID}"]="features-mtf-pred-${AWS_ACCOUNT_ID}"

    # ML Group B (no env-tier): drop prefix (no -store suffix for artifacts)
    ["unified-trading-ml-artifacts-${AWS_ACCOUNT_ID}"]="ml-artifacts-${AWS_ACCOUNT_ID}"
    ["unified-trading-ml-training-artifacts-${AWS_ACCOUNT_ID}"]="ml-training-artifacts-${AWS_ACCOUNT_ID}"
    ["unified-trading-commodity-signals-batch-${AWS_ACCOUNT_ID}"]="commodity-signals-batch-${AWS_ACCOUNT_ID}"
)

SYNCED=0
SKIPPED=0
FAILED=0
TAGGED=0

ACCOUNT_BUCKETS="$(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null | tr '\t' '\n' || true)"
_bucket_exists() {
    printf '%s\n' "$ACCOUNT_BUCKETS" | grep -qFx -- "$1"
}

_count_objects() {
    local bucket="$1"
    local out
    out="$(aws s3 ls --recursive --summarize "s3://${bucket}" --region "$REGION" 2>/dev/null)" || true
    local n
    n="$(printf '%s\n' "$out" | awk '/^Total Objects:/ {print $3; exit}')"
    [[ -z "$n" ]] && n=0
    printf '%s' "$n"
}

if [[ "$TAG_OLD" == "true" ]]; then
    echo "=== Tagging old buckets for 30-day archival window ==="
    for OLD_BUCKET in "${!BUCKET_MAP[@]}"; do
        if _bucket_exists "$OLD_BUCKET"; then
            aws s3api put-bucket-tagging --bucket "$OLD_BUCKET" --region "$REGION" \
                --tagging "TagSet=[{Key=archived-by,Value=phase5-bucket-rename-${TAG_DATE}},{Key=safe-to-delete-after,Value=${DELETE_SAFE_AFTER}},{Key=migrated-to,Value=${BUCKET_MAP[$OLD_BUCKET]}}]" 2>/dev/null && \
                echo "  [tagged] $OLD_BUCKET → delete-safe-after=$DELETE_SAFE_AFTER" && TAGGED=$((TAGGED+1)) || \
                echo "  [WARN] tagging failed for $OLD_BUCKET" >&2
        fi
    done
    echo
    echo "Tagged: $TAGGED buckets. Inspect tags: aws s3api get-bucket-tagging --bucket <name>"
    exit 0
fi

echo "=== Phase 5 — ${#BUCKET_MAP[@]} rename pairs from Phase 1 inventory ==="
echo

for OLD_BUCKET in $(printf '%s\n' "${!BUCKET_MAP[@]}" | sort); do
    NEW_BUCKET="${BUCKET_MAP[$OLD_BUCKET]}"

    if ! _bucket_exists "$OLD_BUCKET"; then
        echo "  [skip-no-source] $OLD_BUCKET"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    if ! _bucket_exists "$NEW_BUCKET"; then
        echo "  [BLOCKED] target $NEW_BUCKET does not exist — run provision-aws-buckets.sh first" >&2
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
            echo "    [MISMATCH] src=$OLD_COUNT dst=$NEW_COUNT — re-run with --apply"
        fi
        continue
    fi

    if [[ "$OLD_COUNT" -eq 0 ]]; then
        echo "    [skip-empty] source has 0 objects"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    if [[ "$APPLY" != "true" ]]; then
        aws s3 sync "s3://${OLD_BUCKET}/" "s3://${NEW_BUCKET}/" \
            --region "$REGION" \
            --source-region "$REGION" \
            --dryrun 2>&1 | tail -3
        echo "    [would-sync] $OLD_COUNT objects → $NEW_BUCKET"
        SYNCED=$((SYNCED + 1))
    else
        echo "    Syncing..."
        if aws s3 sync "s3://${OLD_BUCKET}/" "s3://${NEW_BUCKET}/" \
            --region "$REGION" \
            --source-region "$REGION" \
            --only-show-errors 2>&1; then
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
echo "Summary: synced/would-sync=$SYNCED, skipped=$SKIPPED, failed=$FAILED"

if [[ "$APPLY" == "true" && "$FAILED" -eq 0 ]]; then
    echo
    echo "Migration complete. Run --verify to confirm counts, then --tag-old for archival tagging."
    echo "Old buckets may be deleted after ${DELETE_SAFE_AFTER} (30-day window)."
    echo "Delete script: bash scripts/aws/delete-old-unified-trading-buckets.sh --after-date ${DELETE_SAFE_AFTER}"
elif [[ "$APPLY" != "true" && "$VERIFY_ONLY" != "true" ]]; then
    echo
    echo "Dry-run only. When Phase 4 is GREEN, re-run:"
    echo "  bash $0 --phase4-green --apply"
fi

if [[ "$FAILED" -gt 0 ]]; then
    exit 1
fi
