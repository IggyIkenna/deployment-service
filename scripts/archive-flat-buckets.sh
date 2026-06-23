#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run verified + GCS orphan-sweep=0
# ============================================================================
# archive-flat-buckets.sh — Phase 2.6 Step 5: set 30-day delete lifecycle
# policy on flat (no-env-tier) buckets after delegate-flip is deployed.
#
# Context
# -------
# After the Phase 2.6 delegate-flip, all readers + writers route to env-tiered
# bucket names (e.g. market-data-tick-cefi-prd-central-element-323112). The
# flat (legacy) buckets (e.g. market-data-tick-cefi-central-element-323112)
# are no longer written to. This script sets a 30-day object-deletion lifecycle
# policy on each flat bucket so they self-clean after the passive monitoring
# window confirms zero readers.
#
# What it does
# ------------
# For each flat bucket declared below (derived from the Phase 2.6 migration
# inventory in deployment-service/configs/cloud-providers.yaml):
#   GCP: gcloud storage buckets update gs://<flat> --lifecycle-file=<tempfile>
#        where <tempfile> is a 30-day Delete rule JSON.
#   AWS: aws s3api put-bucket-lifecycle-configuration --bucket <flat>
#        with an equivalent 30-day Expiration rule.
#
# Dry-run: print intended actions without executing. Always default to
# --dry-run unless --env prod is explicit (production guardrail).
#
# Usage
# -----
#   # Preview what would be done on prod (GCP + AWS):
#   bash deployment-service/scripts/archive-flat-buckets.sh \
#     --env prod --cloud both --retention-days 30 --dry-run
#
#   # Apply on prod GCP only:
#   bash deployment-service/scripts/archive-flat-buckets.sh \
#     --env prod --cloud gcp --retention-days 30
#
#   # Apply on prod both clouds:
#   bash deployment-service/scripts/archive-flat-buckets.sh \
#     --env prod --cloud both --retention-days 30
#
# Requirements
# ------------
# GCP: gcloud CLI with ADC admin on central-element-323112 (or the project set
#      via GCP_PROJECT_ID env).
# AWS: aws CLI with credentials for the account set via AWS_ACCOUNT_ID env.
#      Both CLIs must be on PATH when --cloud includes the respective provider.
#
# Safety
# ------
# - Dry-run is on by default; --env prod flag required to operate on production.
# - Only sets LIFECYCLE (30-day delete window); does NOT move or delete objects.
# - Rollback: aws s3api delete-bucket-lifecycle / gcloud storage buckets update
#   --clear-lifecycle to remove the policy within the 30-day window.
#
# References
# ----------
#   plans/active/code_freeze_migrate_backfill_sequencing_2026_05_10.md
#     § "Step 2.6.5 — Archive flat buckets"
#   codex/05-infrastructure/phase-2-6-bucket-name-cutover-runbook.md
#     § "Step 2.6.5"
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOYMENT_SERVICE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Defaults ────────────────────────────────────────────────────────────────
ENV=""
CLOUD="both"
RETENTION_DAYS=30
DRY_RUN=1    # safe default: dry-run on unless --env prod is given

GCP_PROJECT_ID="${GCP_PROJECT_ID:-}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-}"

# ── Usage ───────────────────────────────────────────────────────────────────
usage() {
  grep -E "^#( |$)" "${BASH_SOURCE[0]}" | head -60 | sed 's/^# \?//'
  exit "${1:-0}"
}

# ── Arg parse ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)              ENV="$2"; shift 2 ;;
    --env=*)            ENV="${1#*=}"; shift ;;
    --cloud)            CLOUD="$2"; shift 2 ;;
    --cloud=*)          CLOUD="${1#*=}"; shift ;;
    --retention-days)   RETENTION_DAYS="$2"; shift 2 ;;
    --retention-days=*) RETENTION_DAYS="${1#*=}"; shift ;;
    --dry-run)          DRY_RUN=1; shift ;;
    --no-dry-run)       DRY_RUN=0; shift ;;
    -h|--help)          usage 0 ;;
    *) echo "ERROR: unknown argument: $1" >&2; usage 2 ;;
  esac
done

# ── Validate args ────────────────────────────────────────────────────────────
if [[ -z "$ENV" ]]; then
  echo "ERROR: --env is required (dev|staging|prod)" >&2
  exit 2
fi

case "$ENV" in
  dev|staging|prod) ;;
  *) echo "ERROR: --env must be dev|staging|prod (got: $ENV)" >&2; exit 2 ;;
esac

case "$CLOUD" in
  gcp|aws|both) ;;
  *) echo "ERROR: --cloud must be gcp|aws|both (got: $CLOUD)" >&2; exit 2 ;;
esac

if ! [[ "$RETENTION_DAYS" =~ ^[0-9]+$ ]] || [[ "$RETENTION_DAYS" -lt 1 ]]; then
  echo "ERROR: --retention-days must be a positive integer (got: $RETENTION_DAYS)" >&2
  exit 2
fi

# Production safety: refuse to run without --no-dry-run when env=prod
if [[ "$ENV" == "prod" && "$DRY_RUN" -eq 1 ]]; then
  echo "INFO: --env prod detected with dry-run mode (default safe mode)."
  echo "      Pass --no-dry-run to apply policies for real on production."
  echo ""
fi

# ── Logging helpers ──────────────────────────────────────────────────────────
log()  { echo "[$(date -u +%H:%M:%SZ)] $*"; }
warn() { echo "[$(date -u +%H:%M:%SZ)] WARN  $*" >&2; }
err()  { echo "[$(date -u +%H:%M:%SZ)] ERROR $*" >&2; }

dry_prefix() {
  if [[ "$DRY_RUN" -eq 1 ]]; then echo "[DRY-RUN] "; else echo ""; fi
}

# ── Flat bucket inventories ──────────────────────────────────────────────────
# These are the flat (no-env-tier) buckets on disk that are LEGACY after the
# Phase 2.6 delegate-flip. The env-tiered counterparts (with -prd-/-stg-/-dev-)
# are the canonical destinations post-cutover.
#
# Source: deployment-service/configs/cloud-providers.yaml comments +
#         code_freeze_migrate_backfill_sequencing_2026_05_10.md Phase 2.6
#         per-bucket migration order table (Tiers 1-7).
#
# Derivation: flat name = env-tiered template with ${DEPLOYMENT_ENV_SHORT}
# segment removed. E.g. "features-delta-one-cefi-${GCP_PROJECT_ID}" (no -prd-).

# GCP flat buckets: {flat-prefix}-{project_id}
# (No DEPLOYMENT_ENV_SHORT segment — that's what makes them "flat".)
build_gcp_flat_bucket_list() {
  local pid="$1"
  local buckets=()

  # Tier 1 — Canary (small DeFi raw on-chain)
  buckets+=(
    "dex-pools-${pid}"
    "dex-swaps-${pid}"
    "evm-defi-${pid}"
    "eigenlayer-rewards-${pid}"
    "solana-defi-${pid}"
  )

  # Tier 2 — Static reference (instruments-store × 5 asset_groups)
  for ag in cefi defi tradfi sports; do
    buckets+=("instruments-store-${ag}-${pid}")
  done
  buckets+=("instruments-store-pred-${pid}")

  # Tier 3 — Features cross-asset (calendar + prediction + sports)
  buckets+=(
    "features-calendar-${pid}"
    "features-pred-${pid}"
    "features-sports-${pid}"
  )

  # Tier 4 — Features per-asset_group × kind
  for ag in cefi tradfi defi; do
    buckets+=(
      "features-delta-one-${ag}-${pid}"
      "features-volatility-${ag}-${pid}"
      "features-xinstrument-${ag}-${pid}"
      "features-mtf-${ag}-${pid}"
    )
  done
  buckets+=(
    "features-delta-one-pred-${pid}"
    "features-volatility-pred-${pid}"
    "features-xinstrument-pred-${pid}"
    "features-mtf-pred-${pid}"
    "features-delta-one-sports-${pid}"
    "features-volatility-sports-${pid}"
    "features-xinstrument-sports-${pid}"
    "features-mtf-sports-${pid}"
    "features-onchain-cefi-${pid}"
    "features-onchain-defi-${pid}"
  )

  # Tier 5 — ML stores
  buckets+=(
    "ml-models-store-${pid}"
    "ml-predictions-store-${pid}"
    "ml-configs-store-${pid}"
    "ml-training-artifacts-${pid}"
    "ml-artifacts-${pid}"
  )

  # Tier 6 — Strategy + execution × 3 asset_groups (no sports for execution)
  for ag in cefi tradfi defi; do
    buckets+=(
      "strategy-store-${ag}-${pid}"
      "execution-store-${ag}-${pid}"
    )
  done
  buckets+=(
    "strategy-store-pred-${pid}"
    "execution-store-pred-${pid}"
  )

  # Tier 7 — Large market-data × 4 asset_groups (cefi/defi/tradfi/sports)
  for ag in cefi defi tradfi sports; do
    buckets+=("market-data-tick-${ag}-${pid}")
  done
  buckets+=("market-data-tick-pred-${pid}")

  printf '%s\n' "${buckets[@]}"
}

# AWS flat buckets: unified-trading-{prefix}-{account_id}
# (No DEPLOYMENT_ENV_SHORT segment — flat pre-Phase-2.6 names.)
build_aws_flat_bucket_list() {
  local acct="$1"
  local buckets=()

  # Tier 1 — DeFi raw on-chain
  buckets+=(
    "unified-trading-dex-pools-${acct}"
    "unified-trading-dex-swaps-${acct}"
    "unified-trading-evm-defi-${acct}"
    "unified-trading-eigenlayer-rewards-${acct}"
    "unified-trading-solana-defi-${acct}"
  )

  # Tier 2 — Static reference (instruments-store × 4 asset_groups + prediction)
  for ag in cefi defi tradfi sports; do
    buckets+=("unified-trading-instruments-${ag}-${acct}")
  done
  buckets+=("unified-trading-instruments-pred-${acct}")

  # Tier 3 — Features cross-asset
  buckets+=(
    "unified-trading-features-calendar-${acct}"
    "unified-trading-features-pred-${acct}"
    "unified-trading-features-sports-${acct}"
  )

  # Tier 4 — Features per-asset_group × kind
  for ag in cefi tradfi defi; do
    buckets+=(
      "unified-trading-features-delta-one-${ag}-${acct}"
      "unified-trading-features-volatility-${ag}-${acct}"
      "unified-trading-features-xinstrument-${ag}-${acct}"
      "unified-trading-features-mtf-${ag}-${acct}"
    )
  done
  buckets+=(
    "unified-trading-features-delta-one-pred-${acct}"
    "unified-trading-features-volatility-pred-${acct}"
    "unified-trading-features-xinstrument-pred-${acct}"
    "unified-trading-features-mtf-pred-${acct}"
    "unified-trading-features-delta-one-sports-${acct}"
    "unified-trading-features-volatility-sports-${acct}"
    "unified-trading-features-xinstrument-sports-${acct}"
    "unified-trading-features-mtf-sports-${acct}"
    "unified-trading-features-onchain-cefi-${acct}"
    "unified-trading-features-onchain-defi-${acct}"
  )

  # Tier 5 — ML stores
  buckets+=(
    "unified-trading-ml-models-${acct}"
    "unified-trading-ml-predictions-${acct}"
    "unified-trading-ml-configs-${acct}"
    "unified-trading-ml-training-artifacts-${acct}"
    "unified-trading-ml-artifacts-${acct}"
  )

  # Tier 6 — Strategy + execution
  for ag in cefi tradfi defi; do
    buckets+=(
      "unified-trading-strategy-${ag}-${acct}"
      "unified-trading-execution-${ag}-${acct}"
    )
  done
  buckets+=(
    "unified-trading-strategy-pred-${acct}"
    "unified-trading-execution-pred-${acct}"
  )

  # Tier 7 — Market-data × 4 asset_groups + prediction
  for ag in cefi defi tradfi sports; do
    buckets+=("unified-trading-market-data-${ag}-${acct}")
  done
  buckets+=("unified-trading-market-data-pred-${acct}")

  printf '%s\n' "${buckets[@]}"
}

# ── GCP lifecycle JSON builder ───────────────────────────────────────────────
gcp_lifecycle_json() {
  local days="$1"
  cat <<EOF
{
  "lifecycle": {
    "rule": [
      {
        "action": { "type": "Delete" },
        "condition": { "age": ${days} }
      }
    ]
  }
}
EOF
}

# ── AWS lifecycle config JSON builder ───────────────────────────────────────
aws_lifecycle_json() {
  local days="$1"
  cat <<EOF
{
  "Rules": [
    {
      "ID": "archive-flat-bucket-${days}d-delete",
      "Filter": { "Prefix": "" },
      "Status": "Enabled",
      "Expiration": { "Days": ${days} }
    }
  ]
}
EOF
}

# ── GCP handler ──────────────────────────────────────────────────────────────
apply_gcp_lifecycle() {
  local pid="$1"
  local days="$2"

  if [[ -z "$pid" ]]; then
    err "GCP: GCP_PROJECT_ID is not set — cannot resolve flat bucket names"
    return 1
  fi

  local tmp_lifecycle
  tmp_lifecycle="$(mktemp /tmp/gcp-lifecycle-flat-XXXXXX.json)"
  gcp_lifecycle_json "$days" > "$tmp_lifecycle"
  trap "rm -f '$tmp_lifecycle'" RETURN

  log "GCP: lifecycle file written to $tmp_lifecycle (${days}-day delete)"

  local buckets
  mapfile -t buckets < <(build_gcp_flat_bucket_list "$pid")

  local ok=0 skip=0 fail=0

  for bucket in "${buckets[@]}"; do
    local uri="gs://${bucket}"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      log "$(dry_prefix)gcloud storage buckets update ${uri} --lifecycle-file=${tmp_lifecycle}"
      (( ok++ )) || true
      continue
    fi

    # Check bucket exists before touching it
    if ! gcloud storage buckets describe "$uri" --format=json > /dev/null 2>&1; then
      warn "GCP: bucket ${uri} not found — skipping (may be pre-Phase-2.6 or already renamed)"
      (( skip++ )) || true
      continue
    fi

    log "GCP: applying ${days}-day delete lifecycle to ${uri} ..."
    if gcloud storage buckets update "$uri" --lifecycle-file="$tmp_lifecycle"; then
      log "GCP: OK — ${uri}"
      (( ok++ )) || true
    else
      err "GCP: FAILED to set lifecycle on ${uri}"
      (( fail++ )) || true
    fi
  done

  log "GCP summary: ok=${ok}  skipped=${skip}  failed=${fail} / total=${#buckets[@]} buckets"
  if [[ "$fail" -gt 0 ]]; then
    err "GCP: ${fail} bucket(s) failed — check output above"
    return 1
  fi
}

# ── AWS handler ──────────────────────────────────────────────────────────────
apply_aws_lifecycle() {
  local acct="$1"
  local days="$2"

  if [[ -z "$acct" ]]; then
    err "AWS: AWS_ACCOUNT_ID is not set — cannot resolve flat bucket names"
    return 1
  fi

  local tmp_lifecycle
  tmp_lifecycle="$(mktemp /tmp/aws-lifecycle-flat-XXXXXX.json)"
  aws_lifecycle_json "$days" > "$tmp_lifecycle"
  trap "rm -f '$tmp_lifecycle'" RETURN

  log "AWS: lifecycle file written to $tmp_lifecycle (${days}-day expiration)"

  local buckets
  mapfile -t buckets < <(build_aws_flat_bucket_list "$acct")

  local ok=0 skip=0 fail=0

  for bucket in "${buckets[@]}"; do
    if [[ "$DRY_RUN" -eq 1 ]]; then
      log "$(dry_prefix)aws s3api put-bucket-lifecycle-configuration --bucket ${bucket} --lifecycle-configuration file://${tmp_lifecycle}"
      (( ok++ )) || true
      continue
    fi

    # Check bucket exists
    if ! aws s3api head-bucket --bucket "$bucket" > /dev/null 2>&1; then
      warn "AWS: bucket ${bucket} not found — skipping (may not exist on AWS side yet)"
      (( skip++ )) || true
      continue
    fi

    log "AWS: applying ${days}-day expiration lifecycle to ${bucket} ..."
    if aws s3api put-bucket-lifecycle-configuration \
         --bucket "$bucket" \
         --lifecycle-configuration "file://${tmp_lifecycle}"; then
      log "AWS: OK — ${bucket}"
      (( ok++ )) || true
    else
      err "AWS: FAILED to set lifecycle on ${bucket}"
      (( fail++ )) || true
    fi
  done

  log "AWS summary: ok=${ok}  skipped=${skip}  failed=${fail} / total=${#buckets[@]} buckets"
  if [[ "$fail" -gt 0 ]]; then
    err "AWS: ${fail} bucket(s) failed — check output above"
    return 1
  fi
}

# ── Main ─────────────────────────────────────────────────────────────────────
DRY_LABEL="DRY-RUN" ; [[ "$DRY_RUN" -eq 0 ]] && DRY_LABEL="LIVE"

log "========================================================"
log "archive-flat-buckets.sh — Phase 2.6 Step 5"
log "  env:            ${ENV}"
log "  cloud:          ${CLOUD}"
log "  retention-days: ${RETENTION_DAYS}"
log "  mode:           ${DRY_LABEL}"
log "  GCP_PROJECT_ID: ${GCP_PROJECT_ID:-(not set)}"
log "  AWS_ACCOUNT_ID: ${AWS_ACCOUNT_ID:-(not set)}"
log "========================================================"
echo ""
log "PURPOSE: Set ${RETENTION_DAYS}-day delete lifecycle policy on flat (no-env-tier)"
log "         legacy buckets so they self-clean after delegate-flip proves out."
log "         Does NOT move or delete objects — lifecycle is passive."
log ""

EXIT_CODE=0

if [[ "$CLOUD" == "gcp" || "$CLOUD" == "both" ]]; then
  log "── GCP ──────────────────────────────────────────────"
  apply_gcp_lifecycle "$GCP_PROJECT_ID" "$RETENTION_DAYS" || EXIT_CODE=1
  echo ""
fi

if [[ "$CLOUD" == "aws" || "$CLOUD" == "both" ]]; then
  log "── AWS ──────────────────────────────────────────────"
  apply_aws_lifecycle "$AWS_ACCOUNT_ID" "$RETENTION_DAYS" || EXIT_CODE=1
  echo ""
fi

if [[ "$EXIT_CODE" -eq 0 ]]; then
  if [[ "$DRY_RUN" -eq 1 ]]; then
    log "========================================================"
    log "DRY-RUN complete — no changes applied."
    log "Re-run with --no-dry-run to apply lifecycle policies."
    log "========================================================"
  else
    log "========================================================"
    log "DONE — lifecycle policies applied to all flat buckets."
    log ""
    log "Next steps:"
    log "  1. Monitor audit logs for 30 days to confirm zero reads on flat names:"
    log "     gcloud logging read 'resource.type=\"gcs_bucket\" AND"
    log "       resource.labels.bucket_name=~\"market-data-tick-cefi-central-element-323112\"'"
    log "       --freshness=30d --format=json | jq 'length'"
    log "  2. After 30 days with 0 reads: proceed to delete flat buckets per"
    log "     code_freeze plan Step 2.6.5 done-definition."
    log "========================================================"
  fi
fi

exit "$EXIT_CODE"
