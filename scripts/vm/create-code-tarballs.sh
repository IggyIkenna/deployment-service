#!/usr/bin/env bash
# Create and upload code tarballs for tarball-based VM deployments.
#
# This packages the core repos (UAC, UTL, MTDS) plus category-specific service
# repos into tarballs and uploads them to GCS. VMs fetch these during setup
# via setup-data-pipeline-vm.sh.
#
# Usage:
#   bash scripts/vm/create-code-tarballs.sh                    # default (core only)
#   bash scripts/vm/create-code-tarballs.sh --bucket my-bucket # custom bucket
#   bash scripts/vm/create-code-tarballs.sh --dry-run          # show what would be created
#   bash scripts/vm/create-code-tarballs.sh --category CEFI    # core + CEFI services
#   bash scripts/vm/create-code-tarballs.sh --category DEFI    # core + DEFI services
#   bash scripts/vm/create-code-tarballs.sh --all              # core + ALL service repos
#   bash scripts/vm/create-code-tarballs.sh --ml-training      # core + ml-training
#                                                                pipeline (CORE +
#                                                                ml-training-service +
#                                                                features-* consumers)
#
# Also supports additional repos for services beyond the category set:
#   bash scripts/vm/create-code-tarballs.sh --include instruments-service
#   bash scripts/vm/create-code-tarballs.sh --category CEFI --include features-onchain-service
#
# GCS layout:
#   gs://{bucket}/code/unified-api-contracts-code.tar.gz
#   gs://{bucket}/code/unified-trading-library-code.tar.gz
#   gs://{bucket}/code/mtds-code.tar.gz
#   gs://{bucket}/code/{service}-code.tar.gz   (per category/--include/--all)
#   gs://{bucket}/vm/setup-data-pipeline-vm.sh  (the setup script itself)
#
# Prerequisites:
#   - gcloud CLI authenticated
#   - Workspace root at $WORKSPACE_ROOT or auto-detected
#   - Repos checked out locally
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
DEFAULT_BUCKET="deployment-scripts-central-element-323112"
BUCKET="$DEFAULT_BUCKET"
DRY_RUN=false
EXTRA_REPOS=()
CATEGORY=""
ALL_REPOS=false
ML_TRAINING=false

# ── Category → service repo mappings ──
# Each category includes the full pipeline from instruments through risk.
CEFI_REPOS=(
    instruments-service market-tick-data-service market-data-processing-service
    features-delta-one-service features-cross-instrument-service
    features-multi-timeframe-service features-calendar-service
    ml-training-service ml-inference-service
    strategy-service execution-service
    pnl-attribution-service risk-and-exposure-service position-balance-monitor-service
)
TRADFI_REPOS=(
    "${CEFI_REPOS[@]}"
    features-volatility-service
)
DEFI_REPOS=(
    instruments-service market-tick-data-service market-data-processing-service
    features-onchain-service features-delta-one-service
    strategy-service execution-service
    pnl-attribution-service risk-and-exposure-service position-balance-monitor-service
)
SPORTS_REPOS=(
    instruments-service market-tick-data-service market-data-processing-service
    features-sports-service
    ml-training-service ml-inference-service
    strategy-service execution-service
    pnl-attribution-service risk-and-exposure-service
)
PREDICTION_REPOS=(
    instruments-service market-tick-data-service market-data-processing-service
    features-cross-instrument-service
    strategy-service execution-service
    pnl-attribution-service risk-and-exposure-service
)
# ML training — minimal fleet for harness-only runs. Covers the CME S&P 500 ML
# Tier 1 MVP (stitched continuous ES series trained locally / on a training VM).
# Does NOT include strategy-service / execution-service — those live on a
# separate backtest VM fleet launched via launch-tradfi-backfill-vm.sh once
# the model artefact is registered.
ML_TRAINING_REPOS=(
    instruments-service market-tick-data-service
    features-multi-timeframe-service features-calendar-service
    features-volatility-service features-cross-instrument-service
    ml-training-service
)
# All known service repos (union of all categories)
ALL_SERVICE_REPOS=(
    instruments-service market-tick-data-service market-data-processing-service
    features-delta-one-service features-cross-instrument-service
    features-multi-timeframe-service features-calendar-service
    features-volatility-service features-onchain-service features-sports-service
    features-commodity-service
    ml-training-service ml-inference-service
    strategy-service execution-service
    pnl-attribution-service risk-and-exposure-service position-balance-monitor-service
)

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --bucket <name>       GCS bucket (default: $DEFAULT_BUCKET)"
    echo "  --category <CAT>      Include category-specific repos:"
    echo "                        CEFI, TRADFI, DEFI, SPORTS, PREDICTION"
    echo "  --all                 Include ALL service repos"
    echo "  --ml-training         Include the ML training pipeline fleet"
    echo "                        (CORE + ml-training-service + features-*)"
    echo "  --include <repo>      Include additional repo (repeatable)"
    echo "  --dry-run             Show what would be created without uploading"
    exit 1
}

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --bucket) BUCKET="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        --include) EXTRA_REPOS+=("$2"); shift 2 ;;
        --category) CATEGORY="${2^^}"; shift 2 ;;  # uppercase
        --all) ALL_REPOS=true; shift ;;
        --ml-training) ML_TRAINING=true; shift ;;
        --help|-h) usage ;;
        *) echo "Unknown arg: $1"; usage ;;
    esac
done

# Resolve category repos
CATEGORY_REPOS=()
if $ALL_REPOS; then
    CATEGORY_REPOS=("${ALL_SERVICE_REPOS[@]}")
elif $ML_TRAINING; then
    # --ml-training is a separate named tranche, not a --category value, because
    # "ml training" is orthogonal to CEFI/TRADFI/DEFI/SPORTS/PREDICTION — a
    # training run for any of those categories pulls the same ML_TRAINING_REPOS
    # bundle. Combines cleanly with --include for one-off additions.
    CATEGORY_REPOS=("${ML_TRAINING_REPOS[@]}")
elif [[ -n "$CATEGORY" ]]; then
    case "$CATEGORY" in
        CEFI)       CATEGORY_REPOS=("${CEFI_REPOS[@]}") ;;
        TRADFI)     CATEGORY_REPOS=("${TRADFI_REPOS[@]}") ;;
        DEFI)       CATEGORY_REPOS=("${DEFI_REPOS[@]}") ;;
        SPORTS)     CATEGORY_REPOS=("${SPORTS_REPOS[@]}") ;;
        PREDICTION) CATEGORY_REPOS=("${PREDICTION_REPOS[@]}") ;;
        *) echo "ERROR: Unknown category: $CATEGORY"; usage ;;
    esac
fi

# Deduplicate: merge CATEGORY_REPOS + EXTRA_REPOS
declare -A _seen_repos
MERGED_EXTRA_REPOS=()
for repo in "${CATEGORY_REPOS[@]}" "${EXTRA_REPOS[@]}"; do
    if [[ -z "${_seen_repos[$repo]:-}" ]]; then
        _seen_repos[$repo]=1
        # Don't add repos already in CORE_REPOS (MTDS is handled there)
        if [[ "$repo" != "unified-api-contracts" && "$repo" != "unified-trading-library" ]]; then
            MERGED_EXTRA_REPOS+=("$repo")
        fi
    fi
done

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

log() { echo "$(date '+%H:%M:%S') $*"; }

# Core repos always included. deployment-service is in CORE so every VM
# can import `deployment_service.deployments_registry` from the VM
# heartbeat helper — without it, no DEPLOYMENT_STARTED/PROGRESS/COMPLETED
# events reach Pub/Sub or the deployments registry (silent observability
# loss). See plan Phase 8 and the 2026-04-18 event-streaming audit.
CORE_REPOS=(
    "unified-api-contracts:unified-api-contracts-code"
    "unified-trading-library:unified-trading-library-code"
    "market-tick-data-service:mtds-code"
    "deployment-service:deployment-service-code"
)

# Exclusions for all tarballs (keep them small)
EXCLUDES=(
    --exclude='.git'
    --exclude='.venv*'
    --exclude='venv'
    --exclude='__pycache__'
    --exclude='*.egg-info'
    --exclude='node_modules'
    --exclude='.mypy_cache'
    --exclude='.pytest_cache'
    --exclude='.ruff_cache'
    --exclude='*.pyc'
    --exclude='build'
    --exclude='dist'
    # Terraform provider caches (deployment-service/terraform/*/.terraform
    # can be ~900 MB — not needed at runtime; terraform runs locally or in
    # CI, not on batch VMs).
    --exclude='.terraform'
    --exclude='.terraform.lock.hcl'
    --exclude='*.tfstate*'
    # Coverage / test artefacts
    --exclude='coverage.json'
    --exclude='coverage.xml'
    --exclude='htmlcov'
    --exclude='.coverage'
)

create_tarball() {
    local repo_dir="$1"
    local tarball_name="$2"
    local repo_path="$WORKSPACE_ROOT/$repo_dir"

    if [[ ! -d "$repo_path" ]]; then
        log "SKIP $repo_dir — not found at $repo_path"
        return 1
    fi

    local tarball="$TMP_DIR/${tarball_name}.tar.gz"
    log "Creating $tarball_name.tar.gz from $repo_dir..."

    if $DRY_RUN; then
        local size
        size=$(du -sh "$repo_path" 2>/dev/null | cut -f1)
        log "  [DRY RUN] Would create tarball from $repo_dir ($size)"
        return 0
    fi

    tar czf "$tarball" -C "$repo_path" "${EXCLUDES[@]}" .
    local size
    size=$(ls -lh "$tarball" | awk '{print $5}')
    log "  Created: $tarball_name.tar.gz ($size)"
}

# Create tarballs
log "Workspace: $WORKSPACE_ROOT"
log "Bucket: gs://$BUCKET/code/"
[[ -n "$CATEGORY" ]] && log "Category: $CATEGORY"
$ALL_REPOS && log "Mode: ALL service repos"
$ML_TRAINING && log "Mode: ML training (CORE + ${#ML_TRAINING_REPOS[@]} repos)"
[[ ${#MERGED_EXTRA_REPOS[@]} -gt 0 ]] && log "Extra repos (${#MERGED_EXTRA_REPOS[@]}): ${MERGED_EXTRA_REPOS[*]}"
log ""

for entry in "${CORE_REPOS[@]}"; do
    IFS=':' read -r repo_dir tarball_name <<< "$entry"
    create_tarball "$repo_dir" "$tarball_name"
done

for repo in "${MERGED_EXTRA_REPOS[@]}"; do
    # Derive tarball name from repo name (e.g. instruments-service → instruments-service-code)
    tarball_name="${repo}-code"
    create_tarball "$repo" "$tarball_name"
done

if $DRY_RUN; then
    log ""
    log "[DRY RUN] Would upload to gs://$BUCKET/code/"
    log "[DRY RUN] Would upload setup script to gs://$BUCKET/vm/"
    exit 0
fi

# Upload to GCS
log ""
log "Uploading to gs://$BUCKET/code/..."
gsutil -m cp "$TMP_DIR"/*.tar.gz "gs://$BUCKET/code/"

# Also upload the setup script itself
log "Uploading setup script to gs://$BUCKET/vm/..."
gsutil cp "$SCRIPT_DIR/setup-data-pipeline-vm.sh" "gs://$BUCKET/vm/"

# Verify
log ""
log "Uploaded files:"
gsutil ls -lh "gs://$BUCKET/code/" 2>/dev/null
gsutil ls -lh "gs://$BUCKET/vm/" 2>/dev/null

log ""
log "=== Done. VMs can now use: ==="
log "  startup-script-url=gs://$BUCKET/vm/setup-data-pipeline-vm.sh"
log "  Or SSH: gsutil cp gs://$BUCKET/vm/setup-data-pipeline-vm.sh /tmp/ && sudo bash /tmp/setup-data-pipeline-vm.sh"
