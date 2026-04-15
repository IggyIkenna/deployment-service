#!/usr/bin/env bash
# Create and upload code tarballs for tarball-based VM deployments.
#
# This packages the three core repos (UAC, UTL, MTDS) into tarballs and
# uploads them to GCS. VMs fetch these during setup via setup-data-pipeline-vm.sh.
#
# Usage:
#   bash scripts/vm/create-code-tarballs.sh                    # default bucket
#   bash scripts/vm/create-code-tarballs.sh --bucket my-bucket # custom bucket
#   bash scripts/vm/create-code-tarballs.sh --dry-run          # show what would be created
#
# Also supports additional repos for services beyond MTDS:
#   bash scripts/vm/create-code-tarballs.sh --include instruments-service
#   bash scripts/vm/create-code-tarballs.sh --include instruments-service --include features-sports-service
#
# GCS layout:
#   gs://{bucket}/code/unified-api-contracts-code.tar.gz
#   gs://{bucket}/code/unified-trading-library-code.tar.gz
#   gs://{bucket}/code/mtds-code.tar.gz
#   gs://{bucket}/code/{extra-repo}-code.tar.gz   (if --include)
#   gs://{bucket}/vm/setup-data-pipeline-vm.sh     (the setup script itself)
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

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --bucket) BUCKET="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        --include) EXTRA_REPOS+=("$2"); shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

log() { echo "$(date '+%H:%M:%S') $*"; }

# Core repos always included
CORE_REPOS=(
    "unified-api-contracts:unified-api-contracts-code"
    "unified-trading-library:unified-trading-library-code"
    "market-tick-data-service:mtds-code"
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
log ""

for entry in "${CORE_REPOS[@]}"; do
    IFS=':' read -r repo_dir tarball_name <<< "$entry"
    create_tarball "$repo_dir" "$tarball_name"
done

for repo in "${EXTRA_REPOS[@]}"; do
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
