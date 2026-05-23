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
#   bash scripts/vm/create-code-tarballs.sh --allow-dirty-tarball  # override dirty-tree block (audit logged)
#   bash scripts/vm/create-code-tarballs.sh --asset-group CEFI    # core + CEFI services
#   bash scripts/vm/create-code-tarballs.sh --asset-group DEFI    # core + DEFI services
#   bash scripts/vm/create-code-tarballs.sh --all              # core + ALL service repos
#   bash scripts/vm/create-code-tarballs.sh --ml-training      # core + ml pipeline
#                                                                (CORE + ml-service +
#                                                                features-* consumers)
#
# Also supports additional repos for services beyond the category set:
#   bash scripts/vm/create-code-tarballs.sh --include instruments-service
#   bash scripts/vm/create-code-tarballs.sh --asset-group CEFI --include features-onchain-service
#
# GCS layout:
#   gs://{bucket}/code/unified-api-contracts-code.tar.gz
#   gs://{bucket}/code/unified-api-contracts-code.manifest.json  (sibling SHA manifest)
#   gs://{bucket}/code/unified-api-contracts-code@{sha}.tar.gz  (SHA-pinned copy)
#   gs://{bucket}/code/unified-api-contracts-code@{sha}.manifest.json
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
AWS_DEFAULT_BUCKET="uts-prod-deployment-state"
BUCKET="$DEFAULT_BUCKET"
CLOUD="gcp"
DRY_RUN=false
ALLOW_DIRTY_TARBALL=false
EXTRA_REPOS=()
ASSET_GROUP=""
ALL_REPOS=false
ML_TRAINING=false

# ── Category → service repo mappings ──
# Each category includes the full pipeline from instruments through risk.
CEFI_REPOS=(
    instruments-service market-tick-data-service market-data-processing-service
    features-delta-one-service features-cross-instrument-service
    features-multi-timeframe-service features-calendar-service
    ml-service
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
    e2e-testing
)
SPORTS_REPOS=(
    instruments-service market-tick-data-service market-data-processing-service
    features-sports-service
    ml-service
    strategy-service execution-service
    pnl-attribution-service risk-and-exposure-service
)
PREDICTION_REPOS=(
    instruments-service market-tick-data-service market-data-processing-service
    features-cross-instrument-service
    strategy-service execution-service
    pnl-attribution-service risk-and-exposure-service
)
# ML pipeline — minimal fleet for harness-only runs. Covers the CME S&P 500 ML
# Tier 1 MVP (stitched continuous ES series trained locally / on a training VM).
# Does NOT include strategy-service / execution-service — those live on a
# separate backtest VM fleet launched via launch-tradfi-backfill-vm.sh once
# the model artefact is registered.
ML_TRAINING_REPOS=(
    instruments-service market-tick-data-service
    features-multi-timeframe-service features-calendar-service
    features-volatility-service features-cross-instrument-service
    ml-service
)
# All known service repos (union of all categories)
ALL_SERVICE_REPOS=(
    instruments-service market-tick-data-service market-data-processing-service
    features-delta-one-service features-cross-instrument-service
    features-multi-timeframe-service features-calendar-service
    features-volatility-service features-onchain-service features-sports-service
    features-commodity-service
    ml-service
    strategy-service execution-service
    pnl-attribution-service risk-and-exposure-service position-balance-monitor-service
    batch-live-reconciliation-service
)

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --bucket <name>       GCS/S3 bucket (GCP default: $DEFAULT_BUCKET, AWS default: $AWS_DEFAULT_BUCKET)"
    echo "  --cloud gcp|aws       Target cloud (default: gcp)"
    echo "  --asset-group <CAT>      Include category-specific repos:"
    echo "                        CEFI, TRADFI, DEFI, SPORTS, PREDICTION"
    echo "  --all                 Include ALL service repos"
    echo "  --ml-training         Include the ML pipeline fleet"
    echo "                        (CORE + ml-service + features-*)"
    echo "  --include <repo>      Include additional repo (repeatable)"
    echo "  --dry-run             Show what would be created without uploading"
    echo "  --allow-dirty-tarball Override dirty-tree block (audit logged; emergency hotfixes only)"
    exit 1
}

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --bucket) BUCKET="$2"; shift 2 ;;
        --cloud) CLOUD="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        --allow-dirty-tarball) ALLOW_DIRTY_TARBALL=true; shift ;;
        --include) EXTRA_REPOS+=("$2"); shift 2 ;;
        --asset-group) ASSET_GROUP="$(printf '%s' "$2" | tr '[:lower:]' '[:upper:]')"; shift 2 ;;  # uppercase (bash3-safe)
        --all) ALL_REPOS=true; shift ;;
        --ml-training) ML_TRAINING=true; shift ;;
        --help|-h) usage ;;
        *) echo "Unknown arg: $1"; usage ;;
    esac
done

# Validate --cloud and resolve default bucket if not overridden
case "$CLOUD" in
    gcp) ;;
    aws) [[ "$BUCKET" == "$DEFAULT_BUCKET" ]] && BUCKET="$AWS_DEFAULT_BUCKET" ;;
    *) echo "ERROR: --cloud must be gcp or aws (got: $CLOUD)"; usage ;;
esac

# Resolve category repos
ASSET_GROUP_REPOS=()
if $ALL_REPOS; then
    ASSET_GROUP_REPOS=("${ALL_SERVICE_REPOS[@]}")
elif $ML_TRAINING; then
    # --ml-training is a separate named tranche, not a --asset-group value, because
    # "ml training" is orthogonal to CEFI/TRADFI/DEFI/SPORTS/PREDICTION — a
    # training run for any of those categories pulls the same ML_TRAINING_REPOS
    # bundle. Combines cleanly with --include for one-off additions.
    ASSET_GROUP_REPOS=("${ML_TRAINING_REPOS[@]}")
elif [[ -n "$ASSET_GROUP" ]]; then
    case "$ASSET_GROUP" in
        CEFI)       ASSET_GROUP_REPOS=("${CEFI_REPOS[@]}") ;;
        TRADFI)     ASSET_GROUP_REPOS=("${TRADFI_REPOS[@]}") ;;
        DEFI)       ASSET_GROUP_REPOS=("${DEFI_REPOS[@]}") ;;
        SPORTS)     ASSET_GROUP_REPOS=("${SPORTS_REPOS[@]}") ;;
        PREDICTION) ASSET_GROUP_REPOS=("${PREDICTION_REPOS[@]}") ;;
        *) echo "ERROR: Unknown category: $ASSET_GROUP"; usage ;;
    esac
fi

# Deduplicate: merge ASSET_GROUP_REPOS + EXTRA_REPOS
# bash-3.2 safe — no `declare -A` (macOS default bash lacks associative arrays).
# Use a space-delimited sentinel string + substring match for the seen-set.
# Also note: bash 3.2 + `set -u` trips on empty-array expansion `"${arr[@]}"`,
# so we expand with the `${arr[@]+"${arr[@]}"}` guard pattern that's safe on
# both bash 3.2 and bash 4+.
_seen_repos_list=" "
MERGED_EXTRA_REPOS=()
for repo in ${ASSET_GROUP_REPOS[@]+"${ASSET_GROUP_REPOS[@]}"} ${EXTRA_REPOS[@]+"${EXTRA_REPOS[@]}"}; do
    case "$_seen_repos_list" in
        *" $repo "*) ;;  # already seen, skip
        *)
            _seen_repos_list="${_seen_repos_list}${repo} "
            # Don't add repos already in CORE_REPOS (MTDS is handled there)
            if [[ "$repo" != "unified-api-contracts" && "$repo" != "unified-trading-library" ]]; then
                MERGED_EXTRA_REPOS+=("$repo")
            fi
            ;;
    esac
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
        # SKIP is non-fatal: upload step uses $TMP_DIR/*.tar.gz glob which only
        # matches actually-created tarballs. Return 0 so set -e doesn't abort
        # the outer for-loop (features-delta-one-service / features-onchain-service
        # were consolidated into features-service; their entries in the category
        # arrays are historical and should not block builds for present repos).
        return 0
    fi

    # Compute git metadata (always — even in dry-run)
    local commit_sha git_status_clean
    commit_sha=$(git -C "$repo_path" rev-parse HEAD 2>/dev/null || echo "unknown")
    if git -C "$repo_path" diff-index --quiet HEAD -- 2>/dev/null; then
        git_status_clean="true"
    else
        git_status_clean="false"
    fi

    # Dirty-tree check: abort unless --allow-dirty-tarball override
    if [[ "$git_status_clean" == "false" ]]; then
        if $ALLOW_DIRTY_TARBALL; then
            log "  WARNING: $repo_dir has uncommitted changes — --allow-dirty-tarball override active"
            log "  AUDIT: allow-dirty-tarball by $(whoami 2>/dev/null || echo unknown) at $(date -u '+%Y-%m-%dT%H:%M:%SZ') for $repo_dir@${commit_sha:0:12}"
        else
            log "ERROR: $repo_dir has uncommitted changes. Commit or stash first, or use --allow-dirty-tarball."
            return 1
        fi
    fi

    # Extract version from pyproject.toml
    local pyproject_version
    pyproject_version=$(grep '^version' "$repo_path/pyproject.toml" 2>/dev/null | head -1 | sed 's/version = "\(.*\)"/\1/' | tr -d ' ')
    [[ -z "$pyproject_version" ]] && pyproject_version="unknown"

    # Write manifest.json sibling (always — even in dry-run for display)
    local manifest="$TMP_DIR/${tarball_name}.manifest.json"
    local created_at
    created_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    printf '{\n  "repo": "%s",\n  "tarball_name": "%s",\n  "commit_sha": "%s",\n  "pyproject_version": "%s",\n  "git_status_clean": %s,\n  "created_at": "%s",\n  "created_by": "create-code-tarballs.sh"\n}\n' \
        "$repo_dir" "$tarball_name" "$commit_sha" "$pyproject_version" "$git_status_clean" "$created_at" \
        > "$manifest"

    log "Creating $tarball_name.tar.gz from $repo_dir (sha=${commit_sha:0:12} clean=$git_status_clean)..."

    if $DRY_RUN; then
        local size
        size=$(du -sh "$repo_path" 2>/dev/null | cut -f1)
        log "  [DRY RUN] Would create: $tarball_name.tar.gz ($size)"
        log "  [DRY RUN] Would write:  $tarball_name.manifest.json + $tarball_name@${commit_sha:0:12}.[tar.gz|manifest.json]"
        return 0
    fi

    local tarball="$TMP_DIR/${tarball_name}.tar.gz"
    tar czf "$tarball" -C "$repo_path" "${EXCLUDES[@]}" .
    local size
    size=$(ls -lh "$tarball" | awk '{print $5}')
    log "  Created: $tarball_name.tar.gz ($size)"

    # SHA-named copies for immutable GCS references
    cp "$tarball" "$TMP_DIR/${tarball_name}@${commit_sha}.tar.gz"
    cp "$manifest" "$TMP_DIR/${tarball_name}@${commit_sha}.manifest.json"
    log "  SHA-pinned copy: $tarball_name@${commit_sha:0:12}.tar.gz"
}

# Create tarballs
log "Workspace: $WORKSPACE_ROOT"
log "Bucket: gs://$BUCKET/code/"
[[ -n "$ASSET_GROUP" ]] && log "Category: $ASSET_GROUP"
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

# ── Pre-flight: dep-pin conflict check ─────────────────────────────────────
# Per plans/active/issues/features_vm_uv_resolution_unsatisfiable_2026_05_16.md
# Phase 2 action item: catch pyproject.toml dep-pin conflicts at tarball-build
# time, before they kill a VM at uv-pip-install time. Background: 5 features-onchain
# VMs in a row died on (risk@UAC-pin, ml-training@UTL-pin, betfair/requests,
# e2e-testing→execution-service chain) before slot-1-main shipped each fix.
# Each round-trip cost ~30 min (rebuild tarball + relaunch VM + wait for fail).
# This pre-flight runs `uv pip compile --no-deps` against the canonical set
# and surfaces unsatisfiable pins as WARNINGS (does not block upload — the
# VM_TASK-specific NODEPS routing in setup-data-pipeline-vm.sh covers most
# real cases). Skip with --skip-preflight (CI builds where deps already
# checked separately).
if [[ "${SKIP_PREFLIGHT:-false}" != "true" ]]; then
    log ""
    log "Pre-flight: scanning per-repo pyproject pins for mis-floored peer-repo deps..."
    _PEER_VERSIONS=""
    for _peer in unified-api-contracts unified-trading-library; do
        _peer_path="$WORKSPACE_ROOT/$_peer/pyproject.toml"
        if [[ -f "$_peer_path" ]]; then
            _ver=$(grep -m1 '^version' "$_peer_path" | sed -E 's/^version[^"]*"([^"]+)".*/\1/')
            _PEER_VERSIONS="${_PEER_VERSIONS}${_peer}=${_ver} "
        fi
    done
    log "  Workspace peers: ${_PEER_VERSIONS}"
    _CONFLICTS=0
    for entry in "${CORE_REPOS[@]}" "${MERGED_EXTRA_REPOS[@]}"; do
        # CORE_REPOS use "dir:tarball" syntax; MERGED_EXTRA is bare dir.
        _dir="${entry%%:*}"
        _pyproject="$WORKSPACE_ROOT/$_dir/pyproject.toml"
        [[ -f "$_pyproject" ]] || continue
        # Scan for too-high UAC/UTL floors (>0.1.x for UAC, >0.3.x for UTL given
        # current workspace state). Catches the mis-floor class of bugs that
        # killed VMs 2-5 of the B-015 chain.
        if grep -qE 'unified-api-contracts>=0\.[2-9][0-9]?\.|unified-api-contracts>=[1-9]' "$_pyproject" 2>/dev/null; then
            log "  WARN: $_dir pyproject pins unified-api-contracts above 0.1.x — verify against workspace peer"
            _CONFLICTS=$((_CONFLICTS + 1))
        fi
        if grep -qE 'unified-trading-library>=0\.[4-9][0-9]?\.|unified-trading-library>=[1-9]' "$_pyproject" 2>/dev/null; then
            log "  WARN: $_dir pyproject pins unified-trading-library above 0.3.x — verify against workspace peer"
            _CONFLICTS=$((_CONFLICTS + 1))
        fi
    done
    if [[ "$_CONFLICTS" -gt 0 ]]; then
        log "  Pre-flight found $_CONFLICTS mis-floored peer-repo pin(s) — VM may hit unsatisfiable resolution."
        log "  Fix by relaxing the offending pyproject.toml pin(s) OR set SKIP_PREFLIGHT=true to bypass."
    else
        log "  Pre-flight OK: no mis-floored peer-repo pins detected."
    fi
fi

if $DRY_RUN; then
    log ""
    if [[ "$CLOUD" == "aws" ]]; then
        log "[DRY RUN] Would upload to s3://$BUCKET/code/"
        log "[DRY RUN] Would upload setup script to s3://$BUCKET/vm/"
    else
        log "[DRY RUN] Would upload to gs://$BUCKET/code/"
        log "[DRY RUN] Would upload setup script to gs://$BUCKET/vm/"
    fi
    exit 0
fi

if [[ "$CLOUD" == "aws" ]]; then
    # Upload to S3 — tarballs + manifests + SHA-named copies
    log ""
    log "Uploading to s3://$BUCKET/code/..."
    for f in "$TMP_DIR"/*.tar.gz "$TMP_DIR"/*.manifest.json; do
        [[ -f "$f" ]] && aws s3 cp "$f" "s3://$BUCKET/code/$(basename "$f")" --quiet
    done

    # Upload AWS setup script + wrapper
    log "Uploading setup + wrapper scripts to s3://$BUCKET/vm/..."
    aws s3 cp "$SCRIPT_DIR/setup-data-pipeline-vm-aws.sh" "s3://$BUCKET/vm/" --quiet
    aws s3 cp "$SCRIPT_DIR/heartbeat_daemon.py" "s3://$BUCKET/vm/" --quiet 2>/dev/null || true

    # Verify
    log ""
    log "Uploaded files:"
    aws s3 ls "s3://$BUCKET/code/" 2>/dev/null | tail -20
    aws s3 ls "s3://$BUCKET/vm/" 2>/dev/null

    log ""
    log "=== Done. EC2 VMs can now use: ==="
    log "  aws s3 cp s3://$BUCKET/vm/setup-data-pipeline-vm-aws.sh /tmp/ && sudo bash /tmp/setup-data-pipeline-vm-aws.sh"
else
    # Upload to GCS — tarballs + manifests + SHA-named copies
    log ""
    log "Uploading to gs://$BUCKET/code/..."
    gsutil -m cp "$TMP_DIR"/*.tar.gz "gs://$BUCKET/code/"
    gsutil -m cp "$TMP_DIR"/*.manifest.json "gs://$BUCKET/code/"

    # Also upload the setup + execution wrapper scripts. Without the wrapper
    # upload, edits to vm-exec-with-gcs-tee.sh (e.g. BUG-4 exit_code reporting,
    # stall timeout bumps) silently never reach VMs because setup-data-pipeline-vm.sh
    # downloads the wrapper as a standalone object from gs://.../vm/, not from
    # the deployment-service tarball. The wrapper sat at 2026-04-28 mtime for a
    # week despite multiple tarball refreshes (incident 2026-05-05).
    log "Uploading setup + wrapper scripts to gs://$BUCKET/vm/..."
    gsutil cp "$SCRIPT_DIR/setup-data-pipeline-vm.sh" "gs://$BUCKET/vm/"
    gsutil cp "$SCRIPT_DIR/vm-exec-with-gcs-tee.sh" "gs://$BUCKET/vm/"
    gsutil cp "$SCRIPT_DIR/heartbeat_daemon.py" "gs://$BUCKET/vm/" 2>/dev/null || true

    # Bare-launcher publish — cron-VM hosts (launch-*-fwd-daily-cron-vm.sh) fetch
    # individual launch-*-forward-poll.sh scripts from a stable GCS path each cron
    # tick (so updates to a launcher take effect within the cron interval, without
    # rebuilding the tarball). The cron-VM crontabs reference exactly this path:
    #   gs://${CODE_BUCKET}/code/deployment-service/scripts/vm/launch-<name>.sh
    # See launch-cefi-fwd-daily-cron-vm.sh:90 + launch-tradfi-fwd-daily-cron-vm.sh:92.
    # Without this loop, the cron-VM agent had to do manual uploads (incident
    # 2026-05-20). The tarball remains the canonical bundle for one-shot VMs —
    # this just ALSO publishes bare launchers for the cron-VM consumers.
    log "Publishing bare launcher scripts to gs://$BUCKET/code/deployment-service/scripts/vm/..."
    _launcher_count=0
    for _launcher in "$SCRIPT_DIR"/launch-*.sh; do
        [[ -f "$_launcher" ]] || continue
        _launcher_name=$(basename "$_launcher")
        gsutil -q cp "$_launcher" "gs://$BUCKET/code/deployment-service/scripts/vm/$_launcher_name"
        _launcher_count=$((_launcher_count + 1))
    done
    # Also publish the lib/ directory contents — launchers source from it.
    for _libfile in "$SCRIPT_DIR"/lib/*.sh; do
        [[ -f "$_libfile" ]] || continue
        _libfile_name=$(basename "$_libfile")
        gsutil -q cp "$_libfile" "gs://$BUCKET/code/deployment-service/scripts/vm/lib/$_libfile_name"
    done
    log "  Published $_launcher_count bare launchers + lib/ helpers"

    # Verify
    log ""
    log "Uploaded files:"
    gsutil ls -lh "gs://$BUCKET/code/" 2>/dev/null
    gsutil ls -lh "gs://$BUCKET/vm/" 2>/dev/null

    log ""
    log "=== Done. VMs can now use: ==="
    log "  startup-script-url=gs://$BUCKET/vm/setup-data-pipeline-vm.sh"
    log "  Or SSH: gsutil cp gs://$BUCKET/vm/setup-data-pipeline-vm.sh /tmp/ && sudo bash /tmp/setup-data-pipeline-vm.sh"
fi
