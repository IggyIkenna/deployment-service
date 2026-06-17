#!/usr/bin/env bash
# Refresh VM-deployment code tarballs from live-defi-rollout — SHA-skip + clean-by-construction.
#
# WHY: VM-deployment tarballs (gs://{code-bucket}/code/*-code.tar.gz) are the rapid-dev
# delivery path — VMs fetch them in setup-data-pipeline-vm.sh. Unlike prod (stable Docker
# images off `main`), tarballs must track the LDR integration branch. Manually re-running
# create-code-tarballs.sh on every ship is a treadmill, and it tars the WORKING TREE so a
# dirty foreign-WIP clone blocks the build. This wrapper fixes both:
#   * Source = committed LDR — clones each repo FRESH at origin/live-defi-rollout into a temp
#     workspace, so a dirty working tree on the host is irrelevant (clean by construction).
#   * SHA-skip — `git ls-remote` (no clone) gives each repo's LDR tip SHA; compared to the
#     uploaded {tarball}.manifest.json commit_sha. Only repos whose SHA CHANGED are cloned +
#     rebuilt + re-uploaded. So a frequent cron is cheap when idle (just ls-remote calls).
#
# Reuses create-code-tarballs.sh as the actual builder: it SKIPs any repo not present at the
# workspace root (returns 0), so cloning ONLY the changed repos into the temp workspace and
# running `--all` rebuilds exactly the changed set — no fork of the builder's tar/manifest/
# naming logic.
#
# Runs anywhere with git + gcloud (operator box, a VM, or the code-tarball-refresh Cloud Run
# Job). GH auth: GH_PAT from Secret Manager (or $GH_PAT env). GCS write: ADC / the job SA.
#
# Usage:
#   bash scripts/vm/refresh_code_tarballs.sh                 # all repos, SHA-skip, prod bucket
#   bash scripts/vm/refresh_code_tarballs.sh --dry-run       # show what WOULD refresh, no work
#   bash scripts/vm/refresh_code_tarballs.sh --project <id>  # override project
#   GH_PAT=ghp_xxx bash scripts/vm/refresh_code_tarballs.sh  # explicit token (else from SM)
#
# SSOT: codex/05-infrastructure/vm-tarball-deployment.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
GH_ORG="IggyIkenna"
BRANCH="live-defi-rollout"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --project) PROJECT_ID="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

BUCKET="deployment-scripts-${PROJECT_ID}"
# Log to STDERR: Cloud Run Jobs capture stderr but swallow stdout, so a cron run is otherwise
# a black box (the honest-coverage-job lesson). stderr keeps the SHA-scan + build progress visible.
log() { echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') [refresh-tarballs] $*" >&2; }

# repo_dir:tarball_name — MUST match create-code-tarballs.sh (CORE_REPOS + the {svc}->{svc}-code
# convention for services). MTDS is the one non-identity name.
REPOS=(
    "unified-api-contracts:unified-api-contracts-code"
    "unified-trading-library:unified-trading-library-code"
    "market-tick-data-service:mtds-code"
    "deployment-service:deployment-service-code"
    "instruments-service:instruments-service-code"
    "market-data-processing-service:market-data-processing-service-code"
    "features-service:features-service-code"
    "ml-service:ml-service-code"
    "strategy-service:strategy-service-code"
    "execution-service:execution-service-code"
    "batch-live-reconciliation-service:batch-live-reconciliation-service-code"
)

# Resolve GH_PAT (env wins; else Secret Manager). Needed to clone private repos + ls-remote.
if [[ -z "${GH_PAT:-}" ]]; then
    log "Fetching GH_PAT from Secret Manager..."
    GH_PAT="$(gcloud secrets versions access latest --secret=GH_PAT --project="$PROJECT_ID" 2>/dev/null)"
fi
[[ -z "${GH_PAT:-}" ]] && { log "ERROR: no GH_PAT (env or Secret Manager) — cannot reach private repos"; exit 1; }

auth_url() { echo "https://x-access-token:${GH_PAT}@github.com/${GH_ORG}/$1.git"; }

# ── Phase 1: SHA-skip scan (cheap — ls-remote, no clone) ──────────────────────
log "Scanning ${#REPOS[@]} repos for LDR changes vs uploaded tarball manifests..."
CHANGED=()
for entry in "${REPOS[@]}"; do
    IFS=':' read -r repo_dir tarball_name <<< "$entry"
    ldr_sha="$(git ls-remote "$(auth_url "$repo_dir")" "refs/heads/${BRANCH}" 2>/dev/null | awk '{print $1}')"
    if [[ -z "$ldr_sha" ]]; then
        log "  WARN $repo_dir — ls-remote returned nothing (repo/branch missing?), skipping"
        continue
    fi
    # Uploaded manifest commit_sha (empty if the tarball was never built).
    up_sha="$(gcloud storage cat "gs://${BUCKET}/code/${tarball_name}.manifest.json" 2>/dev/null \
                | grep -o '"commit_sha": *"[^"]*"' | head -1 | sed 's/.*"commit_sha": *"\([^"]*\)".*/\1/' || true)"
    if [[ "$ldr_sha" == "$up_sha" ]]; then
        log "  = $repo_dir up-to-date (${ldr_sha:0:12})"
    else
        log "  ↑ $repo_dir CHANGED (tarball=${up_sha:0:12} ldr=${ldr_sha:0:12}) — will rebuild"
        CHANGED+=("$entry")
    fi
done

if [[ ${#CHANGED[@]} -eq 0 ]]; then
    log "All tarballs up-to-date with ${BRANCH}. Nothing to do."
    exit 0
fi
log "${#CHANGED[@]} repo(s) changed: ${CHANGED[*]%%:*}"
if $DRY_RUN; then
    log "[DRY RUN] Would clone + rebuild the above from ${BRANCH} and upload to gs://${BUCKET}/code/"
    exit 0
fi

# ── Phase 2+3: rebuild the changed repos in BOUNDED PARALLEL — clone → build+upload → cleanup ──
# Serial (~3-4 min/repo, clone-bound) can't keep up with LDR's churn (~9-11 repos change between
# runs). Run MAX_PAR at a time: each build is independent (its own temp workspace; create-code-
# tarballs.sh uses its own mktemp TMP_DIR; the shared vm/ script uploads are atomic GCS overwrites).
# MAX_PAR bounds peak scratch (the Cloud Run Job's /tmp is memory-backed) to ~MAX_PAR repos at once.
MAX_PAR="${REFRESH_MAX_PARALLEL:-4}"
status_dir="$(mktemp -d)"
trap 'rm -rf "$status_dir"' EXIT

build_one() {
    local entry="$1"
    local repo_dir="${entry%%:*}"
    local attempt
    # One retry: concurrent clones (up to MAX_PAR) can hit a transient GitHub timeout/rate-limit.
    for attempt in 1 2; do
        local work
        work="$(mktemp -d)"
        if git clone --quiet --depth 1 --branch "$BRANCH" "$(auth_url "$repo_dir")" "$work/$repo_dir" 2>/dev/null \
            && WORKSPACE_ROOT="$work" bash "$SCRIPT_DIR/create-code-tarballs.sh" --all --bucket "$BUCKET" >&2; then
            rm -rf "$work"
            echo ok >"$status_dir/$repo_dir"
            log "  ✓ $repo_dir"
            return
        fi
        rm -rf "$work"
        [[ $attempt -eq 1 ]] && { log "  … $repo_dir attempt 1 failed, retrying"; sleep 3; }
    done
    echo fail >"$status_dir/$repo_dir"
    log "  ✗ $repo_dir (clone or build failed after retry)"
}

log "Rebuilding ${#CHANGED[@]} repo(s), up to ${MAX_PAR} in parallel ..."
for entry in "${CHANGED[@]}"; do
    build_one "$entry" &
    while [[ "$(jobs -rp | wc -l)" -ge "$MAX_PAR" ]]; do wait -n 2>/dev/null || break; done
done
wait

ok=$(grep -rl '^ok$' "$status_dir" 2>/dev/null | wc -l | tr -d ' ')
failed_repos=$(grep -rl '^fail$' "$status_dir" 2>/dev/null | xargs -r -n1 basename | tr '\n' ' ')

# Observability: Cloud Run Jobs do not reliably surface container stdout/stderr in Cloud Logging
# (honest-coverage-job lesson), so write a status object the cron can be audited by regardless:
# gs://{bucket}/code/_refresh_status.json — last run's ts/changed/rebuilt/failed.
status_json=$(printf '{"ts":"%s","branch":"%s","scanned":%d,"changed":%d,"rebuilt_ok":%s,"failed":"%s"}\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$BRANCH" "${#REPOS[@]}" "${#CHANGED[@]}" "$ok" "${failed_repos% }")
echo "$status_json" | gcloud storage cp - "gs://${BUCKET}/code/_refresh_status.json" 2>/dev/null || true

if [[ -z "${failed_repos// /}" ]]; then
    log "Refresh complete — ${ok}/${#CHANGED[@]} tarball(s) updated to ${BRANCH} tip."
else
    log "Refresh PARTIAL — ${ok}/${#CHANGED[@]} updated; FAILED: ${failed_repos}"
    exit 1
fi
