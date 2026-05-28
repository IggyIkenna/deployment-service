#!/usr/bin/env bash
# warm-cache.sh — Packer provisioner: pre-clone agent-orchestrator + UTL + UAC
# into /opt/orchestrator-warm/ and pre-build the python venv. bootstrap_vm.sh
# rsyncs from /opt/orchestrator-warm/ at first boot (a few seconds) then runs
# `git pull --ff-only` to catch up to whatever LDR points at NOW.
#
# Repos cloned anonymously (no GH PAT at build time). All target repos are
# public-readable for clone (the operator-side GH PAT is used at runtime in
# bootstrap_vm.sh for push capability + the private branch handling).
#
# Run as root by Packer.
set -euo pipefail

GIT_BRANCH="${GIT_BRANCH:-live-defi-rollout}"
WARM_DIR="/opt/orchestrator-warm"

log() { printf '[warm-cache] %s\n' "$*"; }

mkdir -p "${WARM_DIR}"
cd "${WARM_DIR}"

log "warming repos on branch=${GIT_BRANCH}"

# Clone each repo at the requested branch. Use --single-branch + --depth 50 to
# keep the AMI small while preserving enough history for typical operator
# inspection. Runtime `git pull --ff-only` deepens as needed.
clone_repo() {
  local repo_url="$1"
  local repo_dir="$2"
  if [[ -d "${repo_dir}/.git" ]]; then
    log "  ${repo_dir}: already present — refreshing"
    git -C "${repo_dir}" fetch --depth 50 origin "${GIT_BRANCH}"
    git -C "${repo_dir}" checkout "${GIT_BRANCH}"
    git -C "${repo_dir}" reset --hard "origin/${GIT_BRANCH}"
  else
    log "  ${repo_dir}: cloning ${repo_url}"
    git clone --branch "${GIT_BRANCH}" --single-branch --depth 50 "${repo_url}" "${repo_dir}"
  fi
}

clone_repo https://github.com/IggyIkenna/agent-orchestrator.git              agent-orchestrator
clone_repo https://github.com/IggyIkenna/unified-trading-library.git         unified-trading-library
clone_repo https://github.com/IggyIkenna/unified-api-contracts.git           unified-api-contracts
clone_repo https://github.com/IggyIkenna/unified-trading-pm.git              unified-trading-pm

# Pre-build the venv. This is the slowest step on cold boot (~60-90s) and the
# biggest win to bake in.
log "building agent-orchestrator/.venv"
cd "${WARM_DIR}/agent-orchestrator"
# uv venv needs python 3.13; ubuntu 24.04 ships 3.12. uv handles the bootstrap.
uv venv --python ">=3.13" .venv
# shellcheck disable=SC1091
source .venv/bin/activate
uv pip install -e .
uv pip install -e ../unified-trading-library
uv pip install -e ../unified-api-contracts
deactivate

log "venv built at ${WARM_DIR}/agent-orchestrator/.venv ($(du -sh "${WARM_DIR}/agent-orchestrator/.venv" | cut -f1))"

# Permissions: leave warm dir owned by root with world-read so bootstrap can
# rsync as the operator user (rsync as operator can read root-owned trees).
chmod -R a+rX "${WARM_DIR}"

log "warm-cache.sh complete; ${WARM_DIR} size: $(du -sh "${WARM_DIR}" | cut -f1)"
