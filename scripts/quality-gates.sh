#!/usr/bin/env bash
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-service.sh
# SSOT: unified-trading-codex/06-coding-standards/quality-gates-service-template.sh
#
# Instructions for a new service:
#   1. Copy this to scripts/quality-gates.sh in your repo (rollout-quality-gates-unified.py does this)
#   2. SERVICE_NAME, SOURCE_DIR, and MIN_COVERAGE are set automatically by rollout (floor=70)
#   3. Set RUN_INTEGRATION=true only if your repo has integration tests
#   4. Add LOCAL_DEPS entries if your service has local editable deps (e.g. unified-events-interface)
SERVICE_NAME="deployment-service"
SOURCE_DIR="deployment_service"
MIN_COVERAGE=76
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
# print() in vm_config.py is inside shell script templates (python3 -c "print(...)"), not Python print
# progress.py uses rich console.print() for CLI progress rendering — not debug print
PRINT_EXCLUDE_GLOBS=("--glob" "!**/vm_config.py" "--glob" "!**/progress.py")
# bootstrap_config.py and env_substitutor.py are approved config-bootstrap: exceptions
# deployment_config.py, shard_builder.py, worker_manager.py contain os.environ in comments only
OS_ENV_EXCLUDE_GLOBS=("--glob" "!**/bootstrap_config.py" "--glob" "!**/env_substitutor.py" "--glob" "!**/deployment_config.py" "--glob" "!**/shard_builder.py" "--glob" "!**/worker_manager.py")
# AWS batch defers boto3 import to control-plane boundary (not always installed)
IMPORT_INSIDE_EXCLUDE_GLOBS=("--glob" "!**/aws_batch.py")
# GCP_PROJECT_ID in CLI help text strings (docstrings/examples), not code references
GCP_PROJECT_ID_EXCLUDE_GLOBS=("--glob" "!**/calculation.py")
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
