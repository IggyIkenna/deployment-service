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

# --- Codex exclusions (documented in QUALITY_GATE_BYPASS_AUDIT.md) ---
# Rich console.print() in progress.py; python3 -c "print(...)" in bash template strings (vm_config.py)
PRINT_EXCLUDE_GLOBS=(--glob "!**/progress.py" --glob "!**/vm_config.py")
# bootstrap_config.py: config-bootstrap exception (os.environ.get for RUNTIME_TOPOLOGY_PATH, WORKSPACE_ROOT)
# env_substitutor.py: config-bootstrap exception (os.environ snapshot for template substitution)
# deployment_config.py, shard_builder.py, worker_manager.py: os.environ only in comments/docstrings
OS_ENV_EXCLUDE_GLOBS=(--glob "!**/bootstrap_config.py" --glob "!**/env_substitutor.py" --glob "!**/deployment_config.py" --glob "!**/shard_builder.py" --glob "!**/worker_manager.py")
# __main__.py: conditional imports for CLI vs API mode dispatch
# state.py: FastAPI route handlers use deferred imports to avoid circular deps
# backends/: cloud SDK boundary — deferred imports for boto3/google-cloud SDKs
# calculators/: deferred imports to avoid circular deps with cloud_client/config_loader
IMPORT_INSIDE_EXCLUDE_GLOBS=("!**/__main__.py" "!**/api/routes/state.py" "!**/backends/**" "!**/calculators/**")
# deployment-service legitimately references GCP_PROJECT_ID in env var injection, template substitution, and CLI
GCP_PROJECT_ID_EXCLUDE_GLOBS=("!**/smoke_test_framework.py" "!**/deployment_config.py" "!**/dependencies.py" "!**/shard_builder.py" "!**/config_loader.py" "!**/cloud_client.py" "!**/vm_config.py" "!**/vm_lifecycle.py" "!**/calculation.py")
# deployment-service has complex backend orchestration, CLI handlers, and VM lifecycle management
# that inherently require larger functions/methods. Per-repo overrides (documented in QUALITY_GATE_BYPASS_AUDIT.md).
MAX_FILE_LINES=1700
MAX_METHOD_LINES=330
# Exclude configs SVG generator (build tool, not service code)
FUNCTION_SIZE_EXTRA_EXCLUDES=("! -path" "./configs/*")

WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
