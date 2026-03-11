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
MIN_COVERAGE=79
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
# Bootstrap exception: bootstrap_config.py/env_substitutor.py use os.environ at init time (codex approved)
OS_ENV_EXCLUDE_GLOBS=(
    "--glob" "!**/config/bootstrap_config.py"
    "--glob" "!**/config/env_substitutor.py"
    "--glob" "!**/deployment_config.py"
)
# deployment-service legitimately uses GCP_PROJECT_ID in infrastructure management code
GCP_PROJECT_ID_EXCLUDE_GLOBS=(
    "--glob" "!**/dependencies.py"
    "--glob" "!**/smoke_test_framework.py"
    "--glob" "!**/cloud_client.py"
    "--glob" "!**/config_loader.py"
    "--glob" "!**/deployment_config.py"
    "--glob" "!**/calculators/**"
    "--glob" "!**/backends/**"
    "--glob" "!**/cli/**"
    "--glob" "!**/live_deployment.py"
    "--glob" "!**/services/**"
)
# Rich console.print() in progress.py; bash template strings in vm_config.py
PRINT_EXCLUDE_GLOBS=(
    "--glob" "!**/deployment/progress.py"
    "--glob" "!**/backends/services/vm_config.py"
)
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "!" "-path" "./deployment_service/catalog.py"
    "!" "-path" "./deployment_service/config_loader.py"
    "!" "-path" "./deployment_service/monitor.py"
    "!" "-path" "./deployment_service/orchestrator.py"
    "!" "-path" "./deployment_service/shard_calculator.py"
    "!" "-path" "./deployment_service/dependencies.py"
    "!" "-path" "./deployment_service/smoke_test_framework.py"
    "!" "-path" "./deployment_service/live_deployment.py"
    "!" "-path" "./deployment_service/backends/*"
    "!" "-path" "./deployment_service/calculators/*"
    "!" "-path" "./deployment_service/deployment/*"
    "!" "-path" "./deployment_service/services/*"
    "!" "-path" "./deployment_service/cli_modules/*"
    "!" "-path" "./deployment_service/cli/utils/*"
    "!" "-path" "./deployment_service/cli/handlers/*"
    "!" "-path" "./configs/*"
    "!" "-path" "./tests/*"
)
# TYPE_CHECKING imports and deferred optional-dep imports in backends/ look like function-level imports
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "--glob" "!**/backends/**"
    "--glob" "!**/calculators/shard_dimensions.py"
)
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
