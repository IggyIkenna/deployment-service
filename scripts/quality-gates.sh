#!/usr/bin/env bash
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-service.sh
SERVICE_NAME="deployment-service"
SOURCE_DIR="deployment_service"
MIN_COVERAGE=79
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
# Bootstrap exception: use os.environ at init time (codex approved) — QUALITY_GATE_BYPASS_AUDIT.md §1.1
OS_ENV_EXCLUDE_GLOBS=(
    "--glob" "!**/config/bootstrap_config.py" "--glob" "!**/config/env_substitutor.py"
    "--glob" "!**/deployment_config.py"
)
# Infra management legitimately uses GCP_PROJECT_ID — QUALITY_GATE_BYPASS_AUDIT.md §1.1
GCP_PROJECT_ID_EXCLUDE_GLOBS=(
    "--glob" "!**/dependencies.py" "--glob" "!**/smoke_test_framework.py"
    "--glob" "!**/cloud_client.py" "--glob" "!**/config_loader.py"
    "--glob" "!**/deployment_config.py" "--glob" "!**/live_deployment.py"
    "--glob" "!**/calculators/**" "--glob" "!**/backends/**"
    "--glob" "!**/cli/**" "--glob" "!**/services/**"
)
# Rich console.print() + bash template strings — QUALITY_GATE_BYPASS_AUDIT.md §1.1
PRINT_EXCLUDE_GLOBS=(
    "--glob" "!**/deployment/progress.py" "--glob" "!**/backends/services/vm_config.py"
)
# TYPE_CHECKING + deferred optional-dep imports — QUALITY_GATE_BYPASS_AUDIT.md §1.2
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "--glob" "!**/backends/**" "--glob" "!**/calculators/shard_dimensions.py"
)
# Infra complexity by design — QUALITY_GATE_BYPASS_AUDIT.md §1.3
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "!" "-path" "./deployment_service/catalog.py" "!" "-path" "./deployment_service/config_loader.py"
    "!" "-path" "./deployment_service/monitor.py" "!" "-path" "./deployment_service/orchestrator.py"
    "!" "-path" "./deployment_service/shard_calculator.py" "!" "-path" "./deployment_service/dependencies.py"
    "!" "-path" "./deployment_service/smoke_test_framework.py" "!" "-path" "./deployment_service/live_deployment.py"
    "!" "-path" "./deployment_service/backends/*" "!" "-path" "./deployment_service/calculators/*"
    "!" "-path" "./deployment_service/deployment/*" "!" "-path" "./deployment_service/services/*"
    "!" "-path" "./deployment_service/cli_modules/*" "!" "-path" "./deployment_service/cli/utils/*"
    "!" "-path" "./deployment_service/cli/handlers/*" "!" "-path" "./configs/*" "!" "-path" "./tests/*"
)
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
