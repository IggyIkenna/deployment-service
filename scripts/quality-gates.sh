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
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"

# ---------------------------------------------------------------------------
# BYPASS EXCLUSIONS — all documented in QUALITY_GATE_BYPASS_AUDIT.md
# ---------------------------------------------------------------------------

# §2.4a — print() in Jinja2 cloud-init bash templates (vm_config.py)
#         and Rich console.print() in progress.py (terminal UI)
PRINT_EXCLUDE_GLOBS=(
    "--glob" "!**/backends/services/vm_config.py"
    "--glob" "!**/deployment/progress.py"
)

# §2.4 — os.environ in bootstrap layer, env-var validator, template substitutor,
#         cli.py process bootstrap, and comment-only mentions in docstrings
OS_ENV_EXCLUDE_GLOBS=(
    "--glob" "!**/config/bootstrap_config.py"
    "--glob" "!**/config/env_substitutor.py"
    "--glob" "!**/config/config_validator.py"
    "--glob" "!**/__main__.py"
    "--glob" "!**/cli.py"
    "--glob" "!**/cli/main.py"
    "--glob" "!**/monitor.py"
    "--glob" "!**/orchestrator.py"
    "--glob" "!**/dependencies.py"
    "--glob" "!**/smoke_test_framework.py"
    "--glob" "!**/deployment_config.py"
    "--glob" "!**/shard_builder.py"
    "--glob" "!**/deployment/worker_manager.py"
    "--glob" "!**/cleanup_old_instruments_parquet.py"
)

# §2.10 — deferred imports in backends/ (boto3, google.cloud) for optional multi-cloud SDK loading
#          and __main__.py deferred startup imports (uvicorn, cli) to avoid circular imports
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "--glob" "!**/backends/**"
    "--glob" "!**/__main__.py"
    "--glob" "!**/api/routes/**"
    "--glob" "!**/calculators/shard_dimensions.py"
)

# §2.8 — .get("key", "") defensive defaults for optional JSON fields in deserialized payloads
EMPTY_STR_EXCLUDE_GLOBS=(
    "--glob" "!**/smoke_test_framework.py"
    "--glob" "!**/services/log_service.py"
    "--glob" "!**/calculators/shard_distribution.py"
    "--glob" "!**/deployment/runtime_topology_validator.py"
    "--glob" "!**/cli/handlers/reporting_handler.py"
    "--glob" "!**/deployment/state.py"
    "--glob" "!**/cli/handlers/maintenance_handler.py"
    "--glob" "!**/services/status_service.py"
    "--glob" "!**/calculators/calculation_handler.py"
    "--glob" "!**/cli/utils/data_status_display_dynamic.py"
    "--glob" "!**/cli/utils/data_status_checkers.py"
    "--glob" "!**/cli/commands/calculation.py"
    "--glob" "!**/calculators/shard_dimensions.py"
    "--glob" "!**/config/config_validator.py"
    "--glob" "!**/shard_builder.py"
)

# §2.4 GROUP B — GCP_PROJECT_ID used in bootstrap/template contexts, error messages,
#                and AliasChoices pydantic field validation (not os.environ direct access)
GCP_PROJECT_ID_EXCLUDE_GLOBS=(
    "--glob" "!**/smoke_test_framework.py"
    "--glob" "!**/deployment_config.py"
    "--glob" "!**/dependencies.py"
    "--glob" "!**/shard_builder.py"
    "--glob" "!**/backends/services/vm_lifecycle.py"
    "--glob" "!**/backends/services/vm_config.py"
    "--glob" "!**/config_loader.py"
    "--glob" "!**/cloud_client.py"
    "--glob" "!**/cli/commands/calculation.py"
)

# §2.1 — tests/, configs/, functions/ excluded from file/function size checks
# §2.9 — deployment_service/ production orchestration files have JUSTIFIED size violations
#         (complex orchestration, cloud-backend abstraction, CLI data-status display)
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "!" "-path" "./tests/*"
    "!" "-path" "./configs/*"
    "!" "-path" "./functions/*"
    "!" "-path" "./deployment_service/*"
)

source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
