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
# Deployment orchestration has inherently large functions:
# - Cloud backends deploy_shard(): 90-324L (API calls, retries, error handling)
# - Worker managers launch_shards_*(): 232-502L (parallel shard dispatch + monitoring)
# - CLI display functions: 200-293L (rich terminal output construction)
# Standard services use 50L; deployment-service needs higher due to orchestration complexity.
MAX_FUNCTION_LINES=510
MAX_METHOD_LINES=510

# Per-repo QG exclusions (see base-service.sh for variable documentation)
# print(): Rich console.print in progress.py, bash heredoc print in vm_config.py
PRINT_EXCLUDE_GLOBS=(--glob "!**/deployment/progress.py" --glob "!**/backends/services/vm_config.py")
# os.environ: bootstrap_config.py (documented exception), worker_manager.py + shard_builder.py (comments only)
OS_ENV_EXCLUDE_GLOBS=(--glob "!**/config/bootstrap_config.py" --glob "!**/deployment/worker_manager.py" --glob "!**/config/env_substitutor.py" --glob "!**/shard_builder.py" --glob "!**/deployment_config.py")
# Imports inside functions: deferred for CLI entry, route handlers, cloud SDK lazy loading, AWS boundary
IMPORT_INSIDE_EXCLUDE_GLOBS=("!**/__main__.py" "!**/api/routes/state.py" "!**/backends/aws_ec2.py" "!**/backends/aws.py" "!**/backends/aws_batch.py" "!**/backends/_gcp_sdk.py" "!**/backends/base.py" "!**/calculators/shard_dimensions.py" "!**/backends/provider_factory.py" "!**/cli/handlers/cluster_handler.py")
# Empty string/dict/list: safe defaults for optional config dict lookups
EMPTY_STR_EXCLUDE_GLOBS=(--glob "!**/config_loader.py" --glob "!**/shard_calculator.py" --glob "!**/dependencies.py")
EMPTY_DICT_LIST_EXCLUDE_GLOBS=(--glob "!**/config_loader.py" --glob "!**/shard_calculator.py" --glob "!**/dependencies.py" --glob "!**/orchestrator.py")
# GCP_PROJECT_ID: docstrings, template substitution strings, error messages, bash heredocs
GCP_PROJECT_ID_EXCLUDE_GLOBS=("!**/cli/commands/calculation.py" "!**/smoke_test_framework.py" "!**/deployment_config.py" "!**/config_loader.py" "!**/cloud_client.py" "!**/backends/services/vm_config.py" "!**/dependencies.py" "!**/shard_builder.py" "!**/backends/services/vm_lifecycle.py")
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
