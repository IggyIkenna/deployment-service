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
LOCAL_DEPS=("../deployment-api")
MAX_DURATION=240  # extended: google-cloud-run/compute adds typecheck overhead
# deployment/progress.py uses Rich console.print (not stdlib); vm_config.py embeds shell print(json) in multiline strings
PRINT_EXCLUDE_GLOBS=("!**/deployment/progress.py" "!**/backends/services/vm_config.py")
# bootstrap_config.py uses os.environ for WORKSPACE_ROOT/RUNTIME_TOPOLOGY_PATH (bootstrap exception)
# env_substitutor.py passes os.environ as a mapping object (not os.getenv() calls)
# shard_builder.py references os.environ only in a docstring
OS_ENV_EXCLUDE_GLOBS=("!**/config/bootstrap_config.py" "!**/config/env_substitutor.py" "!**/shard_builder.py")
# __main__.py lazy-imports uvicorn/app/cli to avoid circular deps at module level
# backends/base.py: ShardEvent is under TYPE_CHECKING + one late import inside method (circular dep)
# backends/aws_ec2.py: boto3 deferred at function level (AWS deployment boundary; avoids import at top)
# api/routes/state.py: ShardCalculator/DataCatalog/VMBackend deferred inside endpoint to avoid circular init
# Docstring false positives, TYPE_CHECKING blocks, and deferred circular-import patterns:
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!**/__main__.py"
    "!**/backends/base.py"
    "!**/backends/aws.py"
    "!**/backends/aws_ec2.py"
    "!**/backends/aws_batch.py"
    "!**/backends/provider_factory.py"
    "!**/backends/_gcp_sdk.py"
    "!**/api/routes/state.py"
    "!**/calculators/shard_dimensions.py"
)
# shard_builder.py: dimensions.get("category", "").upper() is a safe sentinel, not an API fallback
EMPTY_STR_EXCLUDE_GLOBS=("!**/shard_builder.py")
# GCP_PROJECT_ID used throughout as a config-resolved env-var name (not raw os.getenv); config.py already excluded
GCP_PROJECT_ID_EXCLUDE_GLOBS=(
    "!**/smoke_test_framework.py"
    "!**/dependencies.py"
    "!**/config_loader.py"
    "!**/cloud_client.py"
    "!**/backends/services/vm_lifecycle.py"
    "!**/backends/services/vm_config.py"
    "!**/cli/commands/calculation.py"
    "!**/deployment_config.py"
    "!**/deployment/worker_manager.py"
    "!**/shard_builder.py"
)
# Tests and configs contain inherently large files (integration test suites, SVG generator)
FUNCTION_SIZE_EXTRA_EXCLUDES=("! -path ./tests/*" "! -path ./configs/*")
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
