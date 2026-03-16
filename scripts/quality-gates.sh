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

# --- Codex exclusion overrides ---
# print(): console.print() is rich library; vm_config.py has python3 -c "print()" in bash templates
# (PRINT_EXCLUDE_GLOBS expanded directly — needs --glob pairs)
PRINT_EXCLUDE_GLOBS=(
    --glob "!**/progress.py"
    --glob "!**/vm_config.py"
)
# os.environ: bootstrap_config.py is approved bootstrap exception (see codex 06 bootstrap-phase)
#             worker_manager.py + shard_builder.py + env_substitutor.py reference os.environ in comments/docstrings
# (OS_ENV_EXCLUDE_GLOBS expanded directly — needs --glob pairs)
OS_ENV_EXCLUDE_GLOBS=(
    --glob "!**/bootstrap_config.py"
    --glob "!**/worker_manager.py"
    --glob "!**/shard_builder.py"
    --glob "!**/env_substitutor.py"
    --glob "!**/deployment_config.py"
)
# Imports inside functions: API routes use lazy imports to avoid circular deps
# (IMPORT_INSIDE_EXCLUDE_GLOBS: loop adds --glob — patterns only)
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!**/api/routes/**"
    "!**/__main__.py"
    "!**/calculators/shard_dimensions.py"  # deferred imports to avoid circular deps
    "!**/backends/aws.py"                  # provider-conditional boto3 import
    "!**/backends/aws_batch.py"            # provider-conditional boto3 import
)
# GCP_PROJECT_ID: used legitimately in string templates, docstrings, config loaders, CLI help text
# (GCP_PROJECT_ID_EXCLUDE_GLOBS: loop adds --glob — patterns only)
GCP_PROJECT_ID_EXCLUDE_GLOBS=(
    "!**/vm_config.py"
    "!**/vm_lifecycle.py"
    "!**/calculation.py"
    "!**/config_loader.py"
    "!**/shard_builder.py"
    "!**/smoke_test_framework.py"
    "!**/deployment_config.py"
    "!**/dependencies.py"
    "!**/cloud_client.py"
)

# File/function size: SVG generator exceeds 900L limit (pre-existing)
FUNCTION_SIZE_EXTRA_EXCLUDES=("!" "-path" "./configs/*")

WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
