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

# ── CODEX COMPLIANCE EXCLUSIONS ────────────────────────────────────────────────
# All exclusions are documented in QUALITY_GATE_BYPASS_AUDIT.md §2.11+

# print() — Rich console.print() in progress display (not Python built-in print);
# vm_config.py bash heredoc strings contain 'print(...)' in embedded python3 -c snippets
# (these are bash commands, not Python source — the check sees them as print() calls).
PRINT_EXCLUDE_GLOBS=(
    "--glob=!**/deployment/progress.py"
    "--glob=!**/backends/services/vm_config.py"
)

# os.getenv()/os.environ — documented bootstrap exceptions:
# - bootstrap_config.py: topology path resolution before UnifiedCloudConfig exists (bootstrap)
# - env_substitutor.py: intentional full env snapshot for template variable substitution
# - shard_builder.py: "os.environ" only in a docstring (no actual os.environ call in code)
# - deployment_config.py: "os.environ" only in a comment (AliasChoices bootstrap doc)
# - deployment/worker_manager.py: "os.environ" only in a comment explaining env injection
OS_ENV_EXCLUDE_GLOBS=(
    "--glob=!**/config/bootstrap_config.py"
    "--glob=!**/config/env_substitutor.py"
    "--glob=!**/shard_builder.py"
    "--glob=!**/deployment_config.py"
    "--glob=!**/deployment/worker_manager.py"
)

# Imports inside functions — multiple justified deferred-import cases:
# - __main__.py: uvicorn/FastAPI imported only in --serve mode (avoids heavy import on CLI)
# - api/routes/state.py: deferred imports avoid circular deps at startup; lazy backend load
# - calculators/shard_dimensions.py: TYPE_CHECKING block (module-level, not a function)
# - backends/: cloud SDK deferred imports (see §2.10) — aws_ec2/aws_batch/aws/gcp_sdk/etc.
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!**/__main__.py"
    "!**/api/routes/state.py"
    "!**/calculators/shard_dimensions.py"
    "!**/backends/**"
)

# Empty string fallback — shard_builder.py uses .get("category", "") intentionally:
# an absent "category" dimension is a valid state (it means no per-category bucket suffix).
EMPTY_STR_EXCLUDE_GLOBS=(
    "--glob=!**/shard_builder.py"
)

# GCP_PROJECT_ID — appears in: docstrings/comments (smoke_test_framework.py, cloud_client.py,
# shard_builder.py, cli/), Pydantic AliasChoices bootstrap (deployment_config.py), template
# string substitution (backends/services/vm_lifecycle.py, backends/services/vm_config.py),
# and ValueError message (dependencies.py, config_loader.py). None are env var accesses.
GCP_PROJECT_ID_EXCLUDE_GLOBS=(
    "!**/smoke_test_framework.py"
    "!**/deployment_config.py"
    "!**/dependencies.py"
    "!**/config_loader.py"
    "!**/cloud_client.py"
    "!**/shard_builder.py"
    "!**/cli/commands/calculation.py"
    "!**/backends/services/vm_lifecycle.py"
    "!**/backends/services/vm_config.py"
)

# File/function size — test files and configs/generate_topology_svg.py are not
# production service code; complex orchestrators are justified (see §2.1, §2.11).
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "! -path ./tests/*"
    "! -path ./configs/*"
)

source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
