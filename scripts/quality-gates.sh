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

# ── CODEX EXCLUSION CONFIG ─────────────────────────────────────────────────────
# All exclusions documented in QUALITY_GATE_BYPASS_AUDIT.md §2.11–2.16
#
# §2.11 — print(): Rich console.print() and bash heredoc template strings
# deployment/progress.py: Rich console.print() — terminal progress display (not Python print)
# backends/services/vm_config.py: print() inside bash heredoc strings (shell one-liners, not Python calls)
PRINT_EXCLUDE_GLOBS=(
    "--glob=!**/deployment/progress.py"
    "--glob=!**/backends/services/vm_config.py"
)

# §2.12 — os.getenv/os.environ: bootstrap and env-substitutor boundary files
# config/bootstrap_config.py: reads RUNTIME_TOPOLOGY_PATH/WORKSPACE_ROOT before UnifiedCloudConfig is constructed
# config/env_substitutor.py: intentional full env snapshot for ${VAR} template rendering
# shard_builder.py: word "os.environ" in docstring only — no actual call
# deployment_config.py: word "os.environ" in comment only — no actual call
# deployment/worker_manager.py: word "os.environ" in comment only — no actual call
OS_ENV_EXCLUDE_GLOBS=(
    "--glob=!**/config/bootstrap_config.py"
    "--glob=!**/config/env_substitutor.py"
    "--glob=!**/shard_builder.py"
    "--glob=!**/deployment_config.py"
    "--glob=!**/deployment/worker_manager.py"
)

# §2.13 — Imports inside functions: __main__.py lazy load + backends/ deferred cloud SDK imports
# __main__.py: defers uvicorn/app import to avoid heavy startup cost in CLI-only mode
# api/routes/state.py: deferred to avoid circular import chains at module load time
# calculators/shard_dimensions.py: TYPE_CHECKING block — false positive (indented but not inside function)
# backends/**: cloud SDK boundary layer; deferred import is the correct lazy-loading pattern
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!**/__main__.py"
    "!**/api/routes/state.py"
    "!**/calculators/shard_dimensions.py"
    "!**/backends/**"
)

# §2.14 — Empty string fallback: shard_builder.py optional category dimension
# dimensions.get("category", "") is semantically correct — absent category = no suffix (not a bug)
EMPTY_STR_EXCLUDE_GLOBS=(
    "!**/shard_builder.py"
)

# §2.15 — GCP_PROJECT_ID: all usages are docstrings, error messages, template substitutions, or pydantic AliasChoices
# None of these files call os.getenv("GCP_PROJECT_ID") — all are false positives from rg matching non-call contexts
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

# §2.16 — File/function size: tests/ and configs/ are excluded (see §2.1)
# tests/: test scaffolding — long test methods are expected and do not indicate production code quality issues
# configs/: developer tooling including generate_topology_svg.py (974 lines, graphviz data — see §2.1)
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
