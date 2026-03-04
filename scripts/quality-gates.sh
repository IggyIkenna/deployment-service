#!/usr/bin/env bash
#
# CANONICAL QUALITY GATES — unified-trading-system
#
# This is the single source of truth for all repos. Copy to scripts/quality-gates.sh
# and replace REPO_MODULE with the actual Python module name (e.g. my_service).
#
# Usage:
#   ./scripts/quality-gates.sh              # All checks (with auto-fix)
#   ./scripts/quality-gates.sh --lint       # Linting only (with auto-fix)
#   ./scripts/quality-gates.sh --test       # Tests only
#   ./scripts/quality-gates.sh --quick      # Unit tests only (fast)
#   ./scripts/quality-gates.sh --no-fix     # Skip auto-fix (CI mode)
#   ./scripts/quality-gates.sh --skip-tests # Skip pytest (lint+type+codex only)
#   ./scripts/quality-gates.sh --skip-typecheck  # Skip basedpyright only
#
# Pipeline: env-setup → lint → import-check → type-check → tests → codex → ci-validators
#
# Requirements:
#   - Python 3.13 (>=3.13,<3.14)
#   - ruff, basedpyright, pytest, pytest-asyncio, pytest-mock, pytest-xdist, ripgrep
#   - unified-trading-services available (local or via GH_PAT)
#
# Codex: unified-trading-codex/06-coding-standards/README.md
#
MIN_COVERAGE=70
REPO_MODULE="deployment_service"
REPO_ARCH_TIER="devops"

set -e

# ── PORTABLE TIMEOUT ──────────────────────────────────────────────────────────
# Works on macOS (no coreutils), Linux, and CI. Perl is always available.
run_timeout() {
    local secs=$1; shift
    if command -v gtimeout &>/dev/null; then gtimeout "$secs" "$@"
    elif command -v timeout &>/dev/null; then timeout "$secs" "$@"
    elif command -v perl &>/dev/null; then perl -e 'alarm shift; exec @ARGV' -- "$secs" "$@"
    else "$@"; fi
}

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_fail()    { echo -e "${RED}❌ $1${NC}"; }
log_warn()    { echo -e "${YELLOW}⚠️  $1${NC}"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$PROJECT_ROOT")"

cd "$PROJECT_ROOT"

# ============================================================================
# ENSURE ENVIRONMENT (venv + uv + deps) — skip in CI
# ============================================================================
if [ -z "${GITHUB_ACTIONS:-}" ] && [ -z "${CI:-}" ] && [ -z "${CLOUD_BUILD:-}" ]; then
    command -v uv &>/dev/null || pip install uv --quiet
    if [ -f "pyproject.toml" ]; then
        uv lock 2>/dev/null || true
        if [ -f "uv.lock" ] && ! git diff --quiet uv.lock 2>/dev/null; then
            log_warn "uv.lock was updated — include it in your commit."
        fi
    fi
    if [ ! -d ".venv" ]; then
        log_warn "Creating .venv..."
        uv venv .venv
    fi
    if [ -f ".venv/bin/activate" ]; then
        source .venv/bin/activate
    elif [ -f ".venv/Scripts/activate" ]; then
        source .venv/Scripts/activate
    fi
    command -v uv &>/dev/null || pip install uv --quiet
    if [ -f "pyproject.toml" ]; then
        # Install workspace path deps first (local dev parity)
        for dep_dir in "../unified-trading-library" "../unified-config-interface"; do
            if [ -d "$dep_dir" ] && [ -f "$dep_dir/pyproject.toml" ]; then
                uv pip install -e "$dep_dir" --quiet 2>/dev/null || true
            fi
        done
        uv pip install -e ".[dev]" --quiet 2>/dev/null || uv pip install -e . --quiet 2>/dev/null || {
            log_fail "uv pip install failed — dev deps required for quality gates"
            exit 1
        }
    fi
fi

# Python binary
if [ -f ".venv/bin/python" ]; then
    PYTHON_CMD=".venv/bin/python"
elif [ -f ".venv/Scripts/python.exe" ]; then
    PYTHON_CMD=".venv/Scripts/python.exe"
elif command -v python &>/dev/null && python -c "import sys; exit(0 if sys.version_info >= (3,13) else 1)" 2>/dev/null; then
    PYTHON_CMD="python"
else
    PYTHON_CMD="python3"
fi
PYTHON_VERSION="$($PYTHON_CMD --version 2>&1)"

REPO_NAME=$(basename "$PROJECT_ROOT")
echo -e "${BLUE}======================================================================${NC}"
echo -e "${BLUE}$(echo "$REPO_NAME" | tr '[:lower:]' '[:upper:]') QUALITY GATES${NC}"
echo -e "${BLUE}======================================================================${NC}"
echo -e "Project: ${PROJECT_ROOT}"
echo -e "Python:  $PYTHON_VERSION (using: $PYTHON_CMD)"
echo ""

# ── FLAGS ────────────────────────────────────────────────────────────────────
RUN_LINT=true
RUN_TESTS=true
QUICK_MODE=false
AUTO_FIX=true
SKIP_TESTS=false
SKIP_TYPECHECK=false

for arg in "$@"; do
    case $arg in
        --lint)           RUN_LINT=true; RUN_TESTS=false ;;
        --test)           RUN_LINT=false; RUN_TESTS=true ;;
        --quick)          QUICK_MODE=true ;;
        --no-fix)         AUTO_FIX=false ;;
        --fix)            AUTO_FIX=true ;;
        --skip-tests)     SKIP_TESTS=true ;;
        --skip-typecheck) SKIP_TYPECHECK=true ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo "  --lint           Linting only (with auto-fix)"
            echo "  --test           Tests only"
            echo "  --quick          Unit tests only (faster)"
            echo "  --no-fix         Skip auto-fix (CI mode)"
            echo "  --skip-tests     Skip pytest (lint+type+codex only)"
            echo "  --skip-typecheck Skip basedpyright only"
            exit 0 ;;
    esac
done

[ "$SKIP_TESTS" = true ] && RUN_TESTS=false

# ── STATUS TRACKERS ──────────────────────────────────────────────────────────
LINT_STATUS=0
IMPORT_STATUS=0
PYRIGHT_STATUS=0
TEST_STATUS=0
CODEX_STATUS=0
CONFIG_STATUS=0

# ── GIT-AWARE MODE ───────────────────────────────────────────────────────────
# When files are staged (quickmerge --files), check ONLY staged .py files
SOURCE_DIRS="${REPO_MODULE}/ tests/"
STAGED_PY_FILES=$(git diff --cached --name-only --diff-filter=ACMR 2>/dev/null | grep '\.py$' | tr '\n' ' ' || true)
if [ -n "$STAGED_PY_FILES" ]; then
    FILE_COUNT=$(echo "$STAGED_PY_FILES" | wc -w | tr -d ' ')
    SOURCE_DIRS="$STAGED_PY_FILES"
    log_warn "Git-aware mode: Checking ONLY staged files ($FILE_COUNT files)"
fi

# ============================================================================
# STEP 0: ENVIRONMENT & CONFIG VALIDATION
# ============================================================================
echo -e "\n${BLUE}[0/6] ENVIRONMENT & CONFIG VALIDATION${NC}"
echo "----------------------------------------------------------------------"

# Python 3.13 runtime check
REQUIRED_PYTHON="3.13"
ACTUAL_PYTHON=$($PYTHON_CMD --version 2>&1 | awk '{print $2}' | cut -d'.' -f1,2)
if [[ "$ACTUAL_PYTHON" == "$REQUIRED_PYTHON"* ]] || [[ "$ACTUAL_PYTHON" > "3.12" ]]; then
    log_success "Python $ACTUAL_PYTHON"
else
    log_fail "Python $REQUIRED_PYTHON required, found $ACTUAL_PYTHON"
    CONFIG_STATUS=1
fi

# uv.lock existence
if [[ ! -f "uv.lock" ]]; then
    log_warn "uv.lock missing (run: uv lock)"
else
    log_success "uv.lock found"
fi

# ripgrep required for codex compliance
if ! command -v rg &>/dev/null; then
    log_fail "ripgrep (rg) required — brew install ripgrep (macOS) / apt install ripgrep (Linux)"
    CONFIG_STATUS=1
else
    log_success "ripgrep available"
fi

# Ruff version check
RUFF_CMD="ruff"
[ -f ".venv/bin/ruff" ] && RUFF_CMD=".venv/bin/ruff"
RUFF_VER=$($RUFF_CMD --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "0.0.0")
if [[ "$RUFF_VER" == "0.15.0" ]]; then
    log_success "Ruff $RUFF_VER"
else
    log_warn "Ruff 0.15.0 expected, found $RUFF_VER"
fi

# cloudbuild.yaml shell variable escaping
if [ -f "cloudbuild.yaml" ]; then
    UNESCAPED=$(grep -E '\$PYTEST_EXIT|\$\?' cloudbuild.yaml | grep -v '\$\$' || true)
    if [ -n "$UNESCAPED" ]; then
        log_fail "cloudbuild.yaml has unescaped shell variables (use \$\$ instead of \$)"
        echo "$UNESCAPED"
        CONFIG_STATUS=1
    else
        log_success "cloudbuild.yaml shell variables properly escaped"
    fi
else
    log_warn "No cloudbuild.yaml found (skipping)"
fi

# pyproject.toml Python version
if [ -f "pyproject.toml" ]; then
    PY_REQ=$(grep 'requires-python' pyproject.toml | head -1)
    if echo "$PY_REQ" | grep -qE '>=3\.1[123]'; then
        log_success "pyproject.toml Python version: $PY_REQ"
    else
        log_fail "pyproject.toml Python version may be incorrect: $PY_REQ (expected >=3.13,<3.14)"
        CONFIG_STATUS=1
    fi
fi

# ============================================================================
# STEP 1: AUTO-FIX (ruff format + ruff check --fix)
# ============================================================================
if [ "$RUN_LINT" = true ] && [ "$AUTO_FIX" = true ]; then
    echo -e "\n${BLUE}[1/6] AUTO-FIX (ruff format + ruff check --fix)${NC}"
    echo "----------------------------------------------------------------------"
    if ! command -v ruff &>/dev/null; then
        command -v uv &>/dev/null && uv pip install ruff==0.15.0 --quiet || pip install ruff==0.15.0 --quiet
    fi
    echo "Running: ruff format --line-length 120 $SOURCE_DIRS"
    ruff format --line-length 120 $SOURCE_DIRS
    echo "Running: ruff check --fix --line-length 120 $SOURCE_DIRS"
    ruff check --fix --line-length 120 $SOURCE_DIRS
    log_success "Auto-fix complete"
fi

# ============================================================================
# STEP 2: LINTING (ruff check)
# ============================================================================
if [ "$RUN_LINT" = true ]; then
    echo -e "\n${BLUE}[2/6] LINTING (ruff)${NC}"
    echo "----------------------------------------------------------------------"
    if ! command -v ruff &>/dev/null; then
        command -v uv &>/dev/null && uv pip install ruff==0.15.0 --quiet || pip install ruff==0.15.0 --quiet
    fi
    echo "Running: ruff check --line-length 120 $SOURCE_DIRS"
    if ruff check --line-length 120 $SOURCE_DIRS; then
        log_success "Linting PASSED"
    else
        log_fail "Linting FAILED"
        LINT_STATUS=1
    fi
fi

# ============================================================================
# STEP 3: IMPORT PATTERN STANDARDS
# ============================================================================
if [ "$RUN_LINT" = true ]; then
    echo -e "\n${BLUE}[3/6] IMPORT PATTERN STANDARDS${NC}"
    echo "----------------------------------------------------------------------"
    CHECK_IMPORT_SCRIPT=""
    [ -f ".cursor/scripts/check-import-patterns.py" ] && CHECK_IMPORT_SCRIPT=".cursor/scripts/check-import-patterns.py"
    [ -z "$CHECK_IMPORT_SCRIPT" ] && [ -f "scripts/check-import-patterns.py" ] && CHECK_IMPORT_SCRIPT="scripts/check-import-patterns.py"
    [ -z "$CHECK_IMPORT_SCRIPT" ] && [ -f "${REPO_ROOT}/unified-trading-pm/scripts/check-import-patterns.py" ] && CHECK_IMPORT_SCRIPT="${REPO_ROOT}/unified-trading-pm/scripts/check-import-patterns.py"
    if [ -n "$CHECK_IMPORT_SCRIPT" ]; then
        echo "Checking external import patterns..."
        if "$PYTHON_CMD" "$CHECK_IMPORT_SCRIPT" --verbose; then
            log_success "Import patterns: PASSED"
        else
            log_fail "Import patterns: FAILED"
            echo "Auto-fix with: $PYTHON_CMD $CHECK_IMPORT_SCRIPT --fix"
            IMPORT_STATUS=1
        fi
    else
        log_warn "Import pattern checker not found (skipping)"
    fi
fi

# ============================================================================
# STEP 4: TYPE CHECKING (basedpyright — skip with --skip-typecheck)
# ============================================================================
if [ "$RUN_LINT" = true ] && [ "$SKIP_TYPECHECK" != true ]; then
    echo -e "\n${BLUE}[4/6] TYPE CHECKING (basedpyright)${NC}"
    echo "----------------------------------------------------------------------"

    BASEDPYRIGHT_CMD=""
    [ -f ".venv/bin/basedpyright" ]              && BASEDPYRIGHT_CMD=".venv/bin/basedpyright"
    [ -z "$BASEDPYRIGHT_CMD" ] && [ -f ".venv/Scripts/basedpyright.exe" ] && BASEDPYRIGHT_CMD=".venv/Scripts/basedpyright.exe"
    [ -z "$BASEDPYRIGHT_CMD" ] && command -v basedpyright &>/dev/null      && BASEDPYRIGHT_CMD="basedpyright"
    if [ -z "$BASEDPYRIGHT_CMD" ]; then
        log_warn "basedpyright not found — installing..."
        command -v uv &>/dev/null && uv pip install "basedpyright>=1.20.0" --quiet || pip install "basedpyright>=1.20.0" --quiet
        command -v basedpyright &>/dev/null && BASEDPYRIGHT_CMD="basedpyright" || BASEDPYRIGHT_CMD="$PYTHON_CMD -m basedpyright"
    fi

    export BASEDPYRIGHT_CACHE_DIR="${TMPDIR:-/tmp}/basedpyright-cache/${REPO_NAME}"
    mkdir -p "$BASEDPYRIGHT_CACHE_DIR"
    echo "Running: run_timeout 120 $BASEDPYRIGHT_CMD ${REPO_MODULE}/"
    if run_timeout 120 $BASEDPYRIGHT_CMD "${REPO_MODULE}/" 2>&1 | tee /tmp/pyright_qg.log; then
        log_success "Type checking PASSED"
        PYRIGHT_STATUS=0
    else
        log_fail "Type checking FAILED"
        grep -E "error|warning" /tmp/pyright_qg.log | head -10
        PYRIGHT_STATUS=1
    fi
    rm -f /tmp/pyright_qg.log
elif [ "$SKIP_TYPECHECK" = true ]; then
    log_warn "Type checking SKIPPED (--skip-typecheck)"
fi

# ============================================================================
# STEP 5: TESTS (pytest with coverage)
# ============================================================================
if [ "$RUN_TESTS" = true ]; then
    echo -e "\n${BLUE}[5/6] TESTS (pytest)${NC}"
    echo "----------------------------------------------------------------------"

    if ! $PYTHON_CMD -c "import pytest" &>/dev/null; then
        command -v uv &>/dev/null \
            && uv pip install pytest pytest-asyncio pytest-mock pytest-timeout pytest-xdist --quiet \
            || pip install pytest pytest-asyncio pytest-mock pytest-timeout pytest-xdist --quiet
    elif ! $PYTHON_CMD -c "import xdist" &>/dev/null; then
        command -v uv &>/dev/null && uv pip install "pytest-xdist>=3.6.0" --quiet || pip install "pytest-xdist>=3.6.0" --quiet
    fi

    # Parallel execution (75% CPU cores for unit tests; 30% for integration)
    if $PYTHON_CMD -c "import xdist" &>/dev/null; then
        CPU_CORES=$($PYTHON_CMD -c "import os; print(max(1, int((os.cpu_count() or 2) * 3 // 4)))")
        PARALLEL_FLAG="-n $CPU_CORES"
        log_success "Using parallel execution with $CPU_CORES workers (pytest-xdist)"
    else
        PARALLEL_FLAG=""
        log_warn "pytest-xdist not available, running tests sequentially"
    fi

    export CLOUD_MOCK_MODE="true"
    export GCP_PROJECT_ID="test-project"
    export DEPLOYMENT_CONFIG_DIR="${REPO_ROOT}/unified-trading-deployment-v3/configs"

    TIMEOUT_ARG=""
    $PYTHON_CMD -c "import pytest_timeout" 2>/dev/null && TIMEOUT_ARG="--timeout=60"

    COV_ARGS="--cov=${REPO_MODULE} --cov-report=term-missing --cov-fail-under=${MIN_COVERAGE}"

    if [ "$QUICK_MODE" = true ]; then
        echo "Running: pytest tests/unit/ -v --tb=short -m 'not slow' $COV_ARGS $PARALLEL_FLAG"
        if $PYTHON_CMD -m pytest tests/unit/ -v --tb=short -m "not slow" $COV_ARGS $PARALLEL_FLAG; then
            log_success "Unit tests PASSED"
        else
            log_fail "Unit tests FAILED"
            TEST_STATUS=1
        fi
    else
        # Unit tests
        echo -e "\n${YELLOW}Running unit tests (min ${MIN_COVERAGE}% coverage)...${NC}"
        if [ -d "tests/unit" ]; then
            if $PYTHON_CMD -m pytest tests/unit/ -v --tb=short $COV_ARGS $TIMEOUT_ARG $PARALLEL_FLAG; then
                log_success "Unit tests PASSED"
            else
                log_fail "Unit tests FAILED"
                TEST_STATUS=1
            fi
        else
            log_warn "No tests/unit/ directory found"
        fi

        # Integration tests (fewer workers — avoid resource contention)
        if [ -d "tests/integration" ]; then
            echo -e "\n${YELLOW}Running integration tests...${NC}"
            if [ -n "$PARALLEL_FLAG" ]; then
                INTEG_WORKERS=$($PYTHON_CMD -c "import os; print(max(2, min(3, int((os.cpu_count() or 2) * 0.3))))")
                INTEG_PARALLEL="-n $INTEG_WORKERS"
            else
                INTEG_PARALLEL=""
            fi
            set +e
            $PYTHON_CMD -m pytest tests/integration/ -v --tb=short --timeout=120 --no-cov \
                -k "not api and not live and not download" \
                -m "not requires_databento" \
                $INTEG_PARALLEL
            INTEG_EXIT=$?
            set -e
            if   [ $INTEG_EXIT -eq 0 ]; then log_success "Integration tests PASSED"
            elif [ $INTEG_EXIT -eq 5 ]; then log_warn "No integration tests ran (api/databento excluded)"
            else log_fail "Integration tests FAILED"; TEST_STATUS=1; fi
        fi

        # E2E tests
        if [ -d "tests/e2e" ]; then
            echo -e "\n${YELLOW}Running e2e tests...${NC}"
            E2E_PARALLEL="${INTEG_PARALLEL:-}"
            if $PYTHON_CMD -m pytest tests/e2e/ -v --tb=short --timeout=180 $E2E_PARALLEL; then
                log_success "E2E tests PASSED"
            else
                log_fail "E2E tests FAILED"
                TEST_STATUS=1
            fi
        fi

        # Smoke tests
        if [ -d "tests/smoke" ]; then
            echo -e "\n${YELLOW}Running smoke tests...${NC}"
            if $PYTHON_CMD -m pytest tests/smoke/ -v --tb=short --timeout=180 $PARALLEL_FLAG; then
                log_success "Smoke tests PASSED"
            else
                log_fail "Smoke tests FAILED"
                TEST_STATUS=1
            fi
        fi
    fi

    # Import sanity check
    echo -e "\n${YELLOW}Running import sanity check...${NC}"
    if $PYTHON_CMD -c "import ${REPO_MODULE}; print('✅ Top-level import works')" 2>/dev/null; then
        log_success "Import check PASSED"
    else
        log_fail "Import check FAILED — this would break dependent services!"
        TEST_STATUS=1
    fi
fi

# ============================================================================
# STEP 6: CODEX COMPLIANCE
# ============================================================================
echo -e "\n${BLUE}[6/6] CODEX COMPLIANCE${NC}"
echo "----------------------------------------------------------------------"

CODEX_VIOLATIONS=0

if ! command -v rg &>/dev/null; then
    log_fail "ripgrep (rg) required for codex compliance"
    exit 1
fi

# Derive prod source dir
SOURCE_DIR="${REPO_MODULE}/"
[[ "$SOURCE_DIRS" == *".py"* ]] && SOURCE_DIR="."

# Check 1: print() in production code
echo -n "Checking for print() statements... "
if rg "print\(" --type py --glob "!tests/**" --glob "!scripts/**" --glob "!examples/**" . >/dev/null 2>&1; then
    echo -e "${RED}FAIL${NC}"
    rg "print\(" --type py --glob "!tests/**" --glob "!scripts/**" --glob "!examples/**" . | head -5
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 2: os.getenv() — use config class instead
echo -n "Checking for os.getenv() usage... "
if rg "os\.getenv" --type py --glob "!tests/**" --glob "!scripts/**" . >/dev/null 2>&1; then
    echo -e "${RED}FAIL${NC}"
    rg "os\.getenv" --type py --glob "!tests/**" --glob "!scripts/**" . | head -5
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 3: datetime.now() without UTC
echo -n "Checking for datetime.now() without UTC... "
if rg "datetime\.now\(\)" --type py . >/dev/null 2>&1; then
    echo -e "${RED}FAIL${NC}"
    rg "datetime\.now\(\)" --type py . | head -5
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 4: Bare except
echo -n "Checking for bare except clauses... "
if rg "except:" --type py --glob "!tests/**" . >/dev/null 2>&1; then
    echo -e "${RED}FAIL${NC}"
    rg "except:" --type py --glob "!tests/**" . | head -5
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 5: google.cloud direct imports (use UCS abstractions)
echo -n "Checking for direct google.cloud imports in source... "
if rg "from google\.cloud import|import google\.cloud" --type py --glob "!tests/**" "$SOURCE_DIR" >/dev/null 2>&1; then
    echo -e "${RED}FAIL${NC}"
    rg "from google\.cloud import|import google\.cloud" --type py "$SOURCE_DIR" | head -5
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 6: Empty string fallbacks (required config must fail loud)
echo -n "Checking for empty string fallbacks... "
EMPTY_STR=$(rg '\.get\(["'"'"'][\w_]+["'"'"']\s*,\s*["'"'"']["'"'"']' --type py --glob "!tests/**" --glob "!scripts/**" "$SOURCE_DIR" 2>/dev/null || true)
ENV_EMPTY=$(rg 'os\.environ\.get\(["'"'"'][\w_]+["'"'"']\s*,\s*["'"'"']["'"'"']' --type py --glob "!tests/**" --glob "!scripts/**" "$SOURCE_DIR" 2>/dev/null || true)
if [ -n "$EMPTY_STR" ] || [ -n "$ENV_EMPTY" ]; then
    echo -e "${RED}FAIL${NC}"
    [ -n "$EMPTY_STR" ] && echo "$EMPTY_STR" | head -3
    [ -n "$ENV_EMPTY" ] && echo "$ENV_EMPTY" | head -3
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 7: Empty dict/list fallbacks
echo -n "Checking for empty dict/list fallbacks... "
EMPTY_DICT=$(rg '\.get\(["'"'"'][\w_-]+["'"'"']\s*,\s*\{\}' --type py --glob "!tests/**" --glob "!scripts/**" "$SOURCE_DIR" 2>/dev/null || true)
EMPTY_LIST=$(rg '\.get\(["'"'"'][\w_-]+["'"'"']\s*,\s*\[\]' --type py --glob "!tests/**" --glob "!scripts/**" "$SOURCE_DIR" 2>/dev/null || true)
if [ -n "$EMPTY_DICT" ] || [ -n "$EMPTY_LIST" ]; then
    echo -e "${RED}FAIL${NC}"
    [ -n "$EMPTY_DICT" ] && echo "$EMPTY_DICT" | head -3
    [ -n "$EMPTY_LIST" ] && echo "$EMPTY_LIST" | head -3
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 8: Imports inside functions (except whitelisted lazy-load files)
echo -n "Checking for imports inside functions... "
violations=$(rg "^[[:space:]]+import |^[[:space:]]+from .* import" --type py --glob "!tests/**" --glob "!scripts/**" \
    --glob "!**/__init__.py" --glob "!**/adapter_loader.py" --glob "!**/venue_adapter_loader.py" \
    --glob "!**/dependency_checker.py" --glob "!**/symbol_parser.py" --glob "!**/canonical_key_generator.py" \
    --glob "!**/main.py" --glob "!**/parser.py" --glob "!**/orchestrator.py" --glob "!**/reloader.py" \
    --glob "!**/loaders.py" \
    "$SOURCE_DIR" 2>/dev/null || true)
if [ -n "$violations" ]; then
    echo -e "${RED}FAIL${NC}"
    echo "$violations" | head -5
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 9: Any/object type usage
echo -n "Checking for Any/object type usage... "
ANY_USAGE=$(rg ': Any[^[]|-> Any[^[]' --type py --glob "!tests/**" --glob "!**/protocols.py" "$SOURCE_DIR" 2>/dev/null \
    | grep -v "dict\[str, Any\]" | grep -v "# type: ignore\[reportAny\]" || true)
if [ -n "$ANY_USAGE" ]; then
    echo -e "${RED}FAIL${NC}"
    echo "$ANY_USAGE" | head -5
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 10: requests library with async code (per-file check)
echo -n "Checking for requests library in async code... "
REQUESTS_VIOLATION=""
FILES_WITH_REQUESTS=$(rg "import\s+requests" --type py --glob "!scripts/**" -l . 2>/dev/null || true)
for f in $FILES_WITH_REQUESTS; do
    if rg "async\s+def" "$f" >/dev/null 2>&1; then
        REQUESTS_VIOLATION="$f"
        break
    fi
done
if [ -n "$REQUESTS_VIOLATION" ]; then
    echo -e "${RED}FAIL${NC}"
    log_warn "requests + async in same file (use aiohttp): $REQUESTS_VIOLATION"
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 11: asyncio.run() in loops
echo -n "Checking for asyncio.run() in loops... "
ASYNCIO_VIOLATION=false
FILES_WITH_ASYNCIO=$(rg "asyncio\.run\(" --type py --glob "!tests/**" --glob "!scripts/**" \
    --glob "!**/cli/**" --glob "!**/defi_processor.py" -l . 2>/dev/null || true)
for f in $FILES_WITH_ASYNCIO; do
    if grep -q "for \|while " "$f" 2>/dev/null; then
        echo -e "${RED}FAIL${NC}"
        log_warn "asyncio.run() in file with loops (use asyncio.gather()): $f"
        CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
        ASYNCIO_VIOLATION=true
        break
    fi
done
[ "$ASYNCIO_VIOLATION" = false ] && echo -e "${GREEN}PASS${NC}"

# Check 12: time.sleep() in async functions (indentation-aware)
echo -n "Checking for time.sleep() in async code... "
TIME_SLEEP_VIOLATION=false
FILES_WITH_SLEEP=$(rg "time\.sleep\(" --type py -l . 2>/dev/null || true)
for f in $FILES_WITH_SLEEP; do
    if grep -q "async def" "$f" 2>/dev/null; then
        if awk '
            /^[[:space:]]*async def/ { in_async=1; indent=match($0,/[^[:space:]]/)-1 }
            in_async && /time\.sleep\(/ {
                cur=match($0,/[^[:space:]]/)-1
                if (cur > indent) { print "VIOLATION"; exit }
            }
            /^[[:space:]]*def [^(]*\(/ && !/async/ {
                ni=match($0,/[^[:space:]]/)-1
                if (ni <= indent) in_async=0
            }
        ' "$f" | grep -q "VIOLATION"; then
            echo -e "${RED}FAIL${NC}"
            log_warn "time.sleep() inside async function (use asyncio.sleep()): $f"
            CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
            TIME_SLEEP_VIOLATION=true
            break
        fi
    fi
done
[ "$TIME_SLEEP_VIOLATION" = false ] && echo -e "${GREEN}PASS${NC}"

# Check 13: File size limit (max 1500 lines)
echo -n "Checking file size (max 1500 lines)... "
SIZE_VIOLATIONS=""
SIZE_WARNINGS=""
while IFS= read -r -d '' f; do
    lines=$(wc -l < "$f" 2>/dev/null || echo 0)
    if [ "$lines" -gt 1500 ]; then
        SIZE_VIOLATIONS="${SIZE_VIOLATIONS}\n  $f: $lines lines"
    elif [ "$lines" -gt 1200 ]; then
        SIZE_WARNINGS="${SIZE_WARNINGS}\n  $f: $lines lines (plan split)"
    fi
done < <(find . -name "*.py" ! -path "./.venv/*" ! -path "./deps/*" ! -path "./.git/*" ! -path "./build/*" ! -path "./scripts/*" -print0 2>/dev/null)
if [ -n "$SIZE_VIOLATIONS" ]; then
    echo -e "${RED}FAIL${NC}"
    echo -e "$SIZE_VIOLATIONS"
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi
[ -n "$SIZE_WARNINGS" ] && log_warn "Files near limit:$SIZE_WARNINGS"

# Check 14: .gitignore credential file negation
echo -n "Checking .gitignore for credential file negation... "
if [ -f ".gitignore" ] && rg "!central-element|!.*credentials.*\.json" .gitignore >/dev/null 2>&1; then
    echo -e "${RED}FAIL${NC}"
    rg "!central-element|!.*credentials.*\.json" .gitignore
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 15: Hardcoded real project ID in tests
echo -n "Checking for hardcoded project ID in tests... "
HARDCODED=$(rg "central-element-323112|get_config.*central-element" tests/ 2>/dev/null || true)
if [ -n "$HARDCODED" ]; then
    echo -e "${RED}FAIL${NC}"
    echo "$HARDCODED" | head -3
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 16: Swallowed errors (except Exception: pass / return None)
echo -n "Checking for swallowed errors... "
SWALLOWED=$(rg "except Exception:" --type py --glob "!tests/**" "$SOURCE_DIR" -A 2 2>/dev/null \
    | grep -E "^[[:space:]]+(pass|return None)$" || true)
if [ -n "$SWALLOWED" ]; then
    log_fail "Swallowed errors — use @handle_api_errors or re-raise"
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    log_success "No swallowed errors"
fi

# Check 17: Security — no service account JSON
SA_JSON=$(rg '"type"\s*:\s*"service_account"|"private_key_id"' --glob "*.json" . 2>/dev/null \
    | grep -v ".venv\|node_modules" || true)
if [ -n "$SA_JSON" ]; then
    log_fail "Service account JSON detected — use Secret Manager via UCS"
    echo "$SA_JSON" | head -3
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    log_success "No service account JSON"
fi

# Check 18: Security — no private keys embedded
PRIV_KEY=$(rg "BEGIN RSA PRIVATE KEY|BEGIN PRIVATE KEY|BEGIN EC PRIVATE KEY" \
    --type py --type sh --type yaml --glob "!tests/**" --glob "!**/quality-gates.sh" . 2>/dev/null || true)
if [ -n "$PRIV_KEY" ]; then
    log_fail "Private key in codebase — use Secret Manager"
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    log_success "No embedded private keys"
fi

# Check 19: Security — no secrets in Dockerfile ENV
DOCKER_SECRETS=$(rg "^ENV\s+[A-Z_]*(KEY|SECRET|PASSWORD|TOKEN|CREDENTIAL)[A-Z_]*\s*=" \
    --glob "**/Dockerfile*" . 2>/dev/null | grep -v "#" || true)
if [ -n "$DOCKER_SECRETS" ]; then
    log_fail "Secrets in Dockerfile ENV — pass at runtime via Secret Manager"
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    log_success "No secrets in Dockerfile ENV"
fi

# Check 20: GCP auth — tests use ADC not credential file skip
BAD_AUTH_SKIP=$(rg 'pytest\.skip.*[Cc]redential|pytest\.skip.*GOOGLE_APPLICATION_CREDENTIALS' \
    --type py tests/ 2>/dev/null \
    | grep -v "_skip_integration_without_creds\|No GCP credentials.*skipping" || true)
if [ -n "$BAD_AUTH_SKIP" ]; then
    log_fail "Tests skip due to credential file — use google.auth.default() + @pytest.mark.integration"
    echo "$BAD_AUTH_SKIP" | head -3
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    log_success "No credential-file skip patterns in tests"
fi

# Check 21: .env.example must not have GOOGLE_APPLICATION_CREDENTIALS
if [ -f ".env.example" ] && rg "GOOGLE_APPLICATION_CREDENTIALS" .env.example >/dev/null 2>&1; then
    log_fail ".env.example has GOOGLE_APPLICATION_CREDENTIALS — remove it (use ADC)"
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    log_success "No GOOGLE_APPLICATION_CREDENTIALS in .env.example"
fi

# Check 22: Old event logging pattern
EL_OLD=$(rg "from unified_trading_services[. ].*(log_event|setup_events|setup_cloud_logging|observability)" \
    --type py --glob "!tests/**" "$SOURCE_DIR" 2>/dev/null || true)
if [ -n "$EL_OLD" ]; then
    log_fail "Old event logging import — use 'from unified_events_interface import ...'"
    echo "$EL_OLD" | head -3
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    log_success "Event logging imports from unified_events_interface"
fi

# Check 23: Domain clients from wrong package
UCS_DOMAIN=$(rg 'from unified_trading_services import[^#]*?(InstrumentsDomainClient|ExecutionDomainClient|create_instruments_client|create_execution_client)' \
    --type py --glob "!tests/**" "$SOURCE_DIR" 2>/dev/null || true)
if [ -n "$UCS_DOMAIN" ]; then
    log_fail "Domain clients must come from unified_domain_client, not unified_trading_services"
    echo "$UCS_DOMAIN" | head -3
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    log_success "Domain clients imported from unified_domain_client"
fi

# Check 24: ID conventions from wrong package
ID_CONV=$(rg 'from unified_trading_services import[^#]*?(generate_strategy_id|generate_config_id|CATEGORIES|MODES|TIMEFRAMES)' \
    --type py "$SOURCE_DIR" 2>/dev/null || true)
if [ -n "$ID_CONV" ]; then
    log_fail "ID convention functions must come from unified_config_interface, not unified_trading_services"
    echo "$ID_CONV" | head -3
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    log_success "ID convention imports from unified_config_interface"
fi

# Security: pip-audit (BLOCKING — OSV vulnerability database check)
if command -v pip-audit &>/dev/null; then
    pip-audit --format json -o /tmp/pip-audit-output.json 2>/dev/null \
        && log_success "pip-audit clean" \
        || { log_fail "pip-audit vulnerabilities found"; CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1)); }
    # Store SBOM audit trail in GCS (non-blocking — upload failure does not fail the build)
    SERVICE_NAME="${SERVICE_NAME:-unknown}" python3 "$REPO_ROOT/unified-trading-pm/scripts/sbom-store.py" \
        /tmp/pip-audit-output.json 2>/dev/null || true
    # Internal advisory check (BLOCKING — checks unified-trading-pm/security/internal-advisories.yaml)
    if [[ -f "$REPO_ROOT/unified-trading-pm/scripts/check-internal-advisories.sh" ]]; then
        bash "$REPO_ROOT/unified-trading-pm/scripts/check-internal-advisories.sh" \
            && log_success "internal advisory check clean" \
            || { log_fail "internal advisory violation — see unified-trading-pm/security/internal-advisories.yaml"; CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1)); }
    else
        log_warn "check-internal-advisories.sh not found at REPO_ROOT=$REPO_ROOT — skipping internal advisory check"
    fi
elif $PYTHON_CMD -c "import pip_audit" 2>/dev/null; then
    $PYTHON_CMD -m pip_audit --format json -o /tmp/pip-audit-output.json 2>/dev/null \
        && log_success "pip-audit clean" \
        || { log_fail "pip-audit vulnerabilities found"; CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1)); }
    # Store SBOM audit trail in GCS (non-blocking)
    SERVICE_NAME="${SERVICE_NAME:-unknown}" python3 "$REPO_ROOT/unified-trading-pm/scripts/sbom-store.py" \
        /tmp/pip-audit-output.json 2>/dev/null || true
    # Internal advisory check (BLOCKING)
    if [[ -f "$REPO_ROOT/unified-trading-pm/scripts/check-internal-advisories.sh" ]]; then
        bash "$REPO_ROOT/unified-trading-pm/scripts/check-internal-advisories.sh" \
            && log_success "internal advisory check clean" \
            || { log_fail "internal advisory violation — see unified-trading-pm/security/internal-advisories.yaml"; CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1)); }
    fi
else
    log_fail "pip-audit required (uv pip install pip-audit)"
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
fi

if [ $CODEX_VIOLATIONS -eq 0 ]; then
    echo -e "\n${GREEN}✅ Codex compliance PASSED${NC}"
    CODEX_STATUS=0
else
    echo -e "\n${RED}❌ Codex compliance FAILED: $CODEX_VIOLATIONS violations${NC}"
    echo -e "${YELLOW}See: unified-trading-codex/06-coding-standards/README.md${NC}"
    CODEX_STATUS=1
fi

# ============================================================================
# CI/CD VALIDATORS (BLOCKING — libraries only; optional for services)
# ============================================================================
CODEX_ROOT="${REPO_ROOT}/unified-trading-codex"
if [ -f "${CODEX_ROOT}/validators/run_validators.py" ]; then
    echo -e "\n${BLUE}CI/CD VALIDATORS${NC}"
    echo "----------------------------------------------------------------------"

    # CI-02: Local importability (informational when using --dep-branch)
    if "${PYTHON_CMD}" "${CODEX_ROOT}/validators/run_validators.py" --validator CI-02 --workspace "${PROJECT_ROOT}/.."; then
        log_success "CI-02 (Local Import) PASSED"
    else
        log_warn "CI-02: Library not locally importable (OK when using --dep-branch)"
    fi

    # CI-03: GitHub installability (BLOCKING)
    if "${PYTHON_CMD}" "${CODEX_ROOT}/validators/run_validators.py" --validator CI-03 --workspace "${PROJECT_ROOT}/.."; then
        log_success "CI-03 (GitHub Install) PASSED"
    else
        log_fail "CI-03 FAILED: Library not GitHub installable"
        exit 1
    fi

    # CI-04: Artifact Registry readiness (BLOCKING)
    if "${PYTHON_CMD}" "${CODEX_ROOT}/validators/run_validators.py" --validator CI-04 --workspace "${PROJECT_ROOT}/.."; then
        log_success "CI-04 (Artifact Registry) PASSED"
    else
        log_fail "CI-04 FAILED: Library not AR-ready"
        exit 1
    fi
else
    log_warn "Validators not available (unified-trading-codex not found)"
fi

# ============================================================================
# FINAL SUMMARY
# ============================================================================
echo ""
echo -e "${BLUE}======================================================================${NC}"
echo -e "${BLUE}QUALITY GATES SUMMARY${NC}"
echo -e "${BLUE}======================================================================${NC}"

OVERALL_STATUS=0

[ $CONFIG_STATUS -eq 0 ]  && echo -e "Config:   ${GREEN}✅ PASSED${NC}" || { echo -e "Config:   ${RED}❌ FAILED${NC}"; OVERALL_STATUS=1; }
if [ "$RUN_LINT" = true ]; then
    [ $LINT_STATUS -eq 0 ]    && echo -e "Linting:  ${GREEN}✅ PASSED${NC}" || { echo -e "Linting:  ${RED}❌ FAILED${NC}"; OVERALL_STATUS=1; }
    [ $IMPORT_STATUS -eq 0 ]  && echo -e "Imports:  ${GREEN}✅ PASSED${NC}" || { echo -e "Imports:  ${RED}❌ FAILED${NC}"; OVERALL_STATUS=1; }
    if [ "$SKIP_TYPECHECK" != true ]; then
        [ $PYRIGHT_STATUS -eq 0 ] && echo -e "Types:    ${GREEN}✅ PASSED${NC}" || { echo -e "Types:    ${RED}❌ FAILED${NC}"; OVERALL_STATUS=1; }
    else
        echo -e "Types:    ${YELLOW}⏭️  SKIPPED${NC}"
    fi
fi
if [ "$RUN_TESTS" = true ]; then
    [ $TEST_STATUS -eq 0 ]    && echo -e "Tests:    ${GREEN}✅ PASSED${NC}" || { echo -e "Tests:    ${RED}❌ FAILED${NC}"; OVERALL_STATUS=1; }
else
    echo -e "Tests:    ${YELLOW}⏭️  SKIPPED${NC}"
fi
[ $CODEX_STATUS -eq 0 ]   && echo -e "Codex:    ${GREEN}✅ PASSED${NC}" || { echo -e "Codex:    ${RED}❌ FAILED${NC}"; OVERALL_STATUS=1; }

echo -e "${BLUE}======================================================================${NC}"
if [ $OVERALL_STATUS -eq 0 ]; then
    echo -e "\n${GREEN}✅ ALL QUALITY GATES PASSED — Safe to push!${NC}\n"
else
    echo -e "\n${RED}❌ QUALITY GATES FAILED — Fix issues before pushing${NC}\n"
fi

exit $OVERALL_STATUS
