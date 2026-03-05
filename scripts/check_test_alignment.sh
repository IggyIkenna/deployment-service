#!/usr/bin/env bash
#
# Check test alignment across all repos
#
# Verifies that GitHub Actions, Cloud Build, and local scripts
# run the same tests with the same commands.
#
# Usage:
#   ./scripts/check_test_alignment.sh [repo-name]
#   ./scripts/check_test_alignment.sh  # Check all repos
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOYMENT_ROOT="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$DEPLOYMENT_ROOT")"

# Repos to check (all repos with quality gates)
REPOS=(
    "instruments-service"
    "market-tick-data-handler"
    "market-data-processing-service"
    "execution-services"
    "features-onchain-service"
    "features-volatility-service"
    "features-calendar-service"
    "features-delta-one-service"
    "strategy-service"
    "deployment-service"
    "unified-trading-library"
    "ml-inference-service"
    "ml-training-service"
    "sports-betting-service"
)

check_repo_alignment() {
    local repo=$1
    local repo_path="$REPO_ROOT/$repo"

    echo -e "\n${BLUE}======================================================================${NC}"
    echo -e "${BLUE}Checking: $repo${NC}"
    echo -e "${BLUE}======================================================================${NC}"

    if [ ! -d "$repo_path" ]; then
        echo -e "${YELLOW}⚠️  Repo not found: $repo${NC}"
        return 1
    fi

    cd "$repo_path"

    local issues=0

    # Check if all three files exist
    local gh_actions="$repo_path/.github/workflows/quality-gates.yml"
    local cloudbuild="$repo_path/cloudbuild.yaml"
    local local_script="$repo_path/scripts/quality-gates.sh"

    if [ ! -f "$gh_actions" ]; then
        echo -e "${RED}❌ Missing: .github/workflows/quality-gates.yml${NC}"
        issues=$((issues + 1))
    fi

    if [ ! -f "$cloudbuild" ]; then
        echo -e "${YELLOW}⚠️  Missing: cloudbuild.yaml${NC}"
    fi

    if [ ! -f "$local_script" ]; then
        echo -e "${YELLOW}⚠️  Missing: scripts/quality-gates.sh${NC}"
    fi

    # Check pytest command format
    echo -e "\n${BLUE}Checking pytest commands...${NC}"

    if [ -f "$gh_actions" ]; then
        # Look for actual pytest execution commands (not pip install, not echo statements)
        local gh_pytest=$(grep -E "python.*-m pytest|^[[:space:]]*pytest" "$gh_actions" | grep -v "^#" | grep -v "^[[:space:]]*#" | grep -v "pip install" | grep -v "echo" | grep -v "Running:" | head -1 || echo "")
        if [ -n "$gh_pytest" ]; then
            if echo "$gh_pytest" | grep -q "python -m pytest\|python3 -m pytest"; then
                echo -e "${GREEN}✅ GitHub Actions: Uses python -m pytest${NC}"
            else
                echo -e "${RED}❌ GitHub Actions: Should use 'python -m pytest', found: $gh_pytest${NC}"
                issues=$((issues + 1))
            fi
        else
            echo -e "${YELLOW}⚠️  GitHub Actions: No pytest command found${NC}"
        fi
    fi

    if [ -f "$cloudbuild" ]; then
        # Look for actual pytest execution commands (not pip install, not echo statements)
        local cb_pytest=$(grep -E "python.*-m pytest|^[[:space:]]*pytest" "$cloudbuild" | grep -v "^#" | grep -v "^[[:space:]]*#" | grep -v "pip install" | grep -v "echo" | grep -v "Running:" | head -1 || echo "")
        if [ -n "$cb_pytest" ]; then
            if echo "$cb_pytest" | grep -q "python -m pytest"; then
                echo -e "${GREEN}✅ Cloud Build: Uses python -m pytest${NC}"
            else
                echo -e "${RED}❌ Cloud Build: Should use 'python -m pytest', found: $cb_pytest${NC}"
                issues=$((issues + 1))
            fi
        else
            echo -e "${YELLOW}⚠️  Cloud Build: No pytest command found${NC}"
        fi
    fi

    if [ -f "$local_script" ]; then
        # Look for actual pytest execution commands (not pip install, not echo statements)
        local local_pytest=$(grep -E "python.*-m pytest|^[[:space:]]*pytest" "$local_script" | grep -v "^#" | grep -v "^[[:space:]]*#" | grep -v "pip install" | grep -v "echo" | grep -v "Running:" | head -1 || echo "")
        if [ -n "$local_pytest" ]; then
            if echo "$local_pytest" | grep -q "python.*-m pytest"; then
                echo -e "${GREEN}✅ Local script: Uses python -m pytest${NC}"
            else
                echo -e "${YELLOW}⚠️  Local script: Should use 'python3 -m pytest', found: $local_pytest${NC}"
            fi
        else
            echo -e "${YELLOW}⚠️  Local script: No pytest command found${NC}"
        fi
    fi

    # Check environment variables
    echo -e "\n${BLUE}Checking environment variables...${NC}"

    local env_vars=("CLOUD_MOCK_MODE" "GCP_PROJECT_ID" "DEPLOYMENT_CONFIG_DIR")
    for var in "${env_vars[@]}"; do
        local gh_has=$(grep -c "$var" "$gh_actions" 2>/dev/null | tr -d '\n' || echo "0")
        local cb_has=$(grep -c "$var" "$cloudbuild" 2>/dev/null | tr -d '\n' || echo "0")
        local local_has=$(grep -c "$var" "$local_script" 2>/dev/null | tr -d '\n' || echo "0")

        # Convert to integer (handle multi-line output from grep -c)
        gh_has=$(echo "$gh_has" | head -1 | tr -d '\n')
        cb_has=$(echo "$cb_has" | head -1 | tr -d '\n')
        local_has=$(echo "$local_has" | head -1 | tr -d '\n')

        if [ "${gh_has:-0}" -gt 0 ] && [ "${cb_has:-0}" -gt 0 ] && [ "${local_has:-0}" -gt 0 ]; then
            echo -e "${GREEN}✅ $var: Set in all three${NC}"
        elif [ "${gh_has:-0}" -gt 0 ] && [ "${cb_has:-0}" -gt 0 ]; then
            echo -e "${YELLOW}⚠️  $var: Set in GitHub Actions and Cloud Build (missing in local)${NC}"
        elif [ "${gh_has:-0}" -gt 0 ]; then
            echo -e "${YELLOW}⚠️  $var: Only in GitHub Actions${NC}"
        else
            echo -e "${RED}❌ $var: Not found${NC}"
            issues=$((issues + 1))
        fi
    done

    # Check dependency installation order
    echo -e "\n${BLUE}Checking dependency installation...${NC}"

    if [ -f "$gh_actions" ]; then
        local gh_ucs=$(grep -c "unified-trading-library" "$gh_actions" 2>/dev/null | head -1 | tr -d '\n' || echo "0")
        if [ "${gh_ucs:-0}" -gt 0 ]; then
            echo -e "${GREEN}✅ GitHub Actions: Installs unified-trading-library${NC}"
        else
            echo -e "${YELLOW}⚠️  GitHub Actions: unified-trading-library not found${NC}"
        fi
    fi

    if [ -f "$cloudbuild" ]; then
        local cb_ucs=$(grep -c "unified-trading-library" "$cloudbuild" 2>/dev/null | head -1 | tr -d '\n' || echo "0")
        if [ "${cb_ucs:-0}" -gt 0 ]; then
            echo -e "${GREEN}✅ Cloud Build: Installs unified-trading-library${NC}"
        else
            echo -e "${YELLOW}⚠️  Cloud Build: unified-trading-library not found${NC}"
        fi
    fi

    # Check test timeouts match
    echo -e "\n${BLUE}Checking test timeouts...${NC}"

    local timeout_60=$(grep -c "timeout=60\|--timeout 60" "$gh_actions" "$cloudbuild" "$local_script" 2>/dev/null | awk '{sum+=$1} END {print sum}')
    local timeout_120=$(grep -c "timeout=120\|--timeout 120" "$gh_actions" "$cloudbuild" "$local_script" 2>/dev/null | awk '{sum+=$1} END {print sum}')
    local timeout_180=$(grep -c "timeout=180\|--timeout 180" "$gh_actions" "$cloudbuild" "$local_script" 2>/dev/null | awk '{sum+=$1} END {print sum}')

    if [ "$timeout_60" -gt 0 ] && [ "$timeout_120" -gt 0 ] && [ "$timeout_180" -gt 0 ]; then
        echo -e "${GREEN}✅ Test timeouts: 60s (unit), 120s (integration), 180s (e2e/smoke)${NC}"
    else
        echo -e "${YELLOW}⚠️  Test timeouts may not match across all environments${NC}"
    fi

    # Summary
    if [ $issues -eq 0 ]; then
        echo -e "\n${GREEN}✅ $repo: All checks passed${NC}"
        return 0
    else
        echo -e "\n${RED}❌ $repo: Found $issues issue(s)${NC}"
        return 1
    fi
}

# Main
if [ $# -eq 1 ]; then
    # Check single repo
    check_repo_alignment "$1"
else
    # Check all repos
    echo -e "${BLUE}======================================================================${NC}"
    echo -e "${BLUE}TEST ALIGNMENT CHECK - ALL REPOS${NC}"
    echo -e "${BLUE}======================================================================${NC}"

    total_issues=0
    repos_checked=0

    for repo in "${REPOS[@]}"; do
        if check_repo_alignment "$repo"; then
            repos_checked=$((repos_checked + 1))
        else
            total_issues=$((total_issues + 1))
            repos_checked=$((repos_checked + 1))
        fi
    done

    echo -e "\n${BLUE}======================================================================${NC}"
    echo -e "${BLUE}SUMMARY${NC}"
    echo -e "${BLUE}======================================================================${NC}"
    echo -e "Repos checked: $repos_checked"
    echo -e "Repos with issues: $total_issues"

    if [ $total_issues -eq 0 ]; then
        echo -e "\n${GREEN}✅ All repos are aligned!${NC}"
        exit 0
    else
        echo -e "\n${RED}❌ $total_issues repo(s) need alignment fixes${NC}"
        exit 1
    fi
fi
