#!/usr/bin/env bash
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-service.sh
SERVICE_NAME="deployment-service"
SOURCE_DIR="deployment_service"
MIN_COVERAGE=80
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"
