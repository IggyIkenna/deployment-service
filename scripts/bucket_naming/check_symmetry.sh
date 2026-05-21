#!/usr/bin/env bash
# Bucket name symmetry checker — AWS ↔ GCP.
#
# Verifies cloud-providers.yaml: every kind present in both clouds must resolve
# to a symmetric name (only ${GCP_PROJECT_ID} vs ${AWS_ACCOUNT_ID} differs).
# Also checks DEPLOYMENT_ENV_SHORT consistency + 63-char cap.
#
# Plan: mtds_mdps_master.md § Phase 1 item 4
# SSOT: plans/audit/results/aws_gcp_bucket_symmetry_2026_05_20.csv
#
# Exit 0 — all symmetric and within cap.
# Exit 1 — one or more violations (prints table).
# Exit 2 — missing dependency (PyYAML, yaml file not found).
#
# Usage:
#   bash scripts/bucket_naming/check_symmetry.sh
#   bash scripts/bucket_naming/check_symmetry.sh --yaml /path/to/cloud-providers.yaml

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Prefer repo venv if present (quality-gates context), else fall through to system python3.
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
if [[ -x "${REPO_ROOT}/.venv/bin/python3" ]]; then
    PYTHON="${REPO_ROOT}/.venv/bin/python3"
elif command -v python3 &>/dev/null; then
    PYTHON="python3"
else
    echo "ERROR: python3 not found on PATH" >&2
    exit 2
fi

exec "${PYTHON}" "${SCRIPT_DIR}/check_symmetry.py" "$@"
