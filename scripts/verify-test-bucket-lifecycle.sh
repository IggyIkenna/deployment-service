#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Force bash execution (mapfile is bash-only). The shebang above ensures bash
# even if invoked via `sh script.sh`.
# Verify that every `-test-` GCS bucket has the 7-day delete lifecycle policy
# applied. Pairs with provision-test-buckets.sh + test-bucket-lifecycle.json.
#
# Plan: institutional_smoke_matrix_2026_04_20 Phase 1.4.
#
# Usage:
#   bash deployment-service/scripts/verify-test-bucket-lifecycle.sh                 # check + report
#   bash deployment-service/scripts/verify-test-bucket-lifecycle.sh --apply         # apply where missing
#   bash deployment-service/scripts/verify-test-bucket-lifecycle.sh --filter sports # subset by name fragment

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIFECYCLE_JSON="${SCRIPT_DIR}/../configs/test-bucket-lifecycle.json"

if [[ ! -f "${LIFECYCLE_JSON}" ]]; then
  echo "ERROR: lifecycle config not found at ${LIFECYCLE_JSON}" >&2
  exit 1
fi

APPLY=0
FILTER=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply) APPLY=1; shift ;;
    --filter) FILTER="$2"; shift 2 ;;
    --filter=*) FILTER="${1#--filter=}"; shift ;;
    -h|--help)
      sed -n '2,15p' "$0"
      exit 0
      ;;
    *) echo "unknown arg: $1"; exit 2 ;;
  esac
done

if ! command -v gcloud >/dev/null 2>&1; then
  echo "ERROR: gcloud CLI is required" >&2
  exit 1
fi
if ! command -v gsutil >/dev/null 2>&1; then
  echo "ERROR: gsutil CLI is required" >&2
  exit 1
fi

echo "[verify-test-bucket-lifecycle] listing -test- buckets via gsutil..."

# `gsutil ls -p <project>` lists buckets owned by the configured project.
PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
if [[ -z "${PROJECT_ID}" ]]; then
  echo "ERROR: no gcloud project configured. Run: gcloud config set project <id>" >&2
  exit 1
fi

# macOS bash 3.2 lacks `mapfile` — read into an array via while-read (portable).
BUCKETS=()
while IFS= read -r _b; do
  [[ -n "${_b}" ]] && BUCKETS+=("${_b}")
done < <(gsutil ls -p "${PROJECT_ID}" 2>/dev/null \
  | sed -e 's#^gs://##' -e 's#/$##' \
  | grep -E -- '-test-' || true)

if [[ -n "${FILTER}" ]]; then
  FILTERED=()
  for _b in "${BUCKETS[@]}"; do
    if echo "${_b}" | grep -qF -- "${FILTER}"; then
      FILTERED+=("${_b}")
    fi
  done
  BUCKETS=("${FILTERED[@]}")
fi

if [[ ${#BUCKETS[@]} -eq 0 ]]; then
  echo "[verify-test-bucket-lifecycle] no -test- buckets found in project ${PROJECT_ID}${FILTER:+ matching '${FILTER}'}"
  exit 0
fi

echo "[verify-test-bucket-lifecycle] checking ${#BUCKETS[@]} -test- buckets..."

OK=0
MISSING=0
APPLIED=0
FAILED=0
MISSING_LIST=()

for bucket in "${BUCKETS[@]}"; do
  url="gs://${bucket}"
  current="$(gsutil lifecycle get "${url}" 2>/dev/null || echo "")"
  if [[ -n "${current}" ]] \
      && echo "${current}" | grep -q '"type": *"Delete"' \
      && echo "${current}" | grep -q '"age": *7'; then
    OK=$((OK + 1))
    continue
  fi
  MISSING=$((MISSING + 1))
  MISSING_LIST+=("${bucket}")
  if [[ ${APPLY} -eq 1 ]]; then
    if gsutil lifecycle set "${LIFECYCLE_JSON}" "${url}" >/dev/null 2>&1; then
      APPLIED=$((APPLIED + 1))
      echo "  applied lifecycle to ${bucket}"
    else
      FAILED=$((FAILED + 1))
      echo "  FAILED to apply lifecycle to ${bucket}" >&2
    fi
  fi
done

echo "------------------------------------------------------------"
echo "[verify-test-bucket-lifecycle] summary"
echo "  total -test- buckets:       ${#BUCKETS[@]}"
echo "  with 7d-delete lifecycle:   ${OK}"
echo "  missing lifecycle:          ${MISSING}"
if [[ ${APPLY} -eq 1 ]]; then
  echo "  newly applied this run:     ${APPLIED}"
  echo "  failed apply:               ${FAILED}"
fi
echo "------------------------------------------------------------"

if [[ ${MISSING} -gt 0 && ${APPLY} -eq 0 ]]; then
  echo "Buckets missing lifecycle (re-run with --apply to fix):"
  printf '  %s\n' "${MISSING_LIST[@]}"
  exit 1
fi

if [[ ${FAILED} -gt 0 ]]; then
  exit 2
fi
