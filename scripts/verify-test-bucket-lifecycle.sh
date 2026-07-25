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
echo "[verify-test-bucket-lifecycle] listing -test- buckets via gcloud storage..."

# `gcloud storage ls --project=<project>` lists buckets owned by the configured
# project. `gcloud storage`, not `gsutil` — gsutil resolves creds from the
# CLI's active account (a short-lived WIF token in an interactive AO slot
# can't refresh unattended), while `gcloud storage` resolves via ADC, which
# stays valid. See
# plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.
PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
if [[ -z "${PROJECT_ID}" ]]; then
  echo "ERROR: no gcloud project configured. Run: gcloud config set project <id>" >&2
  exit 1
fi

# macOS bash 3.2 lacks `mapfile` — read into an array via while-read (portable).
BUCKETS=()
while IFS= read -r _b; do
  [[ -n "${_b}" ]] && BUCKETS+=("${_b}")
done < <(gcloud storage ls --project="${PROJECT_ID}" 2>/dev/null \
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
  # `--raw` preserves the API's own JSON field shape (matches gsutil's
  # `lifecycle get` output) so the "type": "Delete" / "age": 7 substring
  # checks below still hold.
  current="$(gcloud storage buckets describe "${url}" --raw --format=json 2>/dev/null || echo "")"
  if [[ -n "${current}" ]] \
      && echo "${current}" | grep -q '"type": *"Delete"' \
      && echo "${current}" | grep -q '"age": *7'; then
    OK=$((OK + 1))
    continue
  fi
  MISSING=$((MISSING + 1))
  MISSING_LIST+=("${bucket}")
  if [[ ${APPLY} -eq 1 ]]; then
    if gcloud storage buckets update "${url}" --lifecycle-file="${LIFECYCLE_JSON}" >/dev/null 2>&1; then
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
