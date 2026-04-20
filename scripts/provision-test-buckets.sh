#!/usr/bin/env bash
# Idempotent provisioner for `-test-` GCS buckets used by the institutional
# smoke matrix (plans/active/institutional_smoke_matrix_2026_04_20.plan.md).
#
# Wraps the existing setup-buckets.py SSOT. The Python script reads
# `dependencies.yaml` + `cloud-providers.yaml` + `bucket_config.yaml` and
# derives `{prefix}-{category_lower}-test-{project_id}` siblings for every
# production bucket. `--test-only` skips PROD buckets so this script is safe
# to run in any environment without altering live data.
#
# Usage:
#   bash deployment-service/scripts/provision-test-buckets.sh                # GCP, all services
#   bash deployment-service/scripts/provision-test-buckets.sh --service instruments-service
#   bash deployment-service/scripts/provision-test-buckets.sh --dry-run     # show without creating
#
# Idempotent: setup-buckets.py logs INFO and skips when a bucket already exists.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_PY="${SCRIPT_DIR}/setup-buckets.py"

if [[ ! -f "${SETUP_PY}" ]]; then
  echo "ERROR: setup-buckets.py not found at ${SETUP_PY}" >&2
  exit 1
fi

# Pass-through arguments. Default to GCP if --cloud not supplied.
HAS_CLOUD=0
HAS_REGION=0
for arg in "$@"; do
  if [[ "${arg}" == "--cloud" || "${arg}" == --cloud=* ]]; then
    HAS_CLOUD=1
  fi
  if [[ "${arg}" == "--region" || "${arg}" == --region=* ]]; then
    HAS_REGION=1
  fi
done

EXTRA_ARGS=()
if [[ ${HAS_CLOUD} -eq 0 ]]; then
  EXTRA_ARGS+=(--cloud gcp)
fi

# UTL config can resolve `gcs_region` to a zone (e.g. `asia-northeast1-c`)
# when GCS_REGION env var is unset and gcloud's compute/zone is set. GCS
# bucket creation requires a region, not a zone — pass --region explicitly
# unless the caller already supplied one. Default region matches
# `bucket_config.yaml` defaults.gcp.region.
if [[ ${HAS_REGION} -eq 0 ]]; then
  EXTRA_ARGS+=(--region "${GCS_REGION:-asia-northeast1}")
fi

# Always test-only — this script provisions `-test-` siblings and never touches PROD.
EXTRA_ARGS+=(--test-only)

echo "[provision-test-buckets] running setup-buckets.py ${EXTRA_ARGS[*]} $*"
exec python3 "${SETUP_PY}" "${EXTRA_ARGS[@]}" "$@"
