#!/usr/bin/env bash
# Epic: sports_master
# Lifecycle: campaign
# Delete-when: SOURCE_RATE_LIMITS_RPM + SOURCE_PER_IP_LIMITS calibrated values transcribed into launch_budget_registry.py and operator confirms no re-calibration pending
#
# Launches a throwaway ephemeral GCE VM that runs the ramp-to-429 rate-limit
# calibration probe (instruments-service/scripts/calibrate_source_rate_limit.py)
# from a sacrificial IP, then uploads the JSON result to GCS for transcription.
#
# Usage:
#   bash deployment-service/scripts/vm/launch-rate-calibration-probe-vm.sh
#
# The result lands at:
#   gs://deployment-scripts-central-element-323112/vm-logs/<VM_NAME>/calib_result.json
#
# After the VM exits, transcribe safe_rate_rpm + recovery_seconds from the JSON
# into deployment_service/data_pipeline_monitors/launch_budget_registry.py.

# qg-disk-exempt: throwaway ephemeral rate-limit probe. It ramps to 429 and uploads a single
# small calib_result.json — it does not write bulk data, so the 250GB download-heavy disk policy
# does not apply. It only matches the policy classifier because VM_TASK=phantom-recon contains
# "recon". 20GB pd-balanced is deliberate and adequate.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

PROJECT="central-element-323112"
ZONE="asia-northeast1-c"
MACHINE_TYPE="e2-standard-2"
BOOT_DISK_GB=20
DEPLOYMENT_ENV="prod"
CODE_BUCKET="deployment-scripts-central-element-323112"

RUN_TS=$(date +%Y%m%d-%H%M%S)
VM_NAME="exp-execution-rate-calib-${RUN_TS}"

echo "=== Launching rate-calibration probe VM ==="
echo "VM_NAME: ${VM_NAME}"
echo "Zone:    ${ZONE}"
echo "Result will appear at: gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/calib_result.json"

# VM_BACKFILL_CMD: run calibration probe, then upload JSON result to GCS.
# The setup script replaces 'python ' with '$VENV/bin/python ' automatically.
# gsutil is available on the VM (from google-cloud-sdk included in the base image).
CALIB_CMD="python /home/ikennaigboaka/workspace/instruments/scripts/calibrate_source_rate_limit.py --all --out /tmp/calib.json && gsutil -q cp /tmp/calib.json gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/calib_result.json && echo CALIB_UPLOADED_OK"

METADATA="VM_TASK=phantom-recon,\
VM_SERVICE=instruments_service,\
VM_SHUTDOWN_ON_COMPLETION=true,\
VM_BACKFILL_CMD=${CALIB_CMD},\
DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        instruments-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT}" \
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "${PROJECT}")" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels="purpose=rate-calibration,env=${DEPLOYMENT_ENV},run-ts=${RUN_TS}",managed-by=deployment-service \
    --no-address \
    && echo "=== VM created: ${VM_NAME} ==="

echo "Monitor: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --project=${PROJECT} --format='value(status)'"
echo "Logs:    gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Result:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/calib_result.json"
