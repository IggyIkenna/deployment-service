#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# One-time setup — honest-coverage-daily Cloud Scheduler trigger.
#
# Cloud Run Job was already created (2026-05-15 slot-2):
#   gcloud run jobs describe honest-coverage-daily-launcher --region=asia-northeast1 \
#     --project=central-element-323112
#
# This script creates ONLY the Cloud Scheduler job (requires cloudscheduler.jobs.create,
# which is Ikenna/owner territory — harshkantariya@odum-research.com gets PERMISSION_DENIED).
#
# Terraform SSOT: deployment-service/terraform/gcp/honest_coverage_scheduler.tf
# VM Launcher (cron AND ad-hoc, SSOT — one artifact):
#   deployment-service/scripts/vm/launch-measure-honest-coverage-vm.sh (default --asset-group all
#   when run by the Cloud Run Job; supports --asset-group filter for ad-hoc partial runs)
#   Published by create-code-tarballs.sh's bare-launcher loop to
#   gs://deployment-scripts-central-element-323112/code/deployment-service/scripts/vm/
#   (the Cloud Run Job fetches from that exact path — see honest_coverage_scheduler.tf).
# Cloud Run Job:  honest-coverage-daily-launcher (CREATED 2026-05-15)
# Plan:           plans/active/issues/honest_coverage_cron_vm_scheduling_2026_05_14.md
#
# NOTE (deployment_registry_dualwrite_flag_not_propagated_to_vm_launchers_2026_07_30.md):
# this script is a one-time, human-run `gcloud scheduler jobs create/update` invocation —
# it never runs ON a GCE VM and never reads instance/project metadata, so the
# DEPLOYMENT_REGISTRY_FIRESTORE_DUALWRITE project-metadata fallback (_meta_project(),
# added to the sibling setup-*.sh scripts) does not apply here. Both VM launchers this
# scheduler ultimately triggers (launch-honest-coverage-vm.sh and
# launch-measure-honest-coverage-vm.sh) already pass
# `startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh` — that script carries the fix.
#
# Usage (run as Ikenna / owner account):
#   bash setup-honest-coverage-scheduler.sh              # create scheduler job
#   bash setup-honest-coverage-scheduler.sh --dry-run    # print command only
#   bash setup-honest-coverage-scheduler.sh --update     # update if already exists
set -euo pipefail

PROJECT="central-element-323112"
REGION="asia-northeast1"
SCHEDULER_JOB_NAME="honest-coverage-daily"
SCHEDULER_SA="cloud-scheduler@${PROJECT}.iam.gserviceaccount.com"
CLOUD_RUN_JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/honest-coverage-daily-launcher:run"
DRY_RUN=false
UPDATE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --update) UPDATE=true; shift ;;
    *) echo "ERROR: unknown arg: $1" >&2; exit 1 ;;
  esac
done

run() {
  if $DRY_RUN; then
    echo "[DRY-RUN] $*"
  else
    "$@"
  fi
}

echo "=== Verify Cloud Run Job exists ==="
gcloud run jobs describe honest-coverage-daily-launcher \
  --region="${REGION}" --project="${PROJECT}" --format="value(name)" 2>/dev/null \
  || { echo "ERROR: Cloud Run Job 'honest-coverage-daily-launcher' not found. Run:"; \
       echo "  gcloud run jobs create honest-coverage-daily-launcher --region=${REGION} --project=${PROJECT} ..."; \
       exit 1; }

echo "Cloud Run Job verified."
echo ""
echo "=== Create Cloud Scheduler Job ==="
echo "Schedule: 30 0 * * * UTC  →  00:30 UTC daily"
echo "Target:   ${CLOUD_RUN_JOB_URI}"

if $UPDATE; then
  run gcloud scheduler jobs update http "${SCHEDULER_JOB_NAME}" \
    --project="${PROJECT}" \
    --location="${REGION}" \
    --schedule="30 0 * * *" \
    --time-zone="UTC" \
    --uri="${CLOUD_RUN_JOB_URI}" \
    --oauth-service-account-email="${SCHEDULER_SA}" \
    --attempt-deadline="60s" \
    --description="Daily 00:30 UTC trigger for honest-coverage measurement VM"
else
  run gcloud scheduler jobs create http "${SCHEDULER_JOB_NAME}" \
    --project="${PROJECT}" \
    --location="${REGION}" \
    --schedule="30 0 * * *" \
    --time-zone="UTC" \
    --uri="${CLOUD_RUN_JOB_URI}" \
    --oauth-service-account-email="${SCHEDULER_SA}" \
    --attempt-deadline="60s" \
    --description="Daily 00:30 UTC trigger for honest-coverage measurement VM (instruments measure_honest_coverage.py → gs://${PROJECT}-honest-coverage/{date}/coverage.json)"
fi

echo ""
echo "=== Setup complete ==="
echo "Verify scheduler: gcloud scheduler jobs describe ${SCHEDULER_JOB_NAME} --location=${REGION} --project=${PROJECT}"
echo ""
echo "Trigger manually:  gcloud scheduler jobs run ${SCHEDULER_JOB_NAME} --location=${REGION} --project=${PROJECT}"
echo "OR run VM direct:  bash deployment-service/scripts/vm/launch-measure-honest-coverage-vm.sh"
echo ""
echo "Verify output (after ~10-15 min):"
echo "  gsutil cat gs://${PROJECT}-honest-coverage/\$(date -u +%Y-%m-%d)/coverage.json"
echo "  curl http://localhost:8004/api/data-status/honest-coverage?date=\$(date -u +%Y-%m-%d)"
