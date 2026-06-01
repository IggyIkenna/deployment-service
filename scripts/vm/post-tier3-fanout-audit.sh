#!/usr/bin/env bash
# Post-Tier-3-fanout audit: poll for VM fleet completion, then run reclassifier,
# phantom audit, and per-venue capture summary on the CeFi manifest.
#
# Designed to run as the VM_BACKFILL_CMD body for an audit VM in asia-northeast1-c
# launched with VM_TASK=mdps-backfill VM_SERVICE=instruments_service so that
# setup-data-pipeline-vm.sh provisions /home/ikennaigboaka/venv + extracts the
# instruments-service tarball at /home/ikennaigboaka/workspace/instruments/ before
# this script runs. Polls for the cefi-tier3 fleet to finish, then runs the 3
# scripts, uploads results to gs://deployment-scripts-.../audit-results/<ts>/,
# then shuts down.

set -euo pipefail

PROJECT="central-element-323112"
ZONE="asia-northeast1-c"
# Filter matches the two backfill labels but NOT the audit VM's own label
# (cefi-tier3-audit) so we don't wait forever for ourselves.
FANOUT_FILTER="(labels.purpose=cefi-tier3-fullbackfill OR labels.purpose=cefi-tier3-spot-perp) AND status=RUNNING"
RESULTS_BUCKET="deployment-scripts-${PROJECT}"
INSTRUMENTS_TARBALL="gs://${RESULTS_BUCKET}/code/instruments-service-code.tar.gz"
POLL_SECONDS=300
MAX_WAIT_HOURS=14
TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="/tmp/audit-${TS}"
mkdir -p "${LOG_DIR}"
LOG="${LOG_DIR}/orchestrator.log"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "${LOG}"; }

# ----------------------------------------------------------------------------
# Step 1: wait for fanout VMs to finish
# ----------------------------------------------------------------------------
log "Waiting for cefi-tier3 backfill VMs to complete (poll every ${POLL_SECONDS}s, max ${MAX_WAIT_HOURS}h)"
deadline=$(( $(date +%s) + MAX_WAIT_HOURS * 3600 ))
while :; do
    n=$(gcloud compute instances list \
        --project="${PROJECT}" \
        --filter="${FANOUT_FILTER}" \
        --format="value(name)" 2>/dev/null | wc -l)
    if [[ "${n}" -eq 0 ]]; then
        log "All Tier-3 VMs done."
        break
    fi
    if [[ $(date +%s) -ge ${deadline} ]]; then
        log "WARN: max-wait reached, proceeding with ${n} VMs still running"
        break
    fi
    log "  ${n} Tier-3 VMs still RUNNING — sleep ${POLL_SECONDS}s"
    sleep "${POLL_SECONDS}"
done

# ----------------------------------------------------------------------------
# Step 2: use the venv that setup-data-pipeline-vm.sh already provisioned.
# (VM_SERVICE=instruments_service ensures pandas, google-cloud-storage, requests
# are installed in /home/ikennaigboaka/venv/.)
# ----------------------------------------------------------------------------
VENV="/home/ikennaigboaka/venv"
if [[ ! -x "${VENV}/bin/python" ]]; then
    log "ERROR: ${VENV}/bin/python missing — setup-data-pipeline-vm.sh did not provision the venv"
    exit 1
fi
PYTHON="${VENV}/bin/python"
log "Using venv at ${VENV} (python: $(${PYTHON} --version))"

# ----------------------------------------------------------------------------
# Step 3: pull the uncommitted per-venue summary into the extracted instruments-service tree.
# setup-data-pipeline-vm.sh has already extracted the instruments-service tarball
# at /home/ikennaigboaka/workspace/instruments/, so reclassify_404 and
# reconcile_phantom are already there. Just drop the new per-venue script in.
# ----------------------------------------------------------------------------
SCRIPTS="/home/ikennaigboaka/workspace/instruments/scripts"
if [[ ! -d "${SCRIPTS}" ]]; then
    log "ERROR: ${SCRIPTS} missing — setup-data-pipeline-vm.sh did not extract instruments-service"
    exit 1
fi
gsutil -q cp "gs://${RESULTS_BUCKET}/audit-scripts/cefi_per_venue_capture_summary.py" \
    "${SCRIPTS}/cefi_per_venue_capture_summary.py"
ls -la "${SCRIPTS}/"reclassify_404_failures_to_empty.py "${SCRIPTS}/"reconcile_phantom_manifest_rows_all.py "${SCRIPTS}/"cefi_per_venue_capture_summary.py >> "${LOG}" 2>&1

# ----------------------------------------------------------------------------
# Step 4: run scripts in sequence
# ----------------------------------------------------------------------------
BUCKET="market-data-tick-cefi-${PROJECT}"

log "=== Step 4a: reclassifier (real, not dry-run) ==="
"${PYTHON}" "${SCRIPTS}/reclassify_404_failures_to_empty.py" \
    --bucket "${BUCKET}" \
    > "${LOG_DIR}/reclassify.log" 2>&1 || log "WARN: reclassifier exit $?"
tail -20 "${LOG_DIR}/reclassify.log" | tee -a "${LOG}"

log "=== Step 4b: phantom audit (real, not dry-run) ==="
"${PYTHON}" "${SCRIPTS}/reconcile_phantom_manifest_rows_all.py" \
    --asset-group cefi \
    --workers 64 \
    > "${LOG_DIR}/phantom_audit.log" 2>&1 || log "WARN: phantom audit exit $?"
tail -40 "${LOG_DIR}/phantom_audit.log" | tee -a "${LOG}"

log "=== Step 4c: per-venue capture summary ==="
"${PYTHON}" "${SCRIPTS}/cefi_per_venue_capture_summary.py" \
    > "${LOG_DIR}/per_venue_summary.log" 2>&1 || log "WARN: summary exit $?"
tail -80 "${LOG_DIR}/per_venue_summary.log" | tee -a "${LOG}"

# ----------------------------------------------------------------------------
# Step 5: upload results
# ----------------------------------------------------------------------------
RESULT_PREFIX="gs://${RESULTS_BUCKET}/audit-results/post-tier3-fanout-${TS}"
log "Uploading results to ${RESULT_PREFIX}/"
gsutil -q cp -r "${LOG_DIR}"/* "${RESULT_PREFIX}/"
log "Done — results at ${RESULT_PREFIX}/"

# ----------------------------------------------------------------------------
# Step 6: self-shutdown
# ----------------------------------------------------------------------------
log "Shutting down VM"
shutdown -h +1
