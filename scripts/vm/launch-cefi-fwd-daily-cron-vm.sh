#!/usr/bin/env bash
# Launch a long-lived cron host VM that fires the daily CeFi forward-poll.
#
# Why this exists
# ---------------
# Replaces the previously-broken Cloud Scheduler → Cloud Run trigger pattern.
# The Cloud Run service `trigger-market-tick-cefi-job` had been returning
# HTTP 403 with zero successful executions for 4+ months (audit 2026-05-17
# slot-5, ack'd 2026-05-20 by operator). Operator authorised Option B (cron-VM
# pattern, mirror qg-snapshot/honest-coverage) to be shipped alongside the
# TradFi twin in the same unit of work.
#
# What it does
# ------------
# Boots a tiny e2-micro VM in asia-northeast1-c that installs a crontab firing
# `gcloud compute instances create` (via the existing launch-cefi-forward-poll.sh
# CLI shape) every day at 09:00 UTC — same cadence the broken Cloud Scheduler
# was attempting (`market-tick-cefi-daily-download` @ 09:00 UTC). Output:
#   - Daily worker VM:   cefi-fwd-{TS} (managed by launch-cefi-forward-poll.sh)
#   - Cron host log:     gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#
# The cron host VM is itself SCHEDULED_RECURRING / long-lived: it stays up,
# fires once a day, then sleeps. Singleton-locked on prefix
# `cefi-fwd-daily-cron-` so duplicate launches are refused.
#
# Cadence
# -------
# 09:00 UTC daily. CeFi venues are 24/7 so the choice is operational not
# market-driven; 09:00 UTC matches the pre-existing (broken) Cloud Scheduler
# entry and stays well clear of the TradFi 06:00 UTC fire to avoid co-launching
# many worker VMs in the same window.
#
# Issue ref:
#   plans/active/issues/tradfi_forward_poll_cron_missing_2026_05_17.md
#   (CeFi sibling audited in same issue body; resolved together per operator)
# Pattern SSOTs:
#   codex/05-infrastructure/vm-launcher-runbook.md § 1 (Infrastructure / Cron VMs)
#   codex/05-infrastructure/vm-tarball-deployment.md § "EXIT-trap log upload"
#
# Usage
# -----
#   bash launch-cefi-fwd-daily-cron-vm.sh                # launch cron host
#   bash launch-cefi-fwd-daily-cron-vm.sh --force        # bypass singleton
#   bash launch-cefi-fwd-daily-cron-vm.sh --dry-run      # show plan only
#   bash launch-cefi-fwd-daily-cron-vm.sh --env staging  # staging tier
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/launcher_common.sh
source "${SCRIPT_DIR}/lib/launcher_common.sh"

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
MACHINE_TYPE="e2-micro"
BOOT_DISK_GB="20"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false
# Default cadence: 09:00 UTC daily (mirrors the pre-existing — broken — Cloud
# Scheduler entry `market-tick-cefi-daily-download`).
CRON_HOUR="${CRON_HOUR:-9}"
CRON_MIN="${CRON_MIN:-0}"

VM_PREFIX="cefi-fwd-daily-cron-"

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --force)     FORCE=true; shift ;;
        --dry-run)   DRY_RUN=true; shift ;;
        --env)       DEPLOYMENT_ENV="$2"; shift 2 ;;
        --project)   PROJECT="$2"; shift 2 ;;
        --zone)      ZONE="$2"; shift 2 ;;
        --hour)      CRON_HOUR="$2"; shift 2 ;;
        --minute)    CRON_MIN="$2"; shift 2 ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            echo "Usage: $0 [--force] [--dry-run] [--env prod|staging|dev] [--hour 9] [--minute 0]" >&2
            exit 1
            ;;
    esac
done

CODE_BUCKET="$(lc_code_bucket "$PROJECT")"
lc_validate_env "$DEPLOYMENT_ENV"
lc_singleton_check "$VM_PREFIX" "$ZONE" "$PROJECT" "$FORCE"

RUN_TS="$(lc_run_ts)"
VM_NAME="${VM_PREFIX}${RUN_TS}"

LAUNCHER_GCS_PATH="gs://${CODE_BUCKET}/code/deployment-service/scripts/vm/launch-cefi-forward-poll.sh"

LOG_TRAP="$(lc_log_upload_trap_block "$VM_NAME" "$PROJECT")"

STARTUP_SCRIPT=$(cat <<STARTUP_EOF
#!/bin/bash
${LOG_TRAP}
set -euo pipefail

echo "=== cefi-fwd-daily-cron host boot \$(date -u +%FT%TZ) ==="
echo "VM_NAME=${VM_NAME}"
echo "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
echo "CRON: ${CRON_MIN} ${CRON_HOUR} * * *"

apt-get update -y >/dev/null 2>&1 || true
which gcloud || (apt-get install -y google-cloud-cli >/dev/null 2>&1 || true)

mkdir -p /opt/deployment-service/scripts/vm/lib
gsutil cp "${LAUNCHER_GCS_PATH}" /opt/deployment-service/scripts/vm/launch-cefi-forward-poll.sh
chmod +x /opt/deployment-service/scripts/vm/launch-cefi-forward-poll.sh

# PATH MUST include /snap/bin — gcloud/gsutil are the snap symlinked into
# /snap/bin on Ubuntu-2404 GCE, NOT /usr/bin; cron's minimal default PATH lacks
# it → \`gsutil: command not found\` → the fire never runs. Date/rc double-escaped
# (\\\$) so they evaluate at FIRE time, not startup-script-generation time
# (single-escape baked a frozen launch-minute timestamp + rc=0 into the crontab).
# Same fix as the tradfi-fwd twin (2026-06-23).
cat > /etc/cron.d/cefi-fwd-daily <<CRON_EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin
${CRON_MIN} ${CRON_HOUR} * * * root gsutil cp ${LAUNCHER_GCS_PATH} /opt/deployment-service/scripts/vm/launch-cefi-forward-poll.sh && chmod +x /opt/deployment-service/scripts/vm/launch-cefi-forward-poll.sh && DEPLOYMENT_ENV=${DEPLOYMENT_ENV} bash /opt/deployment-service/scripts/vm/launch-cefi-forward-poll.sh --env ${DEPLOYMENT_ENV} >> /var/log/cefi-fwd-cron.log 2>&1 || echo "[\\\$(date -u +%FT%TZ)] cefi-fwd cron fire FAILED rc=\\\$?" >> /var/log/cefi-fwd-cron.log
CRON_EOF
chmod 0644 /etc/cron.d/cefi-fwd-daily

( tail -F /var/log/cefi-fwd-cron.log 2>/dev/null & ) || true

echo "Cron installed; sleeping forever (cron daemon runs the schedule)."
while true; do
    sleep 3600
    echo "[\$(date -u +%FT%TZ)] cron host heartbeat — last cron fires:"
    tail -n 3 /var/log/cefi-fwd-cron.log 2>/dev/null || echo "  (no fires yet)"
done
STARTUP_EOF
)

METADATA="VM_TASK=cefi-fwd-daily-cron"
METADATA="${METADATA},VM_SERVICE=cefi_fwd_daily_cron"
METADATA="${METADATA},VM_OPERATION=cron-trigger"
METADATA="${METADATA},VM_ASSET_GROUP=CEFI"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},GCP_PROJECT_ID=${PROJECT}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"
METADATA="${METADATA},VM_LIFECYCLE_CLASS=SCHEDULED_RECURRING"

LABELS="purpose=cefi-fwd-daily-cron,env=${DEPLOYMENT_ENV},run-ts=${RUN_TS},lifecycle=scheduled-recurring"

echo "Launching ${VM_NAME}: CeFi forward-poll cron host (fires ${CRON_MIN} ${CRON_HOUR} UTC daily)"

if $DRY_RUN; then
    echo "[DRY-RUN] Would create VM ${VM_NAME} (zone=${ZONE}, machine=${MACHINE_TYPE})"
    echo "[DRY-RUN] Cron line: ${CRON_MIN} ${CRON_HOUR} * * * root ... launch-cefi-forward-poll.sh"
    echo "[DRY-RUN] Metadata: ${METADATA}"
    exit 0
fi

lc_write_startup_file "$STARTUP_SCRIPT"

gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" \
    --scopes=cloud-platform \
    --no-restart-on-failure \
    --metadata="${METADATA}" \
    --metadata-from-file="startup-script=${STARTUP_FILE}" \
    --labels="${LABELS}"

cat <<EOF

VM launched: ${VM_NAME}
Cadence:     daily at ${CRON_MIN} ${CRON_HOUR} UTC (cron host stays up between fires)
Logs:        gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log
Cron log:    gcloud compute ssh ${VM_NAME} --zone=${ZONE} --command 'sudo tail -50 /var/log/cefi-fwd-cron.log'
Stop:        gcloud compute instances delete ${VM_NAME} --zone=${ZONE} --quiet

Verify (T+10min): host RUNNING + first cron fire after ${CRON_HOUR}:${CRON_MIN} UTC
  gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'
EOF
