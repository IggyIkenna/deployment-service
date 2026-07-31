#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a long-lived cron host VM that fires the daily TradFi forward-poll.
#
# Why this exists
# ---------------
# Replaces the previously-broken Cloud Scheduler → Cloud Run trigger pattern
# for TradFi forward-poll. Audit 2026-05-17 (slot-5) showed NO `tradfi-fwd-`
# scheduler in asia-northeast1; the CeFi sibling `trigger-market-tick-cefi-job`
# Cloud Run service had also been returning HTTP 403 with zero executions for
# 4+ months. Operator authorised 2026-05-20 to ship Option B (cron-VM pattern,
# mirroring qg-snapshot/honest-coverage) and the matching CeFi twin in the same
# unit of work.
#
# What it does
# ------------
# Boots a tiny e2-micro VM in asia-northeast1-c that installs a crontab firing
# `gcloud compute instances create` (via the existing launch-tradfi-forward-poll.sh
# CLI shape) every day at 06:00 UTC. Output:
#   - Daily worker VM:   tradfi-fwd-{TS} (managed by launch-tradfi-forward-poll.sh)
#   - Cron host log:     gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Cron host events:  gs://{pid}-events/events/tradfi-fwd-daily-cron/...
#
# The cron host VM is itself SCHEDULED_RECURRING / long-lived: it stays up,
# fires once a day, then sleeps. Singleton-locked on prefix
# `tradfi-fwd-daily-cron-` so duplicate launches are refused.
#
# Cadence
# -------
# 06:00 UTC daily (= 15:00 JST = 02:00 EST, after CME daily settlement +
# Yahoo end-of-day FX/VIX is stable). Same cadence as qg-snapshot.
#
# Issue ref:
#   plans/active/issues/tradfi_forward_poll_cron_missing_2026_05_17.md
# Pattern SSOTs:
#   codex/05-infrastructure/vm-launcher-runbook.md § 1 (Infrastructure / Cron VMs)
#   codex/05-infrastructure/vm-tarball-deployment.md § "EXIT-trap log upload"
#
# Usage
# -----
#   bash launch-tradfi-fwd-daily-cron-vm.sh                # launch cron host
#   bash launch-tradfi-fwd-daily-cron-vm.sh --force        # bypass singleton
#   bash launch-tradfi-fwd-daily-cron-vm.sh --dry-run      # show plan only
#   bash launch-tradfi-fwd-daily-cron-vm.sh --env staging  # staging tier
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
# Default cadence: 06:00 UTC daily (= after CME/CBOE settlement; matches qg-snapshot).
CRON_HOUR="${CRON_HOUR:-6}"
CRON_MIN="${CRON_MIN:-0}"

VM_PREFIX="tradfi-fwd-daily-cron-"

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
            echo "Usage: $0 [--force] [--dry-run] [--env prod|staging|dev] [--hour 6] [--minute 0]" >&2
            exit 1
            ;;
    esac
done

CODE_BUCKET="$(lc_code_bucket "$PROJECT")"
lc_validate_env "$DEPLOYMENT_ENV"
lc_singleton_check "$VM_PREFIX" "$ZONE" "$PROJECT" "$FORCE"

RUN_TS="$(lc_run_ts)"
VM_NAME="${VM_PREFIX}${RUN_TS}"

# The cron host needs the launcher script available locally. We fetch it from
# the deployment-scripts code bucket at boot time. Operator MUST keep that
# bucket synced via:
#     bash deployment-service/scripts/vm/create-code-tarballs.sh \
#         --include deployment-service
LAUNCHER_GCS_PATH="gs://${CODE_BUCKET}/code/deployment-service/scripts/vm/launch-tradfi-forward-poll.sh"

LOG_TRAP="$(lc_log_upload_trap_block "$VM_NAME" "$PROJECT")"

STARTUP_SCRIPT=$(cat <<STARTUP_EOF
#!/bin/bash
${LOG_TRAP}
set -euo pipefail

echo "=== tradfi-fwd-daily-cron host boot \$(date -u +%FT%TZ) ==="
echo "VM_NAME=${VM_NAME}"
echo "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
echo "CRON: ${CRON_MIN} ${CRON_HOUR} * * *"

# Install gcloud + gsutil (already on Ubuntu-2404 GCE images via google-cloud-cli)
# and ensure tooling is on PATH for the cron environment.
apt-get update -y >/dev/null 2>&1 || true
which gcloud || (apt-get install -y google-cloud-cli >/dev/null 2>&1 || true)

# Pull the canonical launcher from the code bucket. Refreshed at every cron
# fire so updates to launch-tradfi-forward-poll.sh take effect within 24h
# without redeploying this cron host.
mkdir -p /opt/deployment-service/scripts/vm/lib
gsutil cp "${LAUNCHER_GCS_PATH}" /opt/deployment-service/scripts/vm/launch-tradfi-forward-poll.sh
chmod +x /opt/deployment-service/scripts/vm/launch-tradfi-forward-poll.sh

# Cron job: refresh launcher then fire forward-poll for yesterday (T-1).
# Log to /var/log/tradfi-fwd-cron.log and tee into the host's run.log so the
# EXIT-trap final upload captures the per-fire output.
# PATH MUST include /snap/bin — on Ubuntu-2404 GCE images gcloud/gsutil are the
# snap package symlinked into /snap/bin, NOT /usr/bin. Cron's default minimal
# PATH (/usr/local/sbin:…:/bin) lacks /snap/bin → \`gsutil: command not found\`
# → the && chain fails → the fire NEVER runs (root cause of "cron fire FAILED
# rc=0", 2026-06-23: the 06:00 CRON CMD executed but gsutil wasn't resolvable).
# The failure echo's date/rc are double-escaped (\\\$) so they evaluate at
# FIRE time inside the cron command, not at startup-script-generation time
# (the prior single-escape baked the launch-minute timestamp + rc=0 into the
# crontab, so the log lied — frozen timestamp, always rc=0).
cat > /etc/cron.d/tradfi-fwd-daily <<CRON_EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin
${CRON_MIN} ${CRON_HOUR} * * * root gsutil cp ${LAUNCHER_GCS_PATH} /opt/deployment-service/scripts/vm/launch-tradfi-forward-poll.sh && chmod +x /opt/deployment-service/scripts/vm/launch-tradfi-forward-poll.sh && DEPLOYMENT_ENV=${DEPLOYMENT_ENV} bash /opt/deployment-service/scripts/vm/launch-tradfi-forward-poll.sh --env ${DEPLOYMENT_ENV} >> /var/log/tradfi-fwd-cron.log 2>&1 || echo "[\\\$(date -u +%FT%TZ)] tradfi-fwd cron fire FAILED rc=\\\$?" >> /var/log/tradfi-fwd-cron.log
CRON_EOF
chmod 0644 /etc/cron.d/tradfi-fwd-daily

# Tail the cron log into the host run.log so EXIT-trap upload captures it.
( tail -F /var/log/tradfi-fwd-cron.log 2>/dev/null & ) || true

echo "Cron installed; sleeping forever (cron daemon runs the schedule)."
# Long-lived host: do not exit. Sleep keeps the startup script alive without
# spinning the CPU. Killing the VM (or shutdown signal) triggers the EXIT-trap
# and uploads run.log.
while true; do
    sleep 3600
    echo "[\$(date -u +%FT%TZ)] cron host heartbeat — last cron fires:"
    tail -n 3 /var/log/tradfi-fwd-cron.log 2>/dev/null || echo "  (no fires yet)"
done
STARTUP_EOF
)

METADATA="VM_TASK=tradfi-fwd-daily-cron"
METADATA="${METADATA},VM_SERVICE=tradfi_fwd_daily_cron"
METADATA="${METADATA},VM_OPERATION=cron-trigger"
METADATA="${METADATA},VM_ASSET_GROUP=TRADFI"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},GCP_PROJECT_ID=${PROJECT}"
# SCHEDULED_RECURRING: long-lived; DO NOT auto-shutdown on script completion
# (the startup script intentionally sleeps forever to keep cron running).
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"
METADATA="${METADATA},VM_LIFECYCLE_CLASS=SCHEDULED_RECURRING"

LABELS="purpose=tradfi-fwd-daily-cron,env=${DEPLOYMENT_ENV},run-ts=${RUN_TS},lifecycle=scheduled-recurring,managed-by=deployment-service"

echo "Launching ${VM_NAME}: TradFi forward-poll cron host (fires ${CRON_MIN} ${CRON_HOUR} UTC daily)"

if $DRY_RUN; then
    echo "[DRY-RUN] Would create VM ${VM_NAME} (zone=${ZONE}, machine=${MACHINE_TYPE})"
    echo "[DRY-RUN] Cron line: ${CRON_MIN} ${CRON_HOUR} * * * root ... launch-tradfi-forward-poll.sh"
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
Cron log:    gcloud compute ssh ${VM_NAME} --zone=${ZONE} --command 'sudo tail -50 /var/log/tradfi-fwd-cron.log'
Stop:        gcloud compute instances delete ${VM_NAME} --zone=${ZONE} --quiet

Verify (T+10min): host RUNNING + first cron fire after ${CRON_HOUR}:${CRON_MIN} UTC
  gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'
EOF
