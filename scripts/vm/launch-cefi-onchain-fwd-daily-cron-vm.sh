#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a long-lived cron host VM that fires the daily CeFi on-chain-perp
# forward-poll (ASTER + LIGHTER-ZKSYNC + EXTENDED-STARKNET + HYPERLIQUID).
#
# Why this exists
# ---------------
# `launch-cefi-onchain-forward-poll.sh` / `launch-aster-forward-poll.sh` were both
# registered EPHEMERAL_BATCH (one-shot) with no scheduler ever wired to them — the
# 2026-07-14..2026-07-27 daily successes visible in the manifest came from someone
# manually re-triggering a launcher each day; once that stopped, funding/
# derivative_ticker capture for ASTER/LIGHTER-ZKSYNC/EXTENDED-STARKNET/HYPERLIQUID
# went silently dark for 5+ consecutive days with no automated freshness monitor on
# this specific data_type/venue combination and no retry/cron backstop. Mirrors the
# same fix already shipped for the Tardis CeFi + TradFi forward-polls (cron-VM
# pattern, mirror qg-snapshot/honest-coverage) — Cloud Scheduler → Cloud Run had a
# 4+ month HTTP 403 / zero-execution track record for the sibling CeFi trigger, so
# this uses the SAME proven cron-host pattern rather than resurrecting that path.
#
# Issue ref:
#   plans/active/issues/cefi_onchain_perp_forward_capture_outage_2026_08_03.md
#
# What it does
# ------------
# Boots a tiny e2-micro VM in asia-northeast1-c that installs a crontab firing
# `launch-cefi-onchain-forward-poll.sh` (via the existing CLI shape — no new
# venue-fetch code, same code path as the historical backfills) every day. That
# launcher already fans out to its own per-venue singleton-locked worker VMs
# (aster-fwd-*, cefi-lighter-*, cefi-extended-*, cefi-hyperliquid-*), so one cron
# fire drives all four venues; a stalled venue never blocks the others. Output:
#   - Daily worker VMs:  aster-fwd-{TS}, cefi-lighter-{TS}, cefi-extended-{TS},
#                        cefi-hyperliquid-{TS} (managed by
#                        launch-cefi-onchain-forward-poll.sh)
#   - Cron host log:     gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#
# The cron host VM is itself SCHEDULED_RECURRING / long-lived: it stays up, fires
# once a day, then sleeps. Singleton-locked on prefix
# `cefi-onchain-fwd-daily-cron-` so duplicate launches are refused.
#
# Cadence
# -------
# 08:00 UTC daily. On-chain-perp venues are 24/7 so the choice is operational, not
# market-driven; staggered clear of tradfi-fwd (06:00 UTC), cefi-fwd (09:00 UTC),
# and deribit-options-chain (09:15 UTC) so worker-VM creation load spreads out
# instead of co-launching many VMs in the same window.
#
# Pattern SSOTs:
#   codex/05-infrastructure/vm-launcher-runbook.md § 1 (Infrastructure / Cron VMs)
#   codex/05-infrastructure/vm-tarball-deployment.md § "EXIT-trap log upload"
#
# Usage
# -----
#   bash launch-cefi-onchain-fwd-daily-cron-vm.sh                # launch cron host
#   bash launch-cefi-onchain-fwd-daily-cron-vm.sh --force        # bypass singleton
#   bash launch-cefi-onchain-fwd-daily-cron-vm.sh --dry-run      # show plan only
#   bash launch-cefi-onchain-fwd-daily-cron-vm.sh --env staging  # staging tier
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
# Default cadence: 08:00 UTC daily (staggered clear of tradfi-fwd 06:00 and
# cefi-fwd 09:00 / deribit-options 09:15).
CRON_HOUR="${CRON_HOUR:-8}"
CRON_MIN="${CRON_MIN:-0}"

VM_PREFIX="cefi-onchain-fwd-daily-cron-"

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
            echo "Usage: $0 [--force] [--dry-run] [--env prod|staging|dev] [--hour 8] [--minute 0]" >&2
            exit 1
            ;;
    esac
done

CODE_BUCKET="$(lc_code_bucket "$PROJECT")"
lc_validate_env "$DEPLOYMENT_ENV"
lc_singleton_check "$VM_PREFIX" "$ZONE" "$PROJECT" "$FORCE"

RUN_TS="$(lc_run_ts)"
VM_NAME="${VM_PREFIX}${RUN_TS}"

LAUNCHER_GCS_PATH="gs://${CODE_BUCKET}/code/deployment-service/scripts/vm/launch-cefi-onchain-forward-poll.sh"

LOG_TRAP="$(lc_log_upload_trap_block "$VM_NAME" "$PROJECT")"

STARTUP_SCRIPT=$(cat <<STARTUP_EOF
#!/bin/bash
${LOG_TRAP}
set -euo pipefail

echo "=== cefi-onchain-fwd-daily-cron host boot \$(date -u +%FT%TZ) ==="
echo "VM_NAME=${VM_NAME}"
echo "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
echo "CRON (cefi-onchain-fwd): ${CRON_MIN} ${CRON_HOUR} * * *"

apt-get update -y >/dev/null 2>&1 || true
which gcloud || (apt-get install -y google-cloud-cli >/dev/null 2>&1 || true)

mkdir -p /opt/deployment-service/scripts/vm/lib
gsutil cp "${LAUNCHER_GCS_PATH}" /opt/deployment-service/scripts/vm/launch-cefi-onchain-forward-poll.sh
chmod +x /opt/deployment-service/scripts/vm/launch-cefi-onchain-forward-poll.sh

# PATH MUST include /snap/bin — gcloud/gsutil are the snap symlinked into
# /snap/bin on Ubuntu-2404 GCE, NOT /usr/bin; cron's minimal default PATH lacks
# it -> \`gsutil: command not found\` -> the fire never runs. Date/rc double-escaped
# (\\\$) so they evaluate at FIRE time, not startup-script-generation time
# (single-escape baked a frozen launch-minute timestamp + rc=0 into the crontab).
# Same fix as the tradfi-fwd / cefi-fwd twins (2026-06-23).
cat > /etc/cron.d/cefi-onchain-fwd-daily <<CRON_EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin
${CRON_MIN} ${CRON_HOUR} * * * root gsutil cp ${LAUNCHER_GCS_PATH} /opt/deployment-service/scripts/vm/launch-cefi-onchain-forward-poll.sh && chmod +x /opt/deployment-service/scripts/vm/launch-cefi-onchain-forward-poll.sh && DEPLOYMENT_ENV=${DEPLOYMENT_ENV} bash /opt/deployment-service/scripts/vm/launch-cefi-onchain-forward-poll.sh --env ${DEPLOYMENT_ENV} >> /var/log/cefi-onchain-fwd-cron.log 2>&1 || echo "[\\\$(date -u +%FT%TZ)] cefi-onchain-fwd cron fire FAILED rc=\\\$?" >> /var/log/cefi-onchain-fwd-cron.log
CRON_EOF
chmod 0644 /etc/cron.d/cefi-onchain-fwd-daily

( tail -F /var/log/cefi-onchain-fwd-cron.log 2>/dev/null & ) || true

echo "Cron installed; sleeping forever (cron daemon runs the schedule)."
while true; do
    sleep 3600
    echo "[\$(date -u +%FT%TZ)] cron host heartbeat — last cron fires:"
    tail -n 3 /var/log/cefi-onchain-fwd-cron.log 2>/dev/null || echo "  (no fires yet)"
done
STARTUP_EOF
)

METADATA="VM_TASK=cefi-onchain-fwd-daily-cron"
METADATA="${METADATA},VM_SERVICE=cefi_onchain_fwd_daily_cron"
METADATA="${METADATA},VM_OPERATION=cron-trigger"
METADATA="${METADATA},VM_ASSET_GROUP=CEFI"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},GCP_PROJECT_ID=${PROJECT}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"
METADATA="${METADATA},VM_LIFECYCLE_CLASS=SCHEDULED_RECURRING"

LABELS="purpose=cefi-onchain-fwd-daily-cron,env=${DEPLOYMENT_ENV},run-ts=${RUN_TS},lifecycle=scheduled-recurring,managed-by=deployment-service"

echo "Launching ${VM_NAME}: CeFi on-chain-perp forward-poll cron host (fires ${CRON_MIN} ${CRON_HOUR} UTC daily)"

if $DRY_RUN; then
    echo "[DRY-RUN] Would create VM ${VM_NAME} (zone=${ZONE}, machine=${MACHINE_TYPE})"
    echo "[DRY-RUN] Cron line: ${CRON_MIN} ${CRON_HOUR} * * * root ... launch-cefi-onchain-forward-poll.sh"
    echo "[DRY-RUN] Metadata: ${METADATA}"
    exit 0
fi

lc_write_startup_file "$STARTUP_SCRIPT"

gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT}" \
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "${PROJECT}")" \
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
Cron log:    gcloud compute ssh ${VM_NAME} --zone=${ZONE} --command 'sudo tail -50 /var/log/cefi-onchain-fwd-cron.log'
Stop:        gcloud compute instances delete ${VM_NAME} --zone=${ZONE} --quiet

Verify (T+10min): host RUNNING + first cron fire after ${CRON_HOUR}:${CRON_MIN} UTC
  gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'
EOF
