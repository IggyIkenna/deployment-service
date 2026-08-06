#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a long-lived cron host VM that fires the daily CeFi perp-funding corpus
# compute for the CARRY_BASIS_PERP / CARRY_FUNDING_DISPERSION live-strategy venues.
#
# Why this exists
# ---------------
# The DeFi-bucket perp-funding corpus (`perp_funding`/`perp_daily_ctx`) — the single
# thing `CanonicalPerpFundingProvider.funding_window()` reads for the 6
# `catalog_carry.py` CARRY_BASIS_PERP venues — is produced by
# `features_service.cefi.calculators.perp_funding_corpus.compute_cefi_perp_funding_corpus_for_day`.
# For months that compute was driven ONLY by a manual one-off script
# (`features-service/scripts/run_cefi_perp_funding_corpus.py`) whose own header
# documented the gap: "# Delete-when: CeFi perp_funding corpus compute is promoted to
# a features-service CLI subcommand and scheduled". The promotion half shipped
# 2026-08-06 (`features-service@b2d14c9d` — `cefi` family CLI subcommand). This cron
# host is the scheduling half: a daily fire keeps the corpus current automatically
# the moment raw `derivative_ticker` input flows, with zero manual re-runs.
# Issue ref: plans/active/issues/defi_cefi_venue_chain_axis_contamination_2026_07_28.md
#   (P2 "scheduling/cron half of the corpus-compute promotion")
# Pattern SSOTs:
#   codex/05-infrastructure/vm-launcher-runbook.md § 1 (Infrastructure / Cron VMs)
#   codex/05-infrastructure/vm-tarball-deployment.md § "EXIT-trap log upload"
#
# What it does
# ------------
# Boots a tiny e2-micro VM in asia-northeast1-c that installs a crontab firing
# `launch-features-vm.sh --feature-family cefi --operation compute --mode batch
# --asset-group CEFI --start-date <today> --end-date <today> --launch-mode full`
# every day at 07:00 UTC. `launch-features-vm.sh` spawns its own worker VM
# (`features-cefi-cefi-{TS}`, VM_SHUTDOWN_ON_COMPLETION=true, SPOT) which runs the
# corpus compute for today's date and auto-deletes. The cron host copies
# `launch-features-vm.sh` fresh from GCS each fire (same bare-launcher publish
# pattern as the forward-poll cron hosts) so launcher updates take effect within one
# cron interval. Until raw `derivative_ticker` lands for a day, the compute
# honest-skips that day — the cron is meaningful the moment raw flows, and harmless
# before it (the plan's own gate: "the cron is only meaningful once raw flows; until
# then it honest-skips").
# Output:
#   - Daily worker VM:   features-cefi-cefi-{TS} (managed by launch-features-vm.sh)
#   - Cron host log:     gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#
# The cron host VM is itself SCHEDULED_RECURRING / long-lived: it stays up, fires
# once a day, then sleeps. Singleton-locked on prefix
# `cefi-perp-funding-daily-cron-` so duplicate launches are refused.
#
# Cadence
# -------
# 07:00 UTC daily. Staggered clear of tradfi-fwd (06:00 UTC), cefi-onchain-fwd
# (08:00 UTC), cefi-fwd (09:00 UTC) and deribit-options-chain (09:15 UTC) so
# worker-VM creation load spreads out instead of co-launching many VMs in the same
# window. The compute is batch, one day per fire, so timing is operational, not
# market-driven.
#
# Usage
# -----
#   bash launch-cefi-perp-funding-daily-cron-vm.sh                # launch cron host
#   bash launch-cefi-perp-funding-daily-cron-vm.sh --force        # bypass singleton
#   bash launch-cefi-perp-funding-daily-cron-vm.sh --dry-run      # show plan only
#   bash launch-cefi-perp-funding-daily-cron-vm.sh --env staging  # staging tier
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
# Default cadence: 07:00 UTC daily (staggered clear of tradfi-fwd 06:00,
# cefi-onchain-fwd 08:00, cefi-fwd 09:00, deribit-options 09:15).
CRON_HOUR="${CRON_HOUR:-7}"
CRON_MIN="${CRON_MIN:-0}"

VM_PREFIX="cefi-perp-funding-daily-cron-"

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
            echo "Usage: $0 [--force] [--dry-run] [--env prod|staging|dev] [--hour 7] [--minute 0]" >&2
            exit 1
            ;;
    esac
done

CODE_BUCKET="$(lc_code_bucket "$PROJECT")"
lc_validate_env "$DEPLOYMENT_ENV"
lc_singleton_check "$VM_PREFIX" "$ZONE" "$PROJECT" "$FORCE"

RUN_TS="$(lc_run_ts)"
VM_NAME="${VM_PREFIX}${RUN_TS}"

LAUNCHER_GCS_PATH="gs://${CODE_BUCKET}/code/deployment-service/scripts/vm/launch-features-vm.sh"
# `launch-features-vm.sh` sources `lib/launcher_common.sh` (and uses its `lc_*`
# helpers) at fire time, so the cron host must hold a copy of it too — refreshed
# alongside the launcher each fire so they ship in lockstep. (The forward-poll
# cron hosts copy only the launcher; their fired launchers share the same lib
# dependency, so this is the strictly-correct form for a fresh cron host.)
LIB_LAUNCHER_GCS_PATH="gs://${CODE_BUCKET}/code/deployment-service/scripts/vm/lib/launcher_common.sh"

LOG_TRAP="$(lc_log_upload_trap_block "$VM_NAME" "$PROJECT")"

STARTUP_SCRIPT=$(cat <<STARTUP_EOF
#!/bin/bash
${LOG_TRAP}
set -euo pipefail

echo "=== cefi-perp-funding-daily-cron host boot \$(date -u +%FT%TZ) ==="
echo "VM_NAME=${VM_NAME}"
echo "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
echo "CRON (cefi-perp-funding): ${CRON_MIN} ${CRON_HOUR} * * *"

apt-get update -y >/dev/null 2>&1 || true
which gcloud || (apt-get install -y google-cloud-cli >/dev/null 2>&1 || true)

mkdir -p /opt/deployment-service/scripts/vm/lib
gsutil cp "${LAUNCHER_GCS_PATH}" /opt/deployment-service/scripts/vm/launch-features-vm.sh
chmod +x /opt/deployment-service/scripts/vm/launch-features-vm.sh
gsutil cp "${LIB_LAUNCHER_GCS_PATH}" /opt/deployment-service/scripts/vm/lib/launcher_common.sh

# PATH MUST include /snap/bin — gcloud/gsutil are the snap symlinked into
# /snap/bin on Ubuntu-2404 GCE, NOT /usr/bin; cron's minimal default PATH lacks
# it -> \`gsutil: command not found\` -> the fire never runs. Date/rc double-escaped
# (\\\$) so they evaluate at FIRE time, not startup-script-generation time
# (single-escape baked a frozen launch-minute timestamp + rc=0 into the crontab).
# Same fix as the tradfi-fwd / cefi-fwd / cefi-onchain-fwd cron-host twins.
#
# --launch-mode full is REQUIRED: launch-features-vm.sh defaults to dry, and a dry
# launch only prints a plan (never creates the worker VM), so a bare invocation
# here would silently no-op the daily fire.
cat > /etc/cron.d/cefi-perp-funding-daily <<CRON_EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin
${CRON_MIN} ${CRON_HOUR} * * * root gsutil cp ${LAUNCHER_GCS_PATH} /opt/deployment-service/scripts/vm/launch-features-vm.sh && chmod +x /opt/deployment-service/scripts/vm/launch-features-vm.sh && gsutil cp ${LIB_LAUNCHER_GCS_PATH} /opt/deployment-service/scripts/vm/lib/launcher_common.sh && DEPLOYMENT_ENV=${DEPLOYMENT_ENV} bash /opt/deployment-service/scripts/vm/launch-features-vm.sh --feature-family cefi --operation compute --mode batch --asset-group CEFI --start-date \\\$(date -u +%F) --end-date \\\$(date -u +%F) --launch-mode full --env ${DEPLOYMENT_ENV} >> /var/log/cefi-perp-funding-cron.log 2>&1 || echo "[\\\$(date -u +%FT%TZ)] cefi-perp-funding cron fire FAILED rc=\\\$?" >> /var/log/cefi-perp-funding-cron.log
CRON_EOF
chmod 0644 /etc/cron.d/cefi-perp-funding-daily

( tail -F /var/log/cefi-perp-funding-cron.log 2>/dev/null & ) || true

echo "Cron installed; sleeping forever (cron daemon runs the schedule)."
while true; do
    sleep 3600
    echo "[\$(date -u +%FT%TZ)] cron host heartbeat — last cron fires:"
    tail -n 3 /var/log/cefi-perp-funding-cron.log 2>/dev/null || echo "  (no fires yet)"
done
STARTUP_EOF
)

METADATA="VM_TASK=cefi-perp-funding-daily-cron"
METADATA="${METADATA},VM_SERVICE=cefi_perp_funding_daily_cron"
METADATA="${METADATA},VM_OPERATION=cron-trigger"
METADATA="${METADATA},VM_ASSET_GROUP=CEFI"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},GCP_PROJECT_ID=${PROJECT}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"
METADATA="${METADATA},VM_LIFECYCLE_CLASS=SCHEDULED_RECURRING"

LABELS="purpose=cefi-perp-funding-daily-cron,env=${DEPLOYMENT_ENV},run-ts=${RUN_TS},lifecycle=scheduled-recurring,managed-by=deployment-service"

echo "Launching ${VM_NAME}: CeFi perp-funding corpus compute cron host (fires ${CRON_MIN} ${CRON_HOUR} UTC daily)"

if $DRY_RUN; then
    echo "[DRY-RUN] Would create VM ${VM_NAME} (zone=${ZONE}, machine=${MACHINE_TYPE})"
    echo "[DRY-RUN] Cron line: ${CRON_MIN} ${CRON_HOUR} * * * root ... launch-features-vm.sh --feature-family cefi --launch-mode full"
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
Cron log:    gcloud compute ssh ${VM_NAME} --zone=${ZONE} --command 'sudo tail -50 /var/log/cefi-perp-funding-cron.log'
Stop:        gcloud compute instances delete ${VM_NAME} --zone=${ZONE} --quiet

Verify (T+10min): host RUNNING + first cron fire after ${CRON_HOUR}:${CRON_MIN} UTC
  gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'
EOF
