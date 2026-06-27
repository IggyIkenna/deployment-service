#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a long-lived GCE VM that runs the sports fixture-aware trigger
# scheduler as a daemon (polling loop).
#
# Purpose: activate Plan 3 (sports_scheduler_cron_activation_2026_04_21) via
# VM-based deployment instead of Cloud Run Job + Cloud Scheduler cron. Cloud
# Run path is blocked on Plans 12 + 13 (deployment-service Dockerfile /
# cloudbuild.yaml broken for ~2 months; UTL base image stale). VM-based
# scheduling bypasses both: existing tarball infrastructure (SSOT:
# codex/05-infrastructure/vm-tarball-deployment.md) already ships UAC / UTL /
# deployment-service to any VM, and the already-shipped
# SportsTriggerScheduler.run() is a continuous 300-s poll loop — the exact
# daemon shape a VM gives you for free.
#
# SSOT: unified-trading-pm/codex/02-data/sports-scheduling-and-sharding.md §8
# (Cloud Run vs VM economics). For a long-running poll-every-5-min process
# where wall-time IS the product, a single e2-small VM (~$12/mo) beats a
# Cloud Run Job that pays warm-start cost 288 times/day.
#
# Invocation inside the VM (assembled by setup-data-pipeline-vm.sh from
# VM_TASK=sports-scheduler-poll):
#   cd $WORKSPACE/deployment
#   $VENV/bin/python -m deployment_service sports-trigger run \
#       --config configs/sports-trigger-tiers.yaml \
#       --poll-interval 300
#
# Runs until manually deleted. Systemd-managed nohup keeps the scheduler
# alive across transient crashes (the scheduler itself has a try/except in
# run() that swallows exceptions and continues polling — VM just has to stay
# up).
#
# Prerequisites:
#   - SPORTS tarballs on gs://deployment-scripts-central-element-323112/code/
#     (refresh with: bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS)
#   - deployment-service-code.tar.gz with sports_trigger_periodic.py + CLI
#   - Service account with storage.objectAdmin + pubsub.publisher + compute.instanceAdmin.v1
#     (child-VM dispatch via gcloud compute instances create)
#
# Usage:
#   bash launch-sports-scheduler-vm.sh               # launch singleton scheduler
#   bash launch-sports-scheduler-vm.sh --dry-run     # log dispatches, do not execute
#   bash launch-sports-scheduler-vm.sh --force       # bypass singleton lock
#
# Cost: e2-small 24/7 ~= $12/mo (asia-northeast1). The scheduler is lightweight
# polling — CPU is mostly idle between 5-min ticks.
#
# Singleton lock: refuses to launch if any sports-scheduler-* VM is already
# running in the zone. Multiple schedulers would double-fire every Tier-1
# discovery + Tier-2 reference cycle and race on the last_run GCS state file.
# Pass --force to bypass for legitimate parallel investigation (staging vs
# prod scheduler running side by side).
#
# Stop / delete:
#   gcloud compute instances delete <vm-name> --zone=asia-northeast1-c --quiet
#
# Log tail:
#   gsutil cat gs://deployment-scripts-central-element-323112/vm-logs/<vm-name>/run.log
#
# Incident reference: 2026-04-19 SFI thundering-herd — 10 concurrent VMs
# sharing one rate-limited API key produced ~4 useful writes in 6 hours.
# The singleton pattern below copies launch-sfi-forward-poll.sh to prevent
# the same thing happening for sports-scheduler state-bucket coordination.
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

FORCE=false
DRY_RUN=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Scheduler defaults to SPOT (~60-91% cheaper); it resumes safely from GCS
# state after preemption (singleton lock + sports_scheduler_state/).
# --on-demand forces standard provisioning when SPOT capacity is unavailable.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND="${ON_DEMAND:-false}"
while [[ $# -gt 0 ]]; do
  case "${1:-}" in
    --force) FORCE=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand) ON_DEMAND=true; shift ;;
    --help|-h)
      sed -n '1,55p' "$0"
      exit 0
      ;;
    *)
      echo "ERROR: unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

# ── Singleton lock: only ONE scheduler VM at a time ──
# Two schedulers would double-dispatch every Tier-1 / Tier-2 cycle and race
# on the last_run[tier] state file in GCS.
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^sports-scheduler-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: sports-scheduler VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — two schedulers would double-dispatch
Tier-1/Tier-2 cycles and race on last_run[tier] GCS state.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="sports-scheduler-${RUN_TS}"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

# Preemption auto-relaunch: when GCE preempts this SPOT VM the shutdown script
# detects the preemption flag, then launches a fresh scheduler VM so the tier
# poll cadence resumes within one poll interval (~5 min). The singleton lock
# bypassed via --force because the preempted VM is still briefly visible.
# GCS state (sports_scheduler_state/) ensures the new VM picks up where the
# preempted one left off (no tier double-fire, no gap > one poll interval).
SHUTDOWN_FILE=$(mktemp)
trap 'rm -f "$SHUTDOWN_FILE"' EXIT
cat > "$SHUTDOWN_FILE" <<'SHUTDOWN_EOF'
#!/usr/bin/env bash
PREEMPTED=$(curl -sf -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/preempted' 2>/dev/null || echo 'false')
[[ "$PREEMPTED" == "true" ]] || exit 0
SELF_VM=$(curl -sf -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/name' 2>/dev/null || echo '')
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
# Write PREEMPTED GCS marker so the fleet monitor classifies this as a benign
# relaunch (not DP_VM_GONE_NO_CAPTURE CRITICAL). Best-effort — never block relaunch.
[[ -n "$SELF_VM" ]] && echo "1" | gsutil -q cp - "gs://${CODE_BUCKET}/vm-logs/${SELF_VM}/PREEMPTED" 2>/dev/null || true
DEPL_ENV=$(curl -sf -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/attributes/DEPLOYMENT_ENV' \
  2>/dev/null || echo 'prod')
DRY=$(curl -sf -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_SCHEDULER_DRY_RUN' \
  2>/dev/null || echo '')
ZONE="asia-northeast1-c"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
NEW_VM="sports-scheduler-${RUN_TS}"
META="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
META="${META},VM_TASK=sports-scheduler-poll,VM_SERVICE=deployment_service"
META="${META},VM_ASSET_GROUP=sports,VM_MODE=live,DEPLOYMENT_ENV=${DEPL_ENV}"
[[ "$DRY" == "true" ]] && META="${META},VM_SCHEDULER_DRY_RUN=true"
echo "[preemption-relaunch] launching ${NEW_VM} (SPOT, --force bypasses singleton)" >&2
# nohup + & so SIGTERM from preemption can't kill us before gcloud finishes
nohup gcloud compute instances create "${NEW_VM}" \
  --project="${PROJECT}" --zone="${ZONE}" \
  --machine-type=e2-small \
  --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
  --boot-disk-size=30GB --scopes=cloud-platform \
  --metadata="${META}" \
  --labels="purpose=sports-scheduler,env=${DEPL_ENV},run-ts=${RUN_TS},tier=scheduler" \
  --provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure \
  >/tmp/preempt-relaunch.log 2>&1 &
SHUTDOWN_EOF

echo "Launching $VM_NAME: sports fixture-aware trigger scheduler (daemon, poll=300s) [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
if $DRY_RUN; then
  echo "  DRY-RUN MODE: scheduler will log dispatches without firing CLI commands"
fi

METADATA="VM_TASK=sports-scheduler-poll"
METADATA="${METADATA},VM_SERVICE=deployment_service"
METADATA="${METADATA},VM_ASSET_GROUP=sports"
METADATA="${METADATA},VM_MODE=live"
if $DRY_RUN; then
  METADATA="${METADATA},VM_SCHEDULER_DRY_RUN=true"
fi
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
# No VM_SHUTDOWN_ON_COMPLETION — this is a daemon, not a one-shot backfill.

# shellcheck disable=SC2086
gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type=e2-small \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=30GB \
  --scopes=cloud-platform \
  ${PROVISIONING_FLAGS} \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --metadata-from-file="shutdown-script=${SHUTDOWN_FILE}" \
  --labels=purpose=sports-scheduler,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",tier=scheduler

echo ""
echo "VM launched: $VM_NAME"
echo "Zone: $ZONE"
echo "Machine type: e2-small"
echo "Provisioning: $([[ -n "$PROVISIONING_FLAGS" ]] && echo 'SPOT (auto-relaunches on preemption)' || echo 'on-demand')"
echo ""
echo "Logs:"
echo "  SSH tail:   gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/sports-scheduler.log'"
echo "  GCS tail:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "State bucket: gs://${CODE_BUCKET}/sports_scheduler_state/"
echo ""
echo "First tick expected within ~5 min of boot (VM setup ~3 min + first poll ~1 min)"
echo ""
echo "Stop scheduler:"
echo "  gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
