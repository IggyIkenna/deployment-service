#!/usr/bin/env bash
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
#     (refresh with: bash deployment-service/scripts/vm/create-code-tarballs.sh --category SPORTS)
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
set -euo pipefail

FORCE=false
DRY_RUN=false
while [[ $# -gt 0 ]]; do
  case "${1:-}" in
    --force) FORCE=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
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

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

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

echo "Launching $VM_NAME: sports fixture-aware trigger scheduler (daemon, poll=300s)"
if $DRY_RUN; then
  echo "  DRY-RUN MODE: scheduler will log dispatches without firing CLI commands"
fi

METADATA="VM_TASK=sports-scheduler-poll"
METADATA="${METADATA},VM_SERVICE=deployment_service"
METADATA="${METADATA},VM_CATEGORY=SPORTS"
METADATA="${METADATA},VM_MODE=live"
if $DRY_RUN; then
  METADATA="${METADATA},VM_SCHEDULER_DRY_RUN=true"
fi
# No VM_SHUTDOWN_ON_COMPLETION — this is a daemon, not a one-shot backfill.

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type=e2-small \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=30GB \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --labels=purpose=sports-scheduler,run-ts="${RUN_TS}",tier=scheduler

echo ""
echo "VM launched: $VM_NAME"
echo "Zone: $ZONE"
echo "Machine type: e2-small"
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
