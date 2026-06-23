#!/usr/bin/env bash
# Epic: observability_master
# Lifecycle: permanent
# Delete-when: NA
#
# Ad-hoc fire / smoke the data-pipeline fleet monitors (Wave 4a of
# data_pipeline_hardening_self_monitoring_2026_06_22).
#
# The canonical schedule is the Cloud Run Jobs + Scheduler crons in
# terraform/gcp/data_pipeline_fleet_monitor_scheduler.tf (exit-code + heartbeat
# every */5, meta every */15). This launcher is the manual-fire convenience for
# a one-off run / verification — it `gcloud run jobs execute`s the already-
# provisioned job for the chosen mode, so it does NOT create infra (terraform
# owns that), it only triggers an existing job.
#
# Why Cloud Run Jobs (not a VM): these monitors are stateless cheap GCS/compute
# reads on a cron — the consolidator-liveness / catalogue-regen pattern, NOT a
# long-lived poll VM. The zombie watchdog stays a VM because it must KILL VMs;
# these only OBSERVE + emit DP_* events.
#
# Usage:
#   bash launch-data-pipeline-fleet-monitor.sh --mode exit-code   # DP-VM-001/002
#   bash launch-data-pipeline-fleet-monitor.sh --mode heartbeat   # DP-VM-003/004
#   bash launch-data-pipeline-fleet-monitor.sh --mode meta        # DP-CATALOG/WATCHER
#   bash launch-data-pipeline-fleet-monitor.sh --mode exit-code --dry-run
#
# Post-fire verification (no fire-and-forget): the command waits for the
# execution and prints its status; an exit-0 execution + a #data-pipeline-alerts
# message (or a clean "0 non-clean" log line) is the success signal.
set -euo pipefail

PROJECT="central-element-323112"
REGION="asia-northeast1"
ENV_PREFIX="prod"
MODE=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)    MODE="$2"; shift 2 ;;
    --region)  REGION="$2"; shift 2 ;;
    --env)     ENV_PREFIX="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --help|-h) grep '^#' "$0" | head -30; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

case "$MODE" in
  exit-code) JOB="${ENV_PREFIX}-dp-exit-code-monitor" ;;
  heartbeat) JOB="${ENV_PREFIX}-dp-heartbeat-watcher" ;;
  meta)      JOB="${ENV_PREFIX}-dp-meta-watchers" ;;
  *) echo "ERROR: --mode must be one of exit-code|heartbeat|meta (got: '${MODE}')" >&2; exit 1 ;;
esac

ARGS=()
if $DRY_RUN; then
  ARGS+=(--args="--mode,${MODE},--dry-run")
fi

echo "Firing Cloud Run Job ${JOB} (mode=${MODE}, dry_run=${DRY_RUN}) in ${REGION}..."
gcloud run jobs execute "${JOB}" \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --wait \
  "${ARGS[@]}" 2>&1 | tail -20

echo ""
echo "Verify the execution + the #data-pipeline-alerts channel for any DP_* alerts."
