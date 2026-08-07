#!/usr/bin/env bash
# Epic: batch_live_symmetry_master
# Lifecycle: temporary
# Delete-when: cefi cold-tier backfill for 2026-08-01..2026-08-07 verified complete
#              (cold parquet exists for each cefi data_type × each missed date)
#
# Backfill trigger for live-event-log-compactor missed dates.
#
# Context: the daily Cloud Run Job OOM-killed (512Mi) on every run from 2026-08-01
# onward while compacting the cefi/book_snapshot_5 shard (1,497 warm files).
# After the memory fix ships (todo 1 in the issue doc), run this script to
# backfill the cold parquet for each missed date.
#
# Usage (AFTER the OOM fix is deployed and a clean daily run confirms it works):
#   bash scripts/jobs/run-compactor-date-range-backfill.sh
#
# Optional env overrides:
#   DRY_RUN=1            — pass DRY_RUN=1 to each job execution (no GCS writes)
#   REGION=<region>      — Cloud Run region (default: asia-northeast1)
#   JOB_NAME=<name>      — Cloud Run Job name (default: live-event-log-compactor)
#
# Each date is submitted as a separate Cloud Run Job execution with
# COMPACTION_DATE=<date> env override (see CompactorConfig in live_event_log_compactor.py).
# Executions run sequentially (one at a time) to avoid concurrent writes to the
# same cold GCS prefix.
#
# Plan: plans/active/issues/cefi_live_event_cold_compactor_oom_and_legacy_path_check_2026_08_07.md

set -euo pipefail

REGION="${REGION:-asia-northeast1}"
JOB_NAME="${JOB_NAME:-live-event-log-compactor}"
DRY_RUN_VAL="${DRY_RUN:-0}"

# Missed dates: 2026-08-01 through today-1 (the dates where the daily cron failed).
# Adjust this list if the fix ships later and more days accumulate.
MISSED_DATES=(
  "2026-08-01"
  "2026-08-02"
  "2026-08-03"
  "2026-08-04"
  "2026-08-05"
  "2026-08-06"
  "2026-08-07"
)

echo "[backfill] Starting cefi cold-tier backfill via Cloud Run Job: ${JOB_NAME} region=${REGION}"
echo "[backfill] Dates to compact: ${MISSED_DATES[*]}"
echo "[backfill] DRY_RUN=${DRY_RUN_VAL}"
echo ""

for TARGET_DATE in "${MISSED_DATES[@]}"; do
  echo "[backfill] Submitting execution for date=${TARGET_DATE} ..."

  ENV_OVERRIDES="COMPACTION_DATE=${TARGET_DATE}"
  if [[ "${DRY_RUN_VAL}" == "1" ]]; then
    ENV_OVERRIDES="${ENV_OVERRIDES},DRY_RUN=1"
  fi

  EXEC_NAME=$(gcloud run jobs execute "${JOB_NAME}" \
    --region="${REGION}" \
    --update-env-vars="${ENV_OVERRIDES}" \
    --wait \
    --format="value(metadata.name)" 2>&1)

  EXIT_CODE=$?
  if [[ ${EXIT_CODE} -ne 0 ]]; then
    echo "[backfill] ERROR: execution for ${TARGET_DATE} failed (exit ${EXIT_CODE})"
    echo "[backfill] Execution name: ${EXEC_NAME}"
    echo "[backfill] Check logs: gcloud run jobs executions logs tail ${EXEC_NAME} --region=${REGION}"
    exit 1
  fi

  echo "[backfill] OK: ${TARGET_DATE} compacted — execution ${EXEC_NAME}"
  echo ""
done

echo "[backfill] All dates compacted. Verify cold parquet exists:"
echo "  gcloud storage ls 'gs://central-element-323112-events/live-events/cold/cefi/**'"
echo ""
echo "[backfill] Delete this script after verification:"
echo "  git rm scripts/jobs/run-compactor-date-range-backfill.sh"
