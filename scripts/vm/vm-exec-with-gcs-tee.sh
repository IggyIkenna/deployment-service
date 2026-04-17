#!/usr/bin/env bash
# VM execution wrapper that tees stdout + stderr to GCS every ~30s so we can
# tail a long-running process from outside the VM even when SSH is broken.
#
# Used by setup-data-pipeline-vm.sh: instead of
#   nohup python -m market_tick_data_service ... > /var/log/backfill.log 2>&1 &
# it does
#   nohup bash vm-exec-with-gcs-tee.sh "gs://.../vm-logs/<vm-name>/run.log" \
#       python -m market_tick_data_service ... &
#
# That writes to /tmp/run.log locally AND uploads a rolling copy every 30s to
# gs://.../vm-logs/<vm>/run.log so operators can `gsutil cat gs://.../run.log`
# to see live progress.
#
# On command exit, uploads a final copy + an EXIT_STATUS file with the rc.
set -uo pipefail

GCS_LOG_URI="${1:-}"
shift || true

if [[ -z "$GCS_LOG_URI" || $# -eq 0 ]]; then
    echo "usage: $0 <gs://bucket/path/run.log> <command...>" >&2
    exit 2
fi

LOCAL_LOG="/tmp/vm-exec-$$.log"
GCS_DIR="$(dirname "$GCS_LOG_URI")"
EXIT_STATUS_URI="${GCS_DIR}/EXIT_STATUS"
PID_FILE="/tmp/vm-exec-$$.pid"

echo "[vm-exec-with-gcs-tee] starting: $*" | tee "$LOCAL_LOG"
echo "[vm-exec-with-gcs-tee] log -> $GCS_LOG_URI (upload every 30s)" | tee -a "$LOCAL_LOG"

# Start the command in background, capture its PID, tee stdout+stderr.
( "$@" 2>&1; echo "[vm-exec-with-gcs-tee] command exited rc=$?" ) >> "$LOCAL_LOG" &
CMD_PID=$!
echo "$CMD_PID" > "$PID_FILE"

# Periodic uploader: every 30s, cp the local log to GCS while command is alive.
(
    while kill -0 "$CMD_PID" 2>/dev/null; do
        gsutil -q cp "$LOCAL_LOG" "$GCS_LOG_URI" 2>/dev/null || true
        sleep 30
    done
) &
UPLOADER_PID=$!

# Wait for command to finish, record its exit code, do final upload.
wait "$CMD_PID"
RC=$?
kill "$UPLOADER_PID" 2>/dev/null || true

gsutil -q cp "$LOCAL_LOG" "$GCS_LOG_URI" 2>/dev/null || true
echo "$RC" | gsutil -q cp - "$EXIT_STATUS_URI" 2>/dev/null || true

echo "[vm-exec-with-gcs-tee] final log uploaded, exit=$RC"
exit "$RC"
