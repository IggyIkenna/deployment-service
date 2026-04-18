#!/usr/bin/env bash
# VM execution wrapper (Gate G1 — deployment observability).
#
# Responsibilities:
#   1. Tee stdout + stderr to GCS every ~30s so operators can tail a long-running
#      VM job from outside the VM even when SSH is broken.
#   2. Register the deployment in the GCS-backed deployments registry via
#      deployment_heartbeat.py and emit DEPLOYMENT_STARTED at launch.
#   3. Heartbeat every 60s while the task is running (DEPLOYMENT_PROGRESS, with
#      counters parsed from the last few lines of the log if present).
#   4. On exit, archive the registry entry + emit DEPLOYMENT_COMPLETED or
#      DEPLOYMENT_FAILED with structured stats (rows_in/out/error/events from
#      the log's final "counters=" dict if present).
#
# Usage (backward-compatible with the old 2-arg form):
#
#     bash vm-exec-with-gcs-tee.sh <gs://.../run.log> <command ...>
#
# Extended usage with structured registry metadata (preferred):
#
#     VM_NAME=canonical-migration-cefi-20260418-042359 \
#     VM_CATEGORY=CEFI VM_TASK=canonical-migration VM_MODE=full \
#     VM_START_DATE=2024-06-01 VM_END_DATE=2024-06-30 \
#     bash vm-exec-with-gcs-tee.sh <gs://.../run.log> <command ...>
#
# Env var fallbacks (sensible for legacy callers):
#   VM_NAME       = $(hostname)
#   VM_CATEGORY   = UNKNOWN
#   VM_TASK       = vm-exec
#   VM_MODE       = full     (one of: dry|full|backfill|forward-poll|smoke)
#   VM_START_DATE = today (UTC)
#   VM_END_DATE   = today (UTC)
#   PYTHON_BIN    = python (override to point at the tarball's venv)
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

# ---- structured deployment metadata (registry + events) ----
VM_NAME="${VM_NAME:-$(hostname)}"
VM_CATEGORY="${VM_CATEGORY:-UNKNOWN}"
VM_TASK="${VM_TASK:-vm-exec}"
VM_MODE="${VM_MODE:-full}"
TODAY_UTC="$(date -u +%Y-%m-%d)"
VM_START_DATE="${VM_START_DATE:-$TODAY_UTC}"
VM_END_DATE="${VM_END_DATE:-$TODAY_UTC}"
PYTHON_BIN="${PYTHON_BIN:-python}"

# deployment_heartbeat.py sits next to this script on the VM.
HEARTBEAT_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/deployment_heartbeat.py"

# Generate the deployment_id (uuid4). Prefer python (always available on VMs
# that run our tarball); fall back to /proc/sys/kernel/random/uuid.
if command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    DEPLOYMENT_ID="$("$PYTHON_BIN" -c 'import uuid; print(uuid.uuid4())')"
elif [[ -r /proc/sys/kernel/random/uuid ]]; then
    DEPLOYMENT_ID="$(cat /proc/sys/kernel/random/uuid)"
else
    DEPLOYMENT_ID="$(date +%s)-$$"
fi

# Helper that runs deployment_heartbeat.py. Failures are logged but do NOT
# abort the wrapped task — registry is observability, not control flow.
run_heartbeat() {
    local op="$1"; shift
    if [[ ! -f "$HEARTBEAT_SCRIPT" ]]; then
        echo "[vm-exec] WARN: heartbeat helper missing at $HEARTBEAT_SCRIPT — skipping $op" >> "$LOCAL_LOG"
        return 0
    fi
    "$PYTHON_BIN" "$HEARTBEAT_SCRIPT" "$op" "$@" 2>> "$LOCAL_LOG" || {
        echo "[vm-exec] WARN: heartbeat $op failed (rc=$?) — continuing" >> "$LOCAL_LOG"
    }
}

# Best-effort parser for the final "counters={...}" or "rows_in=X rows_out=Y"
# lines that our migration scripts emit. Echoes space-separated key=value pairs
# that the caller can eval into shell vars.
parse_counters() {
    local log="$1"
    [[ -r "$log" ]] || return 0
    "$PYTHON_BIN" - "$log" <<'PY'
import re, sys, json, pathlib
path = pathlib.Path(sys.argv[1])
try:
    text = path.read_text(errors="ignore")
except OSError:
    sys.exit(0)
# Scan from the tail for the most recent counters= or rows_in= line.
tail = "\n".join(text.splitlines()[-200:])

out = {"rows_in": 0, "rows_out": 0, "rows_error": 0, "events_emitted": 0}
# JSON-ish "counters={...}"
m = re.search(r"counters\s*=\s*(\{[^}]+\})", tail)
if m:
    try:
        data = json.loads(m.group(1).replace("'", '"'))
        for k in out:
            if k in data:
                out[k] = int(data[k])
    except (json.JSONDecodeError, ValueError):
        pass
# Loose key=value pairs
for key in out:
    m2 = re.search(rf"{key}\s*=\s*(\d+)", tail)
    if m2:
        out[key] = max(out[key], int(m2.group(1)))
print(" ".join(f"{k}={v}" for k, v in out.items()))
PY
}

# ---- start ----
echo "[vm-exec] deployment_id=$DEPLOYMENT_ID vm=$VM_NAME category=$VM_CATEGORY task=$VM_TASK mode=$VM_MODE" | tee "$LOCAL_LOG"
echo "[vm-exec] starting: $*" | tee -a "$LOCAL_LOG"
echo "[vm-exec] log -> $GCS_LOG_URI (upload every 30s)" | tee -a "$LOCAL_LOG"

run_heartbeat register \
    --id "$DEPLOYMENT_ID" \
    --name "$VM_NAME" \
    --category "$VM_CATEGORY" \
    --task "$VM_TASK" \
    --mode "$VM_MODE" \
    --start-date "$VM_START_DATE" \
    --end-date "$VM_END_DATE" \
    --log-uri "$GCS_LOG_URI"

# Start the command in background, capture its PID, tee stdout+stderr.
( "$@" 2>&1; echo "[vm-exec] command exited rc=$?" ) >> "$LOCAL_LOG" &
CMD_PID=$!
echo "$CMD_PID" > "$PID_FILE"

# Periodic uploader: every 30s copy the log to GCS while command is alive.
(
    while kill -0 "$CMD_PID" 2>/dev/null; do
        gsutil -q cp "$LOCAL_LOG" "$GCS_LOG_URI" 2>/dev/null || true
        sleep 30
    done
) &
UPLOADER_PID=$!

# Heartbeat loop: every 60s emit DEPLOYMENT_PROGRESS with best-effort counters.
(
    while kill -0 "$CMD_PID" 2>/dev/null; do
        sleep 60
        kill -0 "$CMD_PID" 2>/dev/null || break
        counters="$(parse_counters "$LOCAL_LOG")"
        # shellcheck disable=SC2086
        eval $counters
        run_heartbeat heartbeat \
            --id "$DEPLOYMENT_ID" \
            --rows-in "${rows_in:-0}" \
            --rows-out "${rows_out:-0}" \
            --rows-error "${rows_error:-0}" \
            --events-emitted "${events_emitted:-0}"
    done
) &
HEARTBEAT_PID=$!

# Wait for command, record exit code, stop helpers, final upload.
wait "$CMD_PID"
RC=$?
kill "$UPLOADER_PID" 2>/dev/null || true
kill "$HEARTBEAT_PID" 2>/dev/null || true

gsutil -q cp "$LOCAL_LOG" "$GCS_LOG_URI" 2>/dev/null || true
echo "$RC" | gsutil -q cp - "$EXIT_STATUS_URI" 2>/dev/null || true

# Parse final counters and emit DEPLOYMENT_COMPLETED or _FAILED.
final_counters="$(parse_counters "$LOCAL_LOG")"
# shellcheck disable=SC2086
eval $final_counters
FINAL_STATUS="completed"
[[ "$RC" -ne 0 ]] && FINAL_STATUS="failed"

run_heartbeat complete \
    --id "$DEPLOYMENT_ID" \
    --exit-code "$RC" \
    --status "$FINAL_STATUS" \
    --rows-in "${rows_in:-0}" \
    --rows-out "${rows_out:-0}" \
    --rows-error "${rows_error:-0}" \
    --events-emitted "${events_emitted:-0}"

echo "[vm-exec] final log uploaded, exit=$RC, status=$FINAL_STATUS"
exit "$RC"
