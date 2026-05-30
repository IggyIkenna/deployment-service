#!/usr/bin/env bash
# VM execution wrapper (Gate G1 — deployment observability).
#
# Responsibilities:
#   1. Tee stdout + stderr to a local log.
#   2. Launch the Python heartbeat daemon (scripts/vm/heartbeat_daemon.py),
#      which OWNS both the 60s Pub/Sub heartbeat loop and the 30s GCS log
#      uploader loop inside a single long-lived Python process.
#   3. Local-log activity watchdog (still shell-side — SIGKILLs a stuck
#      Python process, doesn't need UTL).
#   4. On CMD_PID exit: write the final exit status into a file the daemon
#      reads + SIGTERM the daemon. The daemon emits DEPLOYMENT_COMPLETED /
#      DEPLOYMENT_FAILED + final GCS upload and exits.
#
# Why a daemon instead of per-tick subprocesses:
#   The previous layout forked `python deployment_heartbeat.py heartbeat ...`
#   every 60s and `gsutil cp` every 30s. Each fork paid ~4-5s (Python) or
#   ~1s (gsutil) cold-start cost + a fresh gRPC auth token mint. Across
#   105 concurrent VMs that piled up into auth-token rate limits and
#   multi-minute heartbeat latency. The daemon imports UTL once, keeps
#   cached `PubSubEventSink` + `get_storage_client()` singletons warm, and
#   pays zero cold-start per iter.
#
# Usage (backward-compatible with the old 2-arg form):
#
#     bash vm-exec-with-gcs-tee.sh <gs://.../run.log> <command ...>
#
# Extended usage with structured registry metadata (preferred):
#
#     VM_NAME=canonical-migration-cefi-20260418-042359 \
#     VM_ASSET_GROUP=CEFI VM_TASK=canonical-migration VM_MODE=full \
#     VM_START_DATE=2024-06-01 VM_END_DATE=2024-06-30 \
#     bash vm-exec-with-gcs-tee.sh <gs://.../run.log> <command ...>
#
# Env var fallbacks (sensible for legacy callers):
#   VM_NAME         = $(hostname)
#   VM_ASSET_GROUP  = UNKNOWN     (legacy alias: VM_CATEGORY accepted as fallback)
#   VM_TASK         = vm-exec
#   VM_MODE       = full     (one of: dry|full|backfill|forward-poll|smoke)
#   VM_START_DATE = today (UTC)
#   VM_END_DATE   = today (UTC)
#   PYTHON_BIN    = python (override to point at the tarball's venv)
#   HEARTBEAT_INTERVAL_SEC = 60 (forwarded to daemon)
#   UPLOAD_INTERVAL_SEC    = 30 (forwarded to daemon)
set -uo pipefail

# Detach from any inherited process group / session immediately, AND
# reject SIGHUP/SIGTERM from systemd's ``KillMode=control-group`` on the
# google-startup-scripts.service. Without this, when setup-data-pipeline-vm.sh
# (the GCE startup script) finishes and systemd tears its cgroup down, our
# nohup'd child gets SIGTERM mid-execution — typically right after the
# heartbeat daemon exits but BEFORE the self-delete block at the bottom
# of this file fires. Result: VMs finish rc=0, log "DEPLOYMENT_COMPLETED",
# then sit RUNNING forever.
#
# Strategy: trap SIGTERM so the kill becomes a no-op, AND re-exec with setsid
# if not already a session leader so we live in our own session ID.
# We do still honour SIGINT (Ctrl-C from operator) for manual debugging.
trap '' HUP TERM
if [[ -z "${VM_EXEC_DETACHED:-}" ]] && command -v setsid >/dev/null 2>&1; then
    export VM_EXEC_DETACHED=1
    exec setsid bash "$0" "$@"
fi

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
EXIT_STATUS_FILE="/tmp/vm-exec-$$.exit_status"
STALL_BREADCRUMB="/tmp/vm-exec-$$.stalled"
WATCHDOG_HEARTBEAT="/tmp/vm-exec-$$.watchdog_alive"
DAEMON_ALIVE_FILE="/tmp/vm-exec-$$.daemon_alive"

# ---- structured deployment metadata (registry + events) ----
VM_NAME="${VM_NAME:-$(hostname)}"
# Read-side compat: prefer VM_ASSET_GROUP, fall back to legacy VM_CATEGORY,
# then default UNKNOWN. Lets us roll out the rename without breaking any
# launcher still on the old name.
VM_ASSET_GROUP="${VM_ASSET_GROUP:-${VM_CATEGORY:-UNKNOWN}}"
VM_TASK="${VM_TASK:-vm-exec}"
VM_MODE="${VM_MODE:-full}"
TODAY_UTC="$(date -u +%Y-%m-%d)"
VM_START_DATE="${VM_START_DATE:-$TODAY_UTC}"
VM_END_DATE="${VM_END_DATE:-$TODAY_UTC}"
PYTHON_BIN="${PYTHON_BIN:-python}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAEMON_SCRIPT="${SCRIPT_DIR}/heartbeat_daemon.py"

# Generate the deployment_id (uuid4). Prefer python (always available on VMs
# that run our tarball); fall back to /proc/sys/kernel/random/uuid.
if command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    DEPLOYMENT_ID="$("$PYTHON_BIN" -c 'import uuid; print(uuid.uuid4())')"
elif [[ -r /proc/sys/kernel/random/uuid ]]; then
    DEPLOYMENT_ID="$(cat /proc/sys/kernel/random/uuid)"
else
    DEPLOYMENT_ID="$(date +%s)-$$"
fi

# ---- start ----
echo "[vm-exec] deployment_id=$DEPLOYMENT_ID vm=$VM_NAME asset_group=$VM_ASSET_GROUP task=$VM_TASK mode=$VM_MODE" | tee "$LOCAL_LOG"
echo "[vm-exec] starting: $*" | tee -a "$LOCAL_LOG"
echo "[vm-exec] log -> $GCS_LOG_URI (uploaded by heartbeat_daemon.py)" | tee -a "$LOCAL_LOG"

# Launch the Python heartbeat daemon. It handles register → heartbeat loop
# → uploader loop → complete via SIGTERM. Stderr/stdout are teed into the
# local log for debugging.
if [[ ! -f "$DAEMON_SCRIPT" ]]; then
    echo "[vm-exec] WARN: heartbeat daemon missing at $DAEMON_SCRIPT — observability disabled" | tee -a "$LOCAL_LOG"
    DAEMON_PID=""
else
    "$PYTHON_BIN" "$DAEMON_SCRIPT" \
        --id "$DEPLOYMENT_ID" \
        --name "$VM_NAME" \
        --asset-group "$VM_ASSET_GROUP" \
        --task "$VM_TASK" \
        --mode "$VM_MODE" \
        --start-date "$VM_START_DATE" \
        --end-date "$VM_END_DATE" \
        --log-uri "$GCS_LOG_URI" \
        --local-log "$LOCAL_LOG" \
        --exit-status-file "$EXIT_STATUS_FILE" \
        --stall-breadcrumb "$STALL_BREADCRUMB" \
        --watchdog-file "$DAEMON_ALIVE_FILE" \
        >> "$LOCAL_LOG" 2>&1 &
    DAEMON_PID=$!
    echo "[vm-exec] heartbeat daemon pid=$DAEMON_PID" >> "$LOCAL_LOG"
fi

# Start the command in background, capture its PID, tee stdout+stderr.
# BUG-4 fix (2026-05-05): the prior subshell's last command was the trailing
# ``echo``, so its exit status was always 0 and ``wait $CMD_PID`` below
# returned 0 even when the workload exited non-zero. Result: every failed
# backfill VM reported ``exit_code=0`` to the deployment registry, fired
# ``DEPLOYMENT_COMPLETED`` instead of ``DEPLOYMENT_FAILED``, and self-deleted
# via ``VM_SHUTDOWN_ON_COMPLETION=true`` — silent failure for the entire
# CeFi MDPS gap-fill fleet (5 VMs crashed in seconds, all reported success).
# The fix: capture ``$?`` immediately after the workload, then ``exit`` the
# subshell with that status so ``wait`` reads the workload's real rc.
( "$@" 2>&1; rc=$?; echo "[vm-exec] command exited rc=$rc"; exit "$rc" ) >> "$LOCAL_LOG" &
CMD_PID=$!
echo "$CMD_PID" > "$PID_FILE"

# Log-activity watchdog: if LOCAL_LOG hasn't grown in STALL_TIMEOUT_SEC
# (default 30 min), SIGKILL the command process group (including any GCS
# worker threads that are wedged on pool exhaustion) and write a stall
# breadcrumb so the daemon's shutdown path can emit DEPLOYMENT_FAILED
# with a structured reason. Six prior TradFi VMs silently wedged for 5-6h
# in 2026-04 because no watchdog existed. 2026-05-23: reduced from 3600s
# to 1800s — on e2-highmem-8 (64 GB) even the heaviest CeFi shard
# (DERIBIT BTC-PERPETUAL book_snapshot_5) completes in <15 min; 30 min
# gives 2× headroom without letting memory-stalled 32 GB VMs idle for 1h.
STALL_TIMEOUT_SEC="${STALL_TIMEOUT_SEC:-1800}"
STALL_POLL_SEC="${STALL_POLL_SEC:-60}"
# Disabling `set -e` inside the watchdog subshell because v1 of this
# watchdog (5b881bc) silently died on 3 TradFi VMs 2026-04-19 without
# ever writing the STALL: breadcrumb. Any intermediate command returning
# non-zero inside the loop (stat on missing file, pkill with no matches,
# arithmetic misstep) would terminate the subshell under the inherited
# pipefail. Explicitly relax error-handling + emit heartbeats every poll
# so a future hang has diagnostic trail.
(
    set +e
    last_size=-1
    last_change_epoch=$(date +%s)
    iteration=0
    while kill -0 "$CMD_PID" 2>/dev/null; do
        iteration=$((iteration + 1))
        sleep "$STALL_POLL_SEC"
        kill -0 "$CMD_PID" 2>/dev/null || break
        now=$(date +%s)
        cur_size=$(stat -c %s "$LOCAL_LOG" 2>/dev/null)
        cur_size=${cur_size:-0}
        echo "watchdog iter=$iteration size=$cur_size last=$last_size ts=$now" > "$WATCHDOG_HEARTBEAT"
        if [[ "$cur_size" != "$last_size" ]]; then
            last_size=$cur_size
            last_change_epoch=$now
            continue
        fi
        stalled_for=$(( now - last_change_epoch ))
        if [[ $stalled_for -ge $STALL_TIMEOUT_SEC ]]; then
            {
                echo "[vm-exec] STALL: log has not grown in ${stalled_for}s (threshold=${STALL_TIMEOUT_SEC}s) — killing CMD_PID=$CMD_PID"
                if command -v py-spy >/dev/null 2>&1; then
                    py-spy dump --pid "$CMD_PID" 2>&1 || true
                else
                    if [[ -d "/proc/$CMD_PID/task" ]]; then
                        for tid_dir in /proc/"$CMD_PID"/task/*/; do
                            [[ -d "$tid_dir" ]] || continue
                            echo "--- $tid_dir ---"
                            cat "${tid_dir}stack" 2>/dev/null || true
                        done
                    fi
                fi
            } >> "$LOCAL_LOG" 2>&1
            echo "stalled_for=$stalled_for threshold=$STALL_TIMEOUT_SEC" > "$STALL_BREADCRUMB"
            pkill -TERM -P "$CMD_PID" 2>/dev/null || true
            kill -TERM "$CMD_PID" 2>/dev/null || true
            sleep 5
            pkill -KILL -P "$CMD_PID" 2>/dev/null || true
            kill -KILL "$CMD_PID" 2>/dev/null || true
            # Also kill any python subprocess by command match, as a belt-and-
            # braces measure when the subshell PID is a bash wrapper and
            # pkill -P doesn't reach the actual grandchild.
            pkill -KILL -f "market_tick_data_service.scripts.migrate" 2>/dev/null || true
            pkill -KILL -f "market_data_processing_service" 2>/dev/null || true
            break
        fi
    done
    echo "watchdog exiting iter=$iteration reason=$([[ ${stalled_for:-0} -ge $STALL_TIMEOUT_SEC ]] && echo stall || echo cmd_ended)" >> "$LOCAL_LOG"
) &
WATCHDOG_PID=$!

# Wait for command, record exit code.
wait "$CMD_PID"
RC=$?
kill "$WATCHDOG_PID" 2>/dev/null || true

# Hand the exit status off to the daemon and let it emit the terminal event
# + do the final GCS upload. Fall back to inline gsutil / direct EXIT_STATUS
# write if the daemon isn't running.
echo "$RC" > "$EXIT_STATUS_FILE"

if [[ -n "$DAEMON_PID" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
    # Daemon owns final upload + DEPLOYMENT_COMPLETED/FAILED emission.
    kill -TERM "$DAEMON_PID" 2>/dev/null || true
    # Give the daemon a generous window to finish uploading (log files can
    # be large) and publishing the terminal event.
    for _ in $(seq 1 30); do
        kill -0 "$DAEMON_PID" 2>/dev/null || break
        sleep 1
    done
    # Belt-and-braces: if the daemon is still alive after 30s, SIGKILL it
    # and fall through to the inline upload path.
    if kill -0 "$DAEMON_PID" 2>/dev/null; then
        echo "[vm-exec] WARN: daemon did not exit within 30s — SIGKILL" >> "$LOCAL_LOG"
        kill -KILL "$DAEMON_PID" 2>/dev/null || true
    fi
fi

# Inline fallback upload — only does anything useful when the daemon either
# never started or was SIGKILLed before its final upload. Harmless when the
# daemon already uploaded (it just re-uploads the same bytes).
gsutil -q cp "$LOCAL_LOG" "$GCS_LOG_URI" 2>/dev/null || true
echo "$RC" | gsutil -q cp - "$EXIT_STATUS_URI" 2>/dev/null || true

FINAL_STATUS="completed"
[[ "$RC" -ne 0 ]] && FINAL_STATUS="failed"
if [[ -f "$STALL_BREADCRUMB" ]]; then
    FINAL_STATUS="failed"
    RC=124
    echo "[vm-exec] DEPLOYMENT_FAILED cause=stall $(cat "$STALL_BREADCRUMB")" >> "$LOCAL_LOG"
fi

echo "[vm-exec] final log uploaded, exit=$RC, status=$FINAL_STATUS"

# ── Self-delete if launcher set VM_SHUTDOWN_ON_COMPLETION=true ──
# Previously every launcher set VM_SHUTDOWN_ON_COMPLETION=true in metadata
# but nothing read it — VMs finished rc=0 and stayed RUNNING forever.
# Cost leak on every short job + operators had to manually gcloud delete.
#
# Fire the delete in a detached background subshell with a short delay so
# this script can return $RC to systemd cleanly before gcloud tears the VM
# down. --delete-disks=all prevents orphaned PD-balanced disks.
# VM needs cloud-platform scope (every launch-*.sh already sets it).
SHUTDOWN_ON_COMPLETION="$(curl -sf -H 'Metadata-Flavor: Google' \
    'http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_SHUTDOWN_ON_COMPLETION' \
    2>/dev/null || echo '')"
if [[ "$SHUTDOWN_ON_COMPLETION" == "true" ]]; then
    VM_NAME_SELF="$(curl -sf -H 'Metadata-Flavor: Google' \
        'http://metadata.google.internal/computeMetadata/v1/instance/name' 2>/dev/null || echo '')"
    VM_ZONE_SELF="$(curl -sf -H 'Metadata-Flavor: Google' \
        'http://metadata.google.internal/computeMetadata/v1/instance/zone' 2>/dev/null | awk -F/ '{print $NF}')"
    if [[ -n "$VM_NAME_SELF" && -n "$VM_ZONE_SELF" ]]; then
        echo "[vm-exec] VM_SHUTDOWN_ON_COMPLETION=true — scheduling self-delete of $VM_NAME_SELF in $VM_ZONE_SELF" >> "$LOCAL_LOG"
        gsutil -q cp "$LOCAL_LOG" "$GCS_LOG_URI" 2>/dev/null || true
        # Pre-kill hook (2026-05-27): backup logs before self-delete
        # Best-effort — don't block delete if backup fails
        # setsid + nohup + disown detach from this script so SIGHUP on VM
        # teardown doesn't kill the delete mid-flight.
        nohup setsid bash -c "
            sleep 10
            # Try to backup logs (best-effort, timeout-guarded — must not block delete)
            if command -v backup-vm-logs.sh >/dev/null 2>&1; then
                timeout 90 backup-vm-logs.sh --vm '$VM_NAME_SELF' --zone '$VM_ZONE_SELF' 2>/dev/null || true
            elif [[ -f /opt/scripts/backup-vm-logs.sh ]]; then
                timeout 90 bash /opt/scripts/backup-vm-logs.sh --vm '$VM_NAME_SELF' --zone '$VM_ZONE_SELF' 2>/dev/null || true
            fi
            # Delete the VM; fall back to OS halt if gcloud fails so the VM is
            # at minimum TERMINATED (stops billing, zombie-watchdog can clean up).
            gcloud compute instances delete '$VM_NAME_SELF' --zone='$VM_ZONE_SELF' --quiet --delete-disks=all \
                || sudo shutdown -h now
        " </dev/null >/dev/null 2>&1 &
        disown || true
    else
        echo "[vm-exec] WARN: could not read VM name/zone from metadata — skipping self-delete" >> "$LOCAL_LOG"
    fi
fi

exit "$RC"
