#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
#
# On-VM ps/free/dmesg monitor for settling OOM-vs-hang questions on any
# backfill/data VM. Runs entirely INSIDE the VM as its own nohup'd background
# loop (no SSH dependency) so it survives an interactive debugging session
# dying mid-run — the exact gap that lost visibility into the final ~3.5
# minutes before a kill during the 2026-07-27 live-VM profiling attempt at
# unified-trading-pm/plans/active/issues/features_service_defi_backfill_vm_oom_unexplained_2026_07_26.md.
#
# Writes timestamped ps/free/dmesg snapshots to a local file every
# POLL_INTERVAL_SEC (default 3s), and periodically re-uploads the WHOLE file
# to GCS every UPLOAD_INTERVAL_SEC (default 15s) -- so evidence up to the
# last upload survives even if the VM is OOM-killed or self-deletes seconds
# after the interesting window, without needing a live SSH session or the
# VM to still be reachable post-mortem.
#
# Usage (launched by setup-data-pipeline-vm.sh, opt-in via the
# VM_OOM_MONITOR=true instance-metadata flag -- NOT run by default on every
# VM, since the periodic gsutil upload has a real, if small, per-VM cost):
#   nohup bash oom-hang-monitor.sh <local-log-path> <gcs-log-uri> \
#       [poll_interval_sec] [upload_interval_sec] &

set -uo pipefail # NOT -e: one failed ps/free/dmesg/gsutil call must never
                 # kill the whole monitoring loop -- a transient failure
                 # losing one snapshot is fine, the loop dying is not.

LOCAL_LOG="${1:?usage: oom-hang-monitor.sh <local-log-path> <gcs-log-uri> [poll_interval_sec] [upload_interval_sec]}"
GCS_URI="${2:?usage: oom-hang-monitor.sh <local-log-path> <gcs-log-uri> [poll_interval_sec] [upload_interval_sec]}"
POLL_INTERVAL="${3:-3}"
UPLOAD_INTERVAL="${4:-15}"

_last_upload=0

while true; do
  {
    echo "=== $(date -u '+%Y-%m-%dT%H:%M:%S.%3NZ') POLL ==="
    echo "--- ps (top RSS) ---"
    ps -eo pid,ppid,rss,pmem,pcpu,etimes,stat,cmd --sort=-rss 2>/dev/null | head -15
    echo "--- free -m ---"
    free -m 2>/dev/null
    echo "--- dmesg (oom/killed lines only, last 5) ---"
    dmesg 2>/dev/null | grep -iE 'killed process|out of memory|oom' | tail -5
  } >>"$LOCAL_LOG" 2>&1

  now=$(date +%s)
  if ((now - _last_upload >= UPLOAD_INTERVAL)); then
    gsutil -q cp "$LOCAL_LOG" "$GCS_URI" 2>/dev/null || true
    _last_upload=$now
  fi

  sleep "$POLL_INTERVAL"
done
