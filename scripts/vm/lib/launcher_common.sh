#!/usr/bin/env bash
# VM launcher shared library — source this file from launch-*.sh scripts.
#
# Provides:
#   lc_validate_env <env>                  — validate prod|staging|dev; exit 1 on bad value
#   lc_singleton_check <prefix> <zone> <project> [force=false]
#                                          — refuse duplicate launch unless force=true
#   lc_gcloud_create <vm_name> <project> <zone> <machine_type> <disk_gb> <metadata> <labels>
#                                          — standard gcloud compute instances create wrapper
#   lc_code_bucket <project>               — emit deployment-scripts-<project>
#   lc_run_ts                              — emit YYYYmmdd-HHMMSS timestamp
#
# Usage:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "${SCRIPT_DIR}/lib/launcher_common.sh"
#
# Conventions:
#   - All functions are prefixed lc_ (launcher_common) to avoid namespace collisions.
#   - Functions print errors to >&2 and return/exit non-zero on failure.
#   - No global state is mutated (functions are pure or emit to stdout).
#
# Compatibility: this file supports bash 3.2+ (macOS default) and bash 4+/5
# (Linux / homebrew). The lc_log_upload_trap_block helper emits shell snippets
# that run on Ubuntu VMs (bash 5) — runtime targets are bash 5, but the launcher
# itself runs on the operator's local machine, which on macOS is bash 3.2.
# When adding new logic, validate with:
#   /bin/bash -n scripts/vm/lib/launcher_common.sh
#   /bin/bash -c 'source scripts/vm/lib/launcher_common.sh && <function-call>'
# before committing. Avoid bash-4+ idioms (${var,,}, ${var^^}, declare -A,
# mapfile/readarray) — use tr-based / while-read alternatives instead.

set -euo pipefail

# ---------------------------------------------------------------------------
# lc_validate_env <env>
# ---------------------------------------------------------------------------
# Verify DEPLOYMENT_ENV is one of prod|staging|dev.
# Exits 1 with a clear error if invalid.
lc_validate_env() {
    local env="${1:-}"
    case "$env" in
        prod|staging|dev) ;;
        *) echo "ERROR: --env must be one of prod/staging/dev (got: ${env})" >&2; exit 1 ;;
    esac
}

# ---------------------------------------------------------------------------
# lc_singleton_check <prefix> <zone> <project> [force]
# ---------------------------------------------------------------------------
# If any VM whose name starts with <prefix> is RUNNING in <zone>/<project>,
# print a clear error and exit 1 — unless force="true" (case-insensitive).
#
# Example:
#   lc_singleton_check "qg-snapshot-" "asia-northeast1-c" "$PROJECT" "$FORCE"
lc_singleton_check() {
    local prefix="${1:?lc_singleton_check: prefix required}"
    local zone="${2:?lc_singleton_check: zone required}"
    local project="${3:?lc_singleton_check: project required}"
    local force="${4:-false}"

    # bash-3.2 safe lowercase (macOS default bash lacks ${var,,})
    local force_lc
    force_lc="$(printf '%s' "$force" | tr '[:upper:]' '[:lower:]')"
    if [[ "$force_lc" == "true" ]]; then
        return 0
    fi

    local existing
    existing="$(gcloud compute instances list \
        --filter="name~\"^${prefix}\" AND status=RUNNING" \
        --zones="$zone" \
        --project="$project" \
        --format='value(name)' 2>/dev/null | head -1 || true)"

    if [[ -n "$existing" ]]; then
        local code_bucket="deployment-scripts-${project}"
        cat >&2 <<EOF
ERROR: VM with prefix '${prefix}' already running in ${zone}: ${existing}
Refusing duplicate launch (singleton lock).

Options:
  Inspect: gsutil cat gs://${code_bucket}/vm-logs/${existing}/run.log | tail -50
  Stop:    gcloud compute instances delete ${existing} --zone=${zone} --project=${project} --quiet
  Force:   bash \$0 --force
EOF
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# lc_gcloud_create <vm_name> <project> <zone> <machine_type> <disk_gb>
#                  <metadata_str> <labels_str>
# ---------------------------------------------------------------------------
# Standard gcloud compute instances create call with the workspace's canonical
# image (ubuntu-2404-lts-amd64), cloud-platform scope, and no-restart-on-failure.
#
# <metadata_str> : comma-separated key=value pairs (e.g. "VM_TASK=foo,ENV=prod")
# <labels_str>   : comma-separated key=value pairs (e.g. "purpose=foo,env=prod")
#
# Example:
#   lc_gcloud_create "$VM_NAME" "$PROJECT" "$ZONE" "e2-standard-4" "50" \
#       "VM_TASK=foo,DEPLOYMENT_ENV=prod" \
#       "purpose=foo,env=prod"
lc_gcloud_create() {
    local vm_name="${1:?lc_gcloud_create: vm_name required}"
    local project="${2:?lc_gcloud_create: project required}"
    local zone="${3:?lc_gcloud_create: zone required}"
    local machine_type="${4:?lc_gcloud_create: machine_type required}"
    local disk_gb="${5:?lc_gcloud_create: disk_gb required}"
    local metadata_str="${6:?lc_gcloud_create: metadata_str required}"
    local labels_str="${7:?lc_gcloud_create: labels_str required}"

    # Centralised dry-run safety net (codified 2026-05-20 after the
    # launch-tradfi-forward-poll.sh incident — see
    # plans/active/issues/launcher_dry_run_support_gap_2026_05_20.md).
    # Any caller (launcher or wrapper) that exports LC_DRY_RUN=true short-circuits
    # the real `gcloud compute instances create` call. Per-launcher --dry-run
    # parsing should set + export LC_DRY_RUN=true before invoking this helper.
    local lc_dry_run_lc
    lc_dry_run_lc="$(printf '%s' "${LC_DRY_RUN:-false}" | tr '[:upper:]' '[:lower:]')"
    if [[ "$lc_dry_run_lc" == "true" ]]; then
        echo "[DRY-RUN] Would create VM: ${vm_name}"
        echo "[DRY-RUN]   project=${project} zone=${zone} machine=${machine_type} disk=${disk_gb}GB"
        echo "[DRY-RUN]   metadata=${metadata_str}"
        echo "[DRY-RUN]   labels=${labels_str}"
        return 0
    fi

    gcloud compute instances create "$vm_name" \
        --project="$project" \
        --zone="$zone" \
        --machine-type="$machine_type" \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size="${disk_gb}GB" \
        --scopes=cloud-platform \
        --no-restart-on-failure \
        --metadata="$metadata_str" \
        --labels="$labels_str"
}

# ---------------------------------------------------------------------------
# lc_write_startup_file <script_content>
# ---------------------------------------------------------------------------
# Write <script_content> to a mktemp file, set STARTUP_FILE, and register
# a trap to delete it on EXIT. Centralises temp-file lifecycle management.
#
# After calling this function, pass STARTUP_FILE to gcloud:
#   gcloud compute instances create ... \
#       --metadata-from-file="startup-script=${STARTUP_FILE}"
#
# Example:
#   STARTUP_SCRIPT=$(cat << 'STARTUP_EOF'
#   #!/bin/bash
#   echo "hello from VM"
#   STARTUP_EOF
#   )
#   lc_write_startup_file "$STARTUP_SCRIPT"
#   # STARTUP_FILE is now set; cleanup registered via trap EXIT
lc_write_startup_file() {
    local content="${1:?lc_write_startup_file: script content required}"
    STARTUP_FILE="$(mktemp /tmp/startup-XXXX.sh)"
    printf '%s' "$content" > "$STARTUP_FILE"
    # Register cleanup — if caller already has an EXIT trap, this appends to it.
    # shellcheck disable=SC2064
    trap "rm -f '${STARTUP_FILE}'" EXIT
    export STARTUP_FILE
}

# ---------------------------------------------------------------------------
# lc_code_bucket <project>
# ---------------------------------------------------------------------------
# Emit the canonical code/tarballs GCS bucket name for a given project.
lc_code_bucket() {
    local project="${1:?lc_code_bucket: project required}"
    echo "deployment-scripts-${project}"
}

# ---------------------------------------------------------------------------
# lc_run_ts
# ---------------------------------------------------------------------------
# Emit a sortable timestamp suitable for VM name suffixes: YYYYmmdd-HHMMSS
lc_run_ts() {
    date +%Y%m%d-%H%M%S
}

# ---------------------------------------------------------------------------
# lc_log_upload_trap_block <vm_name> <project_id> [asset_group] [task]
# ---------------------------------------------------------------------------
# Emit a bash snippet to inject AT THE TOP of an inline VM startup-script
# heredoc body. The snippet gives a bespoke (non-setup-data-pipeline-vm.sh)
# launcher the SAME durable-observability contract the canonical
# `vm-exec-with-gcs-tee.sh` path provides — without the heavyweight
# tarball+venv+heartbeat-daemon install. It:
#
#   1. Tees stdout+stderr to /var/log/run.log (so every echo is captured).
#   2. Starts a CONTINUOUS background streamer that uploads the growing log
#      to the canonical GCS path
#        gs://deployment-scripts-<project>/vm-logs/<vm-name>/run.log
#      every LC_LOG_STREAM_INTERVAL seconds (default 30), AND writes a
#      liveness/progress heartbeat blob
#        gs://deployment-scripts-<project>/vm-heartbeat/<vm-name>.txt
#      every tick. So a dead/hung VM's log is queryable from GCS WITHOUT
#      SSH (≤30 s loss), and the external vm-zombie-watchdog (which reads
#      that heartbeat blob) can reap a wedged VM. The blob also carries the
#      asset_group/task/start-epoch so liveness is attributable.
#   3. Installs an EXIT trap that ALWAYS does a final upload of the log to
#      GCS regardless of exit status (success, error, signal, `set -e`
#      propagation), writes the terminal EXIT_STATUS marker blob
#        gs://deployment-scripts-<project>/vm-logs/<vm-name>/EXIT_STATUS
#      (the durable STOPPED/FAILED signal: rc=0 → completed, rc!=0 →
#      failed), prints a final marker, stops the streamer, then schedules
#      `shutdown -h +1` to flush before the VM goes away.
#
# Why this exists: inline startup scripts that put `gsutil cp` only at the
# END of the script body lose ALL logs if anything fails before that line,
# AND show a FROZEN log for the whole run (no progress visibility) — both
# observed 2026-05-19 (mtds-solana-drift TERMINATED after 7min, no run.log)
# and 2026-06 (SFI + gas-fees uploaders stalled, logs froze + were lost on
# termination, forcing serial-port/SSH diagnosis). The canonical pattern is
# `vm-exec-with-gcs-tee.sh` via `setup-data-pipeline-vm.sh` (30s stream +
# lifecycle events + log-archive backup); this helper is the SSOT
# lightweight equivalent for launchers that inline their own startup script.
# SSOT: codex/05-infrastructure/vm-tarball-deployment.md § "VM observability".
#
# Lifecycle/cleanup: the vm-logs/ + vm-heartbeat/ prefixes have GCS lifecycle
# delete rules (14d / 15d) + a daily log-archive Cloud Run job
# (terraform/gcp/main.tf deployment_scripts bucket +
# vm_log_archival_scheduler.tf) — logs never accumulate forever.
#
# CALLER MUST emit this snippet inside the heredoc BEFORE any `set -e`
# or workload commands. Inside the heredoc body the snippet's bash $-vars
# are pre-escaped with \$ — caller does NOT need to double-escape.
# Override the stream cadence with LC_LOG_STREAM_INTERVAL in the launcher env.
#
# Usage (inside a launcher):
#     STARTUP_FILE=$(mktemp)
#     LOG_TRAP="$(lc_log_upload_trap_block "$VM_NAME" "$PROJECT_ID" defi gas-fees)"
#     cat > "$STARTUP_FILE" <<STARTUP_EOF
#     #!/bin/bash
#     ${LOG_TRAP}
#     set -euo pipefail
#     # ... rest of script ...
#     STARTUP_EOF
lc_log_upload_trap_block() {
    local vm_name="${1:?lc_log_upload_trap_block: vm_name required}"
    local project_id="${2:?lc_log_upload_trap_block: project_id required}"
    local asset_group="${3:-UNKNOWN}"
    local task="${4:-vm-exec}"
    local code_bucket="deployment-scripts-${project_id}"
    local stream_interval="${LC_LOG_STREAM_INTERVAL:-30}"
    cat <<TRAPSNIPPET
# --- lc_log_upload_trap_block (deployment-service/scripts/vm/lib/launcher_common.sh) ---
# Durable observability for an inline startup script: continuous GCS log
# stream (every ${stream_interval}s) + liveness/progress heartbeat blob +
# terminal EXIT_STATUS marker + guaranteed final upload on any exit.
# Canonical run.log path: gs://${code_bucket}/vm-logs/${vm_name}/run.log
LOG_LOCAL="/var/log/run.log"
GCS_LOG_URI="gs://${code_bucket}/vm-logs/${vm_name}/run.log"
GCS_HEARTBEAT_URI="gs://${code_bucket}/vm-heartbeat/${vm_name}.txt"
GCS_EXIT_URI="gs://${code_bucket}/vm-logs/${vm_name}/EXIT_STATUS"
LC_STREAM_INTERVAL=${stream_interval}
LC_VM_ASSET_GROUP="${asset_group}"
LC_VM_TASK="${task}"
LC_START_EPOCH=\$(date +%s)
mkdir -p "\$(dirname "\$LOG_LOCAL")" 2>/dev/null || true
exec > >(tee -a "\$LOG_LOCAL") 2>&1
echo "=== VM STARTED \$(date -u +'%Y-%m-%dT%H:%M:%SZ') vm=${vm_name} asset_group=${asset_group} task=${task} ==="
# Continuous streamer: ship the growing log to GCS + write a heartbeat blob
# every \$LC_STREAM_INTERVAL seconds so progress is visible WITHOUT SSH and a
# hung VM's log is never lost. Runs detached; killed by the EXIT trap.
_lc_stream_loop() {
    while true; do
        gsutil -q cp "\$LOG_LOCAL" "\$GCS_LOG_URI" 2>/dev/null || true
        printf 'vm=%s asset_group=%s task=%s alive_at=%s uptime_s=%s\n' \
            "${vm_name}" "\$LC_VM_ASSET_GROUP" "\$LC_VM_TASK" \
            "\$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "\$(( \$(date +%s) - LC_START_EPOCH ))" \
            | gsutil -q cp - "\$GCS_HEARTBEAT_URI" 2>/dev/null || true
        sleep "\$LC_STREAM_INTERVAL"
    done
}
_lc_stream_loop &
_LC_STREAM_PID=\$!
_lc_final_upload() {
    local rc=\$?
    kill "\$_LC_STREAM_PID" 2>/dev/null || true
    echo ""
    echo "=== VM EXIT rc=\$rc \$(date -u +'%Y-%m-%dT%H:%M:%SZ') ==="
    # Terminal STOPPED/FAILED signal — durable in GCS even if the VM dies.
    echo "\$rc" | gsutil -q cp - "\$GCS_EXIT_URI" 2>/dev/null || true
    for _i in 1 2 3; do
        if gsutil -q cp "\$LOG_LOCAL" "\$GCS_LOG_URI" 2>/dev/null; then
            echo "log uploaded to \$GCS_LOG_URI (attempt \$_i)"
            break
        fi
        echo "log upload attempt \$_i failed, retrying in 5s..."
        sleep 5
    done
    sleep 2
    gsutil -q cp "\$LOG_LOCAL" "\$GCS_LOG_URI" 2>/dev/null || true
    shutdown -h +1 2>/dev/null || true
    return \$rc
}
trap _lc_final_upload EXIT
# --- end lc_log_upload_trap_block ---
TRAPSNIPPET
}
