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

    if [[ "${force,,}" == "true" ]]; then
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
