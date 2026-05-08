#!/usr/bin/env bash
# Refresh the tarball bundle for ONE asset_group's pipeline.
#
# Phase 1 of plans/active/deploy_missing_auto_launch_2026_05_07.plan.md.
# Wraps `create-code-tarballs.sh --asset-group <X>` so the deployment-api
# auto-launch endpoint (Phase 2) can refresh just the bundle the about-to-
# launch VM needs, not all five asset_groups. Emits a TARBALLS_REFRESHED
# event when complete so the Cloud Build trigger (Phase 1.B) can poll for
# success without parsing stdout.
#
# Usage:
#   bash scripts/vm/refresh-tarballs-for-shard-key.sh <asset_group>
#   bash scripts/vm/refresh-tarballs-for-shard-key.sh CEFI
#   bash scripts/vm/refresh-tarballs-for-shard-key.sh DEFI --bucket my-bucket
#   bash scripts/vm/refresh-tarballs-for-shard-key.sh ALL --dry-run
#
# Args:
#   <asset_group>  CEFI | TRADFI | DEFI | SPORTS | PREDICTION | ALL
#                  ALL = `create-code-tarballs.sh --all` (full fleet)
#   Plus any flag accepted by create-code-tarballs.sh (forwarded as-is).
#
# Why a separate wrapper rather than calling create-code-tarballs.sh
# directly:
#   1. Single canonical entry point for the auto-launch trigger — Phase 2
#      Cloud Build trigger references THIS script by name; future tarball-
#      strategy changes (asset_group → root tarball, image-based deploy)
#      land here without re-wiring the trigger.
#   2. TARBALLS_REFRESHED event emission is non-negotiable for the Phase
#      2 deployment-api staleness-check helper that polls
#      `gs://{pid}-events/events/deployment-service/{date}/{vm_name}/`
#      for the completion signal. create-code-tarballs.sh itself is
#      operator-driven and doesn't emit events.
#   3. Asset_group → tarball-flag normalisation lives in one place
#      (uppercase + ALL → --all + everything else → --asset-group X).
#
# Per CLAUDE.md "VM tarball deployment" SSOT — bare invocation only re-
# tars CORE; this wrapper REQUIRES an explicit asset_group so a Phase 2
# auto-launch never silently boots stale code.
#
# Per CLAUDE.md "no fire-and-forget VM launches" rule — every step that
# can fail emits a structured event. TARBALLS_REFRESH_REQUESTED before
# the tar work starts; TARBALLS_REFRESHED on success;
# TARBALLS_REFRESH_FAILED on non-zero exit.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"

usage() {
    cat <<EOF
Usage: $0 <asset_group> [extra create-code-tarballs.sh flags]

Asset groups (case-insensitive):
  CEFI         core + CEFI service repos
  TRADFI       core + TRADFI service repos
  DEFI         core + DEFI service repos
  SPORTS       core + SPORTS service repos
  PREDICTION   core + PREDICTION service repos
  ALL          core + every service repo (full fleet)

Pass-through flags (forwarded to create-code-tarballs.sh):
  --bucket <name>       custom GCS bucket
  --dry-run             show what would be created without uploading
  --include <repo>      additional repo (repeatable)

Examples:
  $0 CEFI
  $0 DEFI --bucket my-test-bucket
  $0 ALL --dry-run
EOF
    exit 1
}

if [[ $# -lt 1 ]]; then
    usage
fi

ASSET_GROUP_RAW="$1"
shift
ASSET_GROUP="$(printf '%s' "$ASSET_GROUP_RAW" | tr '[:lower:]' '[:upper:]')"

VALID_GROUPS=("CEFI" "TRADFI" "DEFI" "SPORTS" "PREDICTION" "ALL")
GROUP_OK=false
for g in "${VALID_GROUPS[@]}"; do
    if [[ "$ASSET_GROUP" == "$g" ]]; then
        GROUP_OK=true
        break
    fi
done
if ! $GROUP_OK; then
    echo "ERROR: Unknown asset_group '$ASSET_GROUP_RAW'. Expected one of: ${VALID_GROUPS[*]}"
    usage
fi

# Resolve the inner create-code-tarballs.sh flag form. ALL → --all;
# everything else → --asset-group <X>. Pass-through flags follow.
INNER_FLAG_ARGS=()
if [[ "$ASSET_GROUP" == "ALL" ]]; then
    INNER_FLAG_ARGS=("--all")
else
    INNER_FLAG_ARGS=("--asset-group" "$ASSET_GROUP")
fi

CREATE_TARBALLS_SCRIPT="$SCRIPT_DIR/create-code-tarballs.sh"
if [[ ! -x "$CREATE_TARBALLS_SCRIPT" && ! -f "$CREATE_TARBALLS_SCRIPT" ]]; then
    echo "ERROR: $CREATE_TARBALLS_SCRIPT not found. Verify deployment-service is checked out."
    exit 2
fi

# RUN_TS sortable + greppable per CLAUDE.md "VM Naming Convention". Used
# as correlation_id for the event chain so the Phase 2 staleness-check
# helper can pin polling to ONE refresh invocation.
RUN_TS="$(date -u '+%Y%m%d-%H%M%S')"
CORRELATION_ID="tarball-refresh-${ASSET_GROUP}-${RUN_TS}"

log() { echo "$(date '+%H:%M:%S') $*"; }

# Event emission helper. Writes a one-line JSON to stdout (captured by
# vm-exec-with-gcs-tee.sh on a VM, or Cloud Build's log on a build) AND
# to gs://{pid}-events/events/deployment-service/{date}/{correlation_id}/
# hour={H}/0001.jsonl when CORRELATION_ID + GCP_PROJECT_ID are present.
# Schema matches base_service.py:159 SSOT exactly so consumers reading
# the bucket use the same parsing path as every other service.
emit_event() {
    local event_name="$1"
    local severity="$2"
    local details_json="$3"  # raw JSON object string

    local timestamp
    timestamp="$(date -u '+%Y-%m-%dT%H:%M:%S.%6NZ')"
    local payload
    payload=$(cat <<JSON
{"event":"${event_name}","service":"deployment-service","timestamp":"${timestamp}","metadata":{"service_name":"deployment-service","severity":"${severity}","details":${details_json},"correlation_id":"${CORRELATION_ID}"}}
JSON
)
    echo "EVENT $payload"

    # Best-effort GCS write — do NOT block the refresh on event-bucket
    # availability. Cloud Build steps inherit ADC; on a workstation the
    # operator's gcloud auth is enough.
    if [[ -n "${GCP_PROJECT_ID:-}" ]]; then
        local date_partition
        date_partition="$(date -u '+%Y-%m-%d')"
        local hour
        hour="$(date -u '+%H')"
        local target="gs://${GCP_PROJECT_ID}-events/events/deployment-service/${date_partition}/${CORRELATION_ID}/hour=${hour}/0001.jsonl"
        printf '%s\n' "$payload" | gsutil -q cp - "$target" 2>/dev/null || true
    fi
}

# Emit REQUESTED event before the work starts so the Phase 2 staleness-
# check helper knows the refresh kicked off (vs. silently no-op'd).
emit_event "TARBALLS_REFRESH_REQUESTED" "INFO" \
    "{\"asset_group\":\"${ASSET_GROUP}\",\"run_ts\":\"${RUN_TS}\",\"workspace_root\":\"${WORKSPACE_ROOT}\"}"

log "Refreshing tarballs for asset_group=${ASSET_GROUP} (run_ts=${RUN_TS})"
log "  inner: bash $CREATE_TARBALLS_SCRIPT ${INNER_FLAG_ARGS[*]} $*"

START_EPOCH="$(date +%s)"
INNER_EXIT=0
WORKSPACE_ROOT="$WORKSPACE_ROOT" bash "$CREATE_TARBALLS_SCRIPT" "${INNER_FLAG_ARGS[@]}" "$@" || INNER_EXIT=$?
END_EPOCH="$(date +%s)"
DURATION_S=$(( END_EPOCH - START_EPOCH ))

if [[ $INNER_EXIT -ne 0 ]]; then
    emit_event "TARBALLS_REFRESH_FAILED" "ERROR" \
        "{\"asset_group\":\"${ASSET_GROUP}\",\"exit_code\":${INNER_EXIT},\"duration_s\":${DURATION_S}}"
    log "ERROR: create-code-tarballs.sh exited ${INNER_EXIT} after ${DURATION_S}s"
    exit "$INNER_EXIT"
fi

emit_event "TARBALLS_REFRESHED" "INFO" \
    "{\"asset_group\":\"${ASSET_GROUP}\",\"duration_s\":${DURATION_S},\"run_ts\":\"${RUN_TS}\"}"

log "Done in ${DURATION_S}s. correlation_id=${CORRELATION_ID}"
