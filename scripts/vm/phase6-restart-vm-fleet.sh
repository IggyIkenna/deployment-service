#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run verified + GCS orphan-sweep=0
# Phase 6 — Restart ALL writer VMs after GCS+AWS bucket migration (Phases 3-5 complete).
#
# Context: coordinator data_pipeline_master_coordination_2026_05_20.md Phase 6.
#   Follows Phase 5 (AWS bucket rename). VMs restart → startup script pulls fresh
#   tarballs from GCS → services write MANIFEST_SCHEMA_VERSION=8 rows.
#
# GATE (HARD): --phase5-green flag required for --apply. Execution without Phase 5
#   confirmed GREEN means VMs restart against wrong bucket names → data loss.
#
# Steps:
#   1. Inventory all TERMINATED VMs (excl. vm-zombie-watchdog — already RUNNING)
#   2. gcloud compute instances start (re-runs startup script → fresh tarball download)
#   3. Wait for RUNNING status (max 15 min)
#   4. Print post-restart VM list for operator verification
#
# Usage:
#   bash scripts/vm/phase6-restart-vm-fleet.sh --dry-run           # list VMs, no action
#   bash scripts/vm/phase6-restart-vm-fleet.sh --phase5-green --apply  # live restart
#
# Gate: Phase 5 (AWS bucket rename) must be GREEN before running with --apply.
set -euo pipefail

export PATH="/home/ubuntu/google-cloud-sdk/bin:${PATH}"

PROJECT="central-element-323112"
ZONE="asia-northeast1-c"
APPLY=false
PHASE5_GREEN=false
MAX_WAIT_SECONDS=900

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply) APPLY=true; shift ;;
        --dry-run) APPLY=false; shift ;;
        --phase5-green) PHASE5_GREEN=true; shift ;;
        --project) PROJECT="$2"; shift 2 ;;
        --zone) ZONE="$2"; shift 2 ;;
        -h|--help) sed -n '2,34p' "$0"; exit 0 ;;
        *) echo "Unknown flag: $1" >&2; exit 2 ;;
    esac
done

echo "Phase 6 — VM fleet restart"
echo "Project: $PROJECT  Zone: $ZONE"
echo "Mode:    $([[ "$APPLY" == "true" ]] && echo APPLY || echo DRY-RUN)"
echo

# Gate check
if [[ "$APPLY" == "true" && "$PHASE5_GREEN" != "true" ]]; then
    echo "ERROR: --phase5-green flag required before --apply." >&2
    echo "  Phase 5 (AWS bucket rename) must be confirmed GREEN first." >&2
    echo "  Re-run: bash scripts/vm/phase6-restart-vm-fleet.sh --phase5-green --apply" >&2
    exit 1
fi

# VMs to SKIP (must stay running through migration — drained in Phase 3 skip list)
SKIP_PREFIXES=(
    "vm-zombie-watchdog"
)

# Get all TERMINATED VMs
TERMINATED=$(gcloud compute instances list \
    --project="$PROJECT" \
    --filter="status=TERMINATED" \
    --format="value(name,zone)" 2>/dev/null | sort)

if [[ -z "$TERMINATED" ]]; then
    echo "No TERMINATED VMs found."
    echo "If Phase 3 drain was not run, VMs may still be RUNNING."
    exit 0
fi

# Filter out exempt VMs
TO_START=()
while IFS=$'\t' read -r name zone; do
    skip=false
    for pfx in "${SKIP_PREFIXES[@]}"; do
        if [[ "$name" == "${pfx}"* ]]; then
            echo "  [exempt] $name ($pfx)"
            skip=true
            break
        fi
    done
    [[ "$skip" == "false" ]] && TO_START+=("${name}::${zone}")
done <<< "$TERMINATED"

echo "VMs to restart: ${#TO_START[@]}"
for entry in "${TO_START[@]}"; do
    name="${entry%%::*}"
    z="${entry##*::}"
    echo "  $name ($z)"
done
echo

if [[ "$APPLY" != "true" ]]; then
    echo "Dry-run complete. Re-run with --phase5-green --apply to restart VMs."
    echo "On restart, each VM runs startup-script → downloads fresh tarballs → writes v8 rows."
    exit 0
fi

# Batch start all restart targets
NAMES_ONLY=()
for entry in "${TO_START[@]}"; do
    NAMES_ONLY+=("${entry%%::*}")
done

echo "Starting ${#NAMES_ONLY[@]} VMs (startup script will pull fresh tarballs)..."
gcloud compute instances start "${NAMES_ONLY[@]}" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --quiet 2>&1 | tail -5

# Wait for all to reach RUNNING
echo "Waiting for RUNNING status (max ${MAX_WAIT_SECONDS}s)..."
DEADLINE=$(( $(date +%s) + MAX_WAIT_SECONDS ))
while true; do
    still_stopped=$(gcloud compute instances list \
        --project="$PROJECT" \
        --filter="status=TERMINATED AND name!~^vm-zombie-watchdog" \
        --format="value(name)" 2>/dev/null | wc -l | tr -d ' ')
    echo "  $(date -u +%H:%M:%S) — $still_stopped VMs still TERMINATED"
    [[ "$still_stopped" -eq 0 ]] && break
    [[ $(date +%s) -gt $DEADLINE ]] && echo "TIMEOUT waiting for VMs to reach RUNNING" >&2 && exit 1
    sleep 30
done

echo
echo "All writer VMs RUNNING."
echo
echo "Post-restart fleet:"
gcloud compute instances list \
    --project="$PROJECT" \
    --filter="status=RUNNING AND name!~^vm-zombie-watchdog" \
    --format="table(name,zone,status)" 2>/dev/null

echo
echo "Next steps:"
echo "  1. T+10min: verify each VM shows RUNNING + heartbeat in deployment registry"
echo "  2. Monitor manifest consolidator for v8 rows (MANIFEST_SCHEMA_VERSION=8)"
echo "  3. Proceed to Phase 7 (v8 backfill + label-flip)"
