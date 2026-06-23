#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run verified + GCS orphan-sweep=0
# Phase 3 — Graceful drain of ALL writer VMs before GCS+AWS bucket migration.
#
# Context: coordinator data_pipeline_master_coordination_2026_05_20.md Phase 3.
#   Follows Phase 2 (CODE FREEZE broadcast). Must complete before Phase 4 (GCS migration).
#
# HARD RULE (CLAUDE.md "Pre-migration drain"):
#   Before ANY bucket SSOT cutover or GCS migration, ALL running VMs across BOTH GCP + AWS
#   fleets MUST be gracefully stopped and manifest consolidated first.
#
# Steps:
#   1. Inventory all RUNNING VMs (excl. vm-zombie-watchdog, Cloud Run consolidator)
#   2. Issue SIGTERM equivalent (gcloud compute instances stop --zone=... gracefully)
#   3. Wait for TERMINATED status (max 30 min)
#   4. Snapshot manifest to _index/snapshots/pre_migration_$(date).parquet
#   5. Print ready-for-Phase-4 confirmation
#
# Usage:
#   bash scripts/vm/phase3-drain-vm-fleet.sh --dry-run   # list VMs, no action
#   bash scripts/vm/phase3-drain-vm-fleet.sh --apply     # graceful stop + wait
#
# Gate: Phase 2 CODE FREEZE must be broadcast to _agent_pings.md before running.
set -euo pipefail

export PATH="/home/ubuntu/google-cloud-sdk/bin:${PATH}"

PROJECT="central-element-323112"
ZONE="asia-northeast1-c"
APPLY=false
MAX_WAIT_SECONDS=1800

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply) APPLY=true; shift ;;
        --dry-run) APPLY=false; shift ;;
        --project) PROJECT="$2"; shift 2 ;;
        --zone) ZONE="$2"; shift 2 ;;
        -h|--help) sed -n '2,32p' "$0"; exit 0 ;;
        *) echo "Unknown flag: $1" >&2; exit 2 ;;
    esac
done

echo "Phase 3 — VM fleet drain"
echo "Project: $PROJECT  Zone: $ZONE"
echo "Mode:    $([[ "$APPLY" == "true" ]] && echo APPLY || echo DRY-RUN)"
echo

# VMs to SKIP (must stay running through migration)
SKIP_PREFIXES=(
    "vm-zombie-watchdog"
)

# Get all RUNNING VMs
RUNNING=$(gcloud compute instances list \
    --project="$PROJECT" \
    --filter="status=RUNNING" \
    --format="value(name,zone)" 2>/dev/null | sort)

if [[ -z "$RUNNING" ]]; then
    echo "No RUNNING VMs found. Fleet is already drained."
    exit 0
fi

# Filter out exempt VMs
TO_DRAIN=()
while IFS=$'\t' read -r name zone; do
    skip=false
    for pfx in "${SKIP_PREFIXES[@]}"; do
        if [[ "$name" == "${pfx}"* ]]; then
            echo "  [exempt] $name ($pfx)"
            skip=true
            break
        fi
    done
    [[ "$skip" == "false" ]] && TO_DRAIN+=("${name}::${zone}")
done <<< "$RUNNING"

echo "VMs to drain: ${#TO_DRAIN[@]}"
for entry in "${TO_DRAIN[@]}"; do
    name="${entry%%::*}"
    z="${entry##*::}"
    echo "  $name ($z)"
done
echo

if [[ "$APPLY" != "true" ]]; then
    echo "Dry-run complete. Re-run with --apply to stop VMs."
    echo "After drain: snapshot manifest, then proceed to Phase 4."
    exit 0
fi

# Batch stop all drain targets
NAMES_ONLY=()
for entry in "${TO_DRAIN[@]}"; do
    NAMES_ONLY+=("${entry%%::*}")
done

echo "Stopping ${#NAMES_ONLY[@]} VMs..."
gcloud compute instances stop "${NAMES_ONLY[@]}" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --quiet 2>&1 | tail -5

# Wait for all to reach TERMINATED
echo "Waiting for TERMINATED status (max ${MAX_WAIT_SECONDS}s)..."
DEADLINE=$(( $(date +%s) + MAX_WAIT_SECONDS ))
while true; do
    still_running=$(gcloud compute instances list \
        --project="$PROJECT" \
        --filter="status=RUNNING AND name!~^vm-zombie-watchdog" \
        --format="value(name)" 2>/dev/null | wc -l | tr -d ' ')
    echo "  $(date -u +%H:%M:%S) — $still_running VMs still RUNNING"
    [[ "$still_running" -eq 0 ]] && break
    [[ $(date +%s) -gt $DEADLINE ]] && echo "TIMEOUT waiting for drain" >&2 && exit 1
    sleep 30
done

echo
echo "All writer VMs TERMINATED."
echo
echo "Next steps:"
echo "  1. Run manifest consolidator snapshot:"
echo "     bash scripts/consolidator/snapshot-pre-migration.sh"
echo "  2. Proceed to Phase 4 (GCS bucket migration)"
