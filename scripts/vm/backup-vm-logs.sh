#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Canonical VM log backup — snapshot every running VM's logs to a durable archive.
#
# WHY: VMs stream stdout to gs://deployment-scripts-{pid}/vm-logs/<vm>/run.log
# every 30s (heartbeat_daemon.py), but that prefix has a 14-day delete
# lifecycle, AND boot-hung / network-wedged VMs never produce a run.log at all
# (their only record is the serial console, fetched via the GCP API — which
# does NOT depend on the VM's own network). This script captures BOTH into a
# durable, no-expiry archive bucket so a VM can be killed without losing
# forensics (healthy VMs lose at most the ~30s since their last stream upload).
#
# ARCHIVE PATH (stays inside the existing infra bucket — NOT a new bucket):
#   gs://deployment-scripts-{project}/log-archive/snapshot_<UTC-ts>/<vm>/run.log
#   gs://deployment-scripts-{project}/log-archive/snapshot_<UTC-ts>/<vm>/serial-console.txt
#
# Why this bucket + prefix:
#   * `deployment-scripts-{project}` is the de-facto canonical infra bucket
#     (produced by `_resolve_default_bucket()` in deployment_service; same
#     bucket the live `vm-logs/` stream uses). We reuse it rather than mint an
#     unregistered new bucket — infra buckets are intentionally NOT in the
#     resolve_bucket_name SSOT (like terraform-state / secrets).
#   * prefix `log-archive/` is NOT matched by the 14-day delete lifecycle rule
#     (which targets prefix `vm-logs/`), so archived snapshots persist.
#     (Formalising this into a helper + codex SSOT is tracked in
#      plans/active/canonical_vm_log_archival_2026_05_27.md.)
#
# Usage:
#   bash backup-vm-logs.sh                       # all RUNNING VMs in the zone
#   bash backup-vm-logs.sh --vm NAME [--vm NAME] # specific VMs
#   bash backup-vm-logs.sh --zone asia-northeast1-c --project central-element-323112
#
# Read-only on the source (server-side GCS->GCS copy for run.log; serial via
# the metadata/compute API). Safe to run any time, especially before a kill.
set -uo pipefail

ZONE="asia-northeast1-c"
PROJECT="$(gcloud config get-value project 2>/dev/null)"
VMS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --zone)    ZONE="$2"; shift 2 ;;
        --project) PROJECT="$2"; shift 2 ;;
        --vm)      VMS+=("$2"); shift 2 ;;
        --help|-h) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$PROJECT" ]]; then echo "ERROR: no project (set --project or gcloud config)" >&2; exit 2; fi

# Single infra bucket; live stream under vm-logs/, durable archive under log-archive/.
INFRA_BUCKET="gs://deployment-scripts-${PROJECT}"
SRC="${INFRA_BUCKET}/vm-logs"
TS="$(date -u +%Y%m%d_%H%M)"
ARC="${INFRA_BUCKET}/log-archive/snapshot_${TS}"

# Default target = all RUNNING VMs in the zone.
if [[ ${#VMS[@]} -eq 0 ]]; then
    mapfile -t VMS < <(gcloud compute instances list \
        --filter="status=RUNNING" --zones="$ZONE" \
        --format="value(name)" 2>/dev/null)
fi

echo "Archiving ${#VMS[@]} VM(s) -> $ARC"
n_runlog=0; n_serial=0
for vm in "${VMS[@]}"; do
    [[ -z "$vm" ]] && continue
    echo "--- $vm ---"
    # 1. run.log — server-side GCS->GCS copy (fast, no download), if present.
    if gcloud storage ls "$SRC/$vm/run.log" >/dev/null 2>&1; then
        if gcloud storage cp "$SRC/$vm/run.log" "$ARC/$vm/run.log" 2>/dev/null; then
            n_runlog=$((n_runlog + 1)); echo "  run.log archived"
        fi
    else
        echo "  no central run.log (boot-hung / never-started — serial below is the record)"
    fi
    # 2. serial console — the only durable evidence for boot-hung / wedged VMs.
    tmp="$(mktemp)"
    if gcloud compute instances get-serial-port-output "$vm" \
            --zone="$ZONE" --project="$PROJECT" > "$tmp" 2>/dev/null; then
        if gcloud storage cp "$tmp" "$ARC/$vm/serial-console.txt" 2>/dev/null; then
            n_serial=$((n_serial + 1)); echo "  serial-console archived"
        fi
    fi
    rm -f "$tmp"
done

echo ""
echo "BACKUP COMPLETE -> $ARC"
echo "  run.log files:        $n_runlog"
echo "  serial-console files: $n_serial"
echo "Verify: gcloud storage ls -r $ARC/ | tail"
