#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# gap-2.6.A — Bucket-rsync VM launcher (Phase 2.6 cutover Wave 2-5 worker).
#
# Launches a GCE VM that runs `gcloud storage rsync` from a FLAT source bucket
# to an ENV-TIERED destination bucket. Singleton-locked per source-bucket
# (refuses to launch a second VM against the same source). Emits the standard
# BUCKET_RSYNC_STARTED / BUCKET_RSYNC_PROGRESS / BUCKET_RSYNC_STOPPED /
# BUCKET_RSYNC_FAILED event stream so the no-fire-and-forget rule is honoured.
#
# Wired by Wave 2-5 of:
#   codex/05-infrastructure/phase-2-6-bucket-name-cutover-runbook.md
#
# Pattern mirrors `launch-mdps-backfill-vm.sh` shape:
#   * 50 GB boot disk (rsync streams; no large local buffering).
#   * asia-northeast1-c (same region as both source and dest GCS buckets).
#   * Singleton lock by source-bucket basename.
#   * VM auto-deletes via VM_SHUTDOWN_ON_COMPLETION=true on exit.
#
# References:
#   plans/active/code_freeze_migrate_backfill_sequencing_2026_05_10.md § "gap-2.6.A"
#   deployment-service/scripts/vm/vm_zombie_watchdog.py § VM_PREFIX_TO_BUCKET
#   ("bucket-rsync-" prefix registration — Phase 2.6 PR landing alongside this script)
#
# Usage:
#   bash launch-bucket-rsync-vm.sh \
#     --source-bucket gs://market-data-tick-defi-central-element-323112 \
#     --dest-bucket   gs://market-data-tick-defi-prd-central-element-323112 \
#     --workers 16
#
#   # With prefix filter (only copy specific subtree)
#   bash launch-bucket-rsync-vm.sh \
#     --source-bucket gs://strategy-store-cefi-central-element-323112 \
#     --dest-bucket   gs://strategy-store-cefi-prd-central-element-323112 \
#     --workers 8 \
#     --prefix-filter "catalogue/"
#
#   # Dry-run (lists what would be copied; no actual writes)
#   bash launch-bucket-rsync-vm.sh \
#     --source-bucket gs://manual-audit-central-element-323112 \
#     --dest-bucket   gs://manual-audit-prd-central-element-323112 \
#     --dry-run

set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

SOURCE_BUCKET=""
DEST_BUCKET=""
WORKERS="8"
PREFIX_FILTER=""
DRY_RUN="false"
FORCE="false"

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --source-bucket) SOURCE_BUCKET="$2"; shift 2 ;;
        --dest-bucket)   DEST_BUCKET="$2"; shift 2 ;;
        --workers)       WORKERS="$2"; shift 2 ;;
        --prefix-filter) PREFIX_FILTER="$2"; shift 2 ;;
        --dry-run)       DRY_RUN="true"; shift ;;
        --force)         FORCE="true"; shift ;;
        -h|--help)
            head -50 "$0" | grep -E "^#" | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "ERROR: unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$SOURCE_BUCKET" || -z "$DEST_BUCKET" ]]; then
    echo "ERROR: --source-bucket and --dest-bucket are required" >&2
    echo "  bash $0 --help for usage" >&2
    exit 2
fi

if [[ ! "$SOURCE_BUCKET" =~ ^gs:// ]] || [[ ! "$DEST_BUCKET" =~ ^gs:// ]]; then
    echo "ERROR: both buckets must be gs:// URIs (got source=$SOURCE_BUCKET dest=$DEST_BUCKET)" >&2
    exit 2
fi

if [[ "$SOURCE_BUCKET" == "$DEST_BUCKET" ]]; then
    echo "ERROR: source and dest are identical — would be a no-op or destructive overwrite" >&2
    exit 2
fi

# Derive bucket basenames for VM naming + singleton lock
SOURCE_BASENAME="${SOURCE_BUCKET#gs://}"
SOURCE_BASENAME="${SOURCE_BASENAME%/}"  # strip trailing /
# Short hash for VM name uniqueness (basename collisions for similar buckets)
SOURCE_HASH=$(echo -n "$SOURCE_BASENAME" | shasum -a 1 | cut -c1-8)

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="e2-standard-4"  # rsync is I/O bound; 4 vCPU + 16GB RAM ample
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

# ── Singleton lock: refuse 2nd launch against same source bucket ──────────────
if [[ "$FORCE" != "true" ]]; then
    EXISTING="$(gcloud compute instances list \
        --project="$PROJECT" \
        --filter="labels.source-hash=${SOURCE_HASH} AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: bucket-rsync VM already running for source ${SOURCE_BUCKET}: ${EXISTING}
Concurrent rsync against the same source races on the dest write side.
Use --force to bypass (only if you've manually verified safety).

  Inspect: gcloud compute ssh ${EXISTING} --zone=${ZONE}

CAUTION — do NOT delete ${EXISTING} unless you have confirmed via Inspect
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If confirmed stale:
  gcloud compute instances delete ${EXISTING} --zone=${ZONE} --quiet
EOF
        exit 1
    fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="bucket-rsync-${SOURCE_HASH}-${RUN_TS}"

# Build the rsync command — runs inside the VM under setup-data-pipeline-vm.sh
RSYNC_CMD="gcloud storage rsync --recursive"
if [[ "$DRY_RUN" == "true" ]]; then
    RSYNC_CMD="${RSYNC_CMD} --dry-run"
fi

# Worker concurrency: gcloud storage uses parallel slice uploads internally;
# --workers controls process parallelism for the rsync walker.
# Set via storage/process_count + storage/thread_count + GS_NUM_THREADS env.
RSYNC_CMD="GS_NUM_THREADS=${WORKERS} STORAGE_PROCESS_COUNT=${WORKERS} ${RSYNC_CMD}"

SOURCE_TARGET="$SOURCE_BUCKET"
DEST_TARGET="$DEST_BUCKET"
if [[ -n "$PREFIX_FILTER" ]]; then
    SOURCE_TARGET="${SOURCE_BUCKET%/}/${PREFIX_FILTER#/}"
    DEST_TARGET="${DEST_BUCKET%/}/${PREFIX_FILTER#/}"
fi

RSYNC_CMD="${RSYNC_CMD} ${SOURCE_TARGET} ${DEST_TARGET}"

METADATA="VM_TASK=bucket-rsync"
METADATA="${METADATA},VM_SERVICE=deployment_service"
METADATA="${METADATA},VM_BACKFILL_CMD=${RSYNC_CMD}"
METADATA="${METADATA},VM_SOURCE_BUCKET=${SOURCE_BUCKET}"
METADATA="${METADATA},VM_DEST_BUCKET=${DEST_BUCKET}"
METADATA="${METADATA},VM_WORKERS=${WORKERS}"
METADATA="${METADATA},VM_DRY_RUN=${DRY_RUN}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

echo "Launching ${VM_NAME}"
echo "  source: ${SOURCE_BUCKET}"
echo "  dest:   ${DEST_BUCKET}"
echo "  prefix: ${PREFIX_FILTER:-<full>}"
echo "  workers: ${WORKERS}, dry-run: ${DRY_RUN}"
echo "  zone: ${ZONE}, machine: ${MACHINE_TYPE}, boot: ${BOOT_DISK_GB}G"
echo "  cmd: ${RSYNC_CMD}"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        deployment-service unified-api-contracts unified-trading-library \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

# Data-writing VM: disk sized for sustained writes. A pd-standard 50GB boot disk sustains
# only ~6 MB/s and collapsed the CeFi backfill to 2.36 MB/s after ~7.5GB (measured
# 2026-07-18; on pd-balanced 250GB the same workload held 11.09 MB/s steady-state with the
# disk only 14.7% utilised). Enforced by
# scripts/quality_gates/check_backfill_vm_disk_provisioning.py.
gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels="purpose=bucket-rsync,source-hash=${SOURCE_HASH},dry-run=${DRY_RUN},run-ts=${RUN_TS}"

echo ""
echo "VM launched: ${VM_NAME}"
echo "Source: ${SOURCE_BUCKET}"
echo "Dest:   ${DEST_BUCKET}"
echo ""
echo "Monitor:"
echo "  Events: gsutil ls gs://${PROJECT}-events/events/deployment-service/$(date +%Y-%m-%d)/${VM_NAME}/"
echo "  Serial: gcloud compute instances get-serial-port-output ${VM_NAME} --zone=${ZONE}"
echo "  Delete: gcloud compute instances delete ${VM_NAME} --zone=${ZONE} --quiet"
echo ""
echo "Drift verify (after VM STOPPED):"
echo "  python3 unified-trading-pm/scripts/migration/verify_flat_to_env_tiered_drift.py \\"
echo "    --source ${SOURCE_BUCKET} --dest ${DEST_BUCKET}"
