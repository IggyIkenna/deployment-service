#!/usr/bin/env bash
# Launch always-on VM zombie watchdog daemon.
#
# Polls every 5 min via vm_zombie_watchdog.py:
#   1. Lists all RUNNING backfill-class VMs across the project
#   2. For each: checks heartbeat blob (gs://.../vm-heartbeat/{vm}.txt) +
#      per-VM manifest shard mtime
#   3. Kills any VM that's stale beyond thresholds
#
# Why this exists: the 2026-04-29 cefi rollout zombie failure mode (12 VMs
# lost their network namespace; in-VM watchdog couldn't communicate to
# Pub/Sub or anywhere else; VMs sat idle for 36-50h burning ~$1.30/hr each).
# External polling does not depend on the VM's network — checks only
# GCS-side fingerprints.
#
# SSOT:
#   - script:        gs://deployment-scripts-{pid}/scripts/vm_zombie_watchdog.py
#   - heartbeat:     gs://deployment-scripts-{pid}/vm-heartbeat/{vm-name}.txt
#                    (60s sidecar in setup-data-pipeline-vm.sh)
#   - prefixes:      cefi-mr-, cefi-binance-, cefi-deribit-, tradfi-bf-,
#                    fs-backfill-, af-backfill-, tm-backfill-, sfi-backfill-,
#                    us-backfill-, openmeteo-backfill-, combo-migration-, etc.
#                    (extend in vm_zombie_watchdog.py VM_PREFIX_TO_BUCKET)
#
# Invocation:
#   bash launch-vm-zombie-watchdog.sh                    # 5min poll, dry-run=false
#   bash launch-vm-zombie-watchdog.sh --interval 300     # custom interval
#   bash launch-vm-zombie-watchdog.sh --dry-run          # report only, don't kill
#   bash launch-vm-zombie-watchdog.sh --force            # bypass singleton lock
#
# Cost: e2-small 24/7 ≈ $12/mo asia-northeast1. Workload is API + small GCS
# reads — no Python pandas merging, no memory pressure.
#
# Singleton lock: refuses to launch if another vm-zombie-watchdog VM is already
# running. --force bypasses.
set -euo pipefail

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
INTERVAL=300
DRY_RUN=false
FORCE=false
HB_STALE=15
SHARD_STALE=120
MIN_AGE=15

while [[ $# -gt 0 ]]; do
    case "$1" in
        --interval)        INTERVAL="$2"; shift 2 ;;
        --dry-run)         DRY_RUN=true;  shift ;;
        --force)           FORCE=true;    shift ;;
        --heartbeat-stale) HB_STALE="$2"; shift 2 ;;
        --shard-stale)     SHARD_STALE="$2"; shift 2 ;;
        --min-age)         MIN_AGE="$2"; shift 2 ;;
        --help|-h)         grep '^#' "$0" | head -40; exit 0 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

if ! $FORCE; then
    EXISTING=$(gcloud compute instances list \
        --filter='name~"^vm-zombie-watchdog-" AND status=RUNNING' \
        --zones="$ZONE" --format='value(name)' 2>/dev/null | head -1)
    if [[ -n "$EXISTING" ]]; then
        echo "ERROR: vm-zombie-watchdog already running: $EXISTING (use --force to bypass)" >&2
        exit 1
    fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="vm-zombie-watchdog-${RUN_TS}"
DRY_FLAG=""
if $DRY_RUN; then DRY_FLAG="--dry-run"; fi

# Build the in-VM command (poll loop).
#
# Boot pip vs apt notes (2026-05-04 fix for the silent-watchdog incident):
#   - python3-pandas + python3-pyarrow are installed via APT first.  Ubuntu
#     24.04 ships ``typing_extensions`` from Debian without a pip RECORD file,
#     and any pip transitive that wants to upgrade it crashes with
#     "Cannot uninstall typing_extensions ... RECORD file not found" — which
#     is exactly what killed the previous watchdog at boot, leaving 29
#     orphaned backfill VMs running for 3 days.
#   - The remaining google-cloud-* SDKs are not in apt and must come via pip.
#     ``--ignore-installed`` tells pip to skip the uninstall step entirely,
#     so any installed-by-apt deps (typing_extensions, requests, urllib3) stay
#     in place even if a newer version is in the resolution.
#   - The pip command itself runs without ``set -e`` so a single dependency
#     edge-case can't kill the whole startup script — the next line, the
#     poll loop, will simply ImportError-and-retry if a package is missing.
LOOP_CMD="
cd /tmp
gsutil cp gs://${CODE_BUCKET}/scripts/vm_zombie_watchdog.py /tmp/watchdog.py
python3 -m pip install \
    --break-system-packages \
    --ignore-installed \
    --quiet \
    google-cloud-compute google-cloud-storage || true
while true; do
    python3 /tmp/watchdog.py --min-age ${MIN_AGE} --heartbeat-stale ${HB_STALE} --shard-stale ${SHARD_STALE} ${DRY_FLAG} || true
    sleep ${INTERVAL}
done
"

STARTUP="
#!/usr/bin/env bash
set -uo pipefail
export DEBIAN_FRONTEND=noninteractive
# python3-pandas / python3-pyarrow from apt avoids the typing_extensions
# uninstall-record clash that killed the previous watchdog at boot.
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-pandas python3-pyarrow
${LOOP_CMD}
"

echo "Launching watchdog VM: $VM_NAME (interval=${INTERVAL}s, dry_run=${DRY_RUN})"
gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-small \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=20GB \
    --scopes=cloud-platform \
    --metadata=startup-script="$STARTUP" \
    --labels=purpose=vm-zombie-watchdog 2>&1 | tail -5

echo ""
echo "VM running. Tail logs:"
echo "  gcloud compute instances get-serial-port-output $VM_NAME --zone=$ZONE | tail -50"
echo ""
echo "Stop:"
echo "  gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
