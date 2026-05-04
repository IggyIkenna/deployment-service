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

# Boot pip vs apt notes (2026-05-04 — silent-watchdog incident fix):
#
#   * The previous watchdog crashed at boot because pip-installing
#     pandas/pyarrow tried to upgrade ``typing_extensions`` past Ubuntu's
#     apt-installed copy, which has no pip RECORD file and so cannot be
#     uninstalled. ``set -euo pipefail`` then killed the whole startup
#     script BEFORE the poll loop ever started.
#
#   * Fix splits installs into two layered apt calls so ONE missing package
#     can't roll back the must-haves, then pip with ``--ignore-installed``
#     so it never touches apt-managed deps:
#
#       1. apt: python3, python3-pip                  (must-have, atomic)
#       2. apt: python3-pandas                         (apt has it; nice-to-have)
#       3. pip: google-cloud-compute google-cloud-storage pyarrow
#               --ignore-installed (skips uninstall step, leaves
#               typing_extensions + urllib3 + requests as apt put them)
#
#   * Outer ``set -uo pipefail`` (no ``-e``) so a transient apt mirror or
#     pip resolution failure can't take down the daemon — the inner poll
#     loop will simply ImportError on the next iteration if something's
#     missing, which is far easier to diagnose than a silent boot.
LOOP_CMD="
cd /tmp
gsutil cp gs://${CODE_BUCKET}/scripts/vm_zombie_watchdog.py /tmp/watchdog.py
while true; do
    python3 /tmp/watchdog.py --min-age ${MIN_AGE} --heartbeat-stale ${HB_STALE} --shard-stale ${SHARD_STALE} ${DRY_FLAG} || true
    sleep ${INTERVAL}
done
"

STARTUP="
#!/usr/bin/env bash
set -uo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq

# Pass 1 — must-have core; if either of these fails the watchdog cannot run.
apt-get install -y -qq python3 python3-pip

# Pass 2 — nice-to-haves. python3-pandas is in apt; python3-pyarrow is not
# in Ubuntu 24.04 main, so it comes via pip below. Wrapping in '|| true'
# means a missing pandas package still lets pip take over.
apt-get install -y -qq python3-pandas || true

# Pip the rest with --ignore-installed so it never tries to uninstall
# apt-managed deps (the typing_extensions trap that killed the previous
# watchdog at boot).
python3 -m pip install \
    --break-system-packages \
    --ignore-installed \
    --quiet \
    google-cloud-compute google-cloud-storage pyarrow || true

${LOOP_CMD}
"

echo "Launching watchdog VM: $VM_NAME (interval=${INTERVAL}s, dry_run=${DRY_RUN})"

# Materialise the startup script to a temp file so gcloud's
# --metadata-from-file path handles the multi-line + special-char content
# cleanly. Inline --metadata=startup-script="..." trips gcloud's shell-flag
# parser on multi-line bodies (observed 2026-05-04: gcloud aborts with a
# generic "Please see gcloud topic escaping" hint).
STARTUP_FILE="$(mktemp -t vm-zombie-watchdog-startup.XXXXXX.sh)"
trap 'rm -f "$STARTUP_FILE"' EXIT
printf '%s\n' "$STARTUP" > "$STARTUP_FILE"

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-small \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=20GB \
    --scopes=cloud-platform \
    --metadata-from-file=startup-script="$STARTUP_FILE" \
    --labels=purpose=vm-zombie-watchdog 2>&1 | tail -5

echo ""
echo "VM running. Tail logs:"
echo "  gcloud compute instances get-serial-port-output $VM_NAME --zone=$ZONE | tail -50"
echo ""
echo "Stop:"
echo "  gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
