#!/usr/bin/env bash
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
# The watchdog VM itself reads the events bucket (gs://{pid}-events) — env-aware
# resolution lets the watchdog scope its event-stream + heartbeat reads to the
# correct env tier. The watchdog Python (vm_zombie_watchdog.py) keys VM filters
# by VM-NAME prefix; env-tier acts as an orthogonal axis for any future
# per-env bucket reads.
#
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
#   - script:        ./vm_zombie_watchdog.py (repo) — uploaded to
#                    gs://deployment-scripts-{pid}/scripts/vm_zombie_watchdog.py
#                    on every launch so the in-VM bootstrap pulls the latest.
#                    To extend coverage: edit VM_PREFIX_TO_BUCKET in the .py,
#                    then relaunch this watchdog (kill old vm-zombie-watchdog-*
#                    and re-run this script — the running watchdog never
#                    re-fetches mid-loop).
#   - heartbeat:     gs://deployment-scripts-{pid}/vm-heartbeat/{vm-name}.txt
#                    (60s sidecar in setup-data-pipeline-vm.sh)
#   - naming rules:  unified-trading-pm/cursor-configs/CLAUDE.md
#                    § "VM Naming Convention"
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
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --interval)        INTERVAL="$2"; shift 2 ;;
        --dry-run)         DRY_RUN=true;  shift ;;
        --force)           FORCE=true;    shift ;;
        --heartbeat-stale) HB_STALE="$2"; shift 2 ;;
        --shard-stale)     SHARD_STALE="$2"; shift 2 ;;
        --min-age)         MIN_AGE="$2"; shift 2 ;;
        --env)             DEPLOYMENT_ENV="$2"; shift 2 ;;
        --help|-h)         grep '^#' "$0" | head -40; exit 0 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

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

# Upload the repo's vm_zombie_watchdog.py to GCS so the in-VM bootstrap
# pulls the latest VM_PREFIX_TO_BUCKET. The repo is the SSOT; GCS is a
# build artifact. This refresh is idempotent and cheap.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_PY="$SCRIPT_DIR/vm_zombie_watchdog.py"
if [[ ! -f "$LOCAL_PY" ]]; then
    echo "ERROR: $LOCAL_PY not found — cannot refresh GCS copy." >&2
    exit 1
fi
echo "Uploading repo SSOT → gs://${CODE_BUCKET}/scripts/vm_zombie_watchdog.py"
gsutil -q cp "$LOCAL_PY" "gs://${CODE_BUCKET}/scripts/vm_zombie_watchdog.py"

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
    /opt/watchdog-venv/bin/python3 /tmp/watchdog.py --min-age ${MIN_AGE} --heartbeat-stale ${HB_STALE} --shard-stale ${SHARD_STALE} ${DRY_FLAG} || true
    sleep ${INTERVAL}
done
"

STARTUP="
#!/usr/bin/env bash
set -uo pipefail
export DEBIAN_FRONTEND=noninteractive

# Wait up to 60s for any in-flight unattended-upgrades to release the
# dpkg lock; otherwise apt-get install would block.
for i in \$(seq 1 60); do
    if ! fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

# UAC requires Python >=3.13,<3.14; Ubuntu 24.04 ships Python 3.12.
# Install Python 3.13 via deadsnakes PPA, then create a dedicated venv.
apt-get install -y software-properties-common 2>&1 | tail -2 || true
add-apt-repository ppa:deadsnakes/ppa -y 2>&1 | tail -2 || true
apt-get update -y 2>&1 | tail -2 || true
apt-get install -y python3.13 python3.13-venv 2>&1 | tail -2 || true

python3.13 -m venv /opt/watchdog-venv

# Install google-cloud packages + UAC into the Python 3.13 venv.
/opt/watchdog-venv/bin/pip install --quiet google-cloud-compute google-cloud-storage 2>&1 | tail -3 || true

# Install UAC (needed for VmPrefixSpec + LifecycleClass imports).
# Use system tar to extract (avoids Python 3.12+ tarfile security filter on symlinks).
gsutil -q cp "gs://${CODE_BUCKET}/code/unified-api-contracts-code.tar.gz" /tmp/uac.tar.gz 2>&1 || true
if [[ -f /tmp/uac.tar.gz ]]; then
    mkdir -p /tmp/uac-src
    tar xf /tmp/uac.tar.gz -C /tmp/uac-src --strip-components=1 2>&1 | head -5 || true
    /opt/watchdog-venv/bin/pip install --quiet /tmp/uac-src 2>&1 | tail -3 || true
fi

# Install UTL (needed for resolve_bucket_name import).
gsutil -q cp "gs://${CODE_BUCKET}/code/unified-trading-library-code.tar.gz" /tmp/utl.tar.gz 2>&1 || true
if [[ -f /tmp/utl.tar.gz ]]; then
    mkdir -p /tmp/utl-src
    tar xf /tmp/utl.tar.gz -C /tmp/utl-src --strip-components=1 2>&1 | head -5 || true
    /opt/watchdog-venv/bin/pip install --quiet /tmp/utl-src 2>&1 | tail -3 || true
fi

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
    --metadata="DEPLOYMENT_ENV=${DEPLOYMENT_ENV}" \
    --metadata-from-file=startup-script="$STARTUP_FILE" \
    --labels=purpose=vm-zombie-watchdog,tier=daemon,env="${DEPLOYMENT_ENV}" 2>&1 | tail -5

echo ""
echo "VM running. Tail logs:"
echo "  gcloud compute instances get-serial-port-output $VM_NAME --zone=$ZONE | tail -50"
echo ""
echo "Stop:"
echo "  gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
