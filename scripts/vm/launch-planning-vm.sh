#!/usr/bin/env bash
# Launch a GCE VM for the orchestrator planning VM.
#
# The planning VM hosts 2 interactive agent slots (Ikenna + Harsh) plus nightly
# orchestrator cron (orphan-ping audit, FF-pull, slot-git-status-report).
# It runs the agent-orchestrator FastAPI + Vite dashboard on port 8026.
#
# VM naming: agent-orch-planning-vm-{YYYYMMDD}  (date-stamped for easy replacement)
#
# Singleton lock: only one planning VM should run at a time.
# Pass --force to bypass.
#
# Event stream:
#   gs://{PROJECT_ID}-events/orchestrator/planning-vm/{VM_NAME}/STARTED
#   Required: STARTED within 60s of boot.
#
# Watchdog registration: VM prefix "agent-orch-planning-vm-" is registered
# in VM_PREFIX_TO_BUCKET in vm_zombie_watchdog.py with
# lifecycle_class=LONG_LIVED_LIVE. After editing that dict, relaunch the
# watchdog VM:
#   gcloud compute instances delete vm-zombie-watchdog-... --zone=asia-northeast1-c --quiet
#   bash deployment-service/scripts/vm/launch-vm-zombie-watchdog.sh
#
# Usage:
#   bash launch-planning-vm.sh                    # provision planning VM
#   bash launch-planning-vm.sh --dry-run          # print plan only
#   bash launch-planning-vm.sh --force            # bypass singleton lock
#   bash launch-planning-vm.sh --operator ubuntu  # explicit operator name
#   bash launch-planning-vm.sh --zone asia-northeast1-b  # stockout fallback
#
# Post-launch verification (T+10 min):
#   gcloud compute instances describe agent-orch-planning-vm-$(date +%Y%m%d) \
#       --zone=asia-northeast1-c --project=central-element-323112
#   curl -sf http://<EXTERNAL_IP>:8026/healthz
#
# SSH shortcut (printed after launch):
#   ssh agent-orch-planning-vm
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-medium}"
SERVICE_ACCOUNT="t1-batch-sa@central-element-323112.iam.gserviceaccount.com"
OPERATOR="${OPERATOR:-ubuntu}"
FORCE=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)    FORCE=true; shift ;;
    --dry-run)  DRY_RUN=true; shift ;;
    --project)  PROJECT_ID="$2"; shift 2 ;;
    --zone)     ZONE="$2"; shift 2 ;;
    --operator) OPERATOR="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

# ── Zone guard: never fall back to another region ──
case "${ZONE}" in
  asia-northeast1-*) ;;
  *)
    echo "ERROR: zone ${ZONE} is outside asia-northeast1. All GCS data is in asia-northeast1;" >&2
    echo "       cross-region egress adds cost and latency." >&2
    exit 1 ;;
esac

TODAY="$(date +%Y%m%d)"
VM_PREFIX="agent-orch-planning-vm-"
VM_NAME="${VM_PREFIX}${TODAY}"
GCS_EVENTS="gs://${PROJECT_ID}-events/orchestrator/planning-vm/${VM_NAME}"

# ── Singleton lock: refuse duplicate launch ──
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^${VM_PREFIX}\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: Planning VM already running in ${ZONE}: ${EXISTING}
Refusing duplicate launch — only one planning VM should run at a time.

Options:
  Inspect:  gcloud compute instances describe ${EXISTING} --zone=${ZONE} --project=${PROJECT_ID}
  SSH:      gcloud compute ssh ${EXISTING} --zone=${ZONE} --project=${PROJECT_ID}
  Stop:     gcloud compute instances delete ${EXISTING} --zone=${ZONE} --project=${PROJECT_ID} --quiet
  Force:    bash $0 --force
EOF
    exit 1
  fi
fi

echo "=== Orchestrator Planning VM Launch ==="
echo "  VM name:         ${VM_NAME}"
echo "  Zone:            ${ZONE}"
echo "  Machine:         ${MACHINE_TYPE}"
echo "  Service account: ${SERVICE_ACCOUNT}"
echo "  Operator:        ${OPERATOR}"
echo "  Events path:     ${GCS_EVENTS}/STARTED"
echo ""

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — exiting."
  exit 0
fi

# ── Build startup script ──
STARTUP_SCRIPT=$(cat <<STARTUP_EOF
#!/bin/bash
set -euo pipefail
export HOME=/home/${OPERATOR}
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin:/home/${OPERATOR}/.local/bin"
export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"
export CLOUD_PROVIDER=gcp
export CLOUD_MOCK_MODE=false
# Brand tab branches by the canonical ROLE id, NOT the long instance name (operator 2026-06-05).
# bootstrap_vm.sh does VM_ID=\${ORCHESTRATOR_VM_ID:-\${VM_NAME}}; without this it would fall back to
# the dated VM_NAME (agent-orch-planning-vm-YYYYMMDD) and brand tab/<that-long-name>/N instead of
# tab/planning/N. SSOT: plans/active/planning_vm_canonical_bringup_and_topology_reconcile_2026_06_05.md.
export ORCHESTRATOR_VM_ID=planning

exec > >(tee /var/log/planning-vm-bootstrap.log) 2>&1

echo "=== VM Startup: ${VM_NAME} ==="
date

# ── 1. Base packages ──
apt-get update -qq
apt-get install -yqq git tmux curl python3 python3-pip

# ── 2. Fetch GH_PAT from Secret Manager ──
GH_PAT="\$(gcloud secrets versions access latest --secret=GH_PAT --project=${PROJECT_ID})"
if [[ -z "\${GH_PAT}" ]]; then
  echo "ERROR: could not fetch GH_PAT from Secret Manager" >&2
  exit 1
fi

# ── 3. Clone agent-orchestrator ──
ORCH_DIR="/home/${OPERATOR}/agent-orchestrator"
if [[ ! -d "\${ORCH_DIR}/.git" ]]; then
  git clone "https://x-access-token:\${GH_PAT}@github.com/IggyIkenna/agent-orchestrator.git" "\${ORCH_DIR}"
  chown -R ${OPERATOR}:${OPERATOR} "\${ORCH_DIR}"
else
  echo "agent-orchestrator already cloned — pulling latest"
  git -C "\${ORCH_DIR}" pull --ff-only
fi

# ── 4. Run bootstrap_vm.sh ──
bash "\${ORCH_DIR}/scripts/bootstrap_vm.sh" \
  --operator ${OPERATOR} \
  --slots 5 \
  --role planning

# ── 5. Emit STARTED event to GCS ──
echo "STARTED \$(date -u +%Y-%m-%dT%H:%M:%SZ) vm=${VM_NAME}" | \
  gcloud storage cp - "${GCS_EVENTS}/STARTED" --project=${PROJECT_ID} 2>/dev/null || true

echo "=== Planning VM boot complete: ${VM_NAME} ==="
date
STARTUP_EOF
)

# Write startup script to a temp file (same pattern as other launchers)
STARTUP_FILE="$(mktemp /tmp/startup-planning-vm-XXXX.sh)"
printf '%s' "${STARTUP_SCRIPT}" > "${STARTUP_FILE}"

# ── Build metadata ──
METADATA="VM_NAME=${VM_NAME}"
METADATA="${METADATA},OPERATOR=${OPERATOR}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"

echo "Creating VM ${VM_NAME}..."
gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --service-account="${SERVICE_ACCOUNT}" \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=30GB \
  --boot-disk-type=pd-standard \
  --tags="orchestrator-planning-vm,allow-ssh" \
  --labels="managed-by=script,role=planning,operator=ikenna,purpose=orchestrator-planning" \
  --metadata="${METADATA}" \
  --metadata-from-file=startup-script="${STARTUP_FILE}"

rm "${STARTUP_FILE}"

# ── Resolve external IP ──
EXTERNAL_IP="$(gcloud compute instances describe "${VM_NAME}" \
  --zone="${ZONE}" \
  --project="${PROJECT_ID}" \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)' 2>/dev/null || echo "pending")"

echo ""
echo "============================================================"
echo "Planning VM launched: ${VM_NAME}"
echo ""
echo "External IP:  ${EXTERNAL_IP}"
echo ""
echo "Monitor status:"
echo "  gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID}"
echo ""
echo "Boot log (once VM is up ~2-3 min):"
echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} -- 'sudo tail -f /var/log/planning-vm-bootstrap.log'"
echo ""
echo "T+10min health check:"
echo "  curl -sf http://${EXTERNAL_IP}:8026/healthz"
echo ""
echo "STARTED event (within 60s of boot):"
echo "  gcloud storage ls ${GCS_EVENTS}/"
echo "============================================================"
echo ""
echo "Add to ~/.ssh/config:"
echo "Host agent-orch-planning-vm"
echo "    HostName ${EXTERNAL_IP}"
echo "    User ${OPERATOR}"
echo "    IdentityFile ~/.ssh/google_compute_engine"
echo "    StrictHostKeyChecking no"
echo ""
echo "Then SSH with:  ssh agent-orch-planning-vm"
echo "Or via gcloud:  gcloud compute ssh ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID}"
echo ""
echo "NOTE: Register 'agent-orch-planning-vm-' prefix in vm_zombie_watchdog.py"
echo "      VM_PREFIX_TO_BUCKET with lifecycle_class=LONG_LIVED_LIVE, then"
echo "      relaunch the watchdog VM."
