#!/usr/bin/env bash
# Launch an epic VM from the orchestrator VM registry.
#
# Reads unified-trading-pm/orchestrator_vm_registry.yaml to look up the VM spec
# by id, then provisions a GCE VM with the matching configuration.
#
# Each epic VM hosts 8 agent slots (e2-standard-4: 4 vCPU / 16GB RAM) and runs
# the agent-orchestrator service + a dedicated set of master_plans.
#
# VM naming: agent-orch-{vm-id}-{YYYYMMDD}
#
# Singleton lock: refuse if any agent-orch-{vm-id}-* VM is RUNNING (unless --force).
#
# Event stream:
#   gs://{PROJECT_ID}-events/orchestrator/{vm-id}/{VM_NAME}/STARTED
#   Required: STARTED within 60s of boot.
#
# Watchdog registration: prefix "agent-orch-{vm-id}-" must be registered in
# VM_PREFIX_TO_BUCKET with lifecycle_class=LONG_LIVED_LIVE after adding a new VM.
#
# Usage:
#   bash launch-epic-vm.sh --vm-id vm-defi                 # provision single VM
#   bash launch-epic-vm.sh --vm-id vm-defi --dry-run       # print plan only
#   bash launch-epic-vm.sh --vm-id vm-defi --force         # bypass singleton lock
#   bash launch-epic-vm.sh --all                           # provision all non-planning VMs
#   bash launch-epic-vm.sh --all --dry-run                 # dry-run all
#
# The registry file lives at:
#   unified-trading-pm/orchestrator_vm_registry.yaml
# relative to the workspace root (auto-detected from script path or $WORKSPACE_ROOT).
#
# Post-launch verification (T+10 min):
#   gcloud compute instances describe agent-orch-{vm-id}-$(date +%Y%m%d) \
#       --zone=asia-northeast1-c --project=central-element-323112
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
SERVICE_ACCOUNT="t1_batch_sa@central-element-323112.iam.gserviceaccount.com"
OPERATOR="${OPERATOR:-ubuntu}"
VM_ID=""
FORCE=false
DRY_RUN=false
PROVISION_ALL=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --vm-id)    VM_ID="$2"; shift 2 ;;
    --force)    FORCE=true; shift ;;
    --dry-run)  DRY_RUN=true; shift ;;
    --all)      PROVISION_ALL=true; shift ;;
    --project)  PROJECT_ID="$2"; shift 2 ;;
    --zone)     ZONE="$2"; shift 2 ;;
    --operator) OPERATOR="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if ! $PROVISION_ALL && [[ -z "${VM_ID}" ]]; then
  echo "ERROR: --vm-id <id> or --all is required." >&2
  echo "       Run with --dry-run --all to list all available VM ids." >&2
  exit 1
fi

# ── Zone guard: never fall back to another region ──
case "${ZONE}" in
  asia-northeast1-*) ;;
  *)
    echo "ERROR: zone ${ZONE} is outside asia-northeast1. All GCS data is in asia-northeast1;" >&2
    echo "       cross-region egress adds cost and latency." >&2
    exit 1 ;;
esac

# ── Locate registry file ──
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Try to find the PM repo relative to known workspace layouts
# Canonical: workspace_root/unified-trading-pm/orchestrator_vm_registry.yaml
# In worktrees: .tabs/<N>/unified-trading-pm/orchestrator_vm_registry.yaml
REGISTRY_CANDIDATES=(
  "${WORKSPACE_ROOT:-}/unified-trading-pm/orchestrator_vm_registry.yaml"
  "${SCRIPT_DIR}/../../../unified-trading-pm/orchestrator_vm_registry.yaml"
  "/home/${OPERATOR}/unified-trading-system-repos/.pm/orchestrator_vm_registry.yaml"
  "$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel 2>/dev/null || echo '')/../../unified-trading-pm/orchestrator_vm_registry.yaml"
)
REGISTRY=""
for candidate in "${REGISTRY_CANDIDATES[@]}"; do
  if [[ -f "${candidate}" ]]; then
    REGISTRY="$(realpath "${candidate}")"
    break
  fi
done

if [[ -z "${REGISTRY}" ]]; then
  echo "ERROR: could not find orchestrator_vm_registry.yaml. Tried:" >&2
  for c in "${REGISTRY_CANDIDATES[@]}"; do echo "  ${c}" >&2; done
  echo "" >&2
  echo "Set WORKSPACE_ROOT or run from within the workspace." >&2
  exit 1
fi

echo "Registry: ${REGISTRY}"

# ── Parse YAML with Python (python3-yaml is installed on ubuntu-2204) ──
# Returns a JSON list of {id, role, label, master_plans, accounts_primary}
_parse_registry() {
  python3 - "${REGISTRY}" "$1" <<'PYEOF'
import sys, json

registry_path = sys.argv[1]
query_id = sys.argv[2] if len(sys.argv) > 2 else "__ALL__"

try:
    import yaml
except ImportError:
    # Fallback: minimal YAML parser for the flat registry format
    # (shouldn't happen on ubuntu-2204 with python3-yaml installed)
    print(json.dumps({"error": "PyYAML not installed; run: apt-get install -y python3-yaml"}))
    sys.exit(1)

with open(registry_path) as f:
    doc = yaml.safe_load(f)

vms = doc.get("vms", [])
results = []
for vm in vms:
    vm_id = vm.get("id", "")
    if query_id != "__ALL__" and vm_id != query_id:
        continue
    results.append({
        "id": vm_id,
        "role": vm.get("role", "epic"),
        "label": vm.get("label", ""),
        "master_plans": vm.get("master_plans", []),
        "accounts_primary": (vm.get("accounts") or {}).get("primary", ""),
    })

if query_id != "__ALL__" and not results:
    valid_ids = [v.get("id", "") for v in vms]
    print(json.dumps({"error": f"vm-id '{query_id}' not found. Valid ids: {valid_ids}"}))
    sys.exit(1)

print(json.dumps(results))
PYEOF
}

# ── Launch a single VM by registry entry ──
_launch_one() {
  local vm_entry_json="$1"

  local vm_id role label master_plans_json accounts_primary
  vm_id="$(echo "${vm_entry_json}" | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print(d['id'])")"
  role="$(echo "${vm_entry_json}" | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print(d['role'])")"
  label="$(echo "${vm_entry_json}" | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print(d['label'])")"
  master_plans_json="$(echo "${vm_entry_json}" | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print(json.dumps(d['master_plans']))")"
  accounts_primary="$(echo "${vm_entry_json}" | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print(d['accounts_primary'])")"

  TODAY="$(date +%Y%m%d)"
  VM_PREFIX="agent-orch-${vm_id}-"
  VM_NAME="${VM_PREFIX}${TODAY}"
  GCS_EVENTS="gs://${PROJECT_ID}-events/orchestrator/${vm_id}/${VM_NAME}"

  # Print master plans this VM will own
  echo ""
  echo "=== Epic VM: ${vm_id} ==="
  echo "  Label:        ${label}"
  echo "  VM name:      ${VM_NAME}"
  echo "  Zone:         ${ZONE}"
  echo "  Machine:      ${MACHINE_TYPE}"
  echo "  Primary acct: ${accounts_primary}"
  echo "  Master plans:"
  echo "${master_plans_json}" | python3 -c "
import sys, json
plans = json.loads(sys.stdin.read())
for p in plans:
    print(f'    - {p}')
" 2>/dev/null || echo "    (none)"
  echo ""

  # ── Singleton lock ──
  if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
      --filter="name~\"^${VM_PREFIX}\" AND status=RUNNING" \
      --zones="${ZONE}" \
      --project="${PROJECT_ID}" \
      --format='value(name)' 2>/dev/null | head -1 || true)"
    if [[ -n "$EXISTING" ]]; then
      echo "WARN: Epic VM ${vm_id} already running: ${EXISTING}" >&2
      echo "      Use --force to bypass. Skipping ${VM_NAME}." >&2
      return 0
    fi
  fi

  if $DRY_RUN; then
    echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping."
    return 0
  fi

  # ── Build startup script ──
  local STARTUP_SCRIPT
  STARTUP_SCRIPT=$(cat <<STARTUP_EOF
#!/bin/bash
set -euo pipefail
export HOME=/home/${OPERATOR}
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/${OPERATOR}/.local/bin"
export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"
export VM_NAME="${VM_NAME}"
export CLOUD_PROVIDER=gcp
export CLOUD_MOCK_MODE=false

exec > >(tee /var/log/epic-vm-bootstrap.log) 2>&1

echo "=== VM Startup: ${VM_NAME} (${vm_id}) ==="
date

# ── Base packages ──
apt-get update -qq
apt-get install -yqq git tmux curl python3 python3-pip python3-yaml nodejs npm

# ── Fetch GH_PAT from Secret Manager ──
GH_PAT="\$(gcloud secrets versions access latest --secret=GH_PAT --project=${PROJECT_ID})"
if [[ -z "\${GH_PAT}" ]]; then
  echo "ERROR: could not fetch GH_PAT from Secret Manager" >&2
  exit 1
fi

# ── Clone agent-orchestrator ──
ORCH_DIR="/home/${OPERATOR}/agent-orchestrator"
if [[ ! -d "\${ORCH_DIR}/.git" ]]; then
  git clone "https://x-access-token:\${GH_PAT}@github.com/IggyIkenna/agent-orchestrator.git" "\${ORCH_DIR}"
  chown -R ${OPERATOR}:${OPERATOR} "\${ORCH_DIR}"
else
  git -C "\${ORCH_DIR}" pull --ff-only
fi

# ── Run bootstrap_vm.sh ──
bash "\${ORCH_DIR}/scripts/bootstrap_vm.sh" \
  --operator ${OPERATOR} \
  --slots 8 \
  --role epic

# ── Emit STARTED event ──
echo "STARTED \$(date -u +%Y-%m-%dT%H:%M:%SZ) vm=${VM_NAME} epic=${vm_id}" | \
  gcloud storage cp - "${GCS_EVENTS}/STARTED" --project=${PROJECT_ID} 2>/dev/null || true

echo "=== Epic VM boot complete: ${VM_NAME} ==="
date
STARTUP_EOF
)

  local STARTUP_FILE
  STARTUP_FILE="$(mktemp /tmp/startup-epic-vm-XXXX.sh)"
  printf '%s' "${STARTUP_SCRIPT}" > "${STARTUP_FILE}"

  local METADATA
  METADATA="VM_NAME=${VM_NAME}"
  METADATA="${METADATA},VM_ID=${vm_id}"
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
    --tags="orchestrator-epic-vm,allow-ssh" \
    --labels="managed-by=script,role=epic,operator=ikenna,vm-id=${vm_id},purpose=orchestrator-epic" \
    --metadata="${METADATA}" \
    --metadata-from-file=startup-script="${STARTUP_FILE}"

  rm "${STARTUP_FILE}"

  local EXTERNAL_IP
  EXTERNAL_IP="$(gcloud compute instances describe "${VM_NAME}" \
    --zone="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='get(networkInterfaces[0].accessConfigs[0].natIP)' 2>/dev/null || echo "pending")"

  echo ""
  echo "  VM launched: ${VM_NAME}"
  echo "  External IP: ${EXTERNAL_IP}"
  echo "  Boot log:    gcloud compute ssh ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} -- 'sudo tail -f /var/log/epic-vm-bootstrap.log'"
  echo "  Health:      curl -sf http://${EXTERNAL_IP}:8026/healthz"
  echo "  STARTED evt: gcloud storage ls ${GCS_EVENTS}/"
  echo ""
  echo "  NOTE: Register 'agent-orch-${vm_id}-' prefix in vm_zombie_watchdog.py"
  echo "        VM_PREFIX_TO_BUCKET with lifecycle_class=LONG_LIVED_LIVE, then"
  echo "        relaunch the watchdog VM."
}

# ── Main: single or all ──
if $PROVISION_ALL; then
  echo "=== Provisioning all epic VMs (non-planning) ==="
  ALL_VMS_JSON="$(_parse_registry "__ALL__")"

  # Check for parse errors
  if echo "${ALL_VMS_JSON}" | python3 -c "
import sys, json
d = json.loads(sys.stdin.read())
if isinstance(d, dict) and 'error' in d:
    print(d['error'], file=sys.stderr)
    sys.exit(1)
" 2>&1; then
    :
  fi

  # Iterate over all non-planning VMs
  echo "${ALL_VMS_JSON}" | python3 -c "
import sys, json
vms = json.loads(sys.stdin.read())
for vm in vms:
    if vm['role'] != 'planning':
        print(json.dumps(vm))
" | while IFS= read -r vm_json; do
    _launch_one "${vm_json}"
  done

  echo ""
  echo "=== All epic VM provisioning complete ==="
else
  # Single VM
  QUERY_RESULT="$(_parse_registry "${VM_ID}")"

  # Check for errors
  ERROR="$(echo "${QUERY_RESULT}" | python3 -c "
import sys, json
d = json.loads(sys.stdin.read())
if isinstance(d, dict) and 'error' in d:
    print(d['error'])
else:
    print('')
" 2>/dev/null || echo "")"
  if [[ -n "${ERROR}" ]]; then
    echo "ERROR: ${ERROR}" >&2
    exit 1
  fi

  # Extract the single entry
  VM_ENTRY="$(echo "${QUERY_RESULT}" | python3 -c "
import sys, json
vms = json.loads(sys.stdin.read())
print(json.dumps(vms[0]))
")"

  if [[ "${VM_ID}" == "planning-vm" ]]; then
    echo "WARN: planning-vm should be launched via launch-planning-vm.sh, not launch-epic-vm.sh." >&2
    echo "      Use: bash deployment-service/scripts/vm/launch-planning-vm.sh" >&2
    exit 1
  fi

  _launch_one "${VM_ENTRY}"
fi
