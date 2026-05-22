#!/usr/bin/env bash
# Launch an epic VM from the orchestrator VM registry on AWS EC2.
#
# AWS equivalent of launch-epic-vm.sh — reads the same orchestrator_vm_registry.yaml,
# launches m7i.xlarge in ap-northeast-1 using lib/aws_ec2_launch_lib.sh.
#
# Each epic VM hosts 8 agent slots (m7i.xlarge: 4 vCPU / 16GB — matches GCP e2-standard-4)
# and runs the agent-orchestrator service + dedicated master_plans.
#
# VM naming: agent-orch-{vm-id}-{YYYYMMDD}  (same convention as GCP)
#
# Singleton lock: refuse if any agent-orch-{vm-id}-* is running (unless --force).
#
# Prerequisites:
#   AWS_SECURITY_GROUP_IDS  — security group(s), e.g. "sg-xxxxxxxx"
#   AWS_SUBNET_ID           — subnet in ap-northeast-1a/b/c
#   AWS_KEY_PAIR_NAME       — optional (SSH access)
#   uts-orchestrator-epic IAM instance profile (setup-orchestrator-iam.sh)
#   S3 buckets + Secrets Manager (Phase 2 OPERATOR tasks in aws_epic_vm_fleet_2026_05_22.md)
#
# Usage:
#   bash launch-epic-vm-aws.sh --vm-id vm-defi
#   bash launch-epic-vm-aws.sh --vm-id vm-defi --dry-run
#   bash launch-epic-vm-aws.sh --vm-id vm-defi --force
#   bash launch-epic-vm-aws.sh --all
#   bash launch-epic-vm-aws.sh --all --dry-run
#
# Post-launch T+10min check:
#   aws ec2 describe-instances --filters Name=tag:Name,Values=agent-orch-vm-defi-* --region ap-northeast-1 \
#       --query 'Reservations[].Instances[?State.Name==`running`].PublicIpAddress' --output text
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/aws_ec2_launch_lib.sh
source "${SCRIPT_DIR}/lib/aws_ec2_launch_lib.sh"

AWS_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
INSTANCE_TYPE="${INSTANCE_TYPE:-m7i.xlarge}"
DISK_GB=30
INSTANCE_PROFILE="uts-orchestrator-epic"
OPERATOR="${OPERATOR:-ubuntu}"
VM_ID=""
FORCE=false
DRY_RUN=false
PROVISION_ALL=false
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --vm-id)         VM_ID="$2";         shift 2 ;;
    --force)         FORCE=true;         shift ;;
    --dry-run)       DRY_RUN=true;       shift ;;
    --all)           PROVISION_ALL=true; shift ;;
    --region)        AWS_REGION="$2";    shift 2 ;;
    --operator)      OPERATOR="$2";      shift 2 ;;
    --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
    --workspace)     WORKSPACE_ROOT="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if ! $PROVISION_ALL && [[ -z "${VM_ID}" ]]; then
  echo "ERROR: --vm-id <id> or --all is required." >&2
  echo "       Run with --dry-run --all to list all available VM ids." >&2
  exit 1
fi

# Region guard: ap-northeast-1 only (mirrors asia-northeast1; minimises GCS latency)
case "${AWS_REGION}" in
  ap-northeast-1) ;;
  *)
    echo "ERROR: region ${AWS_REGION} is not ap-northeast-1." >&2
    echo "       GCS data lives in asia-northeast1; use ap-northeast-1 to minimise cross-region cost." >&2
    exit 1 ;;
esac

# Resolve AWS account ID
lc_aws_resolve_account

# ── Locate registry file (same search order as launch-epic-vm.sh) ──
REGISTRY_CANDIDATES=(
  "${WORKSPACE_ROOT}/unified-trading-pm/orchestrator_vm_registry.yaml"
  "${SCRIPT_DIR}/../../../unified-trading-pm/orchestrator_vm_registry.yaml"
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

# ── Parse YAML — identical to launch-epic-vm.sh ──
_parse_registry() {
  python3 - "${REGISTRY}" "$1" <<'PYEOF'
import sys, json

registry_path = sys.argv[1]
query_id = sys.argv[2] if len(sys.argv) > 2 else "__ALL__"

try:
    import yaml
except ImportError:
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

# ── Launch a single VM ──
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
  S3_EVENTS="s3://uts-orchestrator-events-${AWS_ACCOUNT_ID}/orchestrator/${role}/${VM_NAME}"

  echo ""
  echo "=== Epic VM (AWS): ${vm_id} ==="
  echo "  Label:        ${label}"
  echo "  VM name:      ${VM_NAME}"
  echo "  Region:       ${AWS_REGION}"
  echo "  Instance:     ${INSTANCE_TYPE}"
  echo "  Profile:      ${INSTANCE_PROFILE}"
  echo "  Primary acct: ${accounts_primary}"
  echo "  Master plans:"
  echo "${master_plans_json}" | python3 -c "
import sys, json
plans = json.loads(sys.stdin.read())
for p in plans:
    print(f'    - {p}')
" 2>/dev/null || echo "    (none)"
  echo ""

  # Singleton check
  lc_aws_singleton_check "${VM_PREFIX}" "${AWS_REGION}" "${FORCE}"

  if $DRY_RUN; then
    echo "[DRY RUN] Would launch ${VM_NAME} on ${INSTANCE_TYPE} in ${AWS_REGION} — skipping."
    return 0
  fi

  # Build user-data startup script
  local USER_DATA_FILE
  USER_DATA_FILE="$(mktemp /tmp/startup-epic-aws-XXXX.sh)"
  # shellcheck disable=SC2064
  trap "rm -f '${USER_DATA_FILE}'" RETURN

  cat > "${USER_DATA_FILE}" <<STARTUP_EOF
#!/bin/bash
set -euo pipefail
export HOME=/home/${OPERATOR}
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin:/home/${OPERATOR}/.local/bin"
export VM_NAME="${VM_NAME}"
export CLOUD_PROVIDER=aws
export CLOUD_MOCK_MODE=false
export AWS_DEFAULT_REGION="${AWS_REGION}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}"

exec > >(tee /var/log/epic-vm-bootstrap.log) 2>&1

echo "=== AWS Epic VM Startup: ${VM_NAME} (${vm_id}) ==="
date

# Base packages
apt-get update -qq
apt-get install -yqq git tmux curl python3 python3-pip python3-yaml unzip

# AWS CLI v2 (Ubuntu 24.04 does not ship it)
if ! command -v aws &>/dev/null 2>&1; then
  curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
  unzip -q /tmp/awscliv2.zip -d /tmp/awscliv2
  bash /tmp/awscliv2/aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli --update
  rm -rf /tmp/awscliv2*
fi

# Fetch GH_PAT from Secrets Manager (instance role provides IAM auth)
GH_PAT="\$(aws secretsmanager get-secret-value --secret-id GH_PAT --query SecretString --output text 2>/dev/null || echo '')"
if [[ -z "\${GH_PAT}" ]]; then
  echo "ERROR: could not fetch GH_PAT from Secrets Manager — check uts-orchestrator-epic IAM role" >&2
  exit 1
fi

# Clone agent-orchestrator
WORKSPACE_ROOT="/home/${OPERATOR}/unified-trading-system-repos"
mkdir -p "\${WORKSPACE_ROOT}"
chown ${OPERATOR}:${OPERATOR} "\${WORKSPACE_ROOT}"
ORCH_DIR="\${WORKSPACE_ROOT}/agent-orchestrator"
if [[ ! -d "\${ORCH_DIR}/.git" ]]; then
  git clone --branch live-defi-rollout "https://x-access-token:\${GH_PAT}@github.com/IggyIkenna/agent-orchestrator.git" "\${ORCH_DIR}"
  chown -R ${OPERATOR}:${OPERATOR} "\${ORCH_DIR}"
else
  git config --global --add safe.directory "\${ORCH_DIR}" 2>/dev/null || true
  git -C "\${ORCH_DIR}" pull --ff-only
fi

# Run bootstrap_vm.sh — CLOUD_PROVIDER=aws set in env + explicit flag
bash "\${ORCH_DIR}/scripts/bootstrap_vm.sh" \
  --operator ${OPERATOR} \
  --slots 8 \
  --role epic \
  --cloud-provider aws

echo "=== AWS Epic VM boot complete: ${VM_NAME} ==="
date
STARTUP_EOF

  local EXTRA_TAGS
  EXTRA_TAGS='[{"Key":"role","Value":"epic"},{"Key":"vm-id","Value":"'"${vm_id}"'"},{"Key":"managed-by","Value":"script"},{"Key":"purpose","Value":"orchestrator-epic"},{"Key":"operator","Value":"ikenna"},{"Key":"cloud","Value":"aws"}]'

  echo "Creating ${VM_NAME} on EC2 (${INSTANCE_TYPE}, ${AWS_REGION})..."
  local INSTANCE_ID
  INSTANCE_ID="$(lc_aws_ec2_run \
    "${VM_NAME}" \
    "${AWS_REGION}" \
    "${INSTANCE_TYPE}" \
    "${DISK_GB}" \
    "${INSTANCE_PROFILE}" \
    "${USER_DATA_FILE}" \
    "${EXTRA_TAGS}")"

  echo ""
  echo "  Instance ID:  ${INSTANCE_ID}"
  echo "  VM name:      ${VM_NAME}"
  echo "  S3 events:    ${S3_EVENTS}"
  echo ""
  echo "  Monitor:"
  echo "    aws ec2 describe-instances --instance-ids ${INSTANCE_ID} --region ${AWS_REGION} --query 'Reservations[].Instances[].State.Name' --output text"
  echo "    aws ssm start-session --target ${INSTANCE_ID}  (SSM access — no key needed)"
  echo ""
  echo "  T+10min health check:"
  echo "    PUBLIC_IP=\$(aws ec2 describe-instances --instance-ids ${INSTANCE_ID} --region ${AWS_REGION} --query 'Reservations[].Instances[].PublicIpAddress' --output text)"
  echo "    curl -sf http://\${PUBLIC_IP}:8026/health"
  echo ""
  echo "  STARTED event:"
  echo "    aws s3 ls ${S3_EVENTS}/"
  echo ""
  echo "  NOTE: After fleet is stable, add 'agent-orch-${vm_id}-' to vm_zombie_watchdog_aws.py"
  echo "        VM_PREFIX_TO_BUCKET with lifecycle_class=LONG_LIVED_LIVE."
}

# ── Main: single or all ──
if $PROVISION_ALL; then
  echo "=== Provisioning all epic VMs (non-planning) on AWS EC2 ==="
  ALL_VMS_JSON="$(_parse_registry "__ALL__")"

  echo "${ALL_VMS_JSON}" | python3 -c "
import sys, json
vms = json.loads(sys.stdin.read())
for vm in vms:
    if vm['role'] != 'planning':
        print(json.dumps(vm))
" | while IFS= read -r vm_json; do
    # Subshell: singleton-check exit 1 for already-running VMs doesn't abort the fleet loop
    ( _launch_one "${vm_json}" ) || {
      _skip_id="$(echo "${vm_json}" | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('id','unknown'))" 2>/dev/null || echo 'unknown')"
      echo "WARN: ${_skip_id} already running or launch failed — skipping (use --vm-id ${_skip_id} --force to re-launch)"
    }
  done

  echo ""
  echo "=== All AWS epic VM provisioning requests submitted ==="
  echo ""
  echo "T+10min verification command (run after ~10min):"
  echo "  for vm_id in vm-defi vm-cefi vm-tradfi vm-sports vm-prediction vm-ml vm-trading-core vm-operator-ops vm-cross-cutting vm-orchestrator; do"
  echo "    IP=\$(aws ec2 describe-instances --filters \"Name=tag:vm-id,Values=\${vm_id}\" \"Name=instance-state-name,Values=running\" --region ${AWS_REGION} --query 'Reservations[].Instances[0].PublicIpAddress' --output text)"
  echo "    echo \"\${vm_id} (\${IP}): \$(curl -sf --max-time 5 http://\${IP}:8026/health 2>/dev/null || echo FAIL)\""
  echo "  done"
else
  if [[ "${VM_ID:-}" == "planning-vm" ]]; then
    echo "WARN: planning-vm should use a dedicated planning launcher, not launch-epic-vm-aws.sh." >&2
    exit 1
  fi

  QUERY_RESULT="$(_parse_registry "${VM_ID}")"
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

  VM_ENTRY="$(echo "${QUERY_RESULT}" | python3 -c "
import sys, json
vms = json.loads(sys.stdin.read())
print(json.dumps(vms[0]))
")"

  _launch_one "${VM_ENTRY}"
fi
