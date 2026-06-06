#!/usr/bin/env bash
# Launch the CENTRAL / PLANNING ("ikenna-brain") VM on AWS EC2 — canonical from-scratch
# relaunch of the one always-on orchestrator box.
#
# This box is NOT an epic VM. It is the single central API + planning host:
#   - Name  : agent-orchestrator-vm-1   (role tag: ikenna-brain)
#   - id    : ORCHESTRATOR_VM_ID=planning → tab/planning/<slot> branches, role planning
#   - public: the Elastic IP 13.113.200.22 (DNS api.agent-orchestrator.odum-research.com)
#             nginx :443 (Let's Encrypt) → orchestrator backend :8765
#   - slots : 5 interactive (Ikenna / Harsh / review / CI-escalation / plan-health)
#   - it auto-spawns SLOTS but does NOT auto-assign backlog jobs (role=planning) — humans drive.
#
# WHY this exists: the live central box (i-0c9b283b31d6b5ca7) was hand-provisioned one-off, so a
# from-scratch relaunch was manual. This makes it canonical. The epic launcher
# (`launch-epic-vm-aws.sh`) is for the per-epic worker VMs and must NOT be used for this box.
# SSOT: unified-trading-pm/plans/active/planning_vm_canonical_bringup_and_topology_reconcile_2026_06_05.md
#       + codex/12-agent-workflow/orchestrator-multi-vm-topology.md.
#
# PREREQUISITES (operator):
#   - The Elastic IP 13.113.200.22 (alloc eipalloc-07b7bfe509d63c477) exists + is free (or this
#     reassociates it from the dying box — reassociation is near-instant, DNS stays valid).
#   - Secrets Manager `ORCHESTRATOR_ENV_LOCAL` holds the central `.env.local` (operator JWT secret,
#     ORCHESTRATOR_MODE=live, ORCHESTRATOR_PUBLIC_URL=https://api.agent-orchestrator…, users.json
#     path) AND its `ORCHESTRATOR_VM_ID=planning` / `ORCHESTRATOR_VM_ROLE=planning` lines —
#     bootstrap_vm.sh fetches this verbatim, so it is the source of truth for the RUNNING backend
#     id (the user-data export below only governs slot branding). Keep them in sync.
#   - nginx + the Let's Encrypt cert for api.agent-orchestrator.odum-research.com: bake them into an
#     AMI and pass AMI_ID=ami-… (recommended — TLS comes up immediately). Without a prebaked AMI the
#     box runs the backend on :8765 but the public :443 perimeter must be set up separately
#     (certbot needs the DNS A-record already pointing at the EIP, which it is).
#   - uts-orchestrator-epic IAM instance profile + GH_PAT in Secrets Manager (as for epic VMs).
#
# Usage:
#   bash launch-central-brain-aws.sh                 # provision the central/planning box
#   bash launch-central-brain-aws.sh --dry-run       # print the plan only
#   bash launch-central-brain-aws.sh --force         # bypass the singleton lock (one brain only)
#   AMI_ID=ami-0123… bash launch-central-brain-aws.sh # prebaked AMI (nginx+cert+deps) — recommended
#
# Post-launch:
#   - EIP 13.113.200.22 is re-associated automatically (see end of script).
#   - T+5min:  curl -sf https://api.agent-orchestrator.odum-research.com/health
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/aws_ec2_launch_lib.sh
source "${SCRIPT_DIR}/lib/aws_ec2_launch_lib.sh"

AWS_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
INSTANCE_TYPE="${INSTANCE_TYPE:-m8i.4xlarge}"   # central box is bigger: API router + 5 slots + state
DISK_GB="${DISK_GB:-60}"                          # state.db (32GB+) + worktrees → 60GB, not the epic 30
INSTANCE_PROFILE="uts-orchestrator-epic"
OPERATOR="${OPERATOR:-ubuntu}"
SLOTS="${SLOTS:-5}"
VM_NAME="agent-orchestrator-vm-1"                # FIXED canonical name (singleton — one brain)
VM_PREFIX="agent-orchestrator-vm-"
ORCHESTRATOR_VM_ID="planning"
EIP_ALLOC="${EIP_ALLOC:-eipalloc-07b7bfe509d63c477}"  # the 13.113.200.22 Elastic IP
FORCE=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)         FORCE=true;          shift ;;
    --dry-run)       DRY_RUN=true;        shift ;;
    --region)        AWS_REGION="$2";     shift 2 ;;
    --operator)      OPERATOR="$2";       shift 2 ;;
    --instance-type) INSTANCE_TYPE="$2";  shift 2 ;;
    --slots)         SLOTS="$2";          shift 2 ;;
    --eip-alloc)     EIP_ALLOC="$2";      shift 2 ;;
    -h|--help)       sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

# Region guard — ap-northeast-1 only (GCS data is in asia-northeast1).
case "${AWS_REGION}" in
  ap-northeast-1) ;;
  *) echo "ERROR: region ${AWS_REGION} is not ap-northeast-1." >&2; exit 1 ;;
esac

lc_aws_resolve_account

echo ""
echo "=== Central / Planning (ikenna-brain) VM — AWS ==="
echo "  VM name:      ${VM_NAME}   (role: ikenna-brain)"
echo "  VM id:        ORCHESTRATOR_VM_ID=${ORCHESTRATOR_VM_ID} → tab/${ORCHESTRATOR_VM_ID}/<slot>, role planning"
echo "  Region:       ${AWS_REGION}"
echo "  Instance:     ${INSTANCE_TYPE}  (disk ${DISK_GB}GB)"
echo "  Slots:        ${SLOTS}  (Ikenna / Harsh / review / CI-escalation / plan-health)"
echo "  Elastic IP:   ${EIP_ALLOC} (13.113.200.22, re-associated post-launch)"
echo "  Profile:      ${INSTANCE_PROFILE}"
echo ""

# Singleton: only one brain may run. Keys on the central Name prefix (NOT the epic agent-orch- one).
lc_aws_singleton_check "${VM_PREFIX}" "${AWS_REGION}" "${FORCE}"

if $DRY_RUN; then
  echo "[DRY RUN] Would launch ${VM_NAME} (${INSTANCE_TYPE}) + re-associate ${EIP_ALLOC} — skipping."
  exit 0
fi

# ── user-data: export the canonical id, then bootstrap as a PLANNING host ──
USER_DATA_FILE="$(mktemp /tmp/startup-central-brain-XXXX.sh)"
# shellcheck disable=SC2064
trap "rm -f '${USER_DATA_FILE}'" EXIT

cat > "${USER_DATA_FILE}" <<STARTUP_EOF
#!/bin/bash
set -euo pipefail
export HOME=/home/${OPERATOR}
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin:/home/${OPERATOR}/.local/bin"
export VM_NAME="${VM_NAME}"
# Canonical SHORT id — brands the planning slots tab/planning/<slot> + role routing. WITHOUT this,
# bootstrap falls back to the long VM_NAME and brands tab/agent-orchestrator-vm-1/<slot>. The RUNNING
# backend's id still comes from the .env.local that bootstrap fetches from Secrets Manager
# (ORCHESTRATOR_ENV_LOCAL) — keep that secret's ORCHESTRATOR_VM_ID=planning in sync.
export ORCHESTRATOR_VM_ID="${ORCHESTRATOR_VM_ID}"
export CLOUD_PROVIDER=aws
export CLOUD_MOCK_MODE=false
export AWS_DEFAULT_REGION="${AWS_REGION}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}"

exec > >(tee /var/log/central-brain-bootstrap.log) 2>&1
echo "=== Central/Planning VM Startup: ${VM_NAME} (id=${ORCHESTRATOR_VM_ID}) ==="
date

apt-get update -qq
apt-get install -yqq git tmux curl python3 python3-pip python3-yaml unzip nginx

if ! command -v aws &>/dev/null 2>&1; then
  curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
  unzip -q /tmp/awscliv2.zip -d /tmp/awscliv2
  bash /tmp/awscliv2/aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli --update
  rm -rf /tmp/awscliv2*
fi

GH_PAT="\$(aws secretsmanager get-secret-value --secret-id GH_PAT --query SecretString --output text 2>/dev/null || echo '')"
if [[ -z "\${GH_PAT}" ]]; then
  echo "ERROR: could not fetch GH_PAT from Secrets Manager — check uts-orchestrator-epic IAM role" >&2
  exit 1
fi

WORKSPACE_ROOT="/home/${OPERATOR}/unified-trading-system-repos"
mkdir -p "\${WORKSPACE_ROOT}"; chown ${OPERATOR}:${OPERATOR} "\${WORKSPACE_ROOT}"
ORCH_DIR="\${WORKSPACE_ROOT}/agent-orchestrator"
if [[ ! -d "\${ORCH_DIR}/.git" ]]; then
  git clone --branch live-defi-rollout "https://x-access-token:\${GH_PAT}@github.com/IggyIkenna/agent-orchestrator.git" "\${ORCH_DIR}"
  chown -R ${OPERATOR}:${OPERATOR} "\${ORCH_DIR}"
else
  git config --global --add safe.directory "\${ORCH_DIR}" 2>/dev/null || true
  git -C "\${ORCH_DIR}" pull --ff-only
fi

# bootstrap as a PLANNING host: fetches ORCHESTRATOR_ENV_LOCAL from Secrets Manager → .env.local,
# provisions ${SLOTS} planning slots (tab/planning/N), installs the orchestrator service + crons.
bash "\${ORCH_DIR}/scripts/bootstrap_vm.sh" \
  --operator ${OPERATOR} \
  --slots ${SLOTS} \
  --role planning \
  --cloud-provider aws

# NOTE: nginx :443 → :8765 (Let's Encrypt for api.agent-orchestrator.odum-research.com) is expected
# from a prebaked AMI. If launching on a bare Ubuntu AMI, provision the cert here once DNS resolves:
#   certbot --nginx -d api.agent-orchestrator.odum-research.com --non-interactive --agree-tos -m ops@odum-research.com
# (the DNS A-record already points at the EIP, so the HTTP-01 challenge succeeds.)

echo "=== Central/Planning VM boot complete: ${VM_NAME} ==="
date
STARTUP_EOF

EXTRA_TAGS='[{"Key":"role","Value":"ikenna-brain"},{"Key":"vm-id","Value":"planning"},{"Key":"managed-by","Value":"script"},{"Key":"purpose","Value":"orchestrator-central-api"},{"Key":"operator","Value":"ikenna"},{"Key":"cloud","Value":"aws"}]'

echo "Creating ${VM_NAME} on EC2 (${INSTANCE_TYPE}, ${AWS_REGION})..."
INSTANCE_ID="$(lc_aws_ec2_run \
  "${VM_NAME}" \
  "${AWS_REGION}" \
  "${INSTANCE_TYPE}" \
  "${DISK_GB}" \
  "${INSTANCE_PROFILE}" \
  "${USER_DATA_FILE}" \
  "${EXTRA_TAGS}")"

echo "  Instance ID:  ${INSTANCE_ID}"

# ── Re-associate the Elastic IP (DNS stays valid; reassociation is near-instant) ──
echo "Waiting for ${INSTANCE_ID} to reach 'running' before associating the EIP..."
aws ec2 wait instance-running --instance-ids "${INSTANCE_ID}" --region "${AWS_REGION}"
echo "Associating Elastic IP ${EIP_ALLOC} (13.113.200.22) → ${INSTANCE_ID}..."
aws ec2 associate-address \
  --instance-id "${INSTANCE_ID}" \
  --allocation-id "${EIP_ALLOC}" \
  --allow-reassociation \
  --region "${AWS_REGION}" >/dev/null
echo "  ✓ EIP associated — api.agent-orchestrator.odum-research.com now points at the new box."

echo ""
echo "  Monitor:   aws ssm start-session --target ${INSTANCE_ID}"
echo "  T+5min:    curl -sf https://api.agent-orchestrator.odum-research.com/health"
echo "  Reminder:  keep the ORCHESTRATOR_ENV_LOCAL secret's ORCHESTRATOR_VM_ID=planning in sync."
