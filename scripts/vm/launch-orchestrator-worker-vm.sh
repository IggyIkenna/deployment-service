#!/usr/bin/env bash
# launch-orchestrator-worker-vm.sh — launch a LONG-RUNNING agent-orchestrator
# worker VM from a bare Ubuntu 24.04 AMI, fully automated (2026-06-12).
#
# Codifies the proven 2026-05-22 epic-fleet user-data shape (recovered from
# i-003be935f72c13d51's instance metadata — the original was launched ad-hoc
# from an operator shell and never checked in): apt deps → AWS CLI v2 → GH_PAT
# from Secrets Manager → clone agent-orchestrator on live-defi-rollout →
# scripts/bootstrap_vm.sh does everything else (repos, creds bucket, systemd
# orchestrator service + pm-pull timer, slots, health check, self-registration).
#
# New vs 2026-05-22: --env KEY=VAL passthrough → exported in the user-data shell
# AND handed to bootstrap via ORCHESTRATOR_EXTRA_ENV (upserted into .env.local
# BEFORE the backend starts). This is how an e2e TEST VM boots straight into
#   --env ORCHESTRATOR_VM_ID=vm-e2e-test \
#   --env ORCHESTRATOR_REGEN_REQUIRE_VM_MATCH=true
# so it can never adopt fleet or global plans (strict regen scoping).
#
# Usage:
#   bash launch-orchestrator-worker-vm.sh \
#     --vm-id vm-e2e-test --role epic --slots 2 \
#     [--name <Name-tag>] [--instance-type m7i.xlarge] [--disk-gb 150] \
#     [--lifecycle long-running|e2e-test] [--operator <tag>] \
#     [--env KEY=VAL ...] [--force]
#
#   bash launch-orchestrator-worker-vm.sh --terminate <instance-id>   # teardown
#   bash launch-orchestrator-worker-vm.sh --stop <instance-id>        # keep disk
#
# Defaults (env-overridable) come from the live orchestrator stack:
#   AWS_REGION=ap-northeast-1, AWS_SECURITY_GROUP_IDS=sg-0080310387e84f613,
#   AWS_SUBNET_ID=subnet-fc09eca6, INSTANCE_PROFILE=uts-orchestrator-epic,
#   AWS_KEY_PAIR_NAME=agent-orchestrator-key.
# AMI: AMI_ID env if set (Packer warm image from
# deployment-service/packer/agent-orchestrator/), else latest Ubuntu 24.04 (SSM).
#
# Long-running contract: shutdown behavior = STOP (not terminate) — an in-VM
# shutdown keeps the disk; teardown is the explicit --terminate path.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/aws_ec2_launch_lib.sh
source "${SCRIPT_DIR}/lib/aws_ec2_launch_lib.sh"

AWS_REGION="${AWS_REGION:-ap-northeast-1}"
export AWS_SECURITY_GROUP_IDS="${AWS_SECURITY_GROUP_IDS:-sg-0080310387e84f613}"
export AWS_SUBNET_ID="${AWS_SUBNET_ID:-subnet-fc09eca6}"
export AWS_KEY_PAIR_NAME="${AWS_KEY_PAIR_NAME:-agent-orchestrator-key}"
INSTANCE_PROFILE="${INSTANCE_PROFILE:-uts-orchestrator-epic}"

VM_ID=""
NAME=""
ROLE="epic"
SLOTS="2"
INSTANCE_TYPE="m7i.xlarge"
DISK_GB="150"
LIFECYCLE="long-running"
OPERATOR_TAG="${USER:-operator}"
EXTRA_ENVS=()
FORCE=false

usage() { grep '^#' "$0" | head -40 | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --vm-id) VM_ID="$2"; shift 2 ;;
    --name) NAME="$2"; shift 2 ;;
    --role) ROLE="$2"; shift 2 ;;
    --slots) SLOTS="$2"; shift 2 ;;
    --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
    --disk-gb) DISK_GB="$2"; shift 2 ;;
    --lifecycle) LIFECYCLE="$2"; shift 2 ;;
    --operator) OPERATOR_TAG="$2"; shift 2 ;;
    --env) EXTRA_ENVS+=("$2"); shift 2 ;;
    --force) FORCE=true; shift ;;
    --stop)
      aws ec2 stop-instances --region "${AWS_REGION}" --instance-ids "$2" --output text
      echo "stop requested: $2 (disk kept — restart with start-instances)"; exit 0 ;;
    --terminate)
      aws ec2 terminate-instances --region "${AWS_REGION}" --instance-ids "$2" --output text
      echo "terminate requested: $2 (root disk is DeleteOnTermination)"; exit 0 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

[[ -n "${VM_ID}" ]] || { echo "ERROR: --vm-id is required (e.g. vm-e2e-test)" >&2; exit 1; }
[[ "${ROLE}" == "epic" || "${ROLE}" == "planning" ]] || { echo "ERROR: --role must be epic|planning" >&2; exit 1; }
NAME="${NAME:-agent-orch-${VM_ID}-$(date -u +%Y%m%d)}"

# Refuse a duplicate running instance with the same Name prefix unless --force.
if ! $FORCE; then
  lc_aws_singleton_check "${NAME}" "${AWS_REGION}"
fi

# Validate --env entries early (KEY=VAL, KEY a shell identifier).
EXTRA_ENV_BLOCK=""
for kv in ${EXTRA_ENVS[@]+"${EXTRA_ENVS[@]}"}; do
  if [[ ! "${kv}" =~ ^[A-Za-z_][A-Za-z0-9_]*=.*$ ]]; then
    echo "ERROR: --env expects KEY=VAL (got: ${kv})" >&2; exit 1
  fi
  EXTRA_ENV_BLOCK+="${kv}"$'\n'
done

# ── user-data (the 2026-05-22 proven shape + the EXTRA_ENV hook) ────────────
USER_DATA_FILE="$(mktemp /tmp/orch-worker-userdata.XXXXXX.sh)"
trap 'rm -f "${USER_DATA_FILE}"' EXIT

{
  cat <<EOF
#!/bin/bash
set -euo pipefail
export HOME=/home/ubuntu
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin:/home/ubuntu/.local/bin"
export VM_NAME="${NAME}"
export CLOUD_PROVIDER=aws
export CLOUD_MOCK_MODE=false
export AWS_DEFAULT_REGION="${AWS_REGION}"
EOF

  # Launcher-injected env: exported in this shell (so bootstrap's VM_ID
  # resolution + worktree branding see them) AND persisted via the
  # ORCHESTRATOR_EXTRA_ENV hook (so .env.local carries them for the service).
  if [[ -n "${EXTRA_ENV_BLOCK}" ]]; then
    while IFS= read -r kv; do
      [[ -z "${kv}" ]] && continue
      printf 'export %s=%q\n' "${kv%%=*}" "${kv#*=}"
    done <<< "${EXTRA_ENV_BLOCK}"
    printf 'export ORCHESTRATOR_EXTRA_ENV=%q\n' "${EXTRA_ENV_BLOCK}"
  fi

  cat <<EOF

exec > >(tee /var/log/orch-worker-bootstrap.log) 2>&1
echo "=== Orchestrator worker VM startup: ${NAME} (${VM_ID}, role=${ROLE}) ==="
date

apt-get update -qq
apt-get install -yqq git tmux curl python3 python3-pip python3-yaml unzip

if ! command -v aws &>/dev/null 2>&1; then
  curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
  unzip -q /tmp/awscliv2.zip -d /tmp/awscliv2
  bash /tmp/awscliv2/aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli --update
  rm -rf /tmp/awscliv2*
fi

GH_PAT="\$(aws secretsmanager get-secret-value --secret-id GH_PAT --query SecretString --output text 2>/dev/null || echo '')"
if [[ -z "\${GH_PAT}" ]]; then
  echo "ERROR: could not fetch GH_PAT from Secrets Manager — check ${INSTANCE_PROFILE} IAM role" >&2
  exit 1
fi

WORKSPACE_ROOT="/home/ubuntu/unified-trading-system-repos"
mkdir -p "\${WORKSPACE_ROOT}"
chown ubuntu:ubuntu "\${WORKSPACE_ROOT}"
ORCH_DIR="\${WORKSPACE_ROOT}/agent-orchestrator"
if [[ ! -d "\${ORCH_DIR}/.git" ]]; then
  git clone --branch live-defi-rollout "https://x-access-token:\${GH_PAT}@github.com/IggyIkenna/agent-orchestrator.git" "\${ORCH_DIR}"
  chown -R ubuntu:ubuntu "\${ORCH_DIR}"
else
  git config --global --add safe.directory "\${ORCH_DIR}" 2>/dev/null || true
  git -C "\${ORCH_DIR}" pull --ff-only
fi

bash "\${ORCH_DIR}/scripts/bootstrap_vm.sh" \
  --operator ubuntu \
  --slots ${SLOTS} \
  --role ${ROLE} \
  --cloud-provider aws

echo "=== Orchestrator worker VM boot complete: ${NAME} ==="
date
EOF
} > "${USER_DATA_FILE}"

bash -n "${USER_DATA_FILE}"

EXTRA_TAGS="$(python3 - "$VM_ID" "$ROLE" "$OPERATOR_TAG" "$LIFECYCLE" <<'PYEOF'
import json, sys
vm_id, role, operator, lifecycle = sys.argv[1:5]
print(json.dumps([
    {"Key": "vm-id", "Value": vm_id},
    {"Key": "role", "Value": role},
    {"Key": "operator", "Value": operator},
    {"Key": "purpose", "Value": "orchestrator-worker"},
    {"Key": "lifecycle", "Value": lifecycle},
    {"Key": "cloud", "Value": "aws"},
]))
PYEOF
)"

echo "Launching ${NAME} (${INSTANCE_TYPE}, ${DISK_GB}GB gp3, role=${ROLE}, slots=${SLOTS}, lifecycle=${LIFECYCLE})"
[[ -n "${EXTRA_ENV_BLOCK}" ]] && echo "  env overrides: $(echo "${EXTRA_ENV_BLOCK}" | tr '\n' ' ')"

INSTANCE_ID="$(LC_AWS_SHUTDOWN_BEHAVIOR=stop lc_aws_ec2_run \
  "${NAME}" "${AWS_REGION}" "${INSTANCE_TYPE}" "${DISK_GB}" \
  "${INSTANCE_PROFILE}" "${USER_DATA_FILE}" "${EXTRA_TAGS}")"

echo "instance-id: ${INSTANCE_ID}"
echo "Waiting for running state + public IP..."
aws ec2 wait instance-running --region "${AWS_REGION}" --instance-ids "${INSTANCE_ID}"
PUBLIC_IP="$(aws ec2 describe-instances --region "${AWS_REGION}" --instance-ids "${INSTANCE_ID}" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)"

cat <<DONE

LAUNCHED ${NAME}
  instance-id : ${INSTANCE_ID}
  public-ip   : ${PUBLIC_IP}
  vm-id       : ${VM_ID}  (role=${ROLE}, slots=${SLOTS}, lifecycle=${LIFECYCLE})

Bootstrap runs via user-data (cold boot 5-15 min; warm AMI <5 min). Next:
  verify (T+10min rule): bash agent-orchestrator/scripts/verify_vm_e2e.sh ${INSTANCE_ID}
  bootstrap log:         aws ssm start-session --target ${INSTANCE_ID} --region ${AWS_REGION}
                         then: sudo tail -f /var/log/orch-worker-bootstrap.log
  teardown:              bash $0 --terminate ${INSTANCE_ID}   (or --stop to keep the disk)
DONE
