#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# AWS EC2 equivalent of launch-instruments-backfill-vm.sh.
# Launches instruments-service parallel backfill fleet on EC2 (ap-northeast-1).
#
# GCP counterpart: launch-instruments-backfill-vm.sh (Pattern A, 2026-05-21)
# VM allocation (7 VMs, default ranges):
#   instr-backfill-cefi-1-aws:  CEFI       2020-01-01 → 2022-06-30
#   instr-backfill-cefi-2-aws:  CEFI       2022-07-01 → 2024-12-31
#   instr-backfill-cefi-3-aws:  CEFI       2025-01-01 → 2026-02-28
#   instr-backfill-defi-aws:    DEFI       2020-01-01 → 2026-02-28
#   instr-backfill-tradfi-aws:  TRADFI     2020-01-01 → 2026-02-28
#   instr-backfill-sports-aws:  SPORTS     2020-06-01 → 2026-03-28
#   instr-backfill-pred-aws:    PREDICTION 2020-01-01 → 2026-02-28
#
# Usage:
#   bash launch-instruments-backfill-vm-aws.sh                            # All 7 VMs
#   bash launch-instruments-backfill-vm-aws.sh --dry-run                  # Print plan
#   bash launch-instruments-backfill-vm-aws.sh --asset-group DEFI         # Only DeFi VM
#   bash launch-instruments-backfill-vm-aws.sh --asset-group CEFI \
#       --start 2026-01-01 --end 2026-03-31                               # Override window
#   bash launch-instruments-backfill-vm-aws.sh --force                    # Bypass manifest skip
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/aws_ec2_launch_lib.sh
source "${SCRIPT_DIR}/lib/aws_ec2_launch_lib.sh"

AWS_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
INSTANCE_TYPE="${INSTANCE_TYPE:-m7i.xlarge}"
DISK_GB=50
INSTANCE_PROFILE="uts-backfill-prod"
DRY_RUN=false
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
ASSET_GROUP_FILTER=""
START_OVERRIDE=""
END_OVERRIDE=""
CHUNK_DAYS="${CHUNK_DAYS:-30}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)     DRY_RUN=true; shift ;;
    --force)       FORCE=true; shift ;;
    --env)         DEPLOYMENT_ENV="$2"; shift 2 ;;
    --asset-group) ASSET_GROUP_FILTER="$(echo "$2" | tr '[:lower:]' '[:upper:]')"; shift 2 ;;
    --start)       START_OVERRIDE="$2"; shift 2 ;;
    --end)         END_OVERRIDE="$2"; shift 2 ;;
    --chunk-days)  CHUNK_DAYS="$2"; shift 2 ;;
    --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
    --region)      AWS_REGION="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -n "${START_OVERRIDE}" || -n "${END_OVERRIDE}" ]]; then
  if [[ -z "${ASSET_GROUP_FILTER}" ]]; then
    echo "ERROR: --start/--end requires --asset-group to scope the override." >&2
    exit 1
  fi
fi

case "${AWS_REGION}" in
  ap-northeast-1) ;;
  *) echo "ERROR: region ${AWS_REGION} is not ap-northeast-1." >&2; exit 1 ;;
esac

lc_aws_validate_env "${DEPLOYMENT_ENV}"
lc_aws_resolve_account
S3_CODE_BUCKET="$(lc_aws_code_bucket "${AWS_ACCOUNT_ID}")"

if $FORCE; then
  echo "MODE: --force ON — manifest skip BYPASSED."
else
  echo "MODE: --force OFF (default) — manifest skip ACTIVE."
fi

echo "============================================================"
echo "Instruments Backfill VM Fleet Launcher — AWS EC2"
echo "  Region:   ${AWS_REGION}"
echo "  Instance: ${INSTANCE_TYPE}"
echo "  Filter:   ${ASSET_GROUP_FILTER:-all}"
echo "  Force:    ${FORCE}"
echo "  Chunk:    ${CHUNK_DAYS} days"
echo "  Env:      ${DEPLOYMENT_ENV}"
echo "  Tarball:  s3://${S3_CODE_BUCKET}/code/instruments-service-code.tar.gz"
echo "============================================================"

FORCE_FLAG=""
$FORCE && FORCE_FLAG="--force"

launch_vm() {
  local VM_NAME="$1"
  local ASSET_GROUP="$2"
  local START_DATE="$3"
  local END_DATE="$4"

  if [[ -n "${ASSET_GROUP_FILTER}" && "${ASSET_GROUP}" != "${ASSET_GROUP_FILTER}" ]]; then
    echo "  Skipping ${VM_NAME} (${ASSET_GROUP} != ${ASSET_GROUP_FILTER})"
    return 0
  fi

  if [[ -n "${START_OVERRIDE}" || -n "${END_OVERRIDE}" ]]; then
    [[ -n "${START_OVERRIDE}" ]] && START_DATE="${START_OVERRIDE}"
    [[ -n "${END_OVERRIDE}" ]] && END_DATE="${END_OVERRIDE}"
    VM_NAME="${VM_NAME}-$(echo "${END_DATE}" | tr -d '-')"
    echo "  Date-window override: ${START_DATE} → ${END_DATE} (VM: ${VM_NAME})"
  fi

  local ASSET_GROUP_LOWER
  ASSET_GROUP_LOWER="$(echo "${ASSET_GROUP}" | tr '[:upper:]' '[:lower:]')"

  echo ""
  echo "--- ${VM_NAME}: ${ASSET_GROUP} ${START_DATE} → ${END_DATE} ---"

  if $DRY_RUN; then
    echo "  [DRY RUN] Would create VM: ${VM_NAME}"
    echo "  VM_TASK=instruments-backfill  VM_ASSET_GROUP=${ASSET_GROUP}"
    return 0
  fi

  # Singleton check per VM (not fleet-level to allow parallel launches)
  lc_aws_singleton_check "${VM_NAME}" "${AWS_REGION}" "${FORCE}"

  local USER_DATA_FILE
  USER_DATA_FILE="$(mktemp /tmp/startup-instr-backfill-XXXX.sh)"

  local LOG_TRAP; LOG_TRAP="$(lc_aws_log_upload_trap_block "${VM_NAME}" "${AWS_ACCOUNT_ID}" "${ASSET_GROUP_LOWER}" "instruments-backfill")"

  cat > "${USER_DATA_FILE}" <<STARTUP_EOF
#!/bin/bash
set -euo pipefail
${LOG_TRAP}

export VM_NAME="${VM_NAME}"
export VM_TASK=instruments-backfill
export VM_SERVICE=instruments_service
export VM_ASSET_GROUP="${ASSET_GROUP}"
export MANIFEST_PER_VM_SHARDS=true
export VM_SHUTDOWN_ON_COMPLETION=true
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"
export VM_CHUNK_DAYS="${CHUNK_DAYS}"
export VM_START_DATE="${START_DATE}"
export VM_END_DATE="${END_DATE}"
export CLOUD_PROVIDER=aws
export CLOUD_MOCK_MODE=false
export AWS_DEFAULT_REGION="${AWS_REGION}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}"

echo "=== AWS Instruments Backfill VM: \${VM_NAME} ==="
date

apt-get update -qq
apt-get install -yqq curl python3 python3-pip python3-venv unzip git

if ! command -v aws &>/dev/null; then
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
  unzip -q /tmp/awscliv2.zip -d /tmp/awscliv2
  bash /tmp/awscliv2/aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli --update
  rm -rf /tmp/awscliv2*
fi

GH_PAT="\$(aws secretsmanager get-secret-value --secret-id GH_PAT --query SecretString --output text 2>/dev/null || echo '')"
if [[ -z "\${GH_PAT}" ]]; then
  echo "ERROR: GH_PAT not found in Secrets Manager" >&2
  shutdown -h now; exit 1
fi

S3_CODE_BUCKET="unified-trading-deployment-scripts-\${AWS_ACCOUNT_ID}"
WORK_DIR=/opt/backfill/instruments
mkdir -p "\${WORK_DIR}"

for entry in \
    "instruments-service-code:instruments-service" \
    "unified-api-contracts-code:unified-api-contracts" \
    "unified-trading-library-code:unified-trading-library" \
    "deployment-service-code:deployment-service"; do
  tarball_name="\${entry%%:*}"
  dir_name="\${entry##*:}"
  mkdir -p "\${WORK_DIR}/\${dir_name}"
  aws s3 cp "s3://\${S3_CODE_BUCKET}/code/\${tarball_name}.tar.gz" /tmp/\${tarball_name}.tar.gz --quiet
  tar xzf /tmp/\${tarball_name}.tar.gz -C "\${WORK_DIR}/\${dir_name}/"
  rm /tmp/\${tarball_name}.tar.gz
done

curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

WHEEL_CACHE=/tmp/wheel-cache
mkdir -p "\${WHEEL_CACHE}"
aws s3 sync "s3://\${S3_CODE_BUCKET}/wheels/py313-linux-x86_64/" "\${WHEEL_CACHE}/" --quiet 2>/dev/null || true

uv venv "\${WORK_DIR}/.venv" --python 3.13
source "\${WORK_DIR}/.venv/bin/activate"

INSTALL_ARGS="--no-sources"
for pkg in unified-api-contracts unified-trading-library instruments-service deployment-service; do
  [[ -d "\${WORK_DIR}/\${pkg}" ]] && INSTALL_ARGS="\${INSTALL_ARGS} -e \${WORK_DIR}/\${pkg}"
done
# Tarballs have no .git history, so hatch-vcs/setuptools-scm can't detect a
# version for UAC/UTL — pretend-version unblocks the editable install.
export SETUPTOOLS_SCM_PRETEND_VERSION="0.99.0"
uv pip install --find-links "\${WHEEL_CACHE}" \${INSTALL_ARGS}
uv pip install --find-links "\${WHEEL_CACHE}" pandas pyarrow boto3 2>/dev/null || true

aws s3 sync "\${WHEEL_CACHE}/" "s3://\${S3_CODE_BUCKET}/wheels/py313-linux-x86_64/" --quiet 2>/dev/null || true

echo ""
echo "=== Running instruments backfill: ${ASSET_GROUP} ${START_DATE} → ${END_DATE} ==="
date

cd "\${WORK_DIR}/instruments-service"
python3 -m instruments_service \
  --operation backfill --mode batch \
  --asset-group "${ASSET_GROUP}" \
  --start-date "${START_DATE}" --end-date "${END_DATE}" \
  --chunk-days "${CHUNK_DAYS}" \
  ${FORCE_FLAG}

echo ""
echo "=== Instruments backfill complete: \${VM_NAME} ==="
date
# Teardown owned by lc_aws_log_upload_trap_block EXIT trap (final S3 upload + EXIT_STATUS, then shutdown -h +1).
STARTUP_EOF

  local EXTRA_TAGS
  EXTRA_TAGS='[{"Key":"purpose","Value":"instruments-backfill"},{"Key":"asset-group","Value":"'"${ASSET_GROUP_LOWER}"'"},{"Key":"cloud","Value":"aws"},{"Key":"env","Value":"'"${DEPLOYMENT_ENV}"'"}]'

  echo "  Creating ${VM_NAME}..."
  local INSTANCE_ID
  INSTANCE_ID="$(lc_aws_ec2_run \
    "${VM_NAME}" \
    "${AWS_REGION}" \
    "${INSTANCE_TYPE}" \
    "${DISK_GB}" \
    "${INSTANCE_PROFILE}" \
    "${USER_DATA_FILE}" \
    "${EXTRA_TAGS}")"
  rm -f "${USER_DATA_FILE}"

  echo "  Instance ID: ${INSTANCE_ID}"
  echo "  Monitor: aws ec2 describe-instances --instance-ids ${INSTANCE_ID} --region ${AWS_REGION} --query 'Reservations[].Instances[].State.Name' --output text"
}

# VM definitions: name|asset_group|start|end  (mirrors GCP fleet with -aws suffix)
declare -a VMS=(
  "instr-backfill-cefi-1-aws|CEFI|2020-01-01|2022-06-30"
  "instr-backfill-cefi-2-aws|CEFI|2022-07-01|2024-12-31"
  "instr-backfill-cefi-3-aws|CEFI|2025-01-01|2026-02-28"
  "instr-backfill-defi-aws|DEFI|2020-01-01|2026-02-28"
  "instr-backfill-tradfi-aws|TRADFI|2020-01-01|2026-02-28"
  "instr-backfill-sports-aws|SPORTS|2020-06-01|2026-03-28"
  "instr-backfill-pred-aws|PREDICTION|2020-01-01|2026-02-28"
)

for VM_DEF in "${VMS[@]}"; do
  IFS='|' read -r VM_NAME ASSET_GROUP START_DATE END_DATE <<< "$VM_DEF"
  launch_vm "$VM_NAME" "$ASSET_GROUP" "$START_DATE" "$END_DATE"
done

echo ""
echo "============================================================"
echo "All AWS instruments VMs submitted."
echo "  Monitor: aws ec2 describe-instances --filters 'Name=tag:purpose,Values=instruments-backfill' --region ${AWS_REGION} --query 'Reservations[].Instances[].[Tags[?Key==\`Name\`]|[0].Value,State.Name]' --output table"
echo "  Logs: aws s3 ls s3://${S3_CODE_BUCKET}/vm-logs/ | grep instr-backfill"
echo "============================================================"
