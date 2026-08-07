#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# AWS EC2 equivalent of launch-defi-backfill-vm.sh.
# Launches instruments-service DeFi-targeted backfill on EC2 (ap-northeast-1).
#
# GCP counterpart: launch-defi-backfill-vm.sh (Pattern A, converted 2026-05-21)
# VM name:    instr-backfill-defi-targeted-aws[-{end_date_compact}]
# Profile:    uts-backfill-prod
# Instance:   m7i.xlarge (equiv GCP e2-standard-4)
# Tarball:    s3://{S3_CODE_BUCKET}/code/instruments-service-code.tar.gz
#
# Backfills 7 DeFi venues:
#   CURVE-AVALANCHE, CURVE-OPTIMISM, BALANCER-ETHEREUM,
#   UNISWAP_V3-ETHEREUM, UNISWAP_V3-POLYGON, RAYDIUM-SOLANA, UNISWAP_V4-ETHEREUM
#
# Usage:
#   bash launch-defi-backfill-vm-aws.sh
#   bash launch-defi-backfill-vm-aws.sh --dry-run
#   bash launch-defi-backfill-vm-aws.sh --start 2026-04-01 --end 2026-05-16
#   bash launch-defi-backfill-vm-aws.sh --force
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
START_OVERRIDE=""
END_OVERRIDE=""
CHUNK_DAYS="${CHUNK_DAYS:-30}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)       DRY_RUN=true; shift ;;
    --force)         FORCE=true; shift ;;
    --env)           DEPLOYMENT_ENV="$2"; shift 2 ;;
    --start)         START_OVERRIDE="$2"; shift 2 ;;
    --end)           END_OVERRIDE="$2"; shift 2 ;;
    --chunk-days)    CHUNK_DAYS="$2"; shift 2 ;;
    --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
    --region)        AWS_REGION="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

case "${AWS_REGION}" in
  ap-northeast-1) ;;
  *) echo "ERROR: region ${AWS_REGION} is not ap-northeast-1." >&2; exit 1 ;;
esac

lc_aws_validate_env "${DEPLOYMENT_ENV}"
lc_aws_resolve_account
S3_CODE_BUCKET="$(lc_aws_code_bucket "${AWS_ACCOUNT_ID}")"

START_DATE="2020-01-01"
END_DATE="2026-04-04"
VM_NAME="instr-backfill-defi-targeted-aws"

if [[ -n "${START_OVERRIDE}" || -n "${END_OVERRIDE}" ]]; then
  [[ -n "${START_OVERRIDE}" ]] && START_DATE="${START_OVERRIDE}"
  [[ -n "${END_OVERRIDE}" ]] && END_DATE="${END_OVERRIDE}"
  VM_NAME="instr-backfill-defi-targeted-aws-$(echo "${END_DATE}" | tr -d '-')"
  echo "  Date-window override: ${START_DATE} → ${END_DATE} (VM: ${VM_NAME})"
fi

echo "============================================================"
echo "DeFi Instruments Backfill VM Launcher — AWS EC2"
echo "  VM:       ${VM_NAME}"
echo "  Region:   ${AWS_REGION}"
echo "  Instance: ${INSTANCE_TYPE}"
echo "  Range:    ${START_DATE} → ${END_DATE}"
echo "  Chunk:    ${CHUNK_DAYS} days"
echo "  Force:    ${FORCE}"
echo "  Env:      ${DEPLOYMENT_ENV}"
echo "  Tarball:  s3://${S3_CODE_BUCKET}/code/instruments-service-code.tar.gz"
echo "============================================================"

lc_aws_singleton_check "instr-backfill-defi-targeted-aws" "${AWS_REGION}" "${FORCE}"

if $DRY_RUN; then
  echo "[DRY RUN] Would launch ${VM_NAME} on ${INSTANCE_TYPE} in ${AWS_REGION} — skipping."
  exit 0
fi

FORCE_FLAG=""
$FORCE && FORCE_FLAG="--force"

USER_DATA_FILE="$(mktemp /tmp/startup-defi-instr-XXXX.sh)"
# shellcheck disable=SC2064
trap "rm -f '${USER_DATA_FILE}'" RETURN

LOG_TRAP="$(lc_aws_log_upload_trap_block "${VM_NAME}" "${AWS_ACCOUNT_ID}" "defi" "instruments-backfill")"

cat > "${USER_DATA_FILE}" <<STARTUP_EOF
#!/bin/bash
set -euo pipefail
${LOG_TRAP}

export VM_NAME="${VM_NAME}"
export VM_TASK=instruments-backfill
export VM_SERVICE=instruments_service
export VM_ASSET_GROUP=DEFI
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

echo "=== AWS DeFi Instruments Backfill VM: \${VM_NAME} ==="
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

S3_CODE_BUCKET="uts-prod-deployment-state"
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
echo "=== Running DeFi instruments backfill ==="
date

cd "\${WORK_DIR}/instruments-service"
python3 -m instruments_service \
  --operation backfill --mode batch \
  --asset-group DEFI \
  --start-date "${START_DATE}" --end-date "${END_DATE}" \
  --chunk-days "${CHUNK_DAYS}" \
  ${FORCE_FLAG}

echo ""
echo "=== DeFi instruments backfill complete: \${VM_NAME} ==="
date
# Teardown owned by lc_aws_log_upload_trap_block EXIT trap (final S3 upload + EXIT_STATUS, then shutdown -h +1).
STARTUP_EOF

EXTRA_TAGS='[{"Key":"purpose","Value":"defi-instruments-backfill"},{"Key":"asset-group","Value":"defi"},{"Key":"cloud","Value":"aws"},{"Key":"env","Value":"'"${DEPLOYMENT_ENV}"'"}]'

echo "Creating ${VM_NAME} on EC2 (${INSTANCE_TYPE}, ${AWS_REGION})..."
INSTANCE_ID="$(lc_aws_ec2_run \
  "${VM_NAME}" \
  "${AWS_REGION}" \
  "${INSTANCE_TYPE}" \
  "${DISK_GB}" \
  "${INSTANCE_PROFILE}" \
  "${USER_DATA_FILE}" \
  "${EXTRA_TAGS}")"

echo "  Instance ID: ${INSTANCE_ID}"
echo "  Monitor: aws ec2 describe-instances --instance-ids ${INSTANCE_ID} --region ${AWS_REGION} --query 'Reservations[].Instances[].State.Name' --output text"
echo "  Logs: aws s3 ls s3://${S3_CODE_BUCKET}/vm-logs/${VM_NAME}/"
