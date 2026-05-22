#!/usr/bin/env bash
# AWS EC2 equivalent of launch-mtds-backfill-vm.sh.
# Launches MTDS category backfill on EC2 (ap-northeast-1) using lib/aws_ec2_launch_lib.sh.
#
# VM name:    mtds-backfill-{category_lower}-{YYYYMMDD}
# Profile:    uts-backfill-prod
# Tarball:    s3://{S3_CODE_BUCKET}/code/mtds-code.tar.gz (+ core deps)
#
# Prerequisites:
#   AWS_SECURITY_GROUP_IDS, AWS_SUBNET_ID
#   S3 code bucket populated: bash scripts/vm/create-code-tarballs.sh
#   uts-backfill-prod IAM role with S3 + Secrets Manager access
#
# Usage:
#   bash launch-mtds-backfill-vm-aws.sh --asset-group CEFI --start 2024-01-01 --end 2026-04-05
#   bash launch-mtds-backfill-vm-aws.sh --asset-group DEFI --start 2024-01-01 --end 2026-04-05 --env staging
#   bash launch-mtds-backfill-vm-aws.sh --asset-group SPORTS --dry-run
#   bash launch-mtds-backfill-vm-aws.sh --asset-group CEFI --start 2024-01-01 --end 2026-04-05 --venues BINANCE-FUTURES --data-types trades
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
ASSET_GROUP=""
START_DATE=""
END_DATE=""
TIER=""
CHUNK_SIZE="${CHUNK_SIZE:-7}"
VENUES=""
DATA_TYPES=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
VM_NAME_OVERRIDE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)       DRY_RUN=true; shift ;;
    --force)         FORCE=true; shift ;;
    --asset-group)   ASSET_GROUP="$2"; shift 2 ;;
    --start)         START_DATE="$2"; shift 2 ;;
    --end)           END_DATE="$2"; shift 2 ;;
    --tier)          TIER="$2"; shift 2 ;;
    --chunk-size)    CHUNK_SIZE="$2"; shift 2 ;;
    --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
    --vm-name)       VM_NAME_OVERRIDE="$2"; shift 2 ;;
    --venues)        VENUES="$2"; shift 2 ;;
    --data-types)    DATA_TYPES="$2"; shift 2 ;;
    --env)           DEPLOYMENT_ENV="$2"; shift 2 ;;
    --region)        AWS_REGION="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

case "${AWS_REGION}" in
  ap-northeast-1) ;;
  *) echo "ERROR: region ${AWS_REGION} is not ap-northeast-1. GCS data lives in asia-northeast1." >&2; exit 1 ;;
esac

lc_aws_validate_env "${DEPLOYMENT_ENV}"

if [[ -z "$ASSET_GROUP" ]]; then
  echo "ERROR: --asset-group is required (CEFI, DEFI, TRADFI, SPORTS, PREDICTION)" >&2
  exit 1
fi

CATEGORY_LOWER="$(echo "$ASSET_GROUP" | tr '[:upper:]' '[:lower:]')"
TODAY="$(date +%Y%m%d)"
VM_NAME="${VM_NAME_OVERRIDE:-mtds-backfill-${CATEGORY_LOWER}-${TODAY}}"
VM_PREFIX="mtds-backfill-${CATEGORY_LOWER}-"

lc_aws_resolve_account
S3_CODE_BUCKET="$(lc_aws_code_bucket "${AWS_ACCOUNT_ID}")"

echo "============================================================"
echo "MTDS Category Backfill VM Launcher — AWS EC2"
echo "  Category:  ${ASSET_GROUP}"
echo "  VM name:   ${VM_NAME}"
echo "  Region:    ${AWS_REGION}"
echo "  Instance:  ${INSTANCE_TYPE}"
echo "  Disk:      ${DISK_GB}GB"
echo "  Profile:   ${INSTANCE_PROFILE}"
echo "  Range:     ${START_DATE:-<not set>} → ${END_DATE:-<not set>}"
echo "  Tier:      ${TIER:-N/A}"
echo "  Chunk:     ${CHUNK_SIZE} days per batch"
echo "  Venues:    ${VENUES:-all}"
echo "  DataTypes: ${DATA_TYPES:-all}"
echo "  Force:     ${FORCE}"
echo "  Env:       ${DEPLOYMENT_ENV}"
echo "  Tarball:   s3://${S3_CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

# Singleton check: prevent concurrent MTDS backfills for same asset_group
lc_aws_singleton_check "${VM_PREFIX}" "${AWS_REGION}" "${FORCE}"

if $DRY_RUN; then
  echo "[DRY RUN] Would launch ${VM_NAME} — skipping EC2 create."
  exit 0
fi

USER_DATA_FILE="$(mktemp /tmp/startup-mtds-backfill-XXXX.sh)"
# shellcheck disable=SC2064
trap "rm -f '${USER_DATA_FILE}'" RETURN

cat > "${USER_DATA_FILE}" <<STARTUP_EOF
#!/bin/bash
set -euo pipefail
exec > >(tee /var/log/mtds-backfill-bootstrap.log) 2>&1

export VM_NAME="${VM_NAME}"
export VM_TASK=mtds-backfill
export VM_SERVICE=market_tick_data_service
export VM_ASSET_GROUP="${ASSET_GROUP}"
export MANIFEST_PER_VM_SHARDS=true
export VM_SHUTDOWN_ON_COMPLETION=true
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"
export VM_START_DATE="${START_DATE}"
export VM_END_DATE="${END_DATE}"
export VM_CHUNK_DAYS="${CHUNK_SIZE}"
export VM_TIER="${TIER}"
export VM_VENUES="${VENUES}"
export VM_DATA_TYPES="${DATA_TYPES}"
export VM_FORCE="${FORCE}"
export CLOUD_PROVIDER=aws
export CLOUD_MOCK_MODE=false
export AWS_DEFAULT_REGION="${AWS_REGION}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}"

echo "=== AWS MTDS Backfill VM: \${VM_NAME} ==="
date

apt-get update -qq
apt-get install -yqq curl python3 python3-pip python3-venv unzip git

# AWS CLI v2
if ! command -v aws &>/dev/null; then
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
  unzip -q /tmp/awscliv2.zip -d /tmp/awscliv2
  bash /tmp/awscliv2/aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli --update
  rm -rf /tmp/awscliv2*
fi

# GH_PAT from Secrets Manager (instance profile provides IAM auth)
GH_PAT="\$(aws secretsmanager get-secret-value --secret-id GH_PAT --query SecretString --output text 2>/dev/null || echo '')"
if [[ -z "\${GH_PAT}" ]]; then
  echo "ERROR: GH_PAT not found in Secrets Manager — check uts-backfill-prod IAM role" >&2
  shutdown -h now; exit 1
fi

S3_CODE_BUCKET="unified-trading-deployment-scripts-\${AWS_ACCOUNT_ID}"
WORK_DIR=/opt/backfill/mtds
mkdir -p "\${WORK_DIR}"

# Download and extract code tarballs
for entry in \
    "mtds-code:market-tick-data-service" \
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

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

# S3 wheel cache — skip compilation for C extensions
WHEEL_CACHE=/tmp/wheel-cache
mkdir -p "\${WHEEL_CACHE}"
aws s3 sync "s3://\${S3_CODE_BUCKET}/wheels/py313-linux-x86_64/" "\${WHEEL_CACHE}/" --quiet 2>/dev/null || true

INSTALL_ARGS="--no-sources"
for pkg in unified-api-contracts unified-trading-library market-tick-data-service deployment-service; do
  [[ -d "\${WORK_DIR}/\${pkg}" ]] && INSTALL_ARGS="\${INSTALL_ARGS} -e \${WORK_DIR}/\${pkg}"
done

cd "\${WORK_DIR}/market-tick-data-service"
uv venv "\${WORK_DIR}/.venv" --python 3.13
source "\${WORK_DIR}/.venv/bin/activate"
uv pip install --find-links "\${WHEEL_CACHE}" \${INSTALL_ARGS}
uv pip install --find-links "\${WHEEL_CACHE}" pandas pyarrow boto3 google-cloud-storage 2>/dev/null || true

# Cache newly built wheels to S3
aws s3 sync "\${WHEEL_CACHE}/" "s3://\${S3_CODE_BUCKET}/wheels/py313-linux-x86_64/" --quiet 2>/dev/null || true

echo ""
echo "=== Running MTDS backfill ==="
date

CMD="CLOUD_PROVIDER=aws CLOUD_MOCK_MODE=false python3 -m market_tick_data_service"
CMD="\${CMD} --operation download --mode batch --asset-group \${VM_ASSET_GROUP}"
[[ -n "\${VM_START_DATE}" ]] && CMD="\${CMD} --start-date \${VM_START_DATE}"
[[ -n "\${VM_END_DATE}" ]]   && CMD="\${CMD} --end-date \${VM_END_DATE}"
[[ -n "\${VM_TIER}" ]]       && CMD="\${CMD} --tier \${VM_TIER}"
[[ -n "\${VM_VENUES}" ]]     && CMD="\${CMD} --venues \${VM_VENUES}"
[[ -n "\${VM_DATA_TYPES}" ]] && CMD="\${CMD} --data-types \${VM_DATA_TYPES}"
[[ "\${VM_FORCE}" == "true" ]] && CMD="\${CMD} --force"

eval "\${CMD}"

echo ""
echo "=== MTDS backfill complete: \${VM_NAME} ==="
date
shutdown -h now
STARTUP_EOF

EXTRA_TAGS='[{"Key":"purpose","Value":"mtds-backfill"},{"Key":"asset-group","Value":"'"${CATEGORY_LOWER}"'"},{"Key":"cloud","Value":"aws"},{"Key":"env","Value":"'"${DEPLOYMENT_ENV}"'"}]'

echo "Creating ${VM_NAME} on EC2 (${INSTANCE_TYPE}, ${AWS_REGION})..."
INSTANCE_ID="$(lc_aws_ec2_run \
  "${VM_NAME}" \
  "${AWS_REGION}" \
  "${INSTANCE_TYPE}" \
  "${DISK_GB}" \
  "${INSTANCE_PROFILE}" \
  "${USER_DATA_FILE}" \
  "${EXTRA_TAGS}")"

echo ""
echo "  Instance ID: ${INSTANCE_ID}"
echo "  VM name:     ${VM_NAME}"
echo ""
echo "  Monitor:"
echo "    aws ec2 describe-instances --instance-ids ${INSTANCE_ID} --region ${AWS_REGION} --query 'Reservations[].Instances[].State.Name' --output text"
echo "    aws ssm start-session --target ${INSTANCE_ID} --region ${AWS_REGION}"
echo "  Logs: aws s3 ls s3://${S3_CODE_BUCKET}/vm-logs/${VM_NAME}/"
