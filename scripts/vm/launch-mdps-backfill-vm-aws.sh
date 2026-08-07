#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# AWS EC2 equivalent of launch-mdps-backfill-vm.sh.
# Launches MDPS (candle re-aggregation) backfill on EC2 (ap-northeast-1).
#
# VM name:    mdps-backfill-{cat}-{YYYYMMDD}
# Profile:    uts-backfill-prod
# Instance:   m7i.2xlarge (MDPS is CPU-bound; matches GCP e2-standard-8)
# Tarball:    s3://{S3_CODE_BUCKET}/code/market-data-processing-service-code.tar.gz
#
# MDPS CLI quirk: uses MDPS_ASSET_GROUP env var + legacy bridge; --asset-group
# is NOT a top-level arg. The bridge in cli/main.py reads MDPS_ASSET_GROUP and
# translates to --CEFI / --DEFI / etc.
#
# Usage:
#   bash launch-mdps-backfill-vm-aws.sh [--env prod|staging|dev] <cefi|tradfi|defi|sports|prediction|all> <start-date> <end-date> [dry|full]
#
# Examples:
#   bash launch-mdps-backfill-vm-aws.sh cefi  2020-01-01 2026-04-18 dry
#   bash launch-mdps-backfill-vm-aws.sh defi  2020-01-01 2026-04-18 full
#   bash launch-mdps-backfill-vm-aws.sh all   2020-01-01 2026-04-18 full
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/aws_ec2_launch_lib.sh
source "${SCRIPT_DIR}/lib/aws_ec2_launch_lib.sh"

AWS_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
# MDPS is CPU-bound (candle aggregation); m7i.2xlarge = 8 vCPU / 32GB matches GCP e2-standard-8
INSTANCE_TYPE="${INSTANCE_TYPE:-m7i.2xlarge}"
DISK_GB=50
INSTANCE_PROFILE="uts-backfill-prod"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# Pre-parse --env before positional args (mirrors GCP counterpart)
POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "${1:-}" in
    --env)    DEPLOYMENT_ENV="$2"; shift 2 ;;
    --region) AWS_REGION="$2"; shift 2 ;;
    *)        POSITIONAL+=("$1"); shift ;;
  esac
done
set -- "${POSITIONAL[@]:-}"

ASSET_GROUP="${1:-}"
START_DATE="${2:-}"
END_DATE="${3:-}"
MODE="${4:-dry}"  # dry | full

case "${AWS_REGION}" in
  ap-northeast-1) ;;
  *) echo "ERROR: region ${AWS_REGION} is not ap-northeast-1." >&2; exit 1 ;;
esac

lc_aws_validate_env "${DEPLOYMENT_ENV}"

if [[ -z "$ASSET_GROUP" || -z "$START_DATE" || -z "$END_DATE" ]]; then
  echo "Usage: $0 [--env prod|staging|dev] <cefi|tradfi|defi|sports|prediction|all> <start-date> <end-date> [dry|full]"
  exit 2
fi

lc_aws_resolve_account
S3_CODE_BUCKET="$(lc_aws_code_bucket "${AWS_ACCOUNT_ID}")"
TODAY="$(date +%Y%m%d)"

_category_upper_for() {
  case "$1" in
    cefi)       echo "CEFI" ;;
    tradfi)     echo "TRADFI" ;;
    defi)       echo "DEFI" ;;
    sports)     echo "SPORTS" ;;
    prediction) echo "PREDICTION" ;;
    *) echo ""; return 1 ;;
  esac
}

_launch() {
  local cat="$1"
  local cat_upper; cat_upper="$(_category_upper_for "$cat")" || { echo "Unknown category: $cat"; return 1; }
  local vm_name="mdps-backfill-${cat}-${TODAY}"
  local vm_prefix="mdps-backfill-${cat}-"

  echo ""
  echo "=== MDPS backfill: ${cat_upper} ${START_DATE} → ${END_DATE} (${MODE}) ==="

  lc_aws_singleton_check "${vm_prefix}" "${AWS_REGION}" "false"

  local USER_DATA_FILE
  USER_DATA_FILE="$(mktemp /tmp/startup-mdps-backfill-XXXX.sh)"
  # shellcheck disable=SC2064
  trap "rm -f '${USER_DATA_FILE}'" RETURN

  local SKIP_DEP=""
  [[ "$cat" == "sports" ]] && SKIP_DEP="--skip-dependency-check"

  local DRY_FLAG=""
  [[ "$MODE" == "dry" ]] && DRY_FLAG="--dry-run"

  local LOG_TRAP
  LOG_TRAP="$(lc_aws_log_upload_trap_block "${vm_name}" "${AWS_ACCOUNT_ID}" "${cat}" "mdps-backfill")"

  cat > "${USER_DATA_FILE}" <<STARTUP_EOF
#!/bin/bash
set -euo pipefail
${LOG_TRAP}

export VM_NAME="${vm_name}"
export VM_TASK=mdps-backfill
export VM_SERVICE=market_data_processing_service
export VM_ASSET_GROUP="${cat_upper}"
export VM_OPERATION=backfill-${cat}
export VM_START_DATE="${START_DATE}"
export VM_END_DATE="${END_DATE}"
export VM_BACKFILL_MODE="${MODE}"
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"
export VM_SHUTDOWN_ON_COMPLETION=true
export CLOUD_PROVIDER=aws
export CLOUD_MOCK_MODE=false
export AWS_DEFAULT_REGION="${AWS_REGION}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}"

echo "=== AWS MDPS Backfill VM: \${VM_NAME} ==="
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

GH_PAT="\$(aws secretsmanager get-secret-value --secret-id GH_PAT --query SecretString --output text 2>/dev/null || echo '')"
if [[ -z "\${GH_PAT}" ]]; then
  echo "ERROR: GH_PAT not found in Secrets Manager" >&2
  shutdown -h now; exit 1
fi

S3_CODE_BUCKET="uts-prod-deployment-state"
WORK_DIR=/opt/backfill/mdps
mkdir -p "\${WORK_DIR}"

for entry in \
    "market-data-processing-service-code:market-data-processing-service" \
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

curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

WHEEL_CACHE=/tmp/wheel-cache
mkdir -p "\${WHEEL_CACHE}"
aws s3 sync "s3://\${S3_CODE_BUCKET}/wheels/py313-linux-x86_64/" "\${WHEEL_CACHE}/" --quiet 2>/dev/null || true

uv venv "\${WORK_DIR}/.venv" --python 3.13
source "\${WORK_DIR}/.venv/bin/activate"

INSTALL_ARGS="--no-sources"
for pkg in unified-api-contracts unified-trading-library market-tick-data-service market-data-processing-service deployment-service; do
  [[ -d "\${WORK_DIR}/\${pkg}" ]] && INSTALL_ARGS="\${INSTALL_ARGS} -e \${WORK_DIR}/\${pkg}"
done
# Tarballs have no .git history, so hatch-vcs/setuptools-scm can't detect a
# version for UAC/UTL — pretend-version unblocks the editable install.
export SETUPTOOLS_SCM_PRETEND_VERSION="0.99.0"
uv pip install --find-links "\${WHEEL_CACHE}" \${INSTALL_ARGS}
uv pip install --find-links "\${WHEEL_CACHE}" pandas pyarrow boto3 2>/dev/null || true

aws s3 sync "\${WHEEL_CACHE}/" "s3://\${S3_CODE_BUCKET}/wheels/py313-linux-x86_64/" --quiet 2>/dev/null || true

echo ""
echo "=== Running MDPS backfill: ${cat_upper} ${START_DATE} → ${END_DATE} (${MODE}) ==="
date

cd "\${WORK_DIR}/market-data-processing-service"

MDPS_ASSET_GROUP=${cat_upper} \
  python3 -m market_data_processing_service \
  --operation process --mode batch \
  --start-date "${START_DATE}" --end-date "${END_DATE}" \
  ${SKIP_DEP} ${DRY_FLAG}

echo ""
echo "=== MDPS backfill complete: \${VM_NAME} ==="
date
# Teardown owned by lc_aws_log_upload_trap_block EXIT trap (final S3 upload + EXIT_STATUS, then shutdown -h +1).
STARTUP_EOF

  local EXTRA_TAGS
  EXTRA_TAGS='[{"Key":"purpose","Value":"mdps-backfill"},{"Key":"asset-group","Value":"'"${cat}"'"},{"Key":"mode","Value":"'"${MODE}"'"},{"Key":"cloud","Value":"aws"},{"Key":"env","Value":"'"${DEPLOYMENT_ENV}"'"}]'

  echo "Creating ${vm_name} on EC2 (${INSTANCE_TYPE}, ${AWS_REGION})..."
  local INSTANCE_ID
  INSTANCE_ID="$(lc_aws_ec2_run \
    "${vm_name}" \
    "${AWS_REGION}" \
    "${INSTANCE_TYPE}" \
    "${DISK_GB}" \
    "${INSTANCE_PROFILE}" \
    "${USER_DATA_FILE}" \
    "${EXTRA_TAGS}")"

  echo "  Instance ID: ${INSTANCE_ID}"
  echo "  Monitor: aws ec2 describe-instances --instance-ids ${INSTANCE_ID} --region ${AWS_REGION} --query 'Reservations[].Instances[].State.Name' --output text"
  echo "  Logs: aws s3 ls s3://${S3_CODE_BUCKET}/vm-logs/${vm_name}/"
}

case "$ASSET_GROUP" in
  cefi|tradfi|defi|sports|prediction) _launch "$ASSET_GROUP" ;;
  all)
    _launch cefi
    _launch tradfi
    _launch defi
    _launch sports
    _launch prediction
    ;;
  *) echo "Unknown category: $ASSET_GROUP"; exit 2 ;;
esac

echo ""
echo "Run timestamp: ${TODAY}"
echo "Mode: ${MODE} (dry = --dry-run; full = live writes)"
