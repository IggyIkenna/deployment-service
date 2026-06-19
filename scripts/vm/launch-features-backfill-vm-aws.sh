#!/usr/bin/env bash
# AWS EC2 features backfill launcher — consolidated features_service.
#
# GCP counterpart: launch-features-backfill-vm.sh (deprecated wrapper → launch-features-vm.sh)
# This AWS version is a standalone launcher using the consolidated features_service
# module (Phase 8A, 2026-05-08); no deprecated wrapper layer needed on AWS since
# launch-features-vm-aws.sh does not exist yet.
#
# VM name:    features-{family}-{category_lower}-backfill-{YYYYMMDD}
# Profile:    uts-backfill-prod
# Instance:   m7i.xlarge (equiv GCP e2-standard-8; features use 4–8 vCPU)
# Tarball:    s3://{S3_CODE_BUCKET}/code/features-service-code.tar.gz
#
# Viability matrix (feature_family × asset_group):
#   delta_one      : CEFI, DEFI, TRADFI, PREDICTION
#   volatility     : CEFI, TRADFI
#   onchain        : DEFI only
#   sports         : SPORTS only
#   calendar       : CEFI, TRADFI  (no --asset-group; use GLOBAL sentinel)
#   multi_timeframe: CEFI, DEFI, TRADFI
#   cross_instrument: CEFI, TRADFI, PREDICTION
#   commodity      : TRADFI only
#
# Usage:
#   bash launch-features-backfill-vm-aws.sh <feature_family> <ASSET_GROUP> <start> <end> [dry|full]
#
# Examples:
#   bash launch-features-backfill-vm-aws.sh delta_one CEFI 2020-01-01 2026-04-18 dry
#   bash launch-features-backfill-vm-aws.sh onchain   DEFI 2020-01-01 2026-04-18 full
#   bash launch-features-backfill-vm-aws.sh calendar  GLOBAL 2020-01-01 2026-04-18 full
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/aws_ec2_launch_lib.sh
source "${SCRIPT_DIR}/lib/aws_ec2_launch_lib.sh"

AWS_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
INSTANCE_TYPE="${INSTANCE_TYPE:-m7i.2xlarge}"
DISK_GB=50
INSTANCE_PROFILE="uts-backfill-prod"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FEATURE_GROUP="${FEATURE_GROUP:-ALL}"
SKIP_DEPENDENCY_CHECK="${SKIP_DEPENDENCY_CHECK:-}"
FORCE="${FORCE:-}"

FEATURE_FAMILY="${1:-}"
ASSET_GROUP="${2:-}"
START_DATE="${3:-}"
END_DATE="${4:-}"
MODE="${5:-dry}"  # dry | full

# Allow --env and --region flags after positional args
while [[ $# -gt 5 ]]; do
  case "${6:-}" in
    --env)    DEPLOYMENT_ENV="${7:-prod}"; shift 2 ;;
    --region) AWS_REGION="${7:-ap-northeast-1}"; shift 2 ;;
    *) break ;;
  esac
done

case "${AWS_REGION}" in
  ap-northeast-1) ;;
  *) echo "ERROR: region ${AWS_REGION} is not ap-northeast-1." >&2; exit 1 ;;
esac

lc_aws_validate_env "${DEPLOYMENT_ENV}"

if [[ -z "$FEATURE_FAMILY" || -z "$ASSET_GROUP" || -z "$START_DATE" || -z "$END_DATE" ]]; then
  cat <<EOF
Usage: $0 <feature_family> <ASSET_GROUP> <start-date> <end-date> [dry|full]

feature_family ∈ {
  delta_one, volatility, onchain, sports,
  calendar, multi_timeframe, cross_instrument, commodity
}
ASSET_GROUP ∈ { CEFI, DEFI, TRADFI, SPORTS, PREDICTION, GLOBAL }
EOF
  exit 2
fi

# Viability check
_is_viable_cell() {
  local fam="$1" ag="$2"
  case "${fam}/${ag}" in
    delta_one/CEFI|delta_one/DEFI|delta_one/TRADFI|delta_one/PREDICTION) return 0 ;;
    volatility/CEFI|volatility/TRADFI) return 0 ;;
    onchain/DEFI) return 0 ;;
    sports/SPORTS) return 0 ;;
    calendar/CEFI|calendar/TRADFI|calendar/GLOBAL) return 0 ;;
    multi_timeframe/CEFI|multi_timeframe/DEFI|multi_timeframe/TRADFI) return 0 ;;
    cross_instrument/CEFI|cross_instrument/TRADFI|cross_instrument/PREDICTION) return 0 ;;
    commodity/TRADFI) return 0 ;;
    *) return 1 ;;
  esac
}

if ! _is_viable_cell "${FEATURE_FAMILY}" "${ASSET_GROUP}"; then
  echo "Not a viable (feature_family × asset_group) cell: ${FEATURE_FAMILY} × ${ASSET_GROUP}" >&2
  echo "See header for the viable matrix." >&2
  exit 2
fi

CATEGORY_LOWER="$(echo "${ASSET_GROUP}" | tr '[:upper:]' '[:lower:]')"
TODAY="$(date +%Y%m%d)"
VM_NAME="features-${FEATURE_FAMILY//_/-}-${CATEGORY_LOWER}-backfill-${TODAY}"
VM_PREFIX="features-${FEATURE_FAMILY//_/-}-${CATEGORY_LOWER}-backfill-"

lc_aws_resolve_account
S3_CODE_BUCKET="$(lc_aws_code_bucket "${AWS_ACCOUNT_ID}")"

echo "============================================================"
echo "Features Backfill VM Launcher — AWS EC2 (consolidated features_service)"
echo "  Feature family: ${FEATURE_FAMILY}"
echo "  Asset group:    ${ASSET_GROUP}"
echo "  VM name:        ${VM_NAME}"
echo "  Region:         ${AWS_REGION}"
echo "  Instance:       ${INSTANCE_TYPE}"
echo "  Range:          ${START_DATE} → ${END_DATE} (${MODE})"
echo "  Feature group:  ${FEATURE_GROUP}"
echo "  Env:            ${DEPLOYMENT_ENV}"
echo "  Tarball:        s3://${S3_CODE_BUCKET}/code/features-service-code.tar.gz"
echo "============================================================"

lc_aws_singleton_check "${VM_PREFIX}" "${AWS_REGION}" "false"

USER_DATA_FILE="$(mktemp /tmp/startup-features-backfill-XXXX.sh)"
# shellcheck disable=SC2064
trap "rm -f '${USER_DATA_FILE}'" RETURN

# Build the CMD for use inside user-data (substituted at launcher time)
BUILD_CMD="python -m features_service --feature-family ${FEATURE_FAMILY}"
BUILD_CMD="${BUILD_CMD} --operation compute --mode batch"
BUILD_CMD="${BUILD_CMD} --start-date ${START_DATE} --end-date ${END_DATE}"
# calendar does not accept --asset-group (GLOBAL is the sentinel)
case "${FEATURE_FAMILY}" in
  calendar) ;;
  *) BUILD_CMD="${BUILD_CMD} --asset-group ${ASSET_GROUP}" ;;
esac
case "${FEATURE_FAMILY}" in
  delta_one|volatility|onchain) BUILD_CMD="${BUILD_CMD} --feature-group ${FEATURE_GROUP}" ;;
esac
[[ -n "${SKIP_DEPENDENCY_CHECK}" ]] && BUILD_CMD="${BUILD_CMD} --skip-dependency-check"
[[ -n "${FORCE}" ]]                 && BUILD_CMD="${BUILD_CMD} --force"
[[ "${MODE}" == "dry" ]]            && BUILD_CMD="${BUILD_CMD} --dry-run"

LOG_TRAP="$(lc_aws_log_upload_trap_block "${VM_NAME}" "${AWS_ACCOUNT_ID}" "${CATEGORY_LOWER}" "features-backfill")"

cat > "${USER_DATA_FILE}" <<STARTUP_EOF
#!/bin/bash
set -euo pipefail
${LOG_TRAP}

export VM_NAME="${VM_NAME}"
export VM_TASK=features-backfill
export VM_SERVICE=features_service
export VM_ASSET_GROUP="${ASSET_GROUP}"
export VM_OPERATION=backfill-${FEATURE_FAMILY}-${CATEGORY_LOWER}
export VM_START_DATE="${START_DATE}"
export VM_END_DATE="${END_DATE}"
export VM_BACKFILL_MODE="${MODE}"
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"
export VM_SHUTDOWN_ON_COMPLETION=true
export CLOUD_PROVIDER=aws
export CLOUD_MOCK_MODE=false
export AWS_DEFAULT_REGION="${AWS_REGION}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}"

echo "=== AWS Features Backfill VM: \${VM_NAME} ==="
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
WORK_DIR=/opt/backfill/features
mkdir -p "\${WORK_DIR}"

for entry in \
    "features-service-code:features-service" \
    "unified-api-contracts-code:unified-api-contracts" \
    "unified-trading-library-code:unified-trading-library" \
    "mtds-code:market-tick-data-service" \
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
for pkg in unified-api-contracts unified-trading-library market-tick-data-service features-service deployment-service; do
  [[ -d "\${WORK_DIR}/\${pkg}" ]] && INSTALL_ARGS="\${INSTALL_ARGS} -e \${WORK_DIR}/\${pkg}"
done
uv pip install --find-links "\${WHEEL_CACHE}" \${INSTALL_ARGS}
uv pip install --find-links "\${WHEEL_CACHE}" pandas pyarrow boto3 2>/dev/null || true

aws s3 sync "\${WHEEL_CACHE}/" "s3://\${S3_CODE_BUCKET}/wheels/py313-linux-x86_64/" --quiet 2>/dev/null || true

echo ""
echo "=== Running features backfill: ${FEATURE_FAMILY} × ${ASSET_GROUP} ==="
date
echo "CMD: ${BUILD_CMD}"

cd "\${WORK_DIR}/features-service"
CLOUD_PROVIDER=aws CLOUD_MOCK_MODE=false ${BUILD_CMD}

echo ""
echo "=== Features backfill complete: \${VM_NAME} ==="
date
# Teardown owned by lc_aws_log_upload_trap_block EXIT trap (final S3 upload + EXIT_STATUS, then shutdown -h +1).
STARTUP_EOF

EXTRA_TAGS='[{"Key":"purpose","Value":"features-backfill"},{"Key":"feature-family","Value":"'"${FEATURE_FAMILY}"'"},{"Key":"asset-group","Value":"'"${CATEGORY_LOWER}"'"},{"Key":"cloud","Value":"aws"},{"Key":"env","Value":"'"${DEPLOYMENT_ENV}"'"}]'

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
