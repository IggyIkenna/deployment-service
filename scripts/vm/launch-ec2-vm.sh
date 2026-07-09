#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# AWS EC2 VM launcher — master launcher for all UTS workloads.
#
# AWS twin of the GCP launch-*-vm.sh family.
# Rather than 80+ individual scripts (one per GCP launcher), this master script
# dispatches by --task, mirroring each GCP launcher's workload config.
#
# Pre-requisites:
#   1. aws sso login --profile operator  (or AWS_PROFILE set + credentials valid)
#   2. AWS_SECURITY_GROUP_IDS — security group(s) for the EC2 instance
#   3. AWS_SUBNET_ID — subnet in ap-northeast-1 for the instance
#   4. IAM instance profiles from 1.B (setup-iam-roles.sh --apply): BLOCKED-AWS-PERMISSIONS
#      pending iam:CreateRole. Scripts will launch without instance profile if not yet created;
#      workloads that need S3 write access will fail at runtime without it.
#
# Usage:
#   bash launch-ec2-vm.sh --task mtds-backfill --asset-group CEFI --start 2025-01-01 --end 2025-01-08
#   bash launch-ec2-vm.sh --task features-backfill --asset-group DEFI --start 2025-01-01 --end 2025-01-08 --dry-run
#   bash launch-ec2-vm.sh --list-tasks           # list all available tasks
#
# Task names mirror GCP launcher names (drop 'launch-' prefix and '-vm' suffix):
#   GCP: launch-mtds-backfill-vm.sh  →  AWS: --task mtds-backfill
#   GCP: launch-strategy-live-vm.sh  →  AWS: --task strategy-live
#
# Environment variables:
#   AWS_DEFAULT_REGION      (default: ap-northeast-1)
#   AWS_ACCOUNT_ID          (auto-resolved via sts get-caller-identity if unset)
#   AWS_SECURITY_GROUP_IDS  (required — space-separated SG IDs, e.g. "sg-abc123")
#   AWS_SUBNET_ID           (required — subnet in ap-northeast-1)
#   AWS_KEY_PAIR_NAME       (optional — for SSH access)
#   DEPLOYMENT_ENV          (default: prod)
#
# SSOT: codex/05-infrastructure/vm-tarball-deployment.md
# Plan: api_keys_wallets_accounts_readiness_2026_05_10.md Phase 1.G
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/aws_ec2_launch_lib.sh
source "${SCRIPT_DIR}/lib/aws_ec2_launch_lib.sh"

AWS_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false
FORCE=false
TASK=""
ASSET_GROUP=""
START_DATE=""
END_DATE=""
MACHINE_TYPE="${MACHINE_TYPE:-m7i.xlarge}"    # 4 vCPU / 16 GB — matches GCP e2-standard-4
DISK_GB=50
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
VM_NAME_OVERRIDE=""
CHUNK_SIZE=7
VENUES=""
DATA_TYPES=""

usage() {
    cat <<EOF
Usage: bash launch-ec2-vm.sh [--task <name>] [options]

Required (or --list-tasks):
  --task <name>        Workload task (see --list-tasks for all names)

Common options:
  --asset-group AG     Asset group: CEFI, DEFI, TRADFI, SPORTS, PREDICTION
  --start YYYY-MM-DD   Backfill start date
  --end YYYY-MM-DD     Backfill end date
  --env ENV            prod|staging|dev  (default: $DEPLOYMENT_ENV)
  --dry-run            Print what would happen, do not launch
  --force              Override singleton check
  --machine-type TYPE  EC2 instance type (default: $MACHINE_TYPE)
  --vm-name NAME       Override VM name
  --chunk-size N       Days per batch (default: $CHUNK_SIZE)
  --venues V           Comma-separated venues filter
  --data-types DT      Comma-separated data-types filter
  --list-tasks         List all available task names
  --workspace PATH     Workspace root (default: auto-detected)

Environment (required before launch):
  AWS_SECURITY_GROUP_IDS  space-separated SG IDs
  AWS_SUBNET_ID           subnet in ap-northeast-1
  AWS_KEY_PAIR_NAME       optional, for SSH

NOTE: Instance profiles (IAM roles) require 1.B to be resolved first.
      See api_keys_wallets_accounts_readiness_2026_05_10.md § 1.B.
EOF
}

# ---------------------------------------------------------------------------
# Task registry — maps task name → (service, vm_prefix, instance_profile, repos[])
# ---------------------------------------------------------------------------
# Each entry: TASK_SERVICE, TASK_VM_PREFIX, TASK_INSTANCE_PROFILE, TASK_REPOS
# TASK_REPOS: space-separated repo names to package into the tarball.

declare -A TASK_SERVICE TASK_VM_PREFIX TASK_INSTANCE_PROFILE

_register() {
    local task="$1" service="$2" prefix="$3" profile="$4"
    TASK_SERVICE["$task"]="$service"
    TASK_VM_PREFIX["$task"]="$prefix"
    TASK_INSTANCE_PROFILE["$task"]="$profile"
}

# Market Tick Data Service (MTDS)
_register "mtds-backfill"                  "market-tick-data-service"   "mtds-backfill-"           "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-dex-pools-backfill"        "market-tick-data-service"   "mtds-dex-pools-"          "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-eigenlayer-rewards-backfill" "market-tick-data-service" "mtds-eigenlayer-"         "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-gas-fees-backfill"         "market-tick-data-service"   "mtds-gas-fees-"           "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-gas-fees-fleet"            "market-tick-data-service"   "mtds-gas-fleet-"          "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-lending-indices-backfill"  "market-tick-data-service"   "mtds-lend-idx-"           "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-liquidations-backfill"     "market-tick-data-service"   "mtds-liq-"                "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-lst-rates-backfill"        "market-tick-data-service"   "mtds-lst-"                "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-perp-funding-backfill"     "market-tick-data-service"   "mtds-perp-"               "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-prediction-backfill"       "market-tick-data-service"   "mtds-pred-"               "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-pyth-archive-backfill"     "market-tick-data-service"   "mtds-pyth-arch-"          "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-pyth-lst-backfill"         "market-tick-data-service"   "mtds-pyth-lst-"           "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-solana-drift-backfill"     "market-tick-data-service"   "mtds-sol-drift-"          "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-solana-gas-backfill"       "market-tick-data-service"   "mtds-sol-gas-"            "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-sports-odds-backfill"      "market-tick-data-service"   "mtds-sports-odds-"        "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "mtds-vault-share-price-backfill" "market-tick-data-service"  "mtds-vault-"              "uts-market-tick-data-service-${DEPLOYMENT_ENV}"

# Features Service
_register "features-backfill"              "features-service"           "feats-backfill-"          "uts-features-service-${DEPLOYMENT_ENV}"
_register "features"                       "features-service"           "feats-live-"              "uts-features-service-${DEPLOYMENT_ENV}"
_register "features-onchain-backfill"      "features-service"           "feats-onchain-"           "uts-features-service-${DEPLOYMENT_ENV}"
_register "features-sports-backfill"       "features-service"           "feats-sports-"            "uts-features-service-${DEPLOYMENT_ENV}"
_register "features-sports-parallel-backfill" "features-service"        "feats-sports-par-"        "uts-features-service-${DEPLOYMENT_ENV}"
_register "sfi-backfill"                   "features-service"           "feats-sfi-"               "uts-features-service-${DEPLOYMENT_ENV}"
_register "sfi-progressive-features-backfill" "features-service"        "feats-sfi-prog-"          "uts-features-service-${DEPLOYMENT_ENV}"

# Strategy Service
_register "strategy-live"                  "strategy-service"           "strat-live-"              "uts-strategy-service-${DEPLOYMENT_ENV}"
_register "strategy-paper"                 "strategy-service"           "strat-paper-"             "uts-strategy-service-${DEPLOYMENT_ENV}"
_register "strategy-test"                  "strategy-service"           "strat-test-"              "uts-strategy-service-${DEPLOYMENT_ENV}"
_register "strategy-backtest-grid"         "strategy-service"           "strat-grid-"              "uts-strategy-service-${DEPLOYMENT_ENV}"
_register "defi-backtest"                  "strategy-service"           "defi-backtest-"           "uts-strategy-service-${DEPLOYMENT_ENV}"
_register "defi-paper-trading"             "strategy-service"           "defi-paper-"              "uts-strategy-service-${DEPLOYMENT_ENV}"
_register "execution-alpha"                "execution-service"          "exec-alpha-"              "uts-execution-service-${DEPLOYMENT_ENV}"
_register "defi-recursive-borrow"          "execution-service"          "defi-recurse-"            "uts-execution-service-${DEPLOYMENT_ENV}"
_register "scenario-runner"                "execution-service"          "scenario-"                "uts-execution-service-${DEPLOYMENT_ENV}"

# Instruments Service
_register "instruments-backfill"           "instruments-service"        "instr-backfill-"          "uts-instruments-service-${DEPLOYMENT_ENV}"
_register "instruments-smoke"              "instruments-service"        "instr-smoke-"             "uts-instruments-service-${DEPLOYMENT_ENV}"
_register "expected-universe-v2"           "instruments-service"        "eu-v2-"                   "uts-instruments-service-${DEPLOYMENT_ENV}"
_register "sports-instruments-reference"   "instruments-service"        "sports-instr-ref-"        "uts-instruments-service-${DEPLOYMENT_ENV}"

# MDPS
_register "mdps-backfill"                  "market-data-processing-service" "mdps-backfill-"       "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "mdps-sports-bucket"             "market-data-processing-service" "mdps-sports-"         "uts-deployment-service-${DEPLOYMENT_ENV}"

# TradFi
_register "tradfi-backfill"                "market-tick-data-service"   "tradfi-backfill-"         "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "tradfi-session-stamps"          "instruments-service"        "tradfi-sess-"             "uts-instruments-service-${DEPLOYMENT_ENV}"
_register "tradfi-session-stamp"           "instruments-service"        "tradfi-stamp-"            "uts-instruments-service-${DEPLOYMENT_ENV}"

# Sports
_register "sports-entity-sweep"            "features-service"           "sports-ent-"              "uts-features-service-${DEPLOYMENT_ENV}"
_register "sports-full-sweep"              "features-service"           "sports-full-"             "uts-features-service-${DEPLOYMENT_ENV}"
_register "sports-manifest-rescan"         "instruments-service"        "sports-mfst-"             "uts-instruments-service-${DEPLOYMENT_ENV}"
_register "sports-scheduler"               "features-service"           "sports-sched-"            "uts-features-service-${DEPLOYMENT_ENV}"
_register "api-football-backfill"          "features-service"           "api-fb-"                  "uts-features-service-${DEPLOYMENT_ENV}"
_register "footystats-backfill"            "features-service"           "footystats-"              "uts-features-service-${DEPLOYMENT_ENV}"
_register "fill-missing-player-stats"      "features-service"           "fill-player-"             "uts-features-service-${DEPLOYMENT_ENV}"
_register "fixtures-recovery"              "features-service"           "fix-recov-"               "uts-features-service-${DEPLOYMENT_ENV}"
_register "fixtures-truthset-audit"        "features-service"           "fix-audit-"               "uts-features-service-${DEPLOYMENT_ENV}"
_register "transfermarkt-backfill"         "features-service"           "transfermkt-"             "uts-features-service-${DEPLOYMENT_ENV}"
_register "understat-backfill"             "features-service"           "understat-"               "uts-features-service-${DEPLOYMENT_ENV}"
_register "openmeteo-backfill"             "features-service"           "openmeteo-"               "uts-features-service-${DEPLOYMENT_ENV}"

# Prediction
_register "prediction-features"            "features-service"           "pred-feats-"              "uts-features-service-${DEPLOYMENT_ENV}"
_register "prediction-pipeline"            "features-service"           "pred-pipe-"               "uts-features-service-${DEPLOYMENT_ENV}"

# DeFi / blockchain-specific backfills
_register "jito-solana-backfill"           "market-tick-data-service"   "jito-sol-"                "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "marinade-solana-backfill"       "market-tick-data-service"   "marinade-sol-"            "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "aave-lending-rate-validation"   "execution-service"          "aave-val-"                "uts-execution-service-${DEPLOYMENT_ENV}"
_register "amm-golden-fixture-validation"  "execution-service"          "amm-val-"                 "uts-execution-service-${DEPLOYMENT_ENV}"
_register "defi-backfill"                  "market-tick-data-service"   "defi-backfill-"           "uts-market-tick-data-service-${DEPLOYMENT_ENV}"
_register "defi-phantom-recon"             "instruments-service"        "defi-phantom-"            "uts-instruments-service-${DEPLOYMENT_ENV}"

# ML
_register "ml-training"                    "ml-service"                 "ml-train-"                "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "synthetic-benchmark"            "ml-service"                 "synth-bench-"             "uts-deployment-service-${DEPLOYMENT_ENV}"

# Infrastructure / ops
# NOTE 2026-05-20: manifest-consolidator on AWS EC2 is DEPRECATED. Cloud Run + Cloud Scheduler is
# the canonical path (GCP). AWS-side consolidation, if needed, must be rebuilt as Lambda+EventBridge
# (separate plan; not currently in scope). SSOT: codex/05-infrastructure/manifest-consolidator-ssot.md.
# Keeping the _register entry stubbed for now so existing automation doesn't break on lookup;
# launching it WILL be a no-op once the AMI is rebuilt without the manifest-consolidator entrypoint.
_register "manifest-consolidator"          "instruments-service"        "manifest-cons-"           "uts-instruments-service-${DEPLOYMENT_ENV}"  # DEPRECATED — use Cloud Run on GCP
_register "manifest-recon-all"             "instruments-service"        "manifest-recon-"          "uts-instruments-service-${DEPLOYMENT_ENV}"
_register "manifest-recon-apply"           "instruments-service"        "manifest-apply-"          "uts-instruments-service-${DEPLOYMENT_ENV}"
_register "governance-backfill"            "deployment-service"         "gov-backfill-"            "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "qg-snapshot"                    "deployment-service"         "qg-snap-"                 "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "canonical-migration"            "deployment-service"         "canon-migr-"              "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "canonical-smoke"                "deployment-service"         "canon-smoke-"             "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "cefi-migration"                 "deployment-service"         "cefi-migr-"               "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "gcs-migration-bundle"           "deployment-service"         "gcs-migr-"                "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "batch-live-recon-cron"          "deployment-service"         "bl-recon-"                "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "blank-reason-recon"             "deployment-service"         "blank-recon-"             "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "bucket-rsync"                   "deployment-service"         "bucket-rsync-"            "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "cross-asset-rescan"             "deployment-service"         "cross-rescan-"            "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "honest-coverage"                "deployment-service"         "honest-cov-"              "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "measure-honest-coverage"        "deployment-service"         "measure-hcov-"            "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "dashboard"                      "deployment-service"         "dashboard-"               "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "disaster-drill-cron"            "deployment-service"         "dr-drill-cron-"           "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "dr-drill-cutover"               "deployment-service"         "dr-cutover-"              "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "client-reporting-cutover"       "deployment-service"         "client-rep-"              "uts-deployment-service-${DEPLOYMENT_ENV}"
_register "wallet-treasury-cutover"        "deployment-service"         "wallet-treas-"            "uts-deployment-service-${DEPLOYMENT_ENV}"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
LIST_TASKS=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --task)          TASK="$2"; shift 2 ;;
        --asset-group)   ASSET_GROUP="$2"; shift 2 ;;
        --start)         START_DATE="$2"; shift 2 ;;
        --end)           END_DATE="$2"; shift 2 ;;
        --env)           DEPLOYMENT_ENV="$2"; shift 2 ;;
        --dry-run)       DRY_RUN=true; shift ;;
        --force)         FORCE=true; shift ;;
        --machine-type)  MACHINE_TYPE="$2"; shift 2 ;;
        --vm-name)       VM_NAME_OVERRIDE="$2"; shift 2 ;;
        --chunk-size)    CHUNK_SIZE="$2"; shift 2 ;;
        --venues)        VENUES="$2"; shift 2 ;;
        --data-types)    DATA_TYPES="$2"; shift 2 ;;
        --workspace)     WORKSPACE_ROOT="$2"; shift 2 ;;
        --region)        AWS_REGION="$2"; shift 2 ;;
        --list-tasks)    LIST_TASKS=true; shift ;;
        -h|--help)       usage; exit 0 ;;
        *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
    esac
done

if $LIST_TASKS; then
    echo "Available --task values (maps to GCP launch-<task>-vm.sh):"
    for t in $(echo "${!TASK_SERVICE[@]}" | tr ' ' '\n' | sort); do
        printf "  %-45s  service: %s\n" "$t" "${TASK_SERVICE[$t]}"
    done
    exit 0
fi

if [[ -z "$TASK" ]]; then
    echo "ERROR: --task is required. Run with --list-tasks to see all task names." >&2
    usage
    exit 1
fi

if [[ -z "${TASK_SERVICE[$TASK]:-}" ]]; then
    echo "ERROR: Unknown task '${TASK}'. Run with --list-tasks to see all task names." >&2
    exit 1
fi

lc_aws_validate_env "$DEPLOYMENT_ENV"

# Resolve account
lc_aws_resolve_account

SERVICE="${TASK_SERVICE[$TASK]}"
VM_PREFIX="${TASK_VM_PREFIX[$TASK]}"
INSTANCE_PROFILE="${TASK_INSTANCE_PROFILE[$TASK]}"
TS="$(lc_aws_run_ts)"
VM_NAME="${VM_NAME_OVERRIDE:-${VM_PREFIX}${TS}}"
CODE_BUCKET="$(lc_aws_code_bucket "$AWS_ACCOUNT_ID")"
S3_STAGING="s3://${CODE_BUCKET}/_vm_staging/${TASK}"
TARBALL_NAME="${TASK}_codebase.tar.gz"
TARBALL_PATH="$(mktemp /tmp/${TASK}-XXXX.tar.gz)"

echo "============================================================"
echo "AWS EC2 VM Launcher"
echo "  Task:          ${TASK}"
echo "  Service:       ${SERVICE}"
echo "  AssetGroup:    ${ASSET_GROUP:-N/A}"
echo "  Region:        ${AWS_REGION}"
echo "  InstanceType:  ${MACHINE_TYPE}"
echo "  DiskGB:        ${DISK_GB}"
echo "  Env:           ${DEPLOYMENT_ENV}"
echo "  Range:         ${START_DATE:-N/A} → ${END_DATE:-N/A}"
echo "  VM:            ${VM_NAME}"
echo "  S3 staging:    ${S3_STAGING}"
echo "  Profile:       ${INSTANCE_PROFILE:-NONE (1.B pending)}"
echo "  Account:       ${AWS_ACCOUNT_ID}"
echo "============================================================"

# ---------------------------------------------------------------------------
# Step 1: Package codebase (same repos as GCP equivalents)
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 1: Packaging codebase ==="

COMMON_REPOS=(
    "unified-api-contracts"
    "unified-trading-library"
    "unified-cloud-interface"
    "unified-config-interface"
)

SERVICE_REPO_MAP=(
    "market-tick-data-service:market-tick-data-service unified-features-interface unified-reference-data-interface"
    "market-data-processing-service:market-data-processing-service"
    "features-service:features-service"
    "instruments-service:instruments-service"
    "strategy-service:strategy-service"
    "execution-service:execution-service"
    "deployment-service:deployment-service"
    "ml-service:ml-service"
)

EXTRA_REPOS=()
for mapping in "${SERVICE_REPO_MAP[@]}"; do
    key="${mapping%%:*}"
    val="${mapping#*:}"
    if [[ "$SERVICE" == "$key" ]]; then
        IFS=' ' read -r -a EXTRA_REPOS <<< "$val"
        break
    fi
done

ALL_REPOS=("${COMMON_REPOS[@]}" "${EXTRA_REPOS[@]}")

if ! $DRY_RUN; then
    STAGING_DIR=$(mktemp -d)
    for repo in "${ALL_REPOS[@]}"; do
        REPO_PATH="${WORKSPACE_ROOT}/${repo}"
        if [[ ! -d "$REPO_PATH" ]]; then
            echo "  WARNING: ${repo} not found at ${REPO_PATH}, skipping"
            continue
        fi
        echo "  Syncing ${repo}..."
        mkdir -p "${STAGING_DIR}/${repo}"
        rsync -a \
            --filter=':- .gitignore' \
            --exclude='.git' \
            --exclude='.venv*' \
            --exclude='__pycache__' \
            --exclude='*.egg-info' \
            --exclude='node_modules' \
            --exclude='.mypy_cache' \
            --exclude='.pytest_cache' \
            "${REPO_PATH}/" "${STAGING_DIR}/${repo}/"
    done
    echo "  Compressing..."
    (cd "${STAGING_DIR}" && tar czf "${TARBALL_PATH}" -- *)
    TARBALL_SIZE=$(du -h "${TARBALL_PATH}" | cut -f1)
    echo "  Tarball: ${TARBALL_PATH} (${TARBALL_SIZE})"
    rm -rf "${STAGING_DIR}"
else
    echo "  [DRY RUN] Would package: ${ALL_REPOS[*]}"
fi

# ---------------------------------------------------------------------------
# Step 2: Upload to S3
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 2: Uploading to S3 ==="

if ! $DRY_RUN; then
    echo "  Uploading tarball to ${S3_STAGING}/"
    aws s3 cp "${TARBALL_PATH}" "${S3_STAGING}/${TARBALL_NAME}" \
        --region "$AWS_REGION" --quiet
    rm -f "${TARBALL_PATH}"
    echo "  Done."
else
    echo "  [DRY RUN] Would upload ${TARBALL_PATH} to ${S3_STAGING}/${TARBALL_NAME}"
fi

# ---------------------------------------------------------------------------
# Step 3: Build user-data script
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 3: Building user-data ==="

ASSET_GROUP_FLAG=""
[[ -n "$ASSET_GROUP" ]] && ASSET_GROUP_FLAG="--asset-group ${ASSET_GROUP}"

DATE_FLAGS=""
[[ -n "$START_DATE" ]] && DATE_FLAGS="--start ${START_DATE}"
[[ -n "$END_DATE" ]] && DATE_FLAGS="${DATE_FLAGS} --end ${END_DATE}"

VENUES_FLAG=""
[[ -n "$VENUES" ]] && VENUES_FLAG="--venues \"${VENUES}\""

DATA_TYPES_FLAG=""
[[ -n "$DATA_TYPES" ]] && DATA_TYPES_FLAG="--data-types \"${DATA_TYPES}\""

LOG_TRAP="$(lc_aws_log_upload_trap_block "$VM_NAME" "$AWS_ACCOUNT_ID")"

USER_DATA_FILE="$(mktemp /tmp/userdata-XXXX.sh)"
trap "rm -f '${USER_DATA_FILE}'" EXIT

cat > "$USER_DATA_FILE" <<USERDATA_EOF
#!/bin/bash
${LOG_TRAP}
set -euo pipefail

export WORK_DIR=/tmp/${TASK}_work
export HOME=/root
export PATH="/root/.local/bin:\$PATH"
export AWS_DEFAULT_REGION="${AWS_REGION}"
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}"

echo "=== EC2 VM Startup: ${VM_NAME} ==="
echo "  Task:       ${TASK}"
echo "  Service:    ${SERVICE}"
echo "  AssetGroup: ${ASSET_GROUP:-N/A}"
date

# Install Python 3.13
apt-get update -qq && apt-get install -yqq curl build-essential ca-certificates software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
echo "  Python: \$(python3.13 --version)"

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

# Install AWS CLI v2 (if not present)
if ! command -v aws &>/dev/null; then
    curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
    apt-get install -yqq unzip
    unzip -q /tmp/awscliv2.zip -d /tmp/awscliv2
    bash /tmp/awscliv2/aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli --update
    rm -rf /tmp/awscliv2*
fi

# Download codebase from S3
mkdir -p \${WORK_DIR}
echo "Downloading codebase tarball..."
aws s3 cp "${S3_STAGING}/${TARBALL_NAME}" "\${WORK_DIR}/codebase.tar.gz" \
    --region "${AWS_REGION}" --quiet

# Unpack
echo "Unpacking..."
tar xzf "\${WORK_DIR}/codebase.tar.gz" -C "\${WORK_DIR}"
rm "\${WORK_DIR}/codebase.tar.gz"
ls -d \${WORK_DIR}/*/

# Set up venv with uv
cd "\${WORK_DIR}/${SERVICE}"
uv venv .venv --python python3.13
source .venv/bin/activate
uv pip install -e "." -q

# Run the workload
echo "Running workload: ${TASK}"
python -m "${SERVICE//-/_}" \\
    ${ASSET_GROUP_FLAG} \\
    ${DATE_FLAGS} \\
    --chunk-size ${CHUNK_SIZE} \\
    ${VENUES_FLAG} \\
    ${DATA_TYPES_FLAG} \\
    --env ${DEPLOYMENT_ENV}

echo "Workload complete. Shutting down..."
date
shutdown -h now
USERDATA_EOF

chmod +x "$USER_DATA_FILE"
echo "  User-data built: ${USER_DATA_FILE}"

# ---------------------------------------------------------------------------
# Step 4: Launch EC2 instance
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 4: Launching EC2 instance ==="

lc_aws_singleton_check "$VM_PREFIX" "$AWS_REGION" "$FORCE"

EXTRA_TAGS='[{"Key":"purpose","Value":"uts-backfill"},{"Key":"task","Value":"'"${TASK}"'"},{"Key":"service","Value":"'"${SERVICE}"'"},{"Key":"env","Value":"'"${DEPLOYMENT_ENV}"'"}]'
[[ -n "$ASSET_GROUP" ]] && EXTRA_TAGS='[{"Key":"purpose","Value":"uts-backfill"},{"Key":"task","Value":"'"${TASK}"'"},{"Key":"service","Value":"'"${SERVICE}"'"},{"Key":"env","Value":"'"${DEPLOYMENT_ENV}"'"},{"Key":"asset-group","Value":"'"${ASSET_GROUP,,}"'"}]'

if $DRY_RUN; then
    echo "  [DRY RUN] Would launch:"
    echo "    Name:     ${VM_NAME}"
    echo "    Type:     ${MACHINE_TYPE}"
    echo "    Profile:  ${INSTANCE_PROFILE:-NONE}"
    echo "    Region:   ${AWS_REGION}"
    rm -f "$USER_DATA_FILE"
else
    INSTANCE_ID="$(lc_aws_ec2_run \
        "$VM_NAME" \
        "$AWS_REGION" \
        "$MACHINE_TYPE" \
        "$DISK_GB" \
        "$INSTANCE_PROFILE" \
        "$USER_DATA_FILE" \
        "$EXTRA_TAGS")"

    echo "  Launched: ${INSTANCE_ID}"
    echo ""
    echo "  Monitor:"
    echo "    aws ec2 describe-instance-status --instance-ids ${INSTANCE_ID} --region ${AWS_REGION}"
    echo "    aws s3 cp s3://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log /dev/stdout"
    echo ""
    echo "  Terminate:"
    echo "    aws ec2 terminate-instances --instance-ids ${INSTANCE_ID} --region ${AWS_REGION}"
fi

echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
