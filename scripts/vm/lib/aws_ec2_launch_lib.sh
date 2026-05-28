#!/usr/bin/env bash
# AWS EC2 VM launcher shared library — AWS equivalent of launcher_common.sh.
#
# Provides:
#   lc_aws_validate_env <env>
#   lc_aws_singleton_check <name_prefix> <region> [force=false]
#   lc_aws_resolve_ami <region>            — emit Ubuntu 24.04 AMI ID via SSM
#   lc_aws_ec2_run <vm_name> <region> <instance_type> <disk_gb>
#                  <instance_profile> <user_data_file> <tags_json>
#   lc_aws_code_bucket <account_id>        — emit S3 staging bucket name
#   lc_aws_run_ts                          — emit YYYYmmdd-HHMMSS timestamp
#   lc_aws_log_upload_trap_block <vm_name> <account_id>
#                                          — emit startup-script trap snippet
#                                            (S3 log upload on EXIT)
#   lc_aws_upload_code <src_tarball> <s3_staging_uri>
#   lc_aws_upload_script <src_script> <s3_staging_uri>
#
# Prerequisites:
#   aws CLI v2+, authenticated (aws sso login --profile operator OR
#   EC2 instance role when running on AWS).
#
# Conventions:
#   All functions are prefixed lc_aws_ to avoid collision with launcher_common.sh.
#   Functions print errors to >&2 and return/exit non-zero on failure.
#   AWS_REGION defaults to ap-northeast-1 if AWS_DEFAULT_REGION unset.
#   AWS_ACCOUNT_ID is resolved lazily via aws sts get-caller-identity if unset.
#
# Analogues:
#   lc_aws_validate_env     ↔ lc_validate_env
#   lc_aws_singleton_check  ↔ lc_singleton_check  (GCP: gcloud list → AWS: ec2 describe)
#   lc_aws_ec2_run          ↔ lc_gcloud_create    (GCP startup-script → AWS user-data)
#   lc_aws_code_bucket      ↔ lc_code_bucket      (gs://deployment-scripts-<pid> → s3://unified-trading-deployment-scripts-<account>)
#   lc_aws_log_upload_trap_block ↔ lc_log_upload_trap_block (gsutil → aws s3 cp)

set -euo pipefail

# ---------------------------------------------------------------------------
# lc_aws_validate_env <env>
# ---------------------------------------------------------------------------
lc_aws_validate_env() {
    local env="${1:-}"
    case "$env" in
        prod|staging|dev) ;;
        *) echo "ERROR: --env must be one of prod/staging/dev (got: ${env})" >&2; exit 1 ;;
    esac
}

# ---------------------------------------------------------------------------
# lc_aws_resolve_account
# ---------------------------------------------------------------------------
# Set AWS_ACCOUNT_ID from caller-identity if not already set.
lc_aws_resolve_account() {
    if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
        AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)" || {
            echo "ERROR: Cannot resolve AWS_ACCOUNT_ID. Authenticate first: aws sso login --profile operator" >&2
            exit 1
        }
        export AWS_ACCOUNT_ID
    fi
}

# ---------------------------------------------------------------------------
# lc_aws_resolve_ami <region>
# ---------------------------------------------------------------------------
# Emit the canonical Ubuntu 24.04 LTS amd64 AMI ID for the given region
# via SSM Parameter Store (canonical source — always the latest AMI).
lc_aws_resolve_ami() {
    local region="${1:-${AWS_DEFAULT_REGION:-ap-northeast-1}}"
    local ssm_param="/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
    aws ssm get-parameter \
        --name "$ssm_param" \
        --region "$region" \
        --query "Parameter.Value" \
        --output text 2>/dev/null || {
        echo "ERROR: Failed to resolve Ubuntu 24.04 AMI in ${region} via SSM" >&2
        exit 1
    }
}

# ---------------------------------------------------------------------------
# lc_aws_singleton_check <name_prefix> <region> [force]
# ---------------------------------------------------------------------------
# If any EC2 instance tagged Name=<name_prefix>* is running, refuse launch
# unless force=true.
lc_aws_singleton_check() {
    local prefix="${1:?lc_aws_singleton_check: name_prefix required}"
    local region="${2:?lc_aws_singleton_check: region required}"
    local force="${3:-false}"

    if [[ "${force,,}" == "true" ]]; then
        return 0
    fi

    local existing
    existing="$(aws ec2 describe-instances \
        --region "$region" \
        --filters \
            "Name=tag:Name,Values=${prefix}*" \
            "Name=instance-state-name,Values=running,pending" \
        --query "Reservations[].Instances[].InstanceId" \
        --output text 2>/dev/null | head -1 || true)"

    if [[ -n "$existing" ]]; then
        echo "ERROR: EC2 instance with Name prefix '${prefix}' already running in ${region}: ${existing}" >&2
        echo "Options:" >&2
        echo "  Inspect: aws ec2 describe-instance-status --instance-ids ${existing} --region ${region}" >&2
        echo "  Stop:    aws ec2 terminate-instances --instance-ids ${existing} --region ${region}" >&2
        echo "  Force:   bash \$0 --force" >&2
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# lc_aws_ec2_run <vm_name> <region> <instance_type> <disk_gb>
#                <instance_profile_name> <user_data_file> <extra_tags_json>
# ---------------------------------------------------------------------------
# Launch EC2 instance with canonical settings (ap-northeast-1 AZ-a/b/c,
# Ubuntu 24.04, instance-terminated-on-shutdown, IMDSv2 required).
#
# <instance_profile_name> : IAM instance profile (role) to attach, e.g. "uts-backfill-prod"
#   Pass "" to skip instance profile (will have no AWS API access).
#   NOTE: profiles are BLOCKED-AWS-PERMISSIONS pending 1.B IAM role creation.
#
# <extra_tags_json> : additional Tag objects as JSON array, e.g.:
#   '[{"Key":"purpose","Value":"mtds-backfill"},{"Key":"asset-group","Value":"cefi"}]'
#
# AWS_SECURITY_GROUP_IDS and AWS_SUBNET_ID must be set in environment.
# AWS_KEY_PAIR_NAME is optional (SSH access).
#
# Returns the instance-id on stdout.
lc_aws_ec2_run() {
    local vm_name="${1:?lc_aws_ec2_run: vm_name required}"
    local region="${2:?lc_aws_ec2_run: region required}"
    local instance_type="${3:?lc_aws_ec2_run: instance_type required}"
    local disk_gb="${4:?lc_aws_ec2_run: disk_gb required}"
    local instance_profile="${5:-}"
    local user_data_file="${6:?lc_aws_ec2_run: user_data_file required}"
    local extra_tags_json="${7:-[]}"

    if [[ -z "${AWS_SECURITY_GROUP_IDS:-}" ]]; then
        echo "ERROR: AWS_SECURITY_GROUP_IDS must be set (e.g. sg-xxxxxxxx)" >&2
        exit 1
    fi
    if [[ -z "${AWS_SUBNET_ID:-}" ]]; then
        echo "ERROR: AWS_SUBNET_ID must be set (ap-northeast-1a/b/c subnet ID)" >&2
        exit 1
    fi

    # AMI resolution: Phase 9 prebaked AMI override > SSM Ubuntu fallback.
    # Callers can set AMI_ID in env to force a specific image — typically the
    # output of `packer build` from deployment-service/packer/agent-orchestrator/.
    # When unset, fall back to the latest Canonical Ubuntu 24.04 via SSM (slow
    # cold-boot path; bootstrap_vm.sh installs everything from scratch).
    local ami_id
    if [[ -n "${AMI_ID:-}" ]]; then
        ami_id="${AMI_ID}"
        echo "[lc_aws_ec2_run] Using operator-supplied AMI: ${ami_id}" >&2
    else
        ami_id="$(lc_aws_resolve_ami "$region")"
        echo "[lc_aws_ec2_run] Using Ubuntu 24.04 latest from SSM: ${ami_id}" >&2
    fi

    local name_tag
    name_tag='[{"Key":"Name","Value":"'"$vm_name"'"}]'

    # Merge name tag with extra tags; build full JSON tag-spec (shorthand form breaks on quotes)
    local all_tags tag_spec
    all_tags="$(python3 -c "
import json, sys
name = json.loads('$name_tag')
extra = json.loads(sys.argv[1]) if len(sys.argv) > 1 else []
merged = name + extra
print(json.dumps(merged))
" "${extra_tags_json}" 2>/dev/null || echo "$name_tag")"
    tag_spec="$(python3 -c "
import json, sys
tags = json.loads(sys.argv[1])
print(json.dumps([{'ResourceType':'instance','Tags':tags}]))
" "${all_tags}" 2>/dev/null || echo "[{\"ResourceType\":\"instance\",\"Tags\":${all_tags}}]")"

    local profile_args=()
    if [[ -n "$instance_profile" ]]; then
        profile_args=(--iam-instance-profile "Name=${instance_profile}")
    fi

    local key_args=()
    if [[ -n "${AWS_KEY_PAIR_NAME:-}" ]]; then
        key_args=(--key-name "$AWS_KEY_PAIR_NAME")
    fi

    local instance_id
    instance_id="$(aws ec2 run-instances \
        --region "$region" \
        --image-id "$ami_id" \
        --instance-type "$instance_type" \
        --security-group-ids $AWS_SECURITY_GROUP_IDS \
        --subnet-id "$AWS_SUBNET_ID" \
        "${profile_args[@]+"${profile_args[@]}"}" \
        "${key_args[@]+"${key_args[@]}"}" \
        --user-data "file://${user_data_file}" \
        --instance-initiated-shutdown-behavior terminate \
        --metadata-options "HttpTokens=required,HttpEndpoint=enabled" \
        --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":${disk_gb},\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]" \
        --tag-specifications "$tag_spec" \
        --count 1 \
        --query "Instances[0].InstanceId" \
        --output text)"

    echo "$instance_id"
}

# ---------------------------------------------------------------------------
# lc_aws_code_bucket <account_id>
# ---------------------------------------------------------------------------
# Emit the canonical S3 staging bucket name.
# Mirrors GCP lc_code_bucket: deployment-scripts-{project} → unified-trading-deployment-scripts-{account}
lc_aws_code_bucket() {
    local account_id="${1:?lc_aws_code_bucket: account_id required}"
    echo "unified-trading-deployment-scripts-${account_id}"
}

# ---------------------------------------------------------------------------
# lc_aws_run_ts
# ---------------------------------------------------------------------------
lc_aws_run_ts() {
    date +%Y%m%d-%H%M%S
}

# ---------------------------------------------------------------------------
# lc_aws_upload_code <tarball_path> <s3_uri_dir>
# ---------------------------------------------------------------------------
lc_aws_upload_code() {
    local tarball="${1:?lc_aws_upload_code: tarball_path required}"
    local s3_dir="${2:?lc_aws_upload_code: s3_uri_dir required}"
    local region="${AWS_DEFAULT_REGION:-ap-northeast-1}"
    aws s3 cp "$tarball" "${s3_dir}/$(basename "$tarball")" --region "$region" --quiet
}

# ---------------------------------------------------------------------------
# lc_aws_upload_script <script_path> <s3_uri_dir>
# ---------------------------------------------------------------------------
lc_aws_upload_script() {
    local script="${1:?lc_aws_upload_script: script_path required}"
    local s3_dir="${2:?lc_aws_upload_script: s3_uri_dir required}"
    local region="${AWS_DEFAULT_REGION:-ap-northeast-1}"
    aws s3 cp "$script" "${s3_dir}/$(basename "$script")" --region "$region" --quiet
}

# ---------------------------------------------------------------------------
# lc_aws_log_upload_trap_block <vm_name> <account_id>
# ---------------------------------------------------------------------------
# Emit a bash snippet to inject at the TOP of an EC2 user-data heredoc.
# Mirrors lc_log_upload_trap_block (gsutil → aws s3 cp).
#
# CALLER: emit this inside the user-data heredoc BEFORE set -e or workload commands.
# Inside the heredoc body, bash $-vars are pre-escaped with \$ — caller does NOT
# need to double-escape.
lc_aws_log_upload_trap_block() {
    local vm_name="${1:?lc_aws_log_upload_trap_block: vm_name required}"
    local account_id="${2:?lc_aws_log_upload_trap_block: account_id required}"
    local code_bucket="unified-trading-deployment-scripts-${account_id}"
    cat <<TRAPSNIPPET
# --- lc_aws_log_upload_trap_block (deployment-service/scripts/vm/lib/aws_ec2_launch_lib.sh) ---
LOG_LOCAL="/var/log/run.log"
S3_LOG_URI="s3://${code_bucket}/vm-logs/${vm_name}/run.log"
mkdir -p "\$(dirname "\$LOG_LOCAL")" 2>/dev/null || true
exec > >(tee -a "\$LOG_LOCAL") 2>&1
_lc_final_upload() {
    local rc=\$?
    echo ""
    echo "=== VM EXIT rc=\$rc \$(date -u +'%Y-%m-%dT%H:%M:%SZ') ==="
    for _i in 1 2 3; do
        if aws s3 cp "\$LOG_LOCAL" "\$S3_LOG_URI" --region "${AWS_DEFAULT_REGION:-ap-northeast-1}" --quiet 2>/dev/null; then
            echo "log uploaded to \$S3_LOG_URI (attempt \$_i)"
            break
        fi
        echo "log upload attempt \$_i failed, retrying in 5s..."
        sleep 5
    done
    sleep 2
    aws s3 cp "\$LOG_LOCAL" "\$S3_LOG_URI" --region "${AWS_DEFAULT_REGION:-ap-northeast-1}" --quiet 2>/dev/null || true
    shutdown -h +1 2>/dev/null || true
    return \$rc
}
trap _lc_final_upload EXIT
# --- end lc_aws_log_upload_trap_block ---
TRAPSNIPPET
}
