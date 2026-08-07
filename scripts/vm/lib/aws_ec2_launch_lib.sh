#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
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

    # Name + a standardized managed-by tag so the deployments cockpit can PROVE provenance: an EC2
    # instance WITHOUT this tag is provably ad-hoc (WS-D provenance robustness — the AWS twin of the
    # GCP managed-by label). Added centrally so all AWS launchers inherit it without a per-copy edit.
    local name_tag
    name_tag='[{"Key":"Name","Value":"'"$vm_name"'"},{"Key":"managed-by","Value":"deployment-service"}]'

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

    # Shutdown behavior (2026-06-12): default `terminate` (the original
    # ephemeral-batch contract). LONG-RUNNING instances (orchestrator worker
    # VMs) set LC_AWS_SHUTDOWN_BEHAVIOR=stop so an in-VM `shutdown` stops the
    # instance (root disk kept, restartable) instead of terminating it and
    # wiping the DeleteOnTermination gp3 root.
    local shutdown_behavior="${LC_AWS_SHUTDOWN_BEHAVIOR:-terminate}"

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
        --instance-initiated-shutdown-behavior "${shutdown_behavior}" \
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
# FIXED 2026-08-07 (operator-authorized code fix, AWS-deferred item #7): this used to emit
# "unified-trading-deployment-scripts-{account}", a bucket that returns 404 -- it was never
# actually provisioned. `create-code-tarballs.sh` (the actual uploader) has always written to
# the real, populated bucket `uts-prod-deployment-state` (single, not account-suffixed). Both
# sides now point at the same bucket.
lc_aws_code_bucket() {
    local account_id="${1:?lc_aws_code_bucket: account_id required}"
    echo "uts-prod-deployment-state"
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
# lc_aws_log_upload_trap_block <vm_name> <account_id> [asset_group] [task]
# ---------------------------------------------------------------------------
# Emit a bash snippet to inject at the TOP of an EC2 user-data heredoc.
# AWS mirror of lc_log_upload_trap_block (gsutil → aws s3 cp): durable
# observability for an inline EC2 bootstrap — continuous S3 log stream
# (every LC_LOG_STREAM_INTERVAL s, default 30) + liveness/progress heartbeat
# object + terminal EXIT_STATUS marker + guaranteed final upload on any exit.
# So a dead/hung EC2 box's full log + its terminal status live in S3,
# queryable WITHOUT SSM/SSH (≤30 s loss). Mirrors the durability contract of
# the GCP `vm-exec-with-gcs-tee.sh` path. SSOT:
# codex/05-infrastructure/vm-tarball-deployment.md § "VM observability".
#
# CALLER: emit this inside the user-data heredoc BEFORE set -e or workload commands.
# Inside the heredoc body, bash $-vars are pre-escaped with \$ — caller does NOT
# need to double-escape.
lc_aws_log_upload_trap_block() {
    local vm_name="${1:?lc_aws_log_upload_trap_block: vm_name required}"
    local account_id="${2:?lc_aws_log_upload_trap_block: account_id required}"
    local asset_group="${3:-UNKNOWN}"
    local task="${4:-vm-exec}"
    local code_bucket="uts-prod-deployment-state"
    local region="${AWS_DEFAULT_REGION:-ap-northeast-1}"
    local stream_interval="${LC_LOG_STREAM_INTERVAL:-30}"
    cat <<TRAPSNIPPET
# --- lc_aws_log_upload_trap_block (deployment-service/scripts/vm/lib/aws_ec2_launch_lib.sh) ---
# Continuous S3 log stream (every ${stream_interval}s) + heartbeat object +
# terminal EXIT_STATUS + guaranteed final upload. Canonical run.log path:
# s3://${code_bucket}/vm-logs/${vm_name}/run.log
LOG_LOCAL="/var/log/run.log"
S3_LOG_URI="s3://${code_bucket}/vm-logs/${vm_name}/run.log"
S3_HEARTBEAT_URI="s3://${code_bucket}/vm-heartbeat/${vm_name}.txt"
S3_EXIT_URI="s3://${code_bucket}/vm-logs/${vm_name}/EXIT_STATUS"
S3_REGION="${region}"
LC_STREAM_INTERVAL=${stream_interval}
LC_VM_ASSET_GROUP="${asset_group}"
LC_VM_TASK="${task}"
LC_START_EPOCH=\$(date +%s)
mkdir -p "\$(dirname "\$LOG_LOCAL")" 2>/dev/null || true
exec > >(tee -a "\$LOG_LOCAL") 2>&1
echo "=== VM STARTED \$(date -u +'%Y-%m-%dT%H:%M:%SZ') vm=${vm_name} asset_group=${asset_group} task=${task} ==="
_lc_stream_loop() {
    while true; do
        aws s3 cp "\$LOG_LOCAL" "\$S3_LOG_URI" --region "\$S3_REGION" --quiet 2>/dev/null || true
        printf 'vm=%s asset_group=%s task=%s alive_at=%s uptime_s=%s\n' \
            "${vm_name}" "\$LC_VM_ASSET_GROUP" "\$LC_VM_TASK" \
            "\$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "\$(( \$(date +%s) - LC_START_EPOCH ))" \
            | aws s3 cp - "\$S3_HEARTBEAT_URI" --region "\$S3_REGION" --quiet 2>/dev/null || true
        sleep "\$LC_STREAM_INTERVAL"
    done
}
_lc_stream_loop &
_LC_STREAM_PID=\$!
_lc_final_upload() {
    local rc=\$?
    kill "\$_LC_STREAM_PID" 2>/dev/null || true
    echo ""
    echo "=== VM EXIT rc=\$rc \$(date -u +'%Y-%m-%dT%H:%M:%SZ') ==="
    echo "\$rc" | aws s3 cp - "\$S3_EXIT_URI" --region "\$S3_REGION" --quiet 2>/dev/null || true
    for _i in 1 2 3; do
        if aws s3 cp "\$LOG_LOCAL" "\$S3_LOG_URI" --region "\$S3_REGION" --quiet 2>/dev/null; then
            echo "log uploaded to \$S3_LOG_URI (attempt \$_i)"
            break
        fi
        echo "log upload attempt \$_i failed, retrying in 5s..."
        sleep 5
    done
    sleep 2
    aws s3 cp "\$LOG_LOCAL" "\$S3_LOG_URI" --region "\$S3_REGION" --quiet 2>/dev/null || true
    shutdown -h +1 2>/dev/null || true
    return \$rc
}
trap _lc_final_upload EXIT
# --- end lc_aws_log_upload_trap_block ---
TRAPSNIPPET
}

# ---------------------------------------------------------------------------
# lc_aws_log_upload_continuous_block <vm_name> <account_id> [asset_group] [task]
# ---------------------------------------------------------------------------
# AWS mirror of lc_log_upload_continuous_block (gcloud storage → aws s3 cp):
# durable observability for a LONG-LIVED, non-self-terminating EC2 instance
# (e.g. the orchestrator planning VM, the orchestrator-worker VM) that:
#   - runs indefinitely (no scheduled self-shutdown)
#   - does NOT need EXIT_STATUS or VM_SHUTDOWN_ON_COMPLETION semantics
#   - still needs continuous S3 log shipping + liveness heartbeat visibility
#
# Emits a bash snippet to inject AT THE TOP of an EC2 user-data heredoc body.
# The snippet:
#   1. Tees stdout+stderr to /var/log/run.log.
#   2. Writes /usr/local/bin/lc-s3-log-stream.sh and starts it via systemd-run
#      as a transient unit named lc-s3-log-stream-<vm_name>. The transient unit
#      survives the user-data service's exit — unlike a plain & subshell, which
#      is killed when the systemd unit managing it tears down its cgroup.
#   3. The streamer uploads the growing log to
#        s3://uts-prod-deployment-state/vm-logs/<vm-name>/run.log
#      and writes a heartbeat object to
#        s3://uts-prod-deployment-state/vm-heartbeat/<vm-name>.txt
#      every LC_LOG_STREAM_INTERVAL seconds (default 30), same contract as
#      lc_aws_log_upload_trap_block — these paths are polled by the vm-zombie-
#      watchdog.
#
# Key differences from lc_aws_log_upload_trap_block:
#   - No EXIT_STATUS marker (long-lived VMs do not self-terminate).
#   - No shutdown-on-exit trap.
#   - Streamer runs in a systemd transient unit (survives user-data service exit).
#
# Caller pattern (identical to lc_aws_log_upload_trap_block):
#   LOG_TRAP="$(lc_aws_log_upload_continuous_block "$VM_NAME" "$ACCOUNT_ID" ao planning-vm)"
#   cat > "${USER_DATA_FILE}" <<STARTUP_EOF
#   #!/bin/bash
#   ${LOG_TRAP}
#   set -euo pipefail
#   # ... rest of user-data ...
#   STARTUP_EOF
lc_aws_log_upload_continuous_block() {
    local vm_name="${1:?lc_aws_log_upload_continuous_block: vm_name required}"
    local account_id="${2:?lc_aws_log_upload_continuous_block: account_id required}"
    local asset_group="${3:-UNKNOWN}"
    local task="${4:-vm-exec}"
    local code_bucket="uts-prod-deployment-state"
    local region="${AWS_DEFAULT_REGION:-ap-northeast-1}"
    local stream_interval="${LC_LOG_STREAM_INTERVAL:-30}"
    cat <<CONTBLOCK
# --- lc_aws_log_upload_continuous_block (deployment-service/scripts/vm/lib/aws_ec2_launch_lib.sh) ---
# Durable observability for a long-lived (non-self-terminating) EC2 instance:
# continuous S3 log stream (every ${stream_interval}s) + liveness heartbeat object.
# No EXIT_STATUS or shutdown semantics — use lc_aws_log_upload_trap_block for
# batch/backfill VMs that self-terminate.
# Canonical run.log path: s3://${code_bucket}/vm-logs/${vm_name}/run.log
LOG_LOCAL="/var/log/run.log"
S3_LOG_URI="s3://${code_bucket}/vm-logs/${vm_name}/run.log"
S3_HEARTBEAT_URI="s3://${code_bucket}/vm-heartbeat/${vm_name}.txt"
S3_REGION="${region}"
LC_STREAM_INTERVAL=${stream_interval}
LC_VM_ASSET_GROUP="${asset_group}"
LC_VM_TASK="${task}"
LC_START_EPOCH=\$(date +%s)
mkdir -p "\$(dirname "\$LOG_LOCAL")" 2>/dev/null || true
exec > >(tee -a "\$LOG_LOCAL") 2>&1
echo "=== VM STARTED \$(date -u +'%Y-%m-%dT%H:%M:%SZ') vm=${vm_name} asset_group=${asset_group} task=${task} ==="
# Write the S3 log-stream helper to a stable path. The helper is started as a
# transient systemd unit (below) so it survives the user-data service exit.
# A plain & subshell would be killed when the user-data systemd unit tears down
# its cgroup on exit — that is the gap this approach closes.
cat > /usr/local/bin/lc-s3-log-stream.sh << '__LC_STREAM_EOF__'
#!/usr/bin/env bash
# Continuous S3 log/heartbeat stream for a long-lived EC2 instance.
# Args: <log_file> <s3_log_uri> <s3_heartbeat_uri> <interval_s> <region> <vm_name> <asset_group> <task> <start_epoch>
_LC_LOG="\$1"; _LC_S3_LOG="\$2"; _LC_S3_HB="\$3"; _LC_INTERVAL="\$4"
_LC_REGION="\$5"; _LC_VM="\$6"; _LC_AG="\$7"; _LC_TASK="\$8"; _LC_EPOCH="\$9"
while true; do
    aws s3 cp "\$_LC_LOG" "\$_LC_S3_LOG" --region "\$_LC_REGION" --quiet 2>/dev/null || true
    printf 'vm=%s asset_group=%s task=%s alive_at=%s uptime_s=%s\n' \
        "\$_LC_VM" "\$_LC_AG" "\$_LC_TASK" \
        "\$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "\$(( \$(date +%s) - _LC_EPOCH ))" \
        | aws s3 cp - "\$_LC_S3_HB" --region "\$_LC_REGION" --quiet 2>/dev/null || true
    sleep "\$_LC_INTERVAL"
done
__LC_STREAM_EOF__
chmod +x /usr/local/bin/lc-s3-log-stream.sh
# Start the streamer as a transient systemd unit so it survives this user-data
# service exit. The stable unit name means repeat runs replace the previous
# instance rather than accumulating stale units.
systemd-run --unit=lc-s3-log-stream-${vm_name} --remain-after-exit \
    /usr/local/bin/lc-s3-log-stream.sh \
    "\$LOG_LOCAL" "\$S3_LOG_URI" "\$S3_HEARTBEAT_URI" "\$LC_STREAM_INTERVAL" "\$S3_REGION" \
    "${vm_name}" "${asset_group}" "${task}" "\$LC_START_EPOCH" 2>/dev/null || true
# --- end lc_aws_log_upload_continuous_block ---
CONTBLOCK
}
