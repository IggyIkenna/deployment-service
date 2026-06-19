# VM-logs S3 lifecycle — AWS mirror of the GCP deployment-scripts bucket rules
# (terraform/gcp/main.tf google_storage_bucket.deployment_scripts).
#
# WHY: bespoke EC2 launchers stream their run log + a liveness/progress
# heartbeat + a terminal EXIT_STATUS marker to
#   s3://unified-trading-deployment-scripts-<account>/vm-logs/<vm>/run.log
#   s3://unified-trading-deployment-scripts-<account>/vm-heartbeat/<vm>.txt
#   s3://unified-trading-deployment-scripts-<account>/vm-logs/<vm>/EXIT_STATUS
# via lc_aws_log_upload_trap_block (scripts/vm/lib/aws_ec2_launch_lib.sh), so a
# dead/hung VM's full log + terminal status survive in S3 (queryable without
# SSM/SSH). Without a lifecycle these accumulate forever — this rule bounds
# them to match the GCP side (vm-logs 14d, vm-heartbeat 15d, log-archive 30d).
#
# The deployment-scripts bucket is created out-of-band (code/tarball staging
# bucket, not a TF-managed data bucket), so this attaches a lifecycle to it by
# name without taking ownership of the bucket resource itself.
# SSOT: codex/05-infrastructure/vm-tarball-deployment.md § "VM observability".

resource "aws_s3_bucket_lifecycle_configuration" "deployment_scripts_vm_logs" {
  bucket = "unified-trading-deployment-scripts-${var.aws_account_id}"

  rule {
    id     = "vm-logs-14d"
    status = "Enabled"
    filter {
      prefix = "vm-logs/"
    }
    expiration {
      days = 14
    }
  }

  rule {
    id     = "vm-heartbeat-15d"
    status = "Enabled"
    filter {
      prefix = "vm-heartbeat/"
    }
    expiration {
      days = 15
    }
  }

  rule {
    id     = "log-archive-30d"
    status = "Enabled"
    filter {
      prefix = "log-archive/"
    }
    expiration {
      days = 30
    }
  }
}
