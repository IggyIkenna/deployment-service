# CF Manifest Audit (Cross-AG data-state audit) — AWS Batch (Fargate) + EventBridge Scheduler
#
# Plan: audit_criteria_automation_2026_06_08.md Phase 2 (Tier-3 continuous-verification cron)
# Pattern source: codex/05-infrastructure/manifest-consolidator-ssot.md
#   + aws/manifest_consolidator_scheduler.tf (Batch-Fargate + EventBridge shape)
#
# AWS parity for gcp/cf_manifest_audit_scheduler.tf.
# Runs `python -m unified_trading_library.cf_manifest_audit --all-ags --json-out
# s3://<audit-bucket>/cf_audit` once per day at 06:00 UTC. CLOUD_PROVIDER=aws routes
# get_storage_client() to S3. Job exits NON-ZERO if any CF is RED — EventBridge +
# CloudWatch Alarm surfaces the failure.
#
# NOTE: AWS equivalent for Athena/Redshift Spectrum external tables over the same hive layout
# is a parallel option tracked separately in bigquery_feature_ml_compute_engine_option_2026_06_08.md
# Phase 1 AWS analogue (not provisioned here — GCP BQ tables are the primary compute engine).

variable "cf_audit_image_aws" {
  description = "ECR image for the cf-manifest-audit job. Defaults to market-tick-data-service (UTL bundled — runs unified_trading_library.cf_manifest_audit). Override only if a dedicated image is ever needed."
  type        = string
  default     = "" # empty = fall through to local default below
}

variable "cf_audit_alert_sns_arn" {
  description = "SNS topic ARN to wire to the CloudWatch Alarm for CF audit failures (RED alert). Empty = alarm exists but no notification until wired."
  type        = string
  default     = ""
}

locals {
  # S3 audit output bucket (flat naming, no env suffix — Group B convention per cloud-providers.yaml).
  cf_audit_s3_bucket = "unified-trading-cf-manifest-audit-${var.aws_account_id}"

  # Image: prefer explicit variable; fall back to MTDS ECR image (UTL bundled).
  cf_audit_ecr_image = var.cf_audit_image_aws != "" ? var.cf_audit_image_aws : "${var.aws_account_id}.dkr.ecr.${var.aws_region}.amazonaws.com/market-tick-data-service:latest"
}

# -------------------------------------------------------
# S3 output bucket — JSON audit summaries, 90-day lifecycle
# -------------------------------------------------------
resource "aws_s3_bucket" "cf_manifest_audit_output" {
  bucket        = local.cf_audit_s3_bucket
  force_destroy = false

  tags = {
    purpose = "cf-manifest-audit"
    plan    = "audit-criteria-automation-2026-06-08"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "cf_manifest_audit_output" {
  bucket = aws_s3_bucket.cf_manifest_audit_output.id

  rule {
    id     = "expire-old-audit-summaries"
    status = "Enabled"

    expiration {
      days = 90
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "cf_manifest_audit_output" {
  bucket = aws_s3_bucket.cf_manifest_audit_output.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# IAM: allow the unified_trading execution role to write to the audit output bucket.
resource "aws_iam_role_policy" "cf_manifest_audit_s3" {
  name = "cf-manifest-audit-s3-${var.environment}"
  role = aws_iam_role.unified_trading.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CfAuditOutputBucketWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::${local.cf_audit_s3_bucket}",
          "arn:aws:s3:::${local.cf_audit_s3_bucket}/*",
        ]
      },
    ]
  })
}

# -------------------------------------------------------
# IAM role for EventBridge Scheduler → Batch SubmitJob
# -------------------------------------------------------
resource "aws_iam_role" "cf_manifest_audit_scheduler" {
  name = "uts-cf-manifest-audit-scheduler-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "cf_manifest_audit_scheduler_batch" {
  name = "batch-submit"
  role = aws_iam_role.cf_manifest_audit_scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["batch:SubmitJob"]
      Resource = [
        aws_batch_job_queue.manifest_consolidator.arn, # reuse existing job queue
        "arn:aws:batch:${var.aws_region}:${var.aws_account_id}:job-definition/uts-prod-cf-manifest-audit-*",
      ]
    }]
  })
}

# -------------------------------------------------------
# Batch Job Definition — cf-manifest-audit-all
# -------------------------------------------------------
# Reuses the shared manifest-consolidator Batch compute environment + job queue
# (declared in manifest_consolidator_scheduler.tf) — no new compute environment needed.
module "cf_manifest_audit_job_aws" {
  source = "../modules/container-job/aws"

  name            = "uts-prod-cf-manifest-audit"
  image           = local.cf_audit_ecr_image
  vcpus           = "8"
  memory_mb       = "32768" # 32 GiB — mirrors the GCP job's fix (cf_manifest_audit_scheduler.tf):
  # 4Gi OOM'd every run for 14 straight days on the GCP side before ever finishing bucket 1/10;
  # unified-trading-library@6ce1ddb6's column-pruned + pyarrow-backed read cut peak RSS ~3.7-4x,
  # but even 16Gi/4vCPU still OOM'd live on the fleet's largest bucket (defi tick, 26.3M rows) —
  # jumped to Cloud Run's ceiling on the GCP side rather than re-guessing; keep this leg in parity
  # so it doesn't repeat the same failure if/when it is activated.
  timeout_seconds = 1800
  max_retries     = 0 # non-zero exit = RED alert; do not retry

  execution_role_arn = aws_iam_role.unified_trading.arn
  job_role_arn       = aws_iam_role.unified_trading.arn

  environment_variables = {
    CLOUD_PROVIDER     = "aws"
    AWS_DEFAULT_REGION = var.aws_region
  }

  # unified_trading_library.cf_manifest_audit — bundled in the MTDS image via UTL
  # (mirrors gcp/cf_manifest_audit_scheduler.tf's 2026-07-10 fix: the prior
  # `cf-manifest-audit-all` console-script was never packaged/installed anywhere).
  # --all-ags: loop all 5 asset_groups × {market-data-tick, instruments-store}.
  # --json-out: S3 prefix — the {YYYY-MM-DD}.json filename is computed at runtime
  # inside the script (AWS Batch `command` is an exec array, no shell, so a literal
  # $(date ...) here is never evaluated).
  # Exits NON-ZERO if any CF is RED.
  command = [
    "python", "-m", "unified_trading_library.cf_manifest_audit",
    "--all-ags",
    "--json-out", "s3://${local.cf_audit_s3_bucket}/cf_audit",
  ]

  assign_public_ip = true
  subnet_ids       = data.aws_subnets.consolidator.ids # reuse consolidator subnets

  # Reuse the shared manifest-consolidator compute environment + queue.
  create_compute_environment = false
  compute_environment_arn    = aws_batch_compute_environment.manifest_consolidator.arn
  create_job_queue           = false
  job_queue_arn              = aws_batch_job_queue.manifest_consolidator.arn

  service_name = "cf-manifest-audit"
  environment  = var.environment

  tags = {
    purpose = "cf-manifest-audit"
    plan    = "audit-criteria-automation-2026-06-08"
  }
}

# -------------------------------------------------------
# EventBridge Scheduler — daily at 06:00 UTC
# -------------------------------------------------------
module "cf_manifest_audit_schedule_aws" {
  source = "../modules/scheduler/aws"

  name        = "uts-prod-cf-manifest-audit"
  description = "Daily cross-AG CF manifest audit — loops all 5 asset_groups; exits non-zero on any RED CF. Continuous-verification path for master plan Groups A-G. Plan: audit_criteria_automation_2026_06_08.md Phase 2."
  schedule    = "cron(0 6 * * ? *)" # 06:00 UTC daily — after manifest consolidator cycles settle
  region      = var.aws_region
  enabled     = true

  target_job_name    = "uts-prod-cf-manifest-audit"
  job_definition_arn = module.cf_manifest_audit_job_aws.arn
  job_queue_name     = aws_batch_job_queue.manifest_consolidator.name
  scheduler_role_arn = aws_iam_role.cf_manifest_audit_scheduler.arn

  retry_count = 0 # non-zero exit = RED alert; mirrors consolidator pattern (no retry)

  create_scheduler_role = false
  create_schedule_group = false
}

# -------------------------------------------------------
# CloudWatch Alarm — fires when the CF audit job fails (RED)
# -------------------------------------------------------
# AWS Batch emits a BatchJobFailed metric when a job exits non-zero.
# This alarm surfaces it as a CloudWatch Alarm incident (→ SNS notification
# if var.cf_audit_alert_sns_arn is wired).
resource "aws_cloudwatch_metric_alarm" "cf_manifest_audit_red" {
  alarm_name          = "uts-cf-manifest-audit-RED-${var.environment}"
  alarm_description   = "CF manifest audit detected a RED cross-file in at least one asset_group (job exited non-zero). Check S3 audit output at s3://${local.cf_audit_s3_bucket}/cf_audit/. Plan: audit_criteria_automation_2026_06_08.md Phase 2."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "FailedJobCount"
  namespace           = "AWS/Batch"
  period              = 86400 # 24h window — one alarm per daily run
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching" # no alarm if no job ran (e.g. first day)

  dimensions = {
    JobQueue      = aws_batch_job_queue.manifest_consolidator.name
    JobDefinition = "uts-prod-cf-manifest-audit"
  }

  alarm_actions = var.cf_audit_alert_sns_arn != "" ? [var.cf_audit_alert_sns_arn] : []
  ok_actions    = var.cf_audit_alert_sns_arn != "" ? [var.cf_audit_alert_sns_arn] : []

  tags = {
    purpose = "cf-manifest-audit-alert"
    plan    = "audit-criteria-automation-2026-06-08"
  }
}

# -------------------------------------------------------
# Outputs
# -------------------------------------------------------
output "cf_manifest_audit_job_name_aws" {
  description = "AWS Batch job definition name for the CF manifest audit."
  value       = "uts-prod-cf-manifest-audit"
}

output "cf_manifest_audit_schedule_name_aws" {
  description = "EventBridge Scheduler rule name for the daily CF manifest audit."
  value       = "uts-prod-cf-manifest-audit"
}

output "cf_manifest_audit_s3_bucket" {
  description = "S3 bucket holding daily CF audit JSON summaries (s3://<bucket>/cf_audit/YYYY-MM-DD.json)."
  value       = aws_s3_bucket.cf_manifest_audit_output.id
}

output "cf_manifest_audit_alarm_name_aws" {
  description = "CloudWatch Alarm name — fires when the CF audit exits non-zero (RED)."
  value       = aws_cloudwatch_metric_alarm.cf_manifest_audit_red.alarm_name
}
