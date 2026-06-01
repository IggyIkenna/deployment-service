# AWS EventBridge (CloudWatch Events) Rule + Batch Target
# Equivalent to GCP Cloud Scheduler — triggers AWS Batch Jobs on a schedule
#
# Uses EventBridge Rules which natively support batch_target.
# EventBridge Scheduler (aws_scheduler_schedule) does NOT support Batch
# as a direct target in all regions; EventBridge Rules is the correct approach.

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0.0"
    }
  }
}

locals {
  # Accept rate() / cron() expressions directly; convert 5-field GCP cron otherwise
  schedule_expression = length(regexall("^(rate|cron)\\(", var.schedule)) > 0 ? var.schedule : "cron(${replace(var.schedule, "/([0-9*,/-]+) ([0-9*,/-]+) ([0-9*,/-]+) ([0-9*,/-]+) ([0-9*,/-]+)/", "$1 $2 $3 $4 ? *")})"
}

# EventBridge (CloudWatch Events) Rule
resource "aws_cloudwatch_event_rule" "schedule" {
  name                = var.name
  description         = var.description
  schedule_expression = local.schedule_expression
  state               = var.enabled ? "ENABLED" : "DISABLED"

  tags = merge(
    {
      "managed-by" = "terraform"
    },
    var.tags
  )
}

# EventBridge Batch Target
resource "aws_cloudwatch_event_target" "batch" {
  rule     = aws_cloudwatch_event_rule.schedule.name
  arn      = "arn:aws:batch:${var.region}:${data.aws_caller_identity.current.account_id}:job-queue/${var.job_queue_name}"
  role_arn = var.scheduler_role_arn

  batch_target {
    job_definition = var.job_definition_arn
    job_name       = "${var.target_job_name}-scheduled"
    job_attempts   = var.retry_count
  }
}

# Get current AWS account ID
data "aws_caller_identity" "current" {}

# Optional: Create IAM role for EventBridge to submit Batch jobs
resource "aws_iam_role" "scheduler_role" {
  count = var.create_scheduler_role ? 1 : 0
  name  = "${var.name}-events-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(
    {
      "managed-by" = "terraform"
    },
    var.tags
  )
}

resource "aws_iam_role_policy" "scheduler_batch_policy" {
  count = var.create_scheduler_role ? 1 : 0
  name  = "${var.name}-batch-submit"
  role  = aws_iam_role.scheduler_role[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "batch:SubmitJob"
        ]
        Resource = [
          var.job_definition_arn,
          "arn:aws:batch:${var.region}:${data.aws_caller_identity.current.account_id}:job-queue/${var.job_queue_name}"
        ]
      }
    ]
  })
}
