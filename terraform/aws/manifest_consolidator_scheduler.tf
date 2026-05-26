# Manifest Consolidator — AWS Batch (Fargate) + EventBridge Scheduler
#
# Port of GCP Cloud Run + Cloud Scheduler consolidator to AWS.
# Runs `python -m unified_trading_library.manifest_consolidator --bucket {X} --once`
# once per minute per bucket. CLOUD_PROVIDER=aws routes get_storage_client() to S3.
#
# Gate: aws_migration_defi_first_2026_05_07 Phase 5+6 complete (archived 2026-05-26).
# Plan: unified-trading-pm/plans/active/aws_manifest_consolidator_scope_2026_05_21.md

locals {
  # Phase A — core buckets (Group A naming: no env suffix, all envs share prod copy)
  manifest_consolidator_buckets_aws = {
    "instruments-cefi"       = "unified-trading-instruments-cefi-${var.aws_account_id}"
    "instruments-tradfi"     = "unified-trading-instruments-tradfi-${var.aws_account_id}"
    "instruments-defi"       = "unified-trading-instruments-defi-${var.aws_account_id}"
    "instruments-sports"     = "unified-trading-instruments-sports-${var.aws_account_id}"
    "instruments-prediction" = "unified-trading-instruments-prediction-${var.aws_account_id}"
    "market-data-cefi"       = "unified-trading-market-data-cefi-${var.aws_account_id}"
    "market-data-tradfi"     = "unified-trading-market-data-tradfi-${var.aws_account_id}"
    "market-data-defi"       = "unified-trading-market-data-defi-${var.aws_account_id}"
    "market-data-sports"     = "unified-trading-market-data-sports-${var.aws_account_id}"
    "market-data-prediction" = "unified-trading-market-data-prediction-${var.aws_account_id}"
  }

  # Phase D — derived-data buckets (Group B naming: env-scoped)
  # Uncomment and apply after Phase C is green.
  # manifest_consolidator_buckets_aws_extended = {
  #   "features-delta-one-cefi"      = "unified-trading-features-delta-one-cefi-${var.environment}-${var.aws_account_id}"
  #   "features-delta-one-tradfi"    = "unified-trading-features-delta-one-tradfi-${var.environment}-${var.aws_account_id}"
  #   "features-delta-one-defi"      = "unified-trading-features-delta-one-defi-${var.environment}-${var.aws_account_id}"
  #   "features-volatility-cefi"     = "unified-trading-features-volatility-cefi-${var.environment}-${var.aws_account_id}"
  #   "features-volatility-tradfi"   = "unified-trading-features-volatility-tradfi-${var.environment}-${var.aws_account_id}"
  #   "features-onchain-defi"        = "unified-trading-features-onchain-defi-${var.environment}-${var.aws_account_id}"
  #   "features-onchain-cefi"        = "unified-trading-features-onchain-cefi-${var.environment}-${var.aws_account_id}"
  #   "features-sports"              = "unified-trading-features-sports-${var.environment}-${var.aws_account_id}"
  #   "features-calendar"            = "unified-trading-features-calendar-${var.aws_account_id}"
  #   "strategy-cefi"                = "unified-trading-strategy-cefi-${var.environment}-${var.aws_account_id}"
  #   "strategy-tradfi"              = "unified-trading-strategy-tradfi-${var.environment}-${var.aws_account_id}"
  #   "strategy-defi"                = "unified-trading-strategy-defi-${var.environment}-${var.aws_account_id}"
  #   "execution-cefi"               = "unified-trading-execution-cefi-${var.environment}-${var.aws_account_id}"
  #   "execution-tradfi"             = "unified-trading-execution-tradfi-${var.environment}-${var.aws_account_id}"
  #   "execution-defi"               = "unified-trading-execution-defi-${var.environment}-${var.aws_account_id}"
  #   "ml-models"                    = "unified-trading-ml-models-${var.environment}-${var.aws_account_id}"
  # }

  mtds_ecr_image = "${var.aws_account_id}.dkr.ecr.${var.aws_region}.amazonaws.com/market-tick-data-service:latest"
}

# ---------------------------------------------------------------------------
# VPC — use default VPC (same account pattern as elasticache section)
# ---------------------------------------------------------------------------

data "aws_vpc" "consolidator" {
  default = true
}

data "aws_subnets" "consolidator" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.consolidator.id]
  }
}

resource "aws_security_group" "manifest_consolidator" {
  name        = "uts-manifest-consolidator-${var.environment}"
  description = "Egress-only SG for manifest consolidator Fargate tasks"
  vpc_id      = data.aws_vpc.consolidator.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "uts-manifest-consolidator-${var.environment}"
  }
}

# ---------------------------------------------------------------------------
# Batch service role (required for managed compute environments)
# ---------------------------------------------------------------------------

resource "aws_iam_role" "batch_service_role" {
  name = "uts-manifest-consolidator-batch-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "batch.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "batch_service_role" {
  role       = aws_iam_role.batch_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

# ---------------------------------------------------------------------------
# Shared Batch infrastructure (one compute env + one job queue)
# ---------------------------------------------------------------------------

resource "aws_batch_compute_environment" "manifest_consolidator" {
  compute_environment_name = "uts-prod-manifest-consolidator"
  type                     = "MANAGED"
  state                    = "ENABLED"
  service_role             = aws_iam_role.batch_service_role.arn

  compute_resources {
    type               = "FARGATE"
    max_vcpus          = 256
    subnets            = data.aws_subnets.consolidator.ids
    security_group_ids = [aws_security_group.manifest_consolidator.id]
  }

  depends_on = [aws_iam_role_policy_attachment.batch_service_role]
}

resource "aws_batch_job_queue" "manifest_consolidator" {
  name     = "uts-prod-manifest-consolidator"
  state    = "ENABLED"
  priority = 1

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.manifest_consolidator.arn
  }
}

# ---------------------------------------------------------------------------
# EventBridge schedule group
# ---------------------------------------------------------------------------

resource "aws_scheduler_schedule_group" "manifest_consolidator" {
  name = "uts-prod-consolidator"
}

# ---------------------------------------------------------------------------
# EventBridge scheduler IAM role
# ---------------------------------------------------------------------------

resource "aws_iam_role" "manifest_consolidator_scheduler" {
  name = "uts-manifest-consolidator-scheduler-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "manifest_consolidator_scheduler_batch" {
  name = "batch-submit"
  role = aws_iam_role.manifest_consolidator_scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["batch:SubmitJob"]
      Resource = [
        aws_batch_job_queue.manifest_consolidator.arn,
        "arn:aws:batch:${var.aws_region}:${var.aws_account_id}:job-definition/uts-prod-manifest-consolidator-*",
      ]
    }]
  })
}

# ---------------------------------------------------------------------------
# S3 IAM policy — attached to the existing unified_trading execution role
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "manifest_consolidator" {
  statement {
    sid    = "ManifestConsolidatorS3"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = concat(
      [for b in local.manifest_consolidator_buckets_aws : "arn:aws:s3:::${b}"],
      [for b in local.manifest_consolidator_buckets_aws : "arn:aws:s3:::${b}/*"],
    )
  }
}

resource "aws_iam_policy" "manifest_consolidator" {
  name   = "uts-manifest-consolidator-s3-${var.environment}"
  policy = data.aws_iam_policy_document.manifest_consolidator.json
}

resource "aws_iam_role_policy_attachment" "manifest_consolidator_unified_trading" {
  role       = aws_iam_role.unified_trading.name
  policy_arn = aws_iam_policy.manifest_consolidator.arn
}

# ---------------------------------------------------------------------------
# Per-bucket: Batch job definition + EventBridge schedule
# ---------------------------------------------------------------------------

module "manifest_consolidator_job" {
  for_each = local.manifest_consolidator_buckets_aws
  source   = "../modules/container-job/aws"

  name            = "uts-prod-manifest-consolidator-${each.key}"
  image           = local.mtds_ecr_image
  vcpus           = "0.25"
  memory_mb       = "512"
  timeout_seconds = 60
  max_retries     = 2

  execution_role_arn = aws_iam_role.unified_trading.arn
  job_role_arn       = aws_iam_role.unified_trading.arn

  environment_variables = {
    CLOUD_PROVIDER     = "aws"
    MANIFEST_BUCKET    = each.value
    AWS_DEFAULT_REGION = var.aws_region
  }

  command = ["python", "-m", "unified_trading_library.manifest_consolidator", "--bucket", each.value, "--once"]

  assign_public_ip = true
  subnet_ids       = data.aws_subnets.consolidator.ids

  create_compute_environment = false
  compute_environment_arn    = aws_batch_compute_environment.manifest_consolidator.arn
  create_job_queue           = false
  job_queue_arn              = aws_batch_job_queue.manifest_consolidator.arn

  service_name = "manifest-consolidator"
  environment  = var.environment

  tags = {
    bucket = each.value
  }
}

module "manifest_consolidator_schedule" {
  for_each = local.manifest_consolidator_buckets_aws
  source   = "../modules/scheduler/aws"

  name                = "uts-prod-consolidator-${each.key}"
  description         = "Manifest consolidator — ${each.value}"
  schedule            = "*/1 * * * *"
  region              = var.aws_region
  schedule_group_name = aws_scheduler_schedule_group.manifest_consolidator.name
  enabled             = true

  target_job_name    = "uts-prod-manifest-consolidator-${each.key}"
  job_definition_arn = module.manifest_consolidator_job[each.key].arn
  job_queue_name     = aws_batch_job_queue.manifest_consolidator.name
  scheduler_role_arn = aws_iam_role.manifest_consolidator_scheduler.arn

  create_scheduler_role = false
  create_schedule_group = false
}
