# Terraform configuration for UCI Cloud Abstraction — AWS bootstrap
# Creates S3 buckets, SQS queues, Athena workgroup, Glue databases/crawlers,
# Secrets Manager stubs, IAM role, and ECS cluster.
#
# Bucket naming follows cloud-providers.yaml two-tier model:
#   Group A (raw data)    — no env suffix; all envs share prod-level copy
#   Group B (derived data)— unified-trading-{domain}-{category}-{env}-{account_id}
#
# Run ONCE per environment before deploying any services to AWS.
# See scripts/bootstrap/bootstrap_aws.sh for idempotent setup.

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }

  backend "s3" {
    # bucket is interpolated at init time via -backend-config or env vars.
    # The bootstrap script passes:
    #   -backend-config="bucket=${BUCKET_PREFIX}-terraform-state-${ACCOUNT_ID}"
    key    = "terraform/state"
    region = "ap-northeast-1"  # Tokyo — closest to Binance exchange
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "unified-trading"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ---------------------------------------------------------------------------
# S3 Buckets — Group A: Raw data (no env suffix; all envs read from same copy)
# Naming: unified-trading-{domain}-{category}-{account_id}
# ---------------------------------------------------------------------------

locals {
  # Group A: raw data buckets — no env suffix, shared prod copy
  group_a_buckets = [
    "unified-trading-instruments-cefi-${var.aws_account_id}",
    "unified-trading-instruments-tradfi-${var.aws_account_id}",
    "unified-trading-instruments-defi-${var.aws_account_id}",
    "unified-trading-market-data-cefi-${var.aws_account_id}",
    "unified-trading-market-data-tradfi-${var.aws_account_id}",
    "unified-trading-market-data-defi-${var.aws_account_id}",
    "unified-trading-features-calendar-${var.aws_account_id}",
  ]

  # Group B: derived data buckets — per-env
  group_b_buckets = [
    "unified-trading-features-delta-one-cefi-${var.environment}-${var.aws_account_id}",
    "unified-trading-features-delta-one-tradfi-${var.environment}-${var.aws_account_id}",
    "unified-trading-features-delta-one-defi-${var.environment}-${var.aws_account_id}",
    "unified-trading-features-volatility-cefi-${var.environment}-${var.aws_account_id}",
    "unified-trading-features-volatility-tradfi-${var.environment}-${var.aws_account_id}",
    "unified-trading-features-onchain-cefi-${var.environment}-${var.aws_account_id}",
    "unified-trading-features-onchain-defi-${var.environment}-${var.aws_account_id}",
    "unified-trading-ml-models-${var.environment}-${var.aws_account_id}",
    "unified-trading-ml-predictions-${var.environment}-${var.aws_account_id}",
    "unified-trading-ml-configs-${var.environment}-${var.aws_account_id}",
    "unified-trading-strategy-cefi-${var.environment}-${var.aws_account_id}",
    "unified-trading-strategy-tradfi-${var.environment}-${var.aws_account_id}",
    "unified-trading-strategy-defi-${var.environment}-${var.aws_account_id}",
    "unified-trading-execution-cefi-${var.environment}-${var.aws_account_id}",
    "unified-trading-execution-tradfi-${var.environment}-${var.aws_account_id}",
    "unified-trading-execution-defi-${var.environment}-${var.aws_account_id}",
    # Deployment state + Athena results
    "${var.bucket_prefix}-${var.environment}-deployment-state",
  ]

  all_buckets = concat(local.group_a_buckets, local.group_b_buckets)

  # SQS topic names (mirroring GCP InternalPubSubTopic enum)
  sqs_topic_names = [
    "fill-events",
    "order-requests",
    "execution-results",
    "position-updates",
    "positions",
    "risk-alerts",
    "margin-warnings",
    "market-ticks",
    "order-book-updates",
    "derivative-tickers",
    "liquidations",
    "feature-updates",
    "strategy-signals",
    "ml-predictions",
    "service-lifecycle-events",
    "health-alerts",
    "circuit-breaker-events",
    "eod-settlement",
    "cascade-predictions",
    "features-mtf-ready",
    "features-delta-one-ready",
    "features-cross-instrument-ready",
    "sports-odds-ready",
    "secret-rotation-alerts",
  ]

  # Static secrets (env-independent)
  static_secret_names = [
    "tardis-api-key",
    "databento-api-key",
    "thegraph-api-key",
    "alchemy-api-key",
    "hyperliquid-aws-s3",
    "binance-read-api-key",
    "deribit-read-api-key",
    "betfair-app-key",
    "odds-api-key",
    "oddsjam-api-key",
    "opticodds-api-key",
    "metabet-api-key",
    "polymarket-private-key",
    "coinglass-api-key",
    "hyblock-api-key",
    "cryptoquant-api-key",
    "binance-write-api-key",
    "deribit-write-api-key",
    "anthropic-api-key",
    "pagerduty-api-key",
  ]

  # Env-scoped secrets
  env_secret_names = [
    "risk-api-key",
    "position-monitor-api-key",
  ]

  deployment_state_bucket = "${var.bucket_prefix}-${var.environment}-deployment-state"
}

resource "aws_s3_bucket" "unified_trading" {
  for_each = toset(local.all_buckets)
  bucket   = each.value
  tags     = { Name = each.value }
}

resource "aws_s3_bucket_versioning" "unified_trading" {
  for_each = aws_s3_bucket.unified_trading
  bucket   = each.value.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "unified_trading" {
  for_each = aws_s3_bucket.unified_trading
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "unified_trading" {
  for_each = aws_s3_bucket.unified_trading
  bucket   = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# SQS FIFO Queues — one per topic (mirrors GCP Pub/Sub topics)
# Naming: unified-trading-{env}-{topic}.fifo
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "unified_trading" {
  for_each = toset(local.sqs_topic_names)

  name                        = "unified-trading-${var.environment}-${each.value}.fifo"
  fifo_queue                  = true
  content_based_deduplication = true

  # 7-day visibility + retention to mirror GCP 7-day retention
  visibility_timeout_seconds  = 60
  message_retention_seconds   = 604800

  tags = {
    Name    = "unified-trading-${var.environment}-${each.value}"
    Purpose = "event-bus"
  }
}

# Dead-letter queues for each topic
resource "aws_sqs_queue" "unified_trading_dlq" {
  for_each = toset(local.sqs_topic_names)

  name                        = "unified-trading-${var.environment}-${each.value}-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  message_retention_seconds   = 1209600  # 14 days for DLQ

  tags = {
    Name    = "unified-trading-${var.environment}-${each.value}-dlq"
    Purpose = "event-bus-dlq"
  }
}

resource "aws_sqs_queue_redrive_policy" "unified_trading" {
  for_each  = aws_sqs_queue.unified_trading
  queue_url = each.value.id
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.unified_trading_dlq[each.key].arn
    maxReceiveCount     = 5
  })
}

# ---------------------------------------------------------------------------
# Athena — workgroup (BigQuery equivalent for external table queries)
# ---------------------------------------------------------------------------

resource "aws_athena_workgroup" "unified_trading" {
  name = "unified-trading-${var.environment}"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${local.deployment_state_bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = { Name = "unified-trading-${var.environment}" }
}

# ---------------------------------------------------------------------------
# Glue Catalog Databases — mirrors BigQuery datasets
# Group A (raw): no env suffix; Group B (derived): per-env
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "raw" {
  for_each = toset([
    "instruments",
    "market_data",
    "features_calendar",
  ])

  name        = "${replace(var.bucket_prefix, "-", "_")}_${each.value}"
  description = "Unified Trading — ${each.value} (raw data, shared across envs)"
}

resource "aws_glue_catalog_database" "derived" {
  for_each = toset([
    "features",
    "ml_models",
    "ml_predictions",
    "strategy",
    "execution",
    "audit",
    "market_data_hft",
  ])

  name        = "${replace(var.bucket_prefix, "-", "_")}_${each.value}_${var.environment}"
  description = "Unified Trading — ${each.value} (${var.environment})"
}

# ---------------------------------------------------------------------------
# Glue IAM Role — for crawlers
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_crawler" {
  name               = "unified-trading-glue-crawler-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  tags               = { Name = "unified-trading-glue-crawler-${var.environment}" }
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    sid     = "S3BucketAccess"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = concat(
      [for b in aws_s3_bucket.unified_trading : b.arn],
      [for b in aws_s3_bucket.unified_trading : "${b.arn}/*"],
    )
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "glue-s3-access"
  role   = aws_iam_role.glue_crawler.id
  policy = data.aws_iam_policy_document.glue_s3_access.json
}

# ---------------------------------------------------------------------------
# Glue Crawlers — one per raw-data domain (Group A)
# Group B tables are written by services with schema — crawlers optional
# ---------------------------------------------------------------------------

resource "aws_glue_crawler" "instruments" {
  database_name = aws_glue_catalog_database.raw["instruments"].name
  name          = "unified-trading-instruments-crawler"
  role          = aws_iam_role.glue_crawler.arn

  dynamic "s3_target" {
    for_each = ["cefi", "tradfi", "defi"]
    content {
      path = "s3://unified-trading-instruments-${s3_target.value}-${var.aws_account_id}/"
    }
  }

  schedule = "cron(0 3 * * ? *)"  # Daily at 03:00 UTC
  tags     = { Name = "unified-trading-instruments-crawler" }
}

resource "aws_glue_crawler" "market_data" {
  database_name = aws_glue_catalog_database.raw["market_data"].name
  name          = "unified-trading-market-data-crawler"
  role          = aws_iam_role.glue_crawler.arn

  dynamic "s3_target" {
    for_each = ["cefi", "tradfi", "defi"]
    content {
      path = "s3://unified-trading-market-data-${s3_target.value}-${var.aws_account_id}/"
    }
  }

  schedule = "cron(0 4 * * ? *)"  # Daily at 04:00 UTC
  tags     = { Name = "unified-trading-market-data-crawler" }
}

resource "aws_glue_crawler" "features_derived" {
  database_name = aws_glue_catalog_database.derived["features"].name
  name          = "unified-trading-features-${var.environment}-crawler"
  role          = aws_iam_role.glue_crawler.arn

  dynamic "s3_target" {
    for_each = [
      "unified-trading-features-delta-one-cefi-${var.environment}-${var.aws_account_id}",
      "unified-trading-features-delta-one-tradfi-${var.environment}-${var.aws_account_id}",
      "unified-trading-features-volatility-cefi-${var.environment}-${var.aws_account_id}",
      "unified-trading-features-volatility-tradfi-${var.environment}-${var.aws_account_id}",
    ]
    content {
      path = "s3://${s3_target.value}/"
    }
  }

  schedule = "cron(0 5 * * ? *)"  # Daily at 05:00 UTC
  tags     = { Name = "unified-trading-features-${var.environment}-crawler" }
}

# ---------------------------------------------------------------------------
# Secrets Manager — stub secrets (values populated out-of-band)
# ---------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "static" {
  for_each    = toset(local.static_secret_names)
  name        = "unified-trading/${var.environment}/${each.value}"
  description = "Unified Trading ${var.environment} — ${each.value} (populate manually)"
  tags        = { Name = each.value }
}

resource "aws_secretsmanager_secret" "env_scoped" {
  for_each    = toset(local.env_secret_names)
  name        = "unified-trading/${var.environment}/${each.value}-${var.environment}"
  description = "Unified Trading ${var.environment} — ${each.value} (populate manually)"
  tags        = { Name = "${each.value}-${var.environment}" }
}

# ---------------------------------------------------------------------------
# IAM role — unified-trading-role
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "unified_trading" {
  name               = "unified-trading-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
  tags               = { Name = "unified-trading-role-${var.environment}" }
}

data "aws_iam_policy_document" "unified_trading_permissions" {
  # S3 access on all unified-trading buckets
  statement {
    sid    = "S3UnifiedTradingBuckets"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = concat(
      [for b in aws_s3_bucket.unified_trading : b.arn],
      [for b in aws_s3_bucket.unified_trading : "${b.arn}/*"],
    )
  }

  # Athena query execution
  statement {
    sid    = "AthenaQueryExecution"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryExecution",
    ]
    resources = [aws_athena_workgroup.unified_trading.arn]
  }

  # Glue catalog read (for Athena external table queries)
  statement {
    sid    = "GlueCatalogRead"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = ["*"]
  }

  # SQS access
  statement {
    sid    = "SQSUnifiedTrading"
    effect = "Allow"
    actions = [
      "sqs:SendMessage",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
    ]
    resources = concat(
      [for q in aws_sqs_queue.unified_trading : q.arn],
      [for q in aws_sqs_queue.unified_trading_dlq : q.arn],
    )
  }

  # Secrets Manager — read unified-trading secrets only
  statement {
    sid     = "SecretsManagerUnifiedTrading"
    effect  = "Allow"
    actions = ["secretsmanager:GetSecretValue"]
    resources = concat(
      [for s in aws_secretsmanager_secret.static : s.arn],
      [for s in aws_secretsmanager_secret.env_scoped : s.arn],
    )
  }
}

resource "aws_iam_policy" "unified_trading" {
  name        = "unified-trading-policy-${var.environment}"
  description = "Permissions for unified-trading services (${var.environment})"
  policy      = data.aws_iam_policy_document.unified_trading_permissions.json
}

resource "aws_iam_role_policy_attachment" "unified_trading" {
  role       = aws_iam_role.unified_trading.name
  policy_arn = aws_iam_policy.unified_trading.arn
}

# ---------------------------------------------------------------------------
# ECS cluster
# ---------------------------------------------------------------------------

resource "aws_ecs_cluster" "unified_trading" {
  name = "unified-trading-${var.environment}"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = { Name = "unified-trading-${var.environment}" }

  # NOTE: ECS task definitions are intentionally absent here.
  # See ARCHITECTURE.md "Deployment Model" section for rationale.
  # ECS task definitions are submitted at runtime by backends/aws_batch.py.
}
