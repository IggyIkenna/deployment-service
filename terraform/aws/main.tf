# Terraform configuration for UCI Cloud Abstraction — AWS bootstrap
# Creates S3 buckets, Athena workgroup/databases, Secrets Manager stubs,
# IAM role, and ECS cluster mirroring the GCP shared infrastructure.
#
# Run ONCE before deploying any services to AWS.
# See scripts/bootstrap/bootstrap_aws.sh for idempotent setup.

terraform {
  required_version = ">= 1.6"

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
    region = "us-east-1"  # override via -backend-config="region=..."
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
# S3 Buckets
# ---------------------------------------------------------------------------

locals {
  bucket_names = [
    "market-data",
    "models",
    "features",
    "deployment-state",
  ]
}

resource "aws_s3_bucket" "unified_trading" {
  for_each = toset(local.bucket_names)

  bucket = "${var.bucket_prefix}-${var.environment}-${each.value}"

  tags = {
    Name = "${var.bucket_prefix}-${var.environment}-${each.value}"
  }
}

resource "aws_s3_bucket_versioning" "unified_trading" {
  for_each = aws_s3_bucket.unified_trading

  bucket = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "unified_trading" {
  for_each = aws_s3_bucket.unified_trading

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "unified_trading" {
  for_each = aws_s3_bucket.unified_trading

  bucket = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# Athena — workgroup and databases (mirroring BigQuery datasets)
# ---------------------------------------------------------------------------

resource "aws_athena_workgroup" "unified_trading" {
  name = "unified-trading-${var.environment}"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.unified_trading["deployment-state"].bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = {
    Name = "unified-trading-${var.environment}"
  }
}

resource "aws_glue_catalog_database" "unified_trading" {
  for_each = toset([
    "market_data",
    "features",
    "ml_models",
    "audit",
  ])

  name        = "${replace(var.bucket_prefix, "-", "_")}_${each.value}_${var.environment}"
  description = "Unified Trading — ${each.value} database (${var.environment})"
}

# ---------------------------------------------------------------------------
# Secrets Manager — stub secrets (values populated out-of-band)
# ---------------------------------------------------------------------------

locals {
  secret_names = [
    "tardis-api-key",
    "databento-api-key",
    "thegraph-api-key",
    "alchemy-api-key",
    "hyperliquid-aws-s3",
    "binance-read-api-key",
    "deribit-read-api-key",
  ]
}

resource "aws_secretsmanager_secret" "unified_trading" {
  for_each = toset(local.secret_names)

  name        = "unified-trading/${var.environment}/${each.value}"
  description = "Unified Trading ${var.environment} — ${each.value} (populate manually)"

  tags = {
    Name = each.value
  }
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

  tags = {
    Name = "unified-trading-role-${var.environment}"
  }
}

data "aws_iam_policy_document" "unified_trading_permissions" {
  # S3 access on unified-trading buckets
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

    resources = [
      aws_athena_workgroup.unified_trading.arn,
    ]
  }

  # Secrets Manager — read unified-trading secrets only
  statement {
    sid    = "SecretsManagerUnifiedTrading"
    effect = "Allow"

    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      for s in aws_secretsmanager_secret.unified_trading :
      s.arn
    ]
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

  tags = {
    Name = "unified-trading-${var.environment}"
  }

  # TODO (p4): Add ECS task definitions per service.
  # Each service registers its own task definition via the deployment-api.
  # Template: terraform/modules/container-job/aws/
}
