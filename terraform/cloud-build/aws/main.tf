# Terraform configuration for AWS CodeBuild projects
# Creates build projects for all 11 services
#
# Equivalent to GCP Cloud Build triggers

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0.0"
    }
  }

  # Uncomment after setting up state bucket
  # backend "s3" {
  #   bucket         = "unified-trading-terraform-state-ACCOUNT_ID"
  #   key            = "cloud-build/terraform.tfstate"
  #   region         = "ap-northeast-1"
  #   encrypt        = true
  #   dynamodb_table = "unified-trading-terraform-locks"
  # }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "unified-trading"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name

  # Canonical live CodeBuild set — reconciled 1:1 with workspace-manifest.json live repos (2026-06-19).
  # Replaces the stale list (archived per-family features-* / mis-named execution-services) that matched
  # NEITHER the live AWS projects NOR GCP. The live projects were created imperatively (see § "terraform import"
  # in plans/active/test_fleet_image_builds_from_current_code_2026_06_17.md) — apply/import against THIS set.
  #
  # build_branch mirrors the GCP firing model exactly: the base library fires on live-defi-rollout, every
  # deployable service fires on main (services build on promotion, like GCP's `^main$` triggers). ECR repo and
  # GitHub repo both equal the map key; the canonical buildspec.aws.yaml derives the ECR repo via basename.
  services = {
    # Base library — fires on live-defi-rollout (GCP parity: base image republished on LDR push)
    "unified-trading-library" = { build_timeout = 45, build_branch = "live-defi-rollout" }

    # Deployable services — fire on main (GCP parity: services build on promotion to main)
    "alerting-service"                  = { build_timeout = 30, build_branch = "main" }
    "batch-live-reconciliation-service" = { build_timeout = 30, build_branch = "main" }
    "client-reporting-api"              = { build_timeout = 30, build_branch = "main" }
    "deployment-api"                    = { build_timeout = 30, build_branch = "main" }
    "deployment-service"                = { build_timeout = 30, build_branch = "main" }
    "deployment-ui"                     = { build_timeout = 30, build_branch = "main" }
    "execution-service"                 = { build_timeout = 45, build_branch = "main" }
    "features-service"                  = { build_timeout = 30, build_branch = "main" }
    "fund-administration-service"       = { build_timeout = 30, build_branch = "main" }
    "greeks-service"                    = { build_timeout = 30, build_branch = "main" }
    "instruments-service"               = { build_timeout = 30, build_branch = "main" }
    "market-data-processing-service"    = { build_timeout = 30, build_branch = "main" }
    "market-tick-data-service"          = { build_timeout = 30, build_branch = "main" }
    "ml-service"                        = { build_timeout = 45, build_branch = "main" }
    "strategy-service"                  = { build_timeout = 30, build_branch = "main" }
    "trading-agent-service"             = { build_timeout = 30, build_branch = "main" }
  }
}

# =============================================================================
# IAM Role for CodeBuild
# =============================================================================

resource "aws_iam_role" "codebuild_role" {
  name = "unified-trading-codebuild-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "codebuild.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "codebuild_policy" {
  name = "unified-trading-codebuild-policy"
  role = aws_iam_role.codebuild_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:GetRepositoryPolicy",
          "ecr:DescribeRepositories",
          "ecr:ListImages",
          "ecr:DescribeImages",
          "ecr:BatchGetImage",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload",
          "ecr:PutImage"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:github-token*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject"
        ]
        Resource = [
          "arn:aws:s3:::unified-trading-*",
          "arn:aws:s3:::unified-trading-*/*"
        ]
      }
    ]
  })
}

# =============================================================================
# GitHub Connection (CodeStar)
# =============================================================================

resource "aws_codestarconnections_connection" "github" {
  name          = "unified-trading-github"
  provider_type = "GitHub"

  # Note: After creating, you must manually authorize the connection in AWS Console
  # This is a one-time setup step
}

# =============================================================================
# CodeBuild Projects
# =============================================================================

resource "aws_codebuild_project" "services" {
  for_each = local.services

  name          = each.key
  description   = "Build and push Docker image for ${each.key}"
  build_timeout = each.value.build_timeout
  service_role  = aws_iam_role.codebuild_role.arn

  artifacts {
    type = "NO_ARTIFACTS"
  }

  environment {
    compute_type                = var.compute_type
    image                       = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
    type                        = "LINUX_CONTAINER"
    privileged_mode             = true  # Required for Docker builds
    image_pull_credentials_type = "CODEBUILD"

    environment_variable {
      name  = "AWS_ACCOUNT_ID"
      value = local.account_id
    }

    environment_variable {
      name  = "AWS_DEFAULT_REGION"
      value = local.region
    }

    environment_variable {
      name  = "CLOUD_BUILD"
      value = "true"
    }

    environment_variable {
      name  = "CLOUD_PROVIDER"
      value = "aws"
    }

    # Buildspec reads github-pat + unified-trading/github-actions-sa-key directly via the AWS CLI;
    # GH_PAT here gates the optional post_build deploy-dispatch (see templates/buildspec.aws.yaml).
    environment_variable {
      name  = "GH_PAT"
      value = "GH_PAT"
      type  = "SECRETS_MANAGER"
    }
  }

  source {
    type            = "GITHUB"
    location        = "https://github.com/${var.github_owner}/${each.key}.git"
    git_clone_depth = 1
    buildspec       = "buildspec.aws.yaml"

    git_submodules_config {
      fetch_submodules = false
    }
  }

  # Branch name (not a regex) — GCP-parity firing: base lib on live-defi-rollout, services on main.
  source_version = each.value.build_branch

  logs_config {
    cloudwatch_logs {
      group_name  = "/codebuild/unified-trading"
      stream_name = each.key
    }
  }

  tags = {
    Service = each.key
  }
}

# =============================================================================
# CodeBuild Webhooks (triggered on push to main)
# =============================================================================

resource "aws_codebuild_webhook" "services" {
  for_each = local.services

  project_name = aws_codebuild_project.services[each.key].name
  build_type   = "BUILD"

  filter_group {
    filter {
      type    = "EVENT"
      pattern = "PUSH"
    }

    filter {
      type    = "HEAD_REF"
      pattern = "^refs/heads/${each.value.build_branch}$"
    }
  }
}
