# Terraform configuration for AWS CodeBuild projects
# Creates build projects for all 18 live services (1:1 with locals.services below)
#
# Equivalent to GCP Cloud Build triggers
#
# =============================================================================
# ⛔ DO NOT `terraform apply` THIS MODULE — NOT APPLY-CLEAN (measured 2026-07-30)
# =============================================================================
# The 18 CodeBuild projects + the IAM role/policy + the CodeArtifact domain/repo were all created
# IMPERATIVELY (out-of-band), never from this module. The S3 backend below was stood up 2026-07-30
# and is live, but the state is deliberately left EMPTY — the import was NOT completed, because a
# full dry-run import into a throwaway local state measured, against this exact file:
#
#     Plan: 19 to add, 22 to change, 0 to destroy
#
# (As first measured it was 20/21/1 — the aws_iam_role_policy rename below removed the destroy.)
# Leaving the state empty is the safety property: with no state, an accidental `apply` fails fast
# on already-exists instead of silently converging live CI onto the diffs listed here.
#
# Several of those diffs would BREAK live CI for all 18 repos if applied. The blocking ones:
#
#  1. `aws_iam_role_policy.codebuild_policy` — in-place UPDATE of the policy body. This block
#     narrows secretsmanager:GetSecretValue from the live `secret:*` to `secret:github-token*`. Every
#     project below injects `GH_PAT` as a SECRETS_MANAGER env var, and the buildspecs also read
#     `github-pat` + `unified-trading/github-actions-sa-key` — NONE of which match `github-token*`.
#     Applying would revoke secret access and fail every build at start. It also drops the live
#     `ecr:CreateRepository` grant.
#  2. `aws_codebuild_webhook.services` (18) — CREATE. ZERO webhooks exist live, on the CodeBuild
#     side or the GitHub side (verified across all 18 repos, 2026-07-30). Builds are dispatched by
#     the GitHub Actions router (`aws codebuild start-build`), NOT by push webhooks. Creating these
#     would switch on a second, duplicate trigger path for every repo.
#  3. `market-tick-data-service` + `unified-trading-library` — the two heavy base builds run live on
#     BUILD_GENERAL1_LARGE / aws/codebuild/standard:7.0. This module would downgrade both to
#     BUILD_GENERAL1_MEDIUM / amazonlinux2-x86_64-standard:5.0.
#  4. `build_timeout` — live is 60 min on 16 of 18 projects; this module would cut them to 30/45.
#
# The rest of the 22 changes are cosmetic (description, `Service`/`Project`/`Environment`/`ManagedBy`
# default_tags, git_clone_depth, logs_config) plus two that look like LIVE is the side that drifted:
# `unified-trading-library` has no source_version at all (should be live-defi-rollout) and
# `instruments-service` still points at the pre-canonical `buildspec.yml`.
#
# Resolving each diff is a per-attribute "is live right, or is this file right?" decision (it is
# genuinely a mix of both) and is operator-gated — it is NOT determinable from the code alone.
# Until it is resolved, treat this module as documentation, not as an executable SSOT.
#
# Full per-attribute drift inventory + the decision list:
#   /plans/active/issues/aws_codebuild_terraform_import_pending_2026_07_22.md
# =============================================================================

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0.0"
    }
  }

  # State backend — stood up 2026-07-30.
  #
  # `bucket` is the state bucket that ALREADY EXISTS and already holds this estate's real state
  # (terraform/state, .../dev, .../prod, .../staging) — `uts-terraform-state-<account_id>`, the
  # `uts-terraform-state-{project_id}` template in configs/bucket_config.yaml. The name previously
  # stubbed here (`unified-trading-terraform-state-ACCOUNT_ID`) has NEVER existed in this account;
  # pointing at it would have forked the convention into a second state bucket. `key` is distinct
  # from every existing key, so this module gets its own isolated state file.
  backend "s3" {
    bucket         = "uts-terraform-state-427895769566"
    key            = "cloud-build/terraform.tfstate"
    region         = "ap-northeast-1"
    encrypt        = true
    dynamodb_table = "unified-trading-terraform-locks"
  }
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
  # build_branch mirrors the GCP firing model exactly. GCP fires three triggers on live-defi-rollout —
  # unified-trading-library (base image), unified-api-contracts (base wheel) and market-tick-data-service
  # (which also has a `-build` main trigger) — and every other service on `^main$`. ECR repo and GitHub repo
  # both equal the map key; the canonical buildspec.aws.yaml derives the ECR repo via basename.
  #
  # unified-api-contracts builds the UAC wheel (no Docker image) and publishes it to the CodeArtifact domain
  # `unified-trading` / repo `unified-libraries` (the AWS analogue of GCP's AR python index) — see the
  # aws_codeartifact_* resources + the CodeArtifactPublish IAM statement below. It fires on live-defi-rollout
  # like the other base lib, matching GCP's UAC LDR trigger.
  services = {
    # Fire on live-defi-rollout (GCP parity: base image + UAC wheel + the mtds LDR-tip build)
    "unified-trading-library"  = { build_timeout = 45, build_branch = "live-defi-rollout" }
    "unified-api-contracts"    = { build_timeout = 30, build_branch = "live-defi-rollout" }
    "market-tick-data-service" = { build_timeout = 30, build_branch = "live-defi-rollout" }

    # Deployable services — fire on main (GCP parity: services build on promotion to main)
    "alerting-service"                  = { build_timeout = 30, build_branch = "main" }
    "batch-live-reconciliation-service" = { build_timeout = 30, build_branch = "main" }
    "client-reporting-api"              = { build_timeout = 30, build_branch = "main" }
    "deployment-api"                    = { build_timeout = 30, build_branch = "main" }
    "deployment-service"                = { build_timeout = 30, build_branch = "main" }
    # deployment-ui builds NO standalone image — its buildspec.aws.yaml dispatches a deployment-api
    # build (the SPA is bundled into deployment-api). Mirrors GCP deployment-ui-main-deploy. The
    # codebuild_role's DispatchDeploymentApiBuild statement grants the StartBuild.
    "deployment-ui"                  = { build_timeout = 30, build_branch = "main" }
    "execution-service"              = { build_timeout = 45, build_branch = "main" }
    "features-service"               = { build_timeout = 30, build_branch = "main" }
    "fund-administration-service"    = { build_timeout = 30, build_branch = "main" }
    "greeks-service"                 = { build_timeout = 30, build_branch = "main" }
    "instruments-service"            = { build_timeout = 30, build_branch = "main" }
    "market-data-processing-service" = { build_timeout = 30, build_branch = "main" }
    "ml-service"                     = { build_timeout = 45, build_branch = "main" }
    "strategy-service"               = { build_timeout = 30, build_branch = "main" }
    "trading-agent-service"          = { build_timeout = 30, build_branch = "main" }
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
  # Renamed 2026-07-30 from "unified-trading-codebuild-policy" to match the LIVE inline policy name
  # on unified-trading-codebuild-role. Direction chosen deliberately: renaming the live policy
  # instead would mean a put+delete of the inline policy on the role that all 18 active CodeBuild
  # projects assume — a live IAM mutation with a window where an in-flight build loses permissions.
  # Editing this string is zero-risk and achieves the same reconciliation, so the code moved.
  #
  # NOTE: the name now matches, but the policy BODY below still does not (see the DO-NOT-APPLY
  # banner at the top of this file — the secretsmanager scope in particular would break every
  # build). Matching the name only downgrades this resource from replace to in-place update.
  name = "codebuild-permissions"
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
      },
      {
        # deployment-ui's buildspec dispatches a deployment-api build instead of building its own
        # image (the SPA is bundled into deployment-api). This grants that dispatch.
        Sid      = "DispatchDeploymentApiBuild"
        Effect   = "Allow"
        Action   = ["codebuild:StartBuild"]
        Resource = "arn:aws:codebuild:${local.region}:${local.account_id}:project/deployment-api"
      },
      {
        # unified-api-contracts publishes its wheel to the CodeArtifact unified-libraries repo.
        Sid      = "CodeArtifactPublish"
        Effect   = "Allow"
        Action   = ["codeartifact:GetAuthorizationToken", "codeartifact:GetRepositoryEndpoint", "codeartifact:ReadFromRepository", "codeartifact:PublishPackageVersion", "codeartifact:DescribePackageVersion", "codeartifact:DescribeRepository"]
        Resource = "*"
      },
      {
        Sid      = "CodeArtifactStsBearer"
        Effect   = "Allow"
        Action   = ["sts:GetServiceBearerToken"]
        Resource = "*"
        Condition = {
          StringEquals = { "sts:AWSServiceName" = "codeartifact.amazonaws.com" }
        }
      }
    ]
  })
}

# =============================================================================
# CodeArtifact — internal Python wheel distribution (AWS analogue of GCP's AR
# python index). unified-api-contracts publishes its wheel here.
# =============================================================================

resource "aws_codeartifact_domain" "internal" {
  domain = "unified-trading"
}

resource "aws_codeartifact_repository" "libraries" {
  repository  = "unified-libraries"
  domain      = aws_codeartifact_domain.internal.domain
  description = "Internal Python wheels (UAC, etc.) — AWS analogue of the GCP AR python index"
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
    privileged_mode             = true # Required for Docker builds
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
