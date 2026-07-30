# Terraform configuration for AWS CodeBuild projects
# Manages the 18 live services (1:1 with locals.services below) + the shared CodeBuild IAM role
# and the CodeArtifact domain/repo the UAC wheel publishes to.
#
# GCP analogue: Cloud Build triggers (see terraform/cloud-build/gcp).
#
# =============================================================================
# STATE: IMPORTED + RECONCILED — this module IS apply-clean (2026-07-30)
# =============================================================================
# The 18 CodeBuild projects, the IAM role/policy and the CodeArtifact domain/repo were originally
# created IMPERATIVELY (out-of-band). On 2026-07-30 they were imported into the S3 state below and
# every per-attribute drift was ruled on with evidence; `terraform plan` is a no-op.
#
# The four rulings, so a future reader does not re-litigate them:
#
#  1. IAM policy body — ADOPT LIVE, with `secretsmanager:GetSecretValue` NARROWED from the live
#     `secret:*` to exactly the three secrets the fleet actually reads (`GH_PAT` as a
#     SECRETS_MANAGER env var on 17 projects; `github-pat` + `unified-trading/github-actions-sa-key`
#     read by the buildspecs via the AWS CLI). The previous `secret:github-token*` scope matched
#     NONE of them and would have failed every build at start. `ecr:CreateRepository` is KEPT —
#     15 buildspecs call `aws ecr create-repository`. The `logs` scope stays at
#     `log-group:/aws/codebuild/*`, which is where CodeBuild actually writes (verified live).
#
#  2. CodeBuild webhooks — REMOVED from this module. Zero webhooks exist; CloudTrail shows all 18
#     were deliberately deleted 2026-07-03, matching the operator decision that AWS image builds are
#     switched OFF (PM Actions variable `AWS_BUILDS_ENABLED`, unset = disabled; GCP Cloud Build is
#     the production path). Re-creating them would switch a retired trigger path back on.
#     /codex/05-infrastructure/dual-cloud-image-builds.md is explicit: "Do not add new PUSH webhooks".
#     `aws_codestarconnections_connection` was removed for the same reason — no live counterpart, no
#     referrer, and creating one leaves a PENDING connection needing manual console authorization.
#
#  3. Compute size / timeout / clone depth / logs / description / env vars — ADOPT LIVE. `timeout=60`
#     is the uniform creation-time value (CloudTrail CreateProject, both provisioning sweeps); no
#     build in 1,300+ inspected has ever TIMED_OUT (longest: 11.8 min). `logs_config` was dropped:
#     the module previously declared `/codebuild/unified-trading`, a group the IAM policy does not
#     authorize, so applying it would have silently killed build logging.
#
#  4. `unified-trading-library` source_version + `instruments-service` buildspec — LIVE had drifted;
#     both are now declared here and were converged. See the per-service notes in locals.services.
#
# Rulings + evidence: /plans/active/issues/aws_codebuild_terraform_import_pending_2026_07_22.md
# =============================================================================

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # Pinned to the v5 line to match every sibling module (terraform/aws pins ~> 5.82 → v5.100.0).
      # The previous open `>= 5.0.0` floated to v6.x, which renames the `aws_region` data source's
      # `name` attribute (used at locals.region) and would silently break this module on a fresh init.
      version = "~> 5.82"
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

  # ---------------------------------------------------------------------------
  # Environment-variable sets. Three real variants exist live; they are NOT drift.
  #
  #  * standard — the 15 Docker service builds.
  #  * mock     — market-tick-data-service + unified-trading-library. Their buildspecs run the
  #               quality gates inside the built image and pass CLOUD_BUILD/CLOUD_MOCK_MODE to
  #               `docker run` explicitly, so the project-level var is CLOUD_MOCK_MODE, not
  #               CLOUD_BUILD. Ordering mirrors live.
  #  * wheel    — unified-api-contracts. Deliberately has NO GH_PAT: it builds and publishes only
  #               its own wheel (no repo clones), and the previous `secrets-manager` binding
  #               JSON-parsed the plain-string `github-pat` secret and hard-failed the build at
  #               DOWNLOAD_SOURCE ("invalid character 'g'"). See its buildspec.aws.yaml header.
  # ---------------------------------------------------------------------------
  env_standard = [
    { name = "AWS_ACCOUNT_ID", value = data.aws_caller_identity.current.account_id, type = "PLAINTEXT" },
    { name = "AWS_DEFAULT_REGION", value = data.aws_region.current.name, type = "PLAINTEXT" },
    { name = "CLOUD_BUILD", value = "true", type = "PLAINTEXT" },
    { name = "CLOUD_PROVIDER", value = "aws", type = "PLAINTEXT" },
    { name = "GH_PAT", value = "GH_PAT", type = "SECRETS_MANAGER" },
  ]

  env_mock = [
    { name = "AWS_DEFAULT_REGION", value = data.aws_region.current.name, type = "PLAINTEXT" },
    { name = "AWS_ACCOUNT_ID", value = data.aws_caller_identity.current.account_id, type = "PLAINTEXT" },
    { name = "CLOUD_MOCK_MODE", value = "true", type = "PLAINTEXT" },
    { name = "CLOUD_PROVIDER", value = "aws", type = "PLAINTEXT" },
    { name = "GH_PAT", value = "GH_PAT", type = "SECRETS_MANAGER" },
  ]

  env_wheel = [
    { name = "AWS_ACCOUNT_ID", value = data.aws_caller_identity.current.account_id, type = "PLAINTEXT" },
    { name = "AWS_DEFAULT_REGION", value = data.aws_region.current.name, type = "PLAINTEXT" },
    { name = "CLOUD_PROVIDER", value = "aws", type = "PLAINTEXT" },
  ]

  # ---------------------------------------------------------------------------
  # Canonical live CodeBuild set — 1:1 with workspace-manifest.json live repos (reconciled
  # 2026-06-19, imported + attribute-reconciled 2026-07-30).
  #
  # build_branch mirrors the GCP firing model: GCP fires three triggers on live-defi-rollout —
  # unified-trading-library (base image), unified-api-contracts (base wheel) and
  # market-tick-data-service — and every other service on main. ECR repo and GitHub repo both equal
  # the map key; the canonical buildspec.aws.yaml derives the ECR repo via basename.
  #
  # Per-service keys are all explicitly present (null = "leave unset live"), because Terraform
  # requires a single object type across the for_each map.
  #   compute_type / image : LARGE + standard:7.0 on the two heavy base builds (mtds, utl); every
  #                          other project is MEDIUM + amazonlinux2:5.0. Long-standing live state
  #                          (unchanged across 600 builds each); adopted, not re-litigated.
  #   clone_depth          : null = full clone. deployment-service's buildspec runs
  #                          `git describe --tags`, which a depth-1 clone would break — do NOT
  #                          blanket-set 1.
  #   privileged           : false for unified-api-contracts (wheel build, no Docker).
  #   log_group            : only market-tick-data-service pins the CloudWatch group explicitly;
  #                          everything else uses the CodeBuild default /aws/codebuild/<project>,
  #                          which is what the IAM policy authorizes.
  # ---------------------------------------------------------------------------
  services = {
    # --- Fire on live-defi-rollout (GCP parity: base image + UAC wheel + the mtds LDR-tip build)
    #
    # unified-trading-library had NO source_version live (so it built the repo default branch,
    # `main`) while its own buildspec clones unified-api-contracts and unified-trading-pm from
    # live-defi-rollout, and GCP runs a `unified-trading-library-live-defi-rollout` trigger.
    # CloudTrail shows it was simply missed by the 2026-06-19 sourceVersion sweep that set the
    # other eight — i.e. LIVE was the side that drifted. Converged to live-defi-rollout 2026-07-30.
    "unified-trading-library" = {
      build_timeout = 60, build_branch = "live-defi-rollout", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_LARGE", image = "aws/codebuild/standard:7.0"
      privileged = true, clone_depth = 1, description = null, log_group = null, env = "mock"
    }
    "unified-api-contracts" = {
      build_timeout = 30, build_branch = "live-defi-rollout", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = false, clone_depth = null, description = null, log_group = null, env = "wheel"
    }
    "market-tick-data-service" = {
      build_timeout = 60, build_branch = "live-defi-rollout", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_LARGE", image = "aws/codebuild/standard:7.0"
      privileged = true, clone_depth = 1
      description  = "Build market-tick-data-service — runs quality gates inside Docker image then pushes to ECR"
      log_group    = "/aws/codebuild/market-tick-data-service", env = "mock"
    }

    # --- Deployable services — fire on main (GCP parity: services build on promotion to main)
    "alerting-service" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
    "batch-live-reconciliation-service" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
    "client-reporting-api" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
    "deployment-api" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
    "deployment-service" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
    # deployment-ui builds NO standalone image — its buildspec.aws.yaml dispatches a deployment-api
    # build (the SPA is bundled into deployment-api). Mirrors GCP deployment-ui-main-deploy. The
    # codebuild_role's DispatchDeploymentApiBuild statement grants the StartBuild.
    "deployment-ui" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
    "execution-service" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
    "features-service" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
    "fund-administration-service" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
    "greeks-service" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
    # instruments-service pointed at the pre-canonical `buildspec.yml` while all 17 other projects
    # use `buildspec.aws.yaml` — LIVE was the side that drifted (the canonical rollout landed the
    # file in the repo but the project was never repointed). Converged 2026-07-30, after the repo's
    # own buildspec.aws.yaml was refreshed from templates/buildspec.aws.yaml — the copy on main was
    # a stale variant binding `env.secrets-manager: gcp-sa-key:json`, a secret that does not exist
    # in this account, which would have hard-failed at DOWNLOAD_SOURCE.
    "instruments-service" = {
      build_timeout = 30, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = 1, description = null, log_group = null, env = "standard"
    }
    "market-data-processing-service" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
    "ml-service" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
    "strategy-service" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
    "trading-agent-service" = {
      build_timeout = 60, build_branch = "main", buildspec = "buildspec.aws.yaml"
      compute_type = "BUILD_GENERAL1_MEDIUM", image = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
      privileged = true, clone_depth = null, description = null, log_group = null, env = "standard"
    }
  }

  env_sets = {
    standard = local.env_standard
    mock     = local.env_mock
    wheel    = local.env_wheel
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
  # Name matches the LIVE inline policy on unified-trading-codebuild-role (renamed here 2026-07-30
  # from "unified-trading-codebuild-policy"; renaming the live policy instead would have meant a
  # put+delete on the role all 18 active projects assume — a window where an in-flight build loses
  # permissions — whereas editing this string is zero-risk and reconciles identically).
  name = "codebuild-permissions"
  role = aws_iam_role.codebuild_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # CodeBuild writes to /aws/codebuild/<project-name> (the service default). Scoped to that
        # prefix — NOT "*". A logs_config pointing anywhere else (e.g. /codebuild/unified-trading)
        # would be unauthorized and silently kill build logging.
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/codebuild/*"
      },
      {
        # GetAuthorizationToken has no resource-level permissions — must be "*".
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        # ecr:CreateRepository is REQUIRED, not vestigial: 15 buildspecs run
        # `aws ecr describe-repositories ... || aws ecr create-repository ...` in pre_build.
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:PutImage",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload",
          "ecr:CreateRepository",
          "ecr:DescribeRepositories"
        ]
        Resource = "arn:aws:ecr:${local.region}:${local.account_id}:repository/*"
      },
      {
        # Least privilege over the live `secret:*`. These are the ONLY three secrets the fleet
        # reads, enumerated from all 18 buildspecs + every project's SECRETS_MANAGER env vars:
        #   GH_PAT                                — SECRETS_MANAGER env var on 17 of 18 projects
        #   github-pat                            — `aws secretsmanager get-secret-value` in 14 buildspecs
        #   unified-trading/github-actions-sa-key — same, GCP Artifact Registry pull auth
        # The `-??????` suffix is Secrets Manager's 6-character random ARN suffix, so this survives
        # secret rotation/recreation without widening to a prefix wildcard.
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          "arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:GH_PAT-??????",
          "arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:github-pat-??????",
          "arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:unified-trading/github-actions-sa-key-??????"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:GetObjectVersion"
        ]
        Resource = "arn:aws:s3:::unified-trading-*/*"
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
# CodeBuild Projects
#
# Builds are dispatched by the GitHub Actions router (`aws codebuild start-build`), never by PUSH
# webhooks — see the ruling in the header. There is deliberately no aws_codebuild_webhook resource
# and no aws_codestarconnections_connection in this module.
# =============================================================================

resource "aws_codebuild_project" "services" {
  for_each = local.services

  name          = each.key
  description   = each.value.description
  build_timeout = each.value.build_timeout
  service_role  = aws_iam_role.codebuild_role.arn

  artifacts {
    type = "NO_ARTIFACTS"
  }

  environment {
    compute_type                = each.value.compute_type
    image                       = each.value.image
    type                        = "LINUX_CONTAINER"
    privileged_mode             = each.value.privileged
    image_pull_credentials_type = "CODEBUILD"

    dynamic "environment_variable" {
      for_each = local.env_sets[each.value.env]
      content {
        name  = environment_variable.value.name
        value = environment_variable.value.value
        type  = environment_variable.value.type
      }
    }
  }

  source {
    type                = "GITHUB"
    location            = "https://github.com/${var.github_owner}/${each.key}.git"
    git_clone_depth     = each.value.clone_depth
    buildspec           = each.value.buildspec
    report_build_status = true
  }

  # Branch name (not a regex) — GCP-parity firing: base lib on live-defi-rollout, services on main.
  source_version = each.value.build_branch

  dynamic "logs_config" {
    for_each = each.value.log_group == null ? [] : [each.value.log_group]
    content {
      cloudwatch_logs {
        status     = "ENABLED"
        group_name = logs_config.value
      }
    }
  }
}
