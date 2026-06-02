# Terraform configuration for Cloud Build triggers
# Creates build triggers for all services and API repos
# Libraries: use scripts/setup-cloud-build-triggers.sh (some created outside Terraform)
#
# Repos not yet linked to connection "iggyikenna-github" (asia-northeast1) must be linked
# in GCP Console (Cloud Build → Repositories → Link repository) before triggers can be created:
#   alerting-service, client-reporting-api, execution-results-api, market-data-api
# Then re-run: terraform apply -var="project_id={project_id}" -auto-approve
#
# NOTE: pnl-attribution-service, position-balance-monitor-service, risk-and-exposure-service
# Cloud Build triggers REMOVED — these repos are archived post-consolidation into strategy-service
# (strategy_repo_consolidation_2026_05_19.md). Their Cloud Run services must be destroyed via
# terraform/services/{service}/gcp/terraform destroy after operator confirms gh repo archive.

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0.0"
    }
  }

  backend "gcs" {
    bucket = "terraform-state-{project_id}"
    prefix = "cloud-build"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  # Service configurations for Cloud Build triggers
  # All services enabled with quality gates in cloudbuild.yaml
  services = {
    # Data I/O services
    "instruments-service" = {
      github_repo            = "instruments-service"
      artifact_registry_repo = "instruments-service"
    }
    "market-tick-data-service" = {
      github_repo            = "market-tick-data-service"
      artifact_registry_repo = "market-tick-data-service"
    }
    # Pricing/Processing services
    "market-data-processing-service" = {
      github_repo            = "market-data-processing-service"
      artifact_registry_repo = "market-data-processing-service"
    }
    # Feature services
    "features-delta-one-service" = {
      github_repo            = "features-delta-one-service"
      artifact_registry_repo = "features-delta-one-service"
    }
    "features-volatility-service" = {
      github_repo            = "features-volatility-service"
      artifact_registry_repo = "features-volatility-service"
    }
    "features-onchain-service" = {
      github_repo            = "features-onchain-service"
      artifact_registry_repo = "features-onchain-service"
    }
    "features-calendar-service" = {
      github_repo            = "features-calendar-service"
      artifact_registry_repo = "features-calendar-service"
    }
    # Repo created + linked to iggyikenna-github connection 2026-02-28
    "features-multi-timeframe-service" = {
      github_repo            = "features-multi-timeframe-service"
      artifact_registry_repo = "features-multi-timeframe-service"
    }
    # ML service (consolidated from ml-training-service + ml-inference-service, 2026-05-21, ml_repo_consolidation_2026_05_19.md)
    "ml-service" = {
      github_repo            = "ml-service"
      artifact_registry_repo = "ml-service"
    }
    # Strategy and execution
    "strategy-service" = {
      github_repo            = "strategy-service"
      artifact_registry_repo = "strategy-service"
    }
    "execution-service" = {
      github_repo            = "execution-service"
      artifact_registry_repo = "execution-service"
    }
    # Layer 6 · Risk · PnL · Ops
    # pnl-attribution-service, position-balance-monitor-service, risk-and-exposure-service
    # REMOVED — archived repos consolidated into strategy-service.
    # (strategy_repo_consolidation_2026_05_19.md)
    "alerting-service" = {
      github_repo            = "alerting-service"
      artifact_registry_repo = "alerting-service"
    }
    # UI services (renamed from ml-deployment-ui 2026-02-28)
    "ml-training-ui" = {
      github_repo            = "ml-training-ui"
      artifact_registry_repo = "ml-training-ui"
    }
    # API services
    "execution-results-api" = {
      github_repo            = "execution-results-api"
      artifact_registry_repo = "execution-results-api"
    }
    "market-data-api" = {
      github_repo            = "market-data-api"
      artifact_registry_repo = "market-data-api"
    }
    "client-reporting-api" = {
      github_repo            = "client-reporting-api"
      artifact_registry_repo = "client-reporting-api"
    }
  }
}

# deployment-service maintenance-jobs image trigger.
# Rebuilds unified-trading-system/deployment-service:latest (the base image for the
# tarball-cleanup + vm-log-archival Cloud Run Jobs) on push to main when image-affecting
# files change. Standalone (NOT via the cloud_build_triggers module) because it uses a
# non-default build config + the live "iggyikenna-github" connection. Adopted via
# `terraform import` (created imperatively 2026-06-02).
# SSOT: plans/active/issues/deployment_scripts_bucket_softdelete_log_churn_2026_06_01.md
# NOTE: the module-based service triggers below default to connection_name="ln", which no
# longer exists (live connections = only "iggyikenna-github") — pre-existing drift tracked
# separately; this resource deliberately pins the correct connection.
resource "google_cloudbuild_trigger" "deployment_service_jobs_image" {
  name        = "deployment-service-jobs-image-build"
  project     = var.project_id
  location    = var.region
  description = "Rebuild+push unified-trading-system/deployment-service:latest (maintenance-jobs image) on main when image-affecting files change"

  repository_event_config {
    repository = "projects/${var.project_id}/locations/${var.region}/connections/iggyikenna-github/repositories/deployment-service"
    push {
      branch = "^main$"
    }
  }

  included_files = [
    "Dockerfile",
    "cloud-build/deployment-service-jobs-image.cloudbuild.yaml",
    "scripts/vm/**",
    "pyproject.toml",
    "uv.lock",
  ]

  filename = "cloud-build/deployment-service-jobs-image.cloudbuild.yaml"
}

# Create Cloud Build triggers for each service
module "cloud_build_triggers" {
  for_each = local.services
  source   = "../../modules/cloud-build/gcp"

  project_id             = var.project_id
  region                 = var.region
  service_name           = each.key
  github_owner           = var.github_owner
  github_repo            = each.value.github_repo
  artifact_registry_repo = each.value.artifact_registry_repo
  branch_pattern         = var.branch_pattern
  machine_type           = var.machine_type
  timeout                = var.timeout
  service_account_id     = var.cloud_build_service_account

  tags = [
    "service-${each.key}",
    "auto-build",
    "main-branch"
  ]
}
