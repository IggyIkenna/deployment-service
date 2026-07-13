# GCP Shared Infrastructure Module
# Creates foundational resources for running services on GCP
#
# This module provisions:
# - Artifact Registry repositories for Docker images
# - The deployment-orchestration GCS bucket (state isolated by path prefix, not env name)
# - Service accounts
# - Secret Manager access configuration
#
# NOTE 2026-07-13: the per-category/domain GCS buckets (features/ml/strategy/execution,
# named with a long `-${var.env}-` infix) + their per-bucket IAM bindings were REMOVED —
# see the "GCS Buckets - Features/ML/Strategy/Execution Services — REMOVED 2026-07-13"
# comment below for why (bucket_estate_consolidation_to_sub100_2026_07_13.md Wave 0).

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0.0"
    }
  }
}

locals {
  # SA email for use in IAM bindings (empty string when not created)
  env_sa_email = var.create_service_accounts ? google_service_account.env_sa[0].email : ""
}

# =============================================================================
# Artifact Registry Repositories
# =============================================================================

resource "google_artifact_registry_repository" "service_repos" {
  for_each = var.create_artifact_registry ? toset(var.services) : []

  project       = var.project_id
  location      = var.region
  repository_id = each.value
  description   = "Docker repository for ${each.value}"
  format        = "DOCKER"

  labels = merge(
    {
      "managed-by" = "terraform"
      "service"    = each.value
    },
    var.labels
  )
}

# =============================================================================
# GCS Buckets - Features/ML/Strategy/Execution Services — REMOVED 2026-07-13
# =============================================================================
#
# The features_delta_one / features_volatility / features_onchain / ml_models_store /
# ml_predictions_store / ml_configs_store / ml_training_artifacts / strategy_store /
# execution_store resources that lived here materialized long-env
# (`-${var.env}-` = staging/prod/development) bucket names that NO resolver path emits
# (the canonical resolver's short map is {dev,stg,prd,test}) — the exact same stale-naming
# bug as the sibling long-env blocks removed from `terraform/gcp/main.tf` the same day, and
# they overlapped those SAME physical bucket names (two TF codepaths fighting over one
# bucket — see terraform_bucket_estate_drift_resurrection_2026_07_13.md). Removed per
# bucket_estate_consolidation_to_sub100_2026_07_13.md Wave 0 terraform reconcile; this
# module (`terraform/shared/gcp`, `create_gcs_buckets = true`) is a SEPARATE state/backend
# from `terraform/gcp` — its own `terraform state rm` ops are in scratchpad/tf_state_surgery.sh.
# `deployment_orchestration` below is a genuine infra bucket (isolated by path prefix, not by
# env name) and is UNCHANGED.

# =============================================================================
# GCS Buckets - Deployment Orchestration State
# (no env suffix — isolated by path prefix deployments.{env}/ inside the bucket)
# =============================================================================

resource "google_storage_bucket" "deployment_orchestration" {
  count = var.create_gcs_buckets ? 1 : 0

  name     = "deployment-orchestration-${var.project_id}"
  project  = var.project_id
  location = var.gcs_location

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = merge(
    {
      "managed-by" = "terraform"
      "service"    = "unified-trading-deployment"
    },
    var.labels
  )
}

# =============================================================================
# Service Accounts
# =============================================================================

# Environment-specific service account for data processing services.
# account_id has a 30-char max; "staging-data-sa" and "prod-data-sa" are safe.
resource "google_service_account" "env_sa" {
  count = var.create_service_accounts ? 1 : 0

  account_id   = "${var.env}-data-sa"
  display_name = "${title(var.env)} Data Service Account"
  description  = "Service account for ${var.env} data processing (features, ML, strategy, execution)"
  project      = var.project_id
}

# Grant Secret Manager access
resource "google_project_iam_member" "env_sa_secrets" {
  count = var.create_service_accounts ? 1 : 0

  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${local.env_sa_email}"
}

# Grant Cloud Run invoker for workflows
resource "google_project_iam_member" "env_sa_run_invoker" {
  count = var.create_service_accounts ? 1 : 0

  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${local.env_sa_email}"
}

# Grant Workflows invoker
resource "google_project_iam_member" "env_sa_workflows_invoker" {
  count = var.create_service_accounts ? 1 : 0

  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${local.env_sa_email}"
}

# =============================================================================
# Per-Bucket IAM (env_sa_admin / cross_env_reader) — REMOVED 2026-07-13
# =============================================================================
#
# Both `for_each`d over `local.data_bucket_names`, which only ever collected the names
# of the features/ml/strategy/execution resources removed above. With no data buckets
# left in this module, both bindings would resolve to an empty for_each (a no-op) —
# removed outright rather than left as vestigial dead code. `cross_env_read_sa_emails`
# stays a live variable (unused inside this module now, but not this task's blast radius
# to remove from the public interface).
