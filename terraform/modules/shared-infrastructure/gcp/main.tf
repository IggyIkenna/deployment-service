# GCP Shared Infrastructure Module
# Creates foundational resources for running services on GCP
#
# This module provisions:
# - Artifact Registry repositories for Docker images
# - GCS Buckets for data storage
# - Service accounts with appropriate permissions
# - Secret Manager access configuration

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
  # Categories for per-category buckets
  categories = ["cefi", "tradfi", "defi"]

  # Domains for backtesting buckets
  domains = ["cefi", "tradfi", "defi"]
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
# GCS Buckets - Features Services (per category)
# =============================================================================

# Features Delta One buckets (CEFI, TRADFI, DEFI)
resource "google_storage_bucket" "features_delta_one" {
  for_each = var.create_gcs_buckets ? toset(local.categories) : []

  name     = "features-delta-one-${each.value}-${var.project_id}"
  project  = var.project_id
  location = var.gcs_location

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = merge(
    {
      "managed-by" = "terraform"
      "service"    = "features-delta-one-service"
      "category"   = each.value
    },
    var.labels
  )
}

# Features Volatility buckets (CEFI, TRADFI only - DEFI has no options)
resource "google_storage_bucket" "features_volatility" {
  for_each = var.create_gcs_buckets ? toset(["cefi", "tradfi"]) : []

  name     = "features-volatility-${each.value}-${var.project_id}"
  project  = var.project_id
  location = var.gcs_location

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = merge(
    {
      "managed-by" = "terraform"
      "service"    = "features-volatility-service"
      "category"   = each.value
    },
    var.labels
  )
}

# Features Onchain buckets (CEFI, DEFI)
resource "google_storage_bucket" "features_onchain" {
  for_each = var.create_gcs_buckets ? toset(["cefi", "defi"]) : []

  name     = "features-onchain-${each.value}-${var.project_id}"
  project  = var.project_id
  location = var.gcs_location

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = merge(
    {
      "managed-by" = "terraform"
      "service"    = "features-onchain-service"
      "category"   = each.value
    },
    var.labels
  )
}

# =============================================================================
# GCS Buckets - ML Services (shared)
# =============================================================================

# ML Models Store
resource "google_storage_bucket" "ml_models_store" {
  count = var.create_gcs_buckets ? 1 : 0

  name     = "ml-models-store-${var.project_id}"
  project  = var.project_id
  location = var.gcs_location

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  labels = merge(
    {
      "managed-by" = "terraform"
      "service"    = "ml-training-service"
    },
    var.labels
  )
}

# ML Predictions Store
resource "google_storage_bucket" "ml_predictions_store" {
  count = var.create_gcs_buckets ? 1 : 0

  name     = "ml-predictions-store-${var.project_id}"
  project  = var.project_id
  location = var.gcs_location

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = merge(
    {
      "managed-by" = "terraform"
      "service"    = "ml-inference-service"
    },
    var.labels
  )
}

# ML Configs Store (for grid configs)
resource "google_storage_bucket" "ml_configs_store" {
  count = var.create_gcs_buckets ? 1 : 0

  name     = "ml-configs-store-${var.project_id}"
  project  = var.project_id
  location = var.gcs_location

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  labels = merge(
    {
      "managed-by" = "terraform"
      "service"    = "ml-training-service"
    },
    var.labels
  )
}

# =============================================================================
# GCS Buckets - Strategy & Execution Services (per domain)
# =============================================================================

# Strategy Store buckets (per domain)
resource "google_storage_bucket" "strategy_store" {
  for_each = var.create_gcs_buckets ? toset(local.domains) : []

  name     = "strategy-store-${each.value}-${var.project_id}"
  project  = var.project_id
  location = var.gcs_location

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  labels = merge(
    {
      "managed-by" = "terraform"
      "service"    = "strategy-service"
      "domain"     = each.value
    },
    var.labels
  )
}

# Execution Store buckets (per domain)
resource "google_storage_bucket" "execution_store" {
  for_each = var.create_gcs_buckets ? toset(local.domains) : []

  name     = "execution-store-${each.value}-${var.project_id}"
  project  = var.project_id
  location = var.gcs_location

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  labels = merge(
    {
      "managed-by" = "terraform"
      "service"    = "execution-service"
      "domain"     = each.value
    },
    var.labels
  )
}

# =============================================================================
# GCS Buckets - Deployment Orchestration State
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

# Shared service account for batch processing services
resource "google_service_account" "batch_processing" {
  count = var.create_service_accounts ? 1 : 0

  account_id   = "batch-processing-sa"
  display_name = "Batch Processing Service Account"
  description  = "Service account for batch processing jobs (features, ML, strategy, execution)"
  project      = var.project_id
}

# Grant GCS access to batch processing service account
resource "google_project_iam_member" "batch_processing_gcs" {
  count = var.create_service_accounts ? 1 : 0

  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.batch_processing[0].email}"
}

# Grant Secret Manager access
resource "google_project_iam_member" "batch_processing_secrets" {
  count = var.create_service_accounts ? 1 : 0

  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.batch_processing[0].email}"
}

# Grant Cloud Run invoker for workflows
resource "google_project_iam_member" "batch_processing_run_invoker" {
  count = var.create_service_accounts ? 1 : 0

  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.batch_processing[0].email}"
}

# Grant Workflows invoker
resource "google_project_iam_member" "batch_processing_workflows_invoker" {
  count = var.create_service_accounts ? 1 : 0

  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${google_service_account.batch_processing[0].email}"
}
