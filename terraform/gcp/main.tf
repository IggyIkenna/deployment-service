# Terraform bootstrap for GCP — UCI cloud abstraction layer (p3-terraform-gcp)
# Provisions: GCS buckets, BigQuery datasets, Secret Manager stubs,
#             unified-trading service account with least-privilege IAM.
#
# Cloud Run service definitions are intentionally omitted here.
# TODO: add Cloud Run service definitions per service image

terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }

  # NOTE: Terraform backend blocks do not support variable interpolation.
  # Set the bucket name at init time with:
  #   terraform init -backend-config="bucket=<bucket_prefix>-terraform-state-<project_id>"
  # Convention: <bucket_prefix>-terraform-state-<project_id>  (e.g. uts-terraform-state-my-project-123)
  backend "gcs" {
    bucket = "REPLACE_WITH_BUCKET_PREFIX-terraform-state-REPLACE_WITH_PROJECT_ID"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  # Safe lower-kebab name fragment shared by all resources in this workspace
  env_prefix = lower(replace("${var.bucket_prefix}-${var.environment}", "_", "-"))

  # Common labels applied to every resource
  common_labels = {
    "environment" = var.environment
    "project"     = "unified-trading"
    "managed-by"  = "terraform"
  }

  # API key secrets — names only; values must be filled manually in Secret Manager
  secret_names = [
    "tardis-api-key",
    "databento-api-key",
    "thegraph-api-key",
    "alchemy-api-key",
    "hyperliquid-aws-s3",
    "binance-read-api-key",
    "deribit-read-api-key",
    # Sports betting secrets
    "betfair-app-key",
    "odds-api-key",
    "oddsjam-api-key",
    "opticodds-api-key",
    "metabet-api-key",
    "polymarket-private-key",
    # On-chain / CEX data secrets
    "coinglass-api-key",
    "hyblock-api-key",
    # Write API keys
    "binance-write-api-key",
    "deribit-write-api-key",
  ]
}

# =============================================================================
# GCS Buckets
# =============================================================================

# Raw and normalized market tick data
resource "google_storage_bucket" "market_data" {
  name     = lower(replace("${local.env_prefix}-market-data", "_", "-"))
  project  = var.project_id
  location = var.region

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

  labels = merge(local.common_labels, {
    "purpose" = "market-data"
  })
}

# ML model artifacts
resource "google_storage_bucket" "models" {
  name     = lower(replace("${local.env_prefix}-models", "_", "-"))
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  labels = merge(local.common_labels, {
    "purpose" = "ml-models"
  })
}

# Computed feature store
resource "google_storage_bucket" "features" {
  name     = lower(replace("${local.env_prefix}-features", "_", "-"))
  project  = var.project_id
  location = var.region

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

  labels = merge(local.common_labels, {
    "purpose" = "feature-store"
  })
}

# Deployment config and state
resource "google_storage_bucket" "deployment_state" {
  name     = lower(replace("${local.env_prefix}-deployment-state", "_", "-"))
  project  = var.project_id
  location = var.region

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

  labels = merge(local.common_labels, {
    "purpose" = "deployment-state"
  })
}

# =============================================================================
# BigQuery Datasets
# =============================================================================

resource "google_bigquery_dataset" "market_data" {
  dataset_id                 = "market_data"
  project                    = var.project_id
  location                   = var.region
  description                = "Market data tables — raw ticks and normalized OHLCV"
  delete_contents_on_destroy = false

  labels = merge(local.common_labels, {
    "purpose" = "market-data"
  })
}

resource "google_bigquery_dataset" "features" {
  dataset_id                 = "features"
  project                    = var.project_id
  location                   = var.region
  description                = "Computed feature tables for ML and strategy services"
  delete_contents_on_destroy = false

  labels = merge(local.common_labels, {
    "purpose" = "feature-store"
  })
}

resource "google_bigquery_dataset" "ml_models" {
  dataset_id                 = "ml_models"
  project                    = var.project_id
  location                   = var.region
  description                = "Model metadata, hyperparameters, and evaluation metrics"
  delete_contents_on_destroy = false

  labels = merge(local.common_labels, {
    "purpose" = "ml-models"
  })
}

resource "google_bigquery_dataset" "audit" {
  dataset_id                 = "audit"
  project                    = var.project_id
  location                   = var.region
  description                = "Audit logs and compliance events"
  delete_contents_on_destroy = false

  labels = merge(local.common_labels, {
    "purpose" = "audit"
  })
}

# =============================================================================
# Secret Manager — stub secrets (names only; values filled manually)
# =============================================================================

resource "google_secret_manager_secret" "api_keys" {
  for_each = toset(local.secret_names)

  secret_id = each.value
  project   = var.project_id

  replication {
    auto {}
  }

  labels = merge(local.common_labels, {
    "purpose" = "api-credentials"
  })
}

# =============================================================================
# Service Account — unified-trading-sa (least privilege)
# =============================================================================

resource "google_service_account" "unified_trading" {
  account_id   = "unified-trading-sa"
  display_name = "Unified Trading Service Account"
  description  = "Least-privilege service account for all unified-trading services"
  project      = var.project_id
}

resource "google_project_iam_member" "unified_trading_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}
