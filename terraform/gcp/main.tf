# Terraform bootstrap for GCP — UCI cloud abstraction layer (p3-terraform-gcp)
# Provisions: GCS buckets (per cloud-providers.yaml SSOT), BigQuery datasets,
#             Pub/Sub topics/subscriptions, Secret Manager stubs,
#             unified-trading service account with least-privilege IAM.
#
# Bucket naming follows cloud-providers.yaml two-tier model:
#   Group A (raw data)    — no env suffix; all envs share prod-level copy
#   Group B (derived data)— {domain}-{category}-{env}-{project_id}
#
# NOTE: Cloud Run Job definitions are intentionally absent here.
# See ARCHITECTURE.md "Deployment Model" section for rationale.
# Cloud Run Jobs are deployed at runtime by backends/cloud_run.py.

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }

  # NOTE: Terraform backend blocks do not support variable interpolation.
  # Pass bucket and prefix at init time:
  #   terraform init \
  #     -backend-config="bucket=uts-terraform-state-<project_id>" \
  #     -backend-config="prefix=terraform/state/<env>"
  # Per-env state keys prevent one env's apply from destroying another env's resources.
  # Convention: prefix=terraform/state/dev | terraform/state/staging | terraform/state/prod
  backend "gcs" {
    bucket = "uts-terraform-state-central-element-323112"
    prefix = "terraform/state/dev"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  # Safe lower-kebab name fragment shared by all resources in this workspace
  env_prefix = lower(replace("${var.bucket_prefix}-${var.environment}", "_", "-"))

  # 3-char short form matching cloud-providers.yaml DEPLOYMENT_ENV_SHORT convention.
  # Canonical bucket names use this infix (e.g. market-data-tick-cefi-prd-<pid>).
  deployment_env_short = {
    "dev"     = "dev"
    "staging" = "stg"
    "prod"    = "prd"
  }[var.environment]

  # Common labels applied to every resource
  common_labels = {
    "environment" = var.environment
    "project"     = "unified-trading"
    "managed-by"  = "terraform"
  }

  # ---------------------------------------------------------------------------
  # API key secrets — names only; values must be filled manually in Secret Manager
  # ---------------------------------------------------------------------------
  # Static secrets (env-independent)
  static_secret_names = [
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
    "cryptoquant-api-key",
    # Write API keys
    "binance-write-api-key",
    "deribit-write-api-key",
    # Alerting / agent secrets
    "anthropic-api-key",
    "pagerduty-api-key",
  ]

  # Env-scoped secrets — get suffix "-{environment}" appended
  env_secret_names = [
    "risk-api-key",
    "position-monitor-api-key",
  ]

  # Canonical Pub/Sub topic names (from unified-internal-contracts InternalPubSubTopic enum)
  pubsub_topic_names = [
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
    # Additional service-to-service coordination topics
    "cascade-predictions",
    "features-mtf-ready",
    "features-delta-one-ready",
    "features-cross-instrument-ready",
    "sports-odds-ready",
    # NOTE: secret-rotation-alerts is managed by google_pubsub_topic.secret_rotation_alerts
    # in secret_rotation.tf — do NOT add it here to avoid duplicate resource conflict.
  ]
}

# =============================================================================
# GCS Buckets — Group A: Raw data (no env suffix; all envs read from same copy)
# Naming: {domain}-{category}-{project_id}
# =============================================================================

resource "google_storage_bucket" "instruments_cefi" {
  name     = "instruments-store-cefi-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  # Bloat fix (2026-06-02): instruments reference data is reproducible. The default 7-day
  # soft-delete was retaining overwrite shadow copies (sports daily fixtures re-poll churned
  # ~600 GiB of soft-deletes; cefi/tradfi/defi/prediction were 90-98% bloated too). Disable
  # soft-delete and bound versioning: delete noncurrent versions >30d old once >=3 newer exist.
  # SSOT: plans/active/issues/deployment_scripts_bucket_softdelete_log_churn_2026_06_01.md
  soft_delete_policy {
    retention_duration_seconds = 0
  }
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 30
      num_newer_versions         = 3
    }
    action {
      type = "Delete"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "instruments-raw", "tier" = "group-a" })
}

resource "google_storage_bucket" "instruments_tradfi" {
  name     = "instruments-store-tradfi-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  # Bloat fix (2026-06-02): instruments reference data is reproducible. The default 7-day
  # soft-delete was retaining overwrite shadow copies (sports daily fixtures re-poll churned
  # ~600 GiB of soft-deletes; cefi/tradfi/defi/prediction were 90-98% bloated too). Disable
  # soft-delete and bound versioning: delete noncurrent versions >30d old once >=3 newer exist.
  # SSOT: plans/active/issues/deployment_scripts_bucket_softdelete_log_churn_2026_06_01.md
  soft_delete_policy {
    retention_duration_seconds = 0
  }
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 30
      num_newer_versions         = 3
    }
    action {
      type = "Delete"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "instruments-raw", "tier" = "group-a" })
}

resource "google_storage_bucket" "instruments_defi" {
  name     = "instruments-store-defi-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  # Bloat fix (2026-06-02): instruments reference data is reproducible. The default 7-day
  # soft-delete was retaining overwrite shadow copies (sports daily fixtures re-poll churned
  # ~600 GiB of soft-deletes; cefi/tradfi/defi/prediction were 90-98% bloated too). Disable
  # soft-delete and bound versioning: delete noncurrent versions >30d old once >=3 newer exist.
  # SSOT: plans/active/issues/deployment_scripts_bucket_softdelete_log_churn_2026_06_01.md
  soft_delete_policy {
    retention_duration_seconds = 0
  }
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 30
      num_newer_versions         = 3
    }
    action {
      type = "Delete"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "instruments-raw", "tier" = "group-a" })
}

resource "google_storage_bucket" "instruments_sports" {
  name     = "instruments-store-sports-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  # Bloat fix (2026-06-02): instruments reference data is reproducible. The default 7-day
  # soft-delete was retaining overwrite shadow copies (sports daily fixtures re-poll churned
  # ~600 GiB of soft-deletes; cefi/tradfi/defi/prediction were 90-98% bloated too). Disable
  # soft-delete and bound versioning: delete noncurrent versions >30d old once >=3 newer exist.
  # SSOT: plans/active/issues/deployment_scripts_bucket_softdelete_log_churn_2026_06_01.md
  soft_delete_policy {
    retention_duration_seconds = 0
  }
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 30
      num_newer_versions         = 3
    }
    action {
      type = "Delete"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "instruments-raw", "tier" = "group-a" })
}

resource "google_storage_bucket" "instruments_prediction" {
  name     = "instruments-store-prediction-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  # Bloat fix (2026-06-02): instruments reference data is reproducible. The default 7-day
  # soft-delete was retaining overwrite shadow copies (sports daily fixtures re-poll churned
  # ~600 GiB of soft-deletes; cefi/tradfi/defi/prediction were 90-98% bloated too). Disable
  # soft-delete and bound versioning: delete noncurrent versions >30d old once >=3 newer exist.
  # SSOT: plans/active/issues/deployment_scripts_bucket_softdelete_log_churn_2026_06_01.md
  soft_delete_policy {
    retention_duration_seconds = 0
  }
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 30
      num_newer_versions         = 3
    }
    action {
      type = "Delete"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "instruments-raw", "tier" = "group-a" })
}

# Test buckets for instruments (30-day lifecycle)
resource "google_storage_bucket" "instruments_cefi_test" {
  name     = "instruments-store-cefi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "instruments-test", "tier" = "group-a" })
}

resource "google_storage_bucket" "instruments_tradfi_test" {
  name     = "instruments-store-tradfi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "instruments-test", "tier" = "group-a" })
}

resource "google_storage_bucket" "instruments_defi_test" {
  name     = "instruments-store-defi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "instruments-test", "tier" = "group-a" })
}

resource "google_storage_bucket" "instruments_sports_test" {
  name     = "instruments-store-sports-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "instruments-test", "tier" = "group-a" })
}

resource "google_storage_bucket" "instruments_prediction_test" {
  name     = "instruments-store-prediction-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "instruments-test", "tier" = "group-a" })
}

# deployment-scripts — SINGLETON bucket (one physical bucket in the central project: VM code
# tarballs + run.log archives + heartbeats). NOT per-env, so guard to the central project
# (dev/staging applies skip it). Adopted via `terraform import` — it predates TF. Settings
# match the 2026-06-02 bloat remediation: soft-delete OFF (was retaining 56 TiB of run.log
# re-upload shadow copies) + prefix-scoped Delete lifecycle. No versioning, fine-grained ACLs
# (UBLA off) — matches live. SSOT: plans/active/issues/deployment_scripts_bucket_softdelete_log_churn_2026_06_01.md
resource "google_storage_bucket" "deployment_scripts" {
  count    = var.project_id == "central-element-323112" ? 1 : 0
  name     = "deployment-scripts-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = false
  force_destroy               = false

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    condition {
      age            = 14
      matches_prefix = ["vm-logs/"]
    }
    action { type = "Delete" }
  }
  lifecycle_rule {
    condition {
      age            = 15
      matches_prefix = ["vm-heartbeat/"]
    }
    action { type = "Delete" }
  }
  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["logs/", "recon-logs/", "audit-results/", "migration-bundle/staging/", "log-archive/", "deployments/archive/"]
    }
    action { type = "Delete" }
  }

  labels = merge(local.common_labels, { "purpose" = "deployment-scripts", "tier" = "group-a" })
}

# Out-of-band buckets codified 2026-06-02 (were live but untracked in terraform/state/prod).
# Both central-project-only; settings match live exactly (durability codification, not a behavior
# change). SSOT: plans/active/issues/deployment_scripts_bucket_softdelete_log_churn_2026_06_01.md
#
# client-reporting-data: client financial reports. Soft-delete 7d KEPT (appropriate for client
# data) + bounded versioning (delete noncurrent >90d once >=5 newer exist). UBLA off (fine-grained).
resource "google_storage_bucket" "client_reporting_data" {
  count    = var.project_id == "central-element-323112" ? 1 : 0
  name     = "client-reporting-data-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = false
  force_destroy               = false

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 90
      num_newer_versions         = 5
    }
    action { type = "Delete" }
  }

  labels = merge(local.common_labels, { "purpose" = "client-reporting-data", "tier" = "group-a" })
}

# instruments-store-sports-prd: the NEW canonical sports instruments bucket (env-suffixed naming
# is the new convention; the old no-env instruments-store-sports-<pid> above is the legacy
# migration source — migration old->new in place). NOT a duplicate. Soft-delete already cleared
# (0); no lifecycle / versioning live — matched exactly. UBLA off (fine-grained).
resource "google_storage_bucket" "instruments_store_sports_prd" {
  count    = var.project_id == "central-element-323112" ? 1 : 0
  name     = "instruments-store-sports-prd-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = false
  force_destroy               = false

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  labels = merge(local.common_labels, { "purpose" = "instruments-raw", "tier" = "group-a" })
}

resource "google_storage_bucket" "market_data_cefi" {
  name     = "market-data-tick-cefi-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-raw", "tier" = "group-a" })
}

resource "google_storage_bucket" "market_data_tradfi" {
  name     = "market-data-tick-tradfi-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-raw", "tier" = "group-a" })
}

resource "google_storage_bucket" "market_data_defi" {
  name     = "market-data-tick-defi-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-raw", "tier" = "group-a" })
}

# Dedicated per-type DeFi market-data buckets (canonical-migration targets;
# cloud-providers.yaml resolves `{stem}-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}`
# → `{stem}-prd-<pid>` in prod). C0-PROVISION for defi_manifest_canonicalisation
# §C — migrate_defi_full_v9_canonical writes oracle/lst/lending/perp/gas data_types
# to these (dex-pools/dex-swaps already exist). Same shape as market_data_defi.
resource "google_storage_bucket" "market_data_defi_oracle_prices_prd" {
  name                        = "oracle-prices-prd-${var.project_id}"
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-raw", "tier" = "group-a" })
}

resource "google_storage_bucket" "market_data_defi_lst_rates_prd" {
  name                        = "lst-rates-prd-${var.project_id}"
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-raw", "tier" = "group-a" })
}

resource "google_storage_bucket" "market_data_defi_lending_indices_prd" {
  name                        = "lending-indices-prd-${var.project_id}"
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-raw", "tier" = "group-a" })
}

resource "google_storage_bucket" "market_data_defi_perp_funding_prd" {
  name                        = "perp-funding-prd-${var.project_id}"
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-raw", "tier" = "group-a" })
}

resource "google_storage_bucket" "market_data_defi_gas_fees_prd" {
  name                        = "gas-fees-prd-${var.project_id}"
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-raw", "tier" = "group-a" })
}

resource "google_storage_bucket" "market_data_sports" {
  name     = "market-data-tick-sports-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-raw", "tier" = "group-a" })
}

resource "google_storage_bucket" "market_data_prediction" {
  name     = "market-data-tick-prediction-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-raw", "tier" = "group-a" })
}

# Test buckets for market data tick (30-day lifecycle)
resource "google_storage_bucket" "market_data_cefi_test" {
  name     = "market-data-tick-cefi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-test", "tier" = "group-a" })
}

resource "google_storage_bucket" "market_data_tradfi_test" {
  name     = "market-data-tick-tradfi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-test", "tier" = "group-a" })
}

resource "google_storage_bucket" "market_data_defi_test" {
  name     = "market-data-tick-defi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-test", "tier" = "group-a" })
}

resource "google_storage_bucket" "market_data_sports_test" {
  name     = "market-data-tick-sports-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-test", "tier" = "group-a" })
}

resource "google_storage_bucket" "market_data_prediction_test" {
  name     = "market-data-tick-prediction-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "market-data-test", "tier" = "group-a" })
}

# Candles buckets retired 2026-04-18: MDPS writes co-located under
# market-data-tick-{category}-* in the `processed_candles/` subprefix, not to a
# separate bucket. All 10 market-data-candles-* buckets (5 prod + 5 test) were
# verified empty (0 objects / 0 bytes) before retirement. See
# unified-trading-pm/plans/active/data_pipeline_completion_2026_04_18.plan Phase 5a.
# terraform will `destroy` the resources below on next apply; the `gcp/main.tf`
# block that once defined them has been deleted intentionally (no recreation).

# Calendar data is shared across envs (no env suffix)
resource "google_storage_bucket" "features_calendar" {
  name     = "features-calendar-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-calendar", "tier" = "group-a" })
}

resource "google_storage_bucket" "gas_fees" {
  name     = "gas-fees-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "gas-fees-reference", "tier" = "group-a" })
}

resource "google_storage_bucket" "gas_fees_test" {
  name     = "gas-fees-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "gas-fees-test", "tier" = "group-a" })
}

resource "google_storage_bucket" "solana_defi" {
  name     = "solana-defi-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "solana-defi-reference", "tier" = "group-a" })
}

resource "google_storage_bucket" "solana_defi_test" {
  name     = "solana-defi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "solana-defi-test", "tier" = "group-a" })
}

resource "google_storage_bucket" "evm_defi" {
  name     = "evm-defi-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "evm-defi-reference", "tier" = "group-a" })
}

resource "google_storage_bucket" "evm_defi_test" {
  name     = "evm-defi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "evm-defi-test", "tier" = "group-a" })
}

# =============================================================================
# GCS Buckets — Group B: Derived data (per-env)
# Naming: {domain}-{category}-{environment}-{project_id}
# =============================================================================

resource "google_storage_bucket" "features_delta_one_cefi" {
  name     = "features-delta-one-cefi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-delta-one", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_delta_one_tradfi" {
  name     = "features-delta-one-tradfi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-delta-one", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_delta_one_defi" {
  name     = "features-delta-one-defi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-delta-one", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_delta_one_sports" {
  name     = "features-delta-one-sports-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-delta-one", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_delta_one_prediction" {
  name     = "features-delta-one-prediction-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-delta-one", "tier" = "group-b" })
}

# Test buckets for features-delta-one (30-day lifecycle)
resource "google_storage_bucket" "features_delta_one_cefi_test" {
  name     = "features-delta-one-cefi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-delta-one-test", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_delta_one_tradfi_test" {
  name     = "features-delta-one-tradfi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-delta-one-test", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_delta_one_defi_test" {
  name     = "features-delta-one-defi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-delta-one-test", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_delta_one_sports_test" {
  name     = "features-delta-one-sports-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-delta-one-test", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_delta_one_prediction_test" {
  name     = "features-delta-one-prediction-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-delta-one-test", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_volatility_cefi" {
  name     = "features-volatility-cefi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-volatility", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_volatility_tradfi" {
  name     = "features-volatility-tradfi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-volatility", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_volatility_defi" {
  name     = "features-volatility-defi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-volatility", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_volatility_sports" {
  name     = "features-volatility-sports-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-volatility", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_volatility_prediction" {
  name     = "features-volatility-prediction-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-volatility", "tier" = "group-b" })
}

# Test buckets for features-volatility (30-day lifecycle)
resource "google_storage_bucket" "features_volatility_cefi_test" {
  name     = "features-volatility-cefi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-volatility-test", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_volatility_tradfi_test" {
  name     = "features-volatility-tradfi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-volatility-test", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_volatility_defi_test" {
  name     = "features-volatility-defi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-volatility-test", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_volatility_sports_test" {
  name     = "features-volatility-sports-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-volatility-test", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_volatility_prediction_test" {
  name     = "features-volatility-prediction-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-volatility-test", "tier" = "group-b" })
}

# Onchain features — cefi + defi only (cross-aspect)
resource "google_storage_bucket" "features_onchain_cefi" {
  name     = "features-onchain-cefi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-onchain", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_onchain_defi" {
  name     = "features-onchain-defi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-onchain", "tier" = "group-b" })
}

resource "google_storage_bucket" "ml_models" {
  name     = "ml-models-store-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "ml-models", "tier" = "group-b" })
}

resource "google_storage_bucket" "ml_predictions" {
  name     = "ml-predictions-store-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "ml-predictions", "tier" = "group-b" })
}

resource "google_storage_bucket" "ml_configs" {
  name     = "ml-configs-store-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "ml-configs", "tier" = "group-b" })
}

resource "google_storage_bucket" "ml_training_artifacts" {
  name     = "ml-training-artifacts-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "ml-training-artifacts", "tier" = "group-b" })
}

resource "google_storage_bucket" "strategy_cefi" {
  name     = "strategy-store-cefi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "strategy", "tier" = "group-b" })
}

resource "google_storage_bucket" "strategy_tradfi" {
  name     = "strategy-store-tradfi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "strategy", "tier" = "group-b" })
}

resource "google_storage_bucket" "strategy_defi" {
  name     = "strategy-store-defi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "strategy", "tier" = "group-b" })
}

resource "google_storage_bucket" "execution_cefi" {
  name     = "execution-store-cefi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "execution", "tier" = "group-b" })
}

resource "google_storage_bucket" "execution_tradfi" {
  name     = "execution-store-tradfi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "execution", "tier" = "group-b" })
}

resource "google_storage_bucket" "execution_defi" {
  name     = "execution-store-defi-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "execution", "tier" = "group-b" })
}

resource "google_storage_bucket" "strategy_sports" {
  name     = "strategy-store-sports-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "strategy", "tier" = "group-b" })
}

resource "google_storage_bucket" "strategy_prediction" {
  name     = "strategy-store-prediction-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "strategy", "tier" = "group-b" })
}

resource "google_storage_bucket" "execution_sports" {
  name     = "execution-store-sports-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "execution", "tier" = "group-b" })
}

resource "google_storage_bucket" "execution_prediction" {
  name     = "execution-store-prediction-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  labels = merge(local.common_labels, { "purpose" = "execution", "tier" = "group-b" })
}

# Features-sports (dedicated bucket, no env suffix — raw sports features)
resource "google_storage_bucket" "features_sports" {
  name     = "features-sports-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-sports", "tier" = "group-a" })
}

resource "google_storage_bucket" "features_sports_test" {
  name     = "features-sports-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-sports-test", "tier" = "group-a" })
}

# Features-calendar test bucket
resource "google_storage_bucket" "features_calendar_test" {
  name     = "features-calendar-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-calendar-test", "tier" = "group-a" })
}

# Onchain features test buckets
resource "google_storage_bucket" "features_onchain_cefi_test" {
  name     = "features-onchain-cefi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-onchain-test", "tier" = "group-b" })
}

resource "google_storage_bucket" "features_onchain_defi_test" {
  name     = "features-onchain-defi-test-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "features-onchain-test", "tier" = "group-b" })
}

# Deployment config and state (per-env, used by Cloud Scheduler + audit)
resource "google_storage_bucket" "deployment_state" {
  name     = lower(replace("${local.env_prefix}-deployment-state", "_", "-"))
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = merge(local.common_labels, { "purpose" = "deployment-state", "tier" = "group-b" })
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
  labels                     = merge(local.common_labels, { "purpose" = "market-data" })
}

resource "google_bigquery_dataset" "market_data_hft" {
  dataset_id                 = "market_data_hft"
  project                    = var.project_id
  location                   = "US" # pre-existing dataset created in US — location is immutable
  description                = "HFT market data tables — high-frequency tick and order book data"
  delete_contents_on_destroy = false
  labels                     = merge(local.common_labels, { "purpose" = "market-data-hft" })

  lifecycle {
    ignore_changes = [description, labels]
  }
}

resource "google_bigquery_dataset" "features" {
  dataset_id                 = "features"
  project                    = var.project_id
  location                   = var.region
  description                = "Computed feature tables for ML and strategy services"
  delete_contents_on_destroy = false
  labels                     = merge(local.common_labels, { "purpose" = "feature-store" })
}

resource "google_bigquery_dataset" "ml_models_bq" {
  dataset_id                 = "ml_models"
  project                    = var.project_id
  location                   = var.region
  description                = "Model metadata, hyperparameters, and evaluation metrics"
  delete_contents_on_destroy = false
  labels                     = merge(local.common_labels, { "purpose" = "ml-models" })
}

resource "google_bigquery_dataset" "ml_predictions_bq" {
  dataset_id                 = "ml_predictions"
  project                    = var.project_id
  location                   = var.region
  description                = "ML model predictions and inference results"
  delete_contents_on_destroy = false
  labels                     = merge(local.common_labels, { "purpose" = "ml-predictions" })
}

resource "google_bigquery_dataset" "audit" {
  dataset_id                 = "audit"
  project                    = var.project_id
  location                   = var.region
  description                = "Audit logs and compliance events"
  delete_contents_on_destroy = false
  labels                     = merge(local.common_labels, { "purpose" = "audit" })
}

# =============================================================================
# Secret Manager — stub secrets (names only; values filled manually)
# =============================================================================

resource "google_secret_manager_secret" "api_keys_static" {
  for_each = toset(local.static_secret_names)

  secret_id = each.value
  project   = var.project_id

  replication {
    auto {}
  }
  labels = merge(local.common_labels, { "purpose" = "api-credentials" })
}

resource "google_secret_manager_secret" "api_keys_env_scoped" {
  for_each = toset(local.env_secret_names)

  secret_id = "${each.value}-${var.environment}"
  project   = var.project_id

  replication {
    auto {}
  }
  labels = merge(local.common_labels, { "purpose" = "api-credentials" })
}

# =============================================================================
# Pub/Sub Topics + Subscriptions
# =============================================================================

resource "google_pubsub_topic" "unified_trading" {
  for_each = toset(local.pubsub_topic_names)

  name    = each.value
  project = var.project_id

  labels = merge(local.common_labels, { "purpose" = "event-bus" })
}

# Default pull subscriptions — one per topic ({topic}-sub)
resource "google_pubsub_subscription" "unified_trading" {
  for_each = toset(local.pubsub_topic_names)

  name    = "${each.value}-sub"
  topic   = google_pubsub_topic.unified_trading[each.value].id
  project = var.project_id

  # 7-day message retention
  message_retention_duration = "604800s"
  retain_acked_messages      = false

  ack_deadline_seconds = 60

  expiration_policy {
    ttl = "" # never expire
  }

  labels = merge(local.common_labels, { "purpose" = "event-bus" })
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

resource "google_project_iam_member" "unified_trading_pubsub_editor" {
  project = var.project_id
  role    = "roles/pubsub.editor"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

# Granted ad-hoc 2026-05-29 to unblock mtds-solana-defi-backfill VM dispatch from vm-ml.
# compute.instanceAdmin.v1 allows the SA to create/delete/start/stop Compute Engine VMs
# (required for backfill VM launchers that run under this SA's identity).
# iam.serviceAccountUser allows the SA to impersonate itself when launching VMs with
# --service-account=unified-trading-sa@... (self-impersonation path used by backfill launchers).
resource "google_project_iam_member" "unified_trading_compute_instance_admin" {
  project = var.project_id
  role    = "roles/compute.instanceAdmin.v1"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_service_account_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

# ---------------------------------------------------------------------------
# Cloud Memorystore (Redis) — optional; guarded by var.enable_memorystore
#
# Not enabled by default because:
#   1. Provisioning takes ~5-10 minutes
#   2. 1 GB basic tier costs ~$35/month per env
#   3. Requires VPC access — Memorystore is only reachable from same VPC
#
# To enable:
#   terraform apply -var="enable_memorystore=true" ...
#
# After enabling, REDIS_URL is stored in Secret Manager under "redis-url".
# Cloud Run services should inject it via:
#   --set-secrets=REDIS_URL=redis-url:latest
#
# Equivalent setup script: scripts/setup-redis.sh
# ---------------------------------------------------------------------------

resource "google_redis_instance" "unified_trading" {
  count  = var.enable_memorystore ? 1 : 0
  name   = "trading-cache-${var.environment}"
  region = var.region

  tier           = "BASIC"
  memory_size_gb = 1
  redis_version  = "REDIS_7_0"
  display_name   = "Unified Trading Cache (${var.environment})"

  auth_enabled            = true
  transit_encryption_mode = "SERVER_AUTHENTICATION"

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# Store Redis URL in Secret Manager so services read it via UCI
resource "google_secret_manager_secret" "redis_url" {
  count     = var.enable_memorystore ? 1 : 0
  secret_id = "redis-url"
  project   = var.project_id

  replication {
    auto {}
  }

  labels = { environment = var.environment }
}

resource "google_secret_manager_secret_version" "redis_url" {
  count  = var.enable_memorystore ? 1 : 0
  secret = google_secret_manager_secret.redis_url[0].id
  # auth_string requires the instance to exist; this builds the rediss:// URL
  secret_data = "rediss://:${google_redis_instance.unified_trading[0].auth_string}@${google_redis_instance.unified_trading[0].host}:${google_redis_instance.unified_trading[0].port}"
}

resource "google_project_iam_member" "unified_trading_redis_viewer" {
  count   = var.enable_memorystore ? 1 : 0
  project = var.project_id
  role    = "roles/redis.viewer"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

# =============================================================================
# GCS Buckets — CI/CD Event Retention
# p5-14: cicd-events (90d). Retention is implemented via lifecycle_rule DELETE
# after the retention window.
#
# alerting_history / alerting_state buckets (originally added here for 1yr /
# 90d compliance-retention splits) were removed 2026-07-10: alerting-service
# never wrote to them (storage_store.py always used the single shared
# alerting-service-{project_id} bucket, under bucket_config.yaml's
# shared_bucket_services convention) — confirmed 0 objects in both buckets
# before deletion. The shared bucket is the actual audit trail; it currently
# has no lifecycle rule (unbounded retention, not auto-pruned at 365d/90d).
# See unified-trading-pm/plans/archive/ui_api_alerting_observability_2026_03_14.plan.md
# =============================================================================

resource "google_storage_bucket" "cicd_events" {
  name     = "cicd-events-${var.environment}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = false }

  # CI/CD event audit trail retained for 90 days
  lifecycle_rule {
    condition { age = 90 }
    action { type = "Delete" }
  }

  labels = merge(local.common_labels, {
    "purpose" = "cicd-events",
    "tier"    = "observability"
  })
}

# IAM: unified-trading SA needs write access to cicd-events bucket
resource "google_storage_bucket_iam_member" "cicd_events_writer" {
  bucket = google_storage_bucket.cicd_events.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.unified_trading.email}"
}

# ---------------------------------------------------------------------------
# Immutable execution audit trail — trading-audit-records
# F-05: Provisioned in terraform with Object Versioning + 7-year Retention Lock.
# Bucket name mirrors cloud-providers.yaml gcp.storage.audit-records pattern.
# Retention policy is locked (irreversible) — complies with ≥7-year regulatory
# requirement. strategy-service writes to this bucket via resolve_bucket_name(kind="audit-records").
# 7 years = 220752000 seconds (7 × 365.25 × 24 × 3600)
# ---------------------------------------------------------------------------
resource "google_storage_bucket" "audit_records" {
  name     = "trading-audit-records-${local.deployment_env_short}-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false
  versioning { enabled = true }

  retention_policy {
    retention_period = 220752000
    is_locked        = true
  }

  labels = merge(local.common_labels, {
    "purpose" = "audit-records",
    "tier"    = "compliance"
  })
}

resource "google_storage_bucket_iam_member" "audit_records_writer" {
  bucket = google_storage_bucket.audit_records.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.unified_trading.email}"
}
