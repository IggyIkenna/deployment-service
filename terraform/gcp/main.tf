# Terraform bootstrap for GCP — UCI cloud abstraction layer (p3-terraform-gcp)
# Provisions: GCS buckets (per cloud-providers.yaml SSOT), BigQuery datasets,
#             Pub/Sub topics/subscriptions, Secret Manager stubs,
#             unified-trading service account with least-privilege IAM.
#
# Bucket naming follows the cloud-providers.yaml SSOT — ALL buckets are env-tiered:
#   {domain}-{category}-{env}-{project_id}  (e.g. instruments-store-sports-prd-…).
# NOTE: the former "Group A (raw data) — no env suffix; all envs share a prod-level
# copy" rule was REVERSED by operator direction 2026-05-11 (cloud-providers.yaml
# Phase 0e). No-env raw-data buckets are being DECOMMISSIONED (last two sports
# buckets deleted 2026-07-16); never (re)introduce a no-env Group-A bucket.
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

# resource google_storage_bucket.instruments_cefi REMOVED 2026-07-17
# (terraform_instruments_cefi_armed_resurrection_2026_07_16.md — DISARMING an ARMED resurrection):
# the flat no-env bucket `instruments-store-cefi-central-element-323112` was PHYSICALLY DELETED
# 2026-07-14 (bucket_estate_consolidation W2 / item E) but — unlike its tradfi/defi/prediction/
# sports twins — the resource block was left DECLARED here and the prod state entry was left in
# place. That is the loaded chamber: a real `tofu plan` against terraform/state/prod on
# 2026-07-16 (Cloud Build ea03c145-25a0-4280-acc3-75a99486ed76) reported
# `google_storage_bucket.instruments_cefi will be created` as the ONLY bucket-create in the
# whole plan, so any routine `bootstrap_gcp.sh --env prod` / full apply would have recreated it
# as an empty shell — the DOCUMENTED failure class that recreated ~30 cleanup-deleted buckets on
# 2026-07-12T21:59Z ([[terraform_bucket_estate_drift_resurrection_2026_07_13]]) and that :316-343
# below records verbatim for the DeFi twins. 404 confirmed via the elevated Cloud Build SA
# (build 0aa821f4-adf2-4ff2-b68d-96d917c4ed1d), not the 403-prone local SA.
# Canonical `instruments-store-cefi-prd-…` (owned by the yaml-derived
# `google_storage_bucket.canonical` for_each, canonical_buckets.tf) is the sole SSOT; the flat
# no-env Group-A name is retired by the 2026-05-11 operator reversal (cloud-providers.yaml:136-140).
# State entry removed via `tofu state rm google_storage_bucket.instruments_cefi` in the same
# change. Its `instruments_cefi_bucket` output went with it (outputs.tf; zero consumers — grep).
# Matches the tradfi/defi removals (2026-07-14, below) and sports (2026-07-16, ds@4637aed).
# NOTE: `module.instruments_cefi_t1_recon_job` (audit03_cron_provisioning.tf:116) is a DIFFERENT
# resource — a live Cloud Run job — and its import block (_imports_reconcile.tf:109-112) STAYS.

# resource google_storage_bucket.instruments_tradfi REMOVED 2026-07-14 (bucket_estate_consolidation W2 / item E):
# legacy flat twin mid-async-purge-delete; TF-unmanaged until the shell is deleted, so a future
# apply cannot revert the out-of-band purge lifecycle. State entry already removed. Matches the
# lending-indices-prd precedent (ds@1dd2159).

# resource google_storage_bucket.instruments_defi REMOVED 2026-07-14 (bucket_estate_consolidation W2 / item E):
# legacy flat twin mid-async-purge-delete; TF-unmanaged until the shell is deleted, so a future
# apply cannot revert the out-of-band purge lifecycle. State entry already removed. Matches the
# lending-indices-prd precedent (ds@1dd2159).

# resource google_storage_bucket.instruments_sports REMOVED 2026-07-16
# (sports_legacy_bucket_cutover_2026_07_16.md T5.1 — the LAST Group-A legacy twin):
# the flat no-env bucket `instruments-store-sports-central-element-323112` was PHYSICALLY
# DELETED at cutover; canonical `instruments-store-sports-prd-…` (owned by the yaml-derived
# `google_storage_bucket.canonical` for_each, canonical_buckets.tf) is the sole SSOT.
# State entry removed via `terraform state rm` BEFORE the physical delete — leaving this
# block declared while the bucket is gone is the DOCUMENTED resurrection class: an
# out-of-band `tofu apply` on 2026-07-12T21:59Z recreated ~30 cleanup-deleted buckets as
# empty shells ([[terraform_bucket_estate_drift_resurrection_2026_07_13]]), and :329-343
# records that exact failure for instruments_cefi. Matches the cefi/tradfi/defi removals
# (2026-07-14, :172-180 / :309-322) and prediction (2026-07-13).
# Delete gates that cleared this: T4.1 object-layer accounting closed at 968,927 with
# UNACCOUNTED = 0 (all 2,078 OR-9 objects dispositioned; 482 legacy-only keys recovered
# into canonical first) + T5.2 final live-writer re-check (0 objects created since the
# 08:18:00Z freeze, 0 genuine writes).
# NOTE: `market_data_sports` (:361 below) is DELIBERATELY RETAINED — that bucket is still
# blocked on OR-5b and is NOT being deleted; its paired import block
# (_imports_reconcile.tf:74-77) must stay with it.

# instruments_prediction (legacy instruments-store-prediction-…) REMOVED 2026-07-13 —
# bucket_name_ssot_legacy_dual_write_remediation_2026_06_01.md Phase 7 L6 decommission:
# prediction L3 is C-GREEN, all versions purged + bucket shell deleted. Canonical
# instruments-store-pred-prd-… is the sole SSOT.

# Test buckets for instruments (cefi/tradfi/defi/sports) REMOVED 2026-07-13 —
# bucket_estate_consolidation_to_sub100_2026_07_13.md Wave 0 terraform reconcile:
# their names (instruments-store-{ag}-test-{pid}) are now owned by the
# derived-from-yaml `google_storage_bucket.canonical` for_each in
# canonical_buckets.tf (state-mv'd, not destroyed). instruments_prediction_test
# (the stale full-word `-prediction-test-` resource, REMOVE_STALE) was deleted
# outright — canonical `instruments-store-pred-test-…` is the sole SSOT.

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
# migration source — migration old->new in place). REMOVED as a hand resource 2026-07-13
# (bucket_estate_consolidation_to_sub100_2026_07_13.md Wave 0) — its name
# (instruments-store-sports-prd-{pid}) is exactly what the derived-from-yaml
# `google_storage_bucket.canonical["instruments-store-sports-prd-…"]` for_each in
# canonical_buckets.tf produces; state-mv'd there (bucket itself untouched, still LIVE).

# resource google_storage_bucket.market_data_cefi REMOVED 2026-07-14 (bucket_estate_consolidation W2 / item E):
# legacy flat twin mid-async-purge-delete; TF-unmanaged until the shell is deleted, so a future
# apply cannot revert the out-of-band purge lifecycle. State entry already removed. Matches the
# lending-indices-prd precedent (ds@1dd2159).

# resource google_storage_bucket.market_data_tradfi REMOVED 2026-07-14 (bucket_estate_consolidation W2 / item E):
# legacy flat twin mid-async-purge-delete; TF-unmanaged until the shell is deleted, so a future
# apply cannot revert the out-of-band purge lifecycle. State entry already removed. Matches the
# lending-indices-prd precedent (ds@1dd2159).

# resource google_storage_bucket.market_data_defi REMOVED 2026-07-14 (bucket_estate_consolidation W2 / item E):
# legacy flat twin mid-async-purge-delete; TF-unmanaged until the shell is deleted, so a future
# apply cannot revert the out-of-band purge lifecycle. State entry already removed. Matches the
# lending-indices-prd precedent (ds@1dd2159).

# Dedicated per-type DeFi market-data buckets (canonical-migration targets;
# cloud-providers.yaml resolves `{stem}-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}`
# → `{stem}-prd-<pid>` in prod). C0-PROVISION for defi_manifest_canonicalisation
# §C — migrate_defi_full_v9_canonical writes oracle/lst/lending/perp/gas data_types
# to these (dex-pools/dex-swaps already exist). Same shape as market_data_defi.
# market_data_defi_oracle_prices_prd (oracle-prices-prd-…) REMOVED 2026-07-13 — this was one of
# the "14 legacy DeFi buckets deleted 2026-07-12" that was recreated as an empty shell by an
# out-of-band `tofu apply` (metageneration=1, creation_time=2026-07-13T00:52:06Z) because this
# resource block was still declared here after the physical bucket was deleted; confirmed 0
# objects live, re-deleted via gcloud. Config no longer declares this resource so a future apply
# will not recreate it again.

# market_data_defi_lst_rates_prd (lst-rates-prd-…) + market_data_defi_perp_funding_prd
# (perp-funding-prd-…) REMOVED 2026-07-13 (defi_dedicated_bucket_shared_migration_2026_07_13):
# every reader migrated to the shared market-data-tick-defi bucket, all
# (venue, data_type, day) shards backfilled there, buckets pending deletion. Removing the
# resource blocks BEFORE the physical delete (paired with `terraform state rm`) so a future
# apply cannot resurrect them as empty shells — the exact failure that recreated ~30
# cleanup-deleted buckets on 2026-07-12T21:59Z
# ([[terraform_bucket_estate_drift_resurrection_2026_07_13]]).

# market_data_defi_lending_indices_prd (lending-indices-prd-…) REMOVED 2026-07-14 — bucket
# is HELD pending Morpho write-target verification then deletion (estate-cleanup §5i /
# bucket_estate_consolidation W2); state entry already removed. Declaring it here while
# deletion is pending is the resurrection class this file's header warns about. The live
# bucket is deliberately TF-unmanaged until its deletion.

# market_data_defi_gas_fees_prd (gas-fees-prd-…) REMOVED 2026-07-13 — this was one of
# the "14 legacy DeFi buckets deleted 2026-07-12" that was recreated as an empty shell by an
# out-of-band `tofu apply` (metageneration=1, creation_time=2026-07-13T00:52:06Z) because this
# resource block was still declared here after the physical bucket was deleted; confirmed 0
# objects live, re-deleted via gcloud. Config no longer declares this resource so a future apply
# will not recreate it again.

# market_data_sports (legacy market-data-tick-sports-…) REMOVED 2026-07-17 —
# sports_legacy_bucket_cutover_2026_07_16.md OR-5b RESOLVED + T5.4-MDT: the 32-day / 549,392-key
# canonical↔legacy gap was recovered into canonical (content-verified legacy_only==0 on every gap
# day, zero loss), the non-raw prefixes were preserved 1:1 to canonical `_legacy_migrated_*`, and
# the physical bucket was deleted (342,629 object-versions removed; describe ⇒ 404). Removing this
# resource block + its import (_imports_reconcile.tf) + the state entry so a future apply will not
# recreate it as an empty shell — the exact failure the comment above documents for the DeFi twins.
# Canonical market-data-tick-sports-prd-… is the sole SSOT.

# market_data_prediction (legacy market-data-tick-prediction-…) REMOVED 2026-07-13 —
# bucket_name_ssot_legacy_dual_write_remediation_2026_06_01.md Phase 7 L6 decommission:
# prediction L3 is C-GREEN, all 2,592,066 object-versions purged + bucket shell deleted.
# Canonical market-data-tick-pred-prd-… is the sole SSOT.

# Test buckets for market data tick (cefi/tradfi/defi/sports) REMOVED 2026-07-13 —
# bucket_estate_consolidation_to_sub100_2026_07_13.md Wave 0 terraform reconcile: their
# names (market-data-tick-{ag}-test-{pid}) are now owned by the derived-from-yaml
# `google_storage_bucket.canonical` for_each in canonical_buckets.tf (state-mv'd, not
# destroyed). market_data_prediction_test (the stale full-word `-prediction-test-`
# resource, REMOVE_STALE) was deleted outright — canonical
# `market-data-tick-pred-test-…` is the sole SSOT.

# Candles buckets retired 2026-04-18: MDPS writes co-located under
# market-data-tick-{category}-* in the `processed_candles/` subprefix, not to a
# separate bucket. All 10 market-data-candles-* buckets (5 prod + 5 test) were
# verified empty (0 objects / 0 bytes) before retirement. See
# unified-trading-pm/plans/active/data_pipeline_completion_2026_04_18.plan Phase 5a.
# terraform will `destroy` the resources below on next apply; the `gcp/main.tf`
# block that once defined them has been deleted intentionally (no recreation).

# resource google_storage_bucket.features_calendar REMOVED 2026-07-15 (bucket_estate_consolidation
# Verify+Delete phase, bucketKey=features-calendar): flat features-calendar-{pid} bucket had 0 live
# objects, 0 noncurrent versions, 0 soft-deleted objects (re-confirmed immediately before delete).
# Canonical features-calendar-{env}-{pid} siblings (env-tiered per cloud-providers.yaml, already
# `google_storage_bucket.canonical` for_each in canonical_buckets.tf) are the sole SSOT going
# forward. State entry removed via `terraform state rm` before the physical bucket was deleted
# (`gcloud storage buckets delete`), so a future apply cannot resurrect it. Matches the
# instruments_defi / instruments_tradfi / market_data_cefi / market_data_tradfi purge-armed-twin
# precedent (ds@1dd2159, ds@39fa8c3).

# gas_fees (legacy gas-fees-…, no env suffix) REMOVED 2026-07-13 — one of the "14 legacy DeFi
# buckets deleted 2026-07-12" that was recreated as an empty shell by an out-of-band `tofu apply`
# (metageneration=1, creation_time=2026-07-13T00:52:06Z) because this resource block was still
# declared here after the physical bucket was deleted; confirmed 0 objects live, re-deleted via
# gcloud. Config no longer declares this resource so a future apply will not recreate it again.

# gas_fees_test / solana_defi_test / evm_defi_test (retired-kind test buckets — kinds
# removed from cloud-providers.yaml 2026-07-10/12) REMOVED 2026-07-13 (REMOVE_STALE,
# bucket_estate_consolidation_to_sub100_2026_07_13.md Wave 0): each was one of the "14
# legacy DeFi buckets deleted 2026-07-12" resurrected as an empty shell by the
# 2026-07-12T21:59Z out-of-band apply because these resource blocks were still declared
# after the physical buckets were deleted; confirmed 0 objects live, re-deleted via gcloud.
# Config no longer declares any of the three so a future apply will not recreate them.

# =============================================================================
# GCS Buckets — Group B: Derived data (per-env)
# Naming: {domain}-{category}-{environment}-{project_id}
#
# NOTE 2026-07-13: every long-env (`-${var.environment}-` = dev/staging/prod) Group-B
# resource that WAS declared in this section (features-delta-one/volatility/onchain,
# ml-models/predictions/configs/training-artifacts, strategy-store, execution-store —
# ×5 asset_groups incl. sports/prediction) has been REMOVED (bucket_estate_consolidation
# _to_sub100_2026_07_13.md Wave 0 terraform reconcile — these names matched no resolver
# path; the resurrected empty buckets they tracked are covered by the Wave-1 delete list).
# Their canonical replacements are the derived-from-yaml `google_storage_bucket.canonical`
# for_each in canonical_buckets.tf. `features-sports` / `deployment_state` below predate
# this section's Group-B convention (kept as-is, see their own comments).

# features_delta_one_{cefi,tradfi,defi,sports,prediction} (long-env `-${var.environment}-`
# Group-B resources) REMOVED 2026-07-13 (REMOVE_STALE, bucket_estate_consolidation_to_sub100
# _2026_07_13.md Wave 0) — names unreachable by any resolver path (`-prod-`/`-staging-`
# via var.environment; the resolver's short map emits {dev,stg,prd,test}). Canonical
# `features-delta-one-{ag}-{pid}` (env-less, per cloud-providers.yaml) lives in
# canonical_buckets.tf.

# Test bucket for features-delta-one REMOVED 2026-07-19 (Wave-3 alias-sunset / terraform-drift
# reconciliation): features-delta-one folded into features-{ag}; the -test- bucket was deleted.

# features_delta_one_{tradfi,defi,sports,prediction}_test, features_volatility_
# {cefi,tradfi,defi,sports,prediction}[_test], features_onchain_{cefi,defi}, ml_
# {models,predictions,configs,training_artifacts}, strategy_{cefi,tradfi,defi,sports,
# prediction}, execution_{cefi,tradfi,defi,sports,prediction} — ALL REMOVED 2026-07-13
# (REMOVE_STALE, bucket_estate_consolidation_to_sub100_2026_07_13.md Wave 0 terraform
# reconcile): the prod-tier resources used the unreachable long-env `-${var.environment}-`
# infix; the test-tier resources' flat `-test-{pid}` names are now owned by the
# derived-from-yaml `google_storage_bucket.canonical` for_each in canonical_buckets.tf
# (features_delta_one_cefi_test above is the sole survivor — canonical for_each is
# env-less for this kind, so it does NOT reach the `-test-` name; kept as its own hand
# resource). ml/strategy/execution had no live `-test-` twins in this file to preserve.

# resource google_storage_bucket.features_sports REMOVED 2026-07-15 (features_sports_service_
# consolidation_deploy_2026_07_15 + bucket_estate_consolidation_to_sub100_2026_07_13
# FinishBucketAndDocs phase, bucketKey=features-sports): the flat, no-env-suffix
# features-sports-{pid} bucket held 1 real migrated object
# (sports_features/by_date/day=2020-01-01/feature_group=sfi_progressive/sfi_progressive.parquet,
# 25,989 B — already copied to the canonical features-sports-prd-{pid} on 2026-07-15) + 2 ephemeral
# _vm_staging/fss_backfill/* objects; all re-confirmed immediately before delete. The canonical
# features-sports-prd-{pid} bucket (google_storage_bucket.canonical["features-sports-prd-central-element-323112"]
# in canonical_buckets.tf — live, GCS-FUSE-mounted by features-service-sports-job) is the sole SSOT
# going forward. State entry removed via `tofu state rm` BEFORE the physical
# `gcloud storage buckets delete`, so a future apply cannot resurrect it. Matches the
# features-calendar / instruments_defi / market_data_cefi purge-armed-twin precedent above.

# features_sports_test / features_calendar_test REMOVED 2026-07-13 as hand resources
# (double-declare, bucket_estate_consolidation_to_sub100_2026_07_13.md Wave 0) — their
# names (features-sports-test-{pid}, features-calendar-test-{pid}) are exactly what the
# derived-from-yaml `google_storage_bucket.canonical` for_each in canonical_buckets.tf
# produces (env-tiered kinds); state-mv'd there (buckets untouched, still LIVE).
# features_onchain_{cefi,defi}_test REMOVED outright (REMOVE_STALE) — the resolver never
# emits a `-test-` name for this env-less kind (see features_delta_one_cefi_test above);
# canonical `features-onchain-{cefi,defi}-{pid}` (no `-test-` twin) is the sole SSOT.

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

# Granted 2026-07-24 to unblock the /ops/artifacts "Artifacts" tab in prod (deployment-api's
# gcp_artifact_registry_images() calls ArtifactRegistryClient.list_docker_images directly). Without
# this the call throws PermissionDenied, is swallowed by the per-source `safe()` isolation wrapper,
# and the tab silently renders empty rows in prod while working locally (where dev uses the
# operator's own broad ADC credentials, not this SA). Dashboard's compute SA already has this same
# role (terraform/dashboard/gcp/main.tf) for the same AR-listing need — this mirrors it for
# unified-trading-sa, deployment-api's actual runtime identity.
resource "google_project_iam_member" "unified_trading_artifactregistry_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

# ---------------------------------------------------------------------------
# unified-trading-sa live-IAM-vs-terraform reconciliation (2026-08-03)
#
# Terraform-imports the 28 project-level roles that were LIVE on
# unified-trading-sa but had zero terraform declaration anywhere in this repo
# (issues/unified_trading_sa_live_iam_drift_vs_terraform_2026_07_31.md P2).
# The finding's P0 audit traced 3/24 of the then-live undeclared roles to
# same-day self-grants by unified-trading-sa's own ambient credential (outside
# terraform, per RULES.md's ambient-identity self-service pattern); the
# remaining 21 (incl. the two self-escalation-capable roles below) predate the
# 400-day Cloud Audit Log retention floor and are permanently untraceable.
# P1 (operator, 2026-08-03) ruled KEEP on both roles/resourcemanager.projectIamAdmin
# and roles/iam.serviceAccountAdmin — insufficient certainty to safely revoke
# given exhausted audit-log evidence; revisit only with stronger evidence later.
# This P2 todo imports ALL roles live at time of import (28, not the finding's
# original 24 — a 4th self-grant, roles/logging.viewAccessor, landed
# 2026-07-31T22:17:57Z, after the P0 audit's snapshot but before this import;
# traced via the same audit-log query, also self-granted by unified-trading-sa
# itself) so `tofu plan` stops showing drift and any FUTURE change to this
# live policy is caught going forward. No live GCP resource is mutated by
# this import — it only associates already-existing IAM bindings with
# terraform state (see _imports_reconcile.tf for the matching `import {}`
# blocks). Never a blanket `set-iam-policy` overwrite.
# ---------------------------------------------------------------------------

resource "google_project_iam_member" "unified_trading_artifactregistry_admin" {
  project = var.project_id
  role    = "roles/artifactregistry.admin"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_cloudbuild_builds_editor" {
  project = var.project_id
  role    = "roles/cloudbuild.builds.editor"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_cloudbuild_builds_viewer" {
  project = var.project_id
  role    = "roles/cloudbuild.builds.viewer"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_cloudfunctions_viewer" {
  project = var.project_id
  role    = "roles/cloudfunctions.viewer"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_cloudkms_viewer" {
  project = var.project_id
  role    = "roles/cloudkms.viewer"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_cloudscheduler_admin" {
  project = var.project_id
  role    = "roles/cloudscheduler.admin"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_cloudscheduler_viewer" {
  project = var.project_id
  role    = "roles/cloudscheduler.viewer"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_cloudsql_admin" {
  project = var.project_id
  role    = "roles/cloudsql.admin"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_compute_admin" {
  project = var.project_id
  role    = "roles/compute.admin"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_datastore_owner" {
  project = var.project_id
  role    = "roles/datastore.owner"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_datastore_user" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

# Both roles below are self-escalation-capable (grant/modify IAM policy +
# create/delete/manage service accounts). P1 (operator, 2026-08-03) ruled
# KEEP on both — see the file-header comment above this section for the
# rationale. Do not revoke without a fresh operator ruling.
resource "google_project_iam_member" "unified_trading_service_account_admin" {
  project = var.project_id
  role    = "roles/iam.serviceAccountAdmin"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_project_iam_admin" {
  project = var.project_id
  role    = "roles/resourcemanager.projectIamAdmin"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_iap_tunnel_resource_accessor" {
  project = var.project_id
  role    = "roles/iap.tunnelResourceAccessor"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_logging_config_writer" {
  project = var.project_id
  role    = "roles/logging.configWriter"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_logging_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_logging_view_accessor" {
  project = var.project_id
  role    = "roles/logging.viewAccessor"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_logging_viewer" {
  project = var.project_id
  role    = "roles/logging.viewer"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_monitoring_alert_policy_editor" {
  project = var.project_id
  role    = "roles/monitoring.alertPolicyEditor"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_pubsub_admin" {
  project = var.project_id
  role    = "roles/pubsub.admin"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_pubsub_viewer" {
  project = var.project_id
  role    = "roles/pubsub.viewer"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_run_admin" {
  project = var.project_id
  role    = "roles/run.admin"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_run_developer" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_secretmanager_admin" {
  project = var.project_id
  role    = "roles/secretmanager.admin"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_secretmanager_viewer" {
  project = var.project_id
  role    = "roles/secretmanager.viewer"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "unified_trading_storage_full_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

# Granted 2026-07-24 — Link 3 of the deployment-registry Firestore dual-write migration
# (plans/active/deployment_registry_firestore_p0_unblock_2026_07_14.md). VMs write the
# deployment registry via the DEFAULT compute SA (as of 2026-08-04: 53/169 launchers under
# deployment-service/scripts/vm/launch-*.sh still pass NO --service-account=; 116 migrated to
# explicit SAs via lc_tier_service_account() per the DP-VM-002 rollout —
# bucket_iam_p2_tier_sa_scope_gap_and_default_compute_sa_overprivilege_2026_07_30.md P0 "hybrid"
# ruling). gcloud falls back to `{project_number}-compute@developer.gserviceaccount.com` for the
# remaining 53 — mirrors the same default-SA pattern already used by
# alerting_relay_pubsub.tf/catalogue_regen_scheduler.tf.
# Firestore writes from `_maybe_build_registry_store()`
# (unified_trading_library/deployment_registry.py) need `roles/datastore.user`; without it
# the dual-write path degrades to GCS-only + a warning (best-effort by design), which is
# exactly the silent-failure mode the plan's own [VERIFY] todo warns about — this grant is
# a prerequisite, not the flag flip itself (the flag stays default-off, see Link 2).
resource "google_project_iam_member" "default_compute_sa_datastore_user" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
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
# p5-14 retention-split buckets (alerting_history, alerting_state, cicd_events)
# were all removed 2026-07-10: none were ever actually written to.
#   - alerting-service always wrote to the single shared alerting-service-{pid}
#     bucket (bucket_config.yaml shared_bucket_services convention).
#   - Every CI/CD event writer/reader (persist-cicd-event.yml, notify-slack.yml,
#     semver-agent.yml, deployment-api _repo_ci_alerts.py/deployments_inventory.py)
#     resolves CICD_EVENTS_BUCKET to the literal "unified-trading-cicd-events"
#     bucket — never this cicd-events-{env}-{project_id} name.
# All three sat at 0 objects since creation and were deleted directly via
# gcloud (bucket not importable into this repo's local terraform state).
# See unified-trading-pm/plans/archive/ui_api_alerting_observability_2026_03_14.plan.md
# =============================================================================

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
