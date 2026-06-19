# BigQuery Hive-Partitioned External Tables over GCS Parquet
#
# Plan: bigquery_feature_ml_compute_engine_option_2026_06_08.md Phase 1 (infra item, non-gated)
# Pattern source: codex/06-coding-standards/data-engine-selection.md
#
# Background
#   The canonical GCS data layout carries explicit hive-partition keys (Phase 3 done):
#     pipeline_mode={mode}_{source}/asset_group={ag}/data_type={dt}/timeframe={tf}/day={d}/
#   BigQuery external tables with hive-partitioning let the ML compute engine query this
#   layout via SQL with partition pruning — no data copy, no ETL, pay-per-query.
#
#   This file defines a for_each set of BigQuery external tables, one per
#   (asset_group, data_type) seed entry, each pointing at the canonical GCS prefix.
#   The full set rides the v9 manifest canonicalisation migration (stable schema);
#   the seed set here is representative and expandable.
#
# Cost-guardrail
#   IMPORTANT: BQ external table queries scan ALL files under the source_uri prefix
#   unless partition filters are applied. Every query MUST include:
#     WHERE asset_group = '<ag>' AND data_type = '<dt>' AND day >= '<date>'
#   to enable partition pruning and avoid full-corpus scans (potentially 10s of TB).
#   Enforce this at the BQ dataset level via `require_partition_filter = true` (see below).
#
# AWS note
#   The equivalent AWS option (Athena external tables / Redshift Spectrum over the same
#   hive layout on S3) is a parallel option tracked separately in
#   bigquery_feature_ml_compute_engine_option_2026_06_08.md Phase 1 AWS analogue.
#   Not provisioned here — GCP BQ is the primary compute engine for ML.
#
# Schema note
#   Source format is PARQUET — BQ infers schema from file metadata. CUSTOM hive
#   partitioning mode is used so BQ reads partition keys from the path template
#   (asset_group, data_type, timeframe, day) without requiring a `_column_metadata` file.
#   The `source_uri_prefix` MUST include the partition key names in the path template
#   so BQ can resolve pruning correctly.
#
# Expansion
#   Add new (asset_group, data_type) entries to `local.bq_feature_external_tables` below.
#   The for_each loop picks them up automatically. The dataset is created once and shared.

# -------------------------------------------------------
# BigQuery dataset — one shared dataset for all external feature tables
# -------------------------------------------------------
# Partition filter is enforced at the dataset level via table-level
# require_partition_filter on each table resource (see below).
resource "google_bigquery_dataset" "feature_external" {
  dataset_id  = "uts_feature_external"
  project     = var.project_id
  location    = var.region
  description = "BigQuery external tables over canonical GCS parquet feature data. Hive-partitioned (asset_group/data_type/timeframe/day). READ-ONLY — no data copy. Plan: bigquery_feature_ml_compute_engine_option_2026_06_08.md Phase 1."

  # BigQuery dataset policy updates are FULL replacements and MUST always
  # include at least one OWNER bound directly to the dataset (no inherited /
  # conditional bindings) — otherwise the update 400s "No owners specified".
  # projectOwners as OWNER is the default BQ grants on dataset creation; we
  # declare it explicitly so the TF-managed access list stays valid on update.
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  # Access: grant the unified_trading SA dataViewer (read-only queries).
  access {
    role          = "READER"
    user_by_email = google_service_account.unified_trading.email
  }

  # Allow BQ to read from GCS on behalf of the project.
  access {
    role          = "WRITER"
    special_group = "projectOwners"
  }

  labels = {
    "purpose"    = "ml-feature-external-tables"
    "plan"       = "bigquery-feature-ml-2026-06-08"
    "managed-by" = "terraform"
  }
}

# -------------------------------------------------------
# Seed set: (asset_group, data_type) → GCS prefix + schema hint
# -------------------------------------------------------
# EXPAND this map as the v9 manifest canonicalisation migration stabilises.
# Full set rides the stable-schema gate (bigquery_feature_ml_compute_engine_option_2026_06_08.md).
#
# Key format: "<asset_group>__<data_type>" (double-underscore avoids hyphen in TF resource names).
# value.gcs_prefix: the canonical path prefix UP TO AND INCLUDING the hive partition root.
#   Must end with "/" and use the pipeline_mode={mode}_{source}/asset_group=<ag>/ shape.
#   The CUSTOM hive partitioning template (columns below) tells BQ what keys follow.
# value.source_uri_prefix: matches BQ's hive_partitioning_options.source_uri_prefix — the
#   root path BEFORE the first partition key. Must end with "/" and be stable (changing it
#   invalidates partition stats). Use the pipeline_mode prefix as the root so the template
#   can express the full key chain.
#
# NOTE on pipeline_mode: the source_uri_prefix ends at pipeline_mode= because BQ's
# CUSTOM hive_partitioning_options.source_uri_prefix is the static part of the path;
# the partition keys (including pipeline_mode) are declared in require_partition_filter
# + schema below. BQ will prune on asset_group/data_type/day regardless.
locals {
  bq_feature_external_tables = {
    # CeFi trades — canonical market-tick data
    # GCS path: pipeline_mode=batch_{source}/asset_group=cefi/data_type=trades/timeframe=tick/day=YYYY-MM-DD/
    "cefi__trades" = {
      friendly_name     = "CeFi Trades (tick)"
      description       = "CeFi canonical tick trades from MTDS. Hive-partitioned by asset_group/data_type/timeframe/day. READ-ONLY external table."
      bucket            = "market-data-tick-cefi-${local.deployment_env_short}-${var.project_id}"
      source_uri_prefix = "gs://market-data-tick-cefi-${local.deployment_env_short}-${var.project_id}/"
    }

    # TradFi OHLCV 1-minute bars — canonical market-tick data
    # GCS path: pipeline_mode=batch_{source}/asset_group=tradfi/data_type=ohlcv_1m/timeframe=1m/day=YYYY-MM-DD/
    "tradfi__ohlcv_1m" = {
      friendly_name     = "TradFi OHLCV 1m bars"
      description       = "TradFi canonical 1-minute OHLCV bars from MTDS. Hive-partitioned by asset_group/data_type/timeframe/day. READ-ONLY external table."
      bucket            = "market-data-tick-tradfi-${local.deployment_env_short}-${var.project_id}"
      source_uri_prefix = "gs://market-data-tick-tradfi-${local.deployment_env_short}-${var.project_id}/"
    }

    # DeFi DEX swaps — canonical on-chain market data
    # GCS path: pipeline_mode=batch_{source}/asset_group=defi/data_type=dex_swaps/timeframe=tick/day=YYYY-MM-DD/
    "defi__dex_swaps" = {
      friendly_name     = "DeFi DEX Swaps (tick)"
      description       = "DeFi canonical DEX swap events from MTDS on-chain handlers. Hive-partitioned by asset_group/data_type/timeframe/day. READ-ONLY external table."
      bucket            = "market-data-tick-defi-${local.deployment_env_short}-${var.project_id}"
      source_uri_prefix = "gs://market-data-tick-defi-${local.deployment_env_short}-${var.project_id}/"
    }

    # CeFi delta-one features (ML-ready parquet)
    # GCS path: features-delta-one-cefi-{pid}/pipeline_mode=batch_{source}/asset_group=cefi/data_type=delta_one/feature_group=.../timeframe=.../day=YYYY-MM-DD/
    "cefi__delta_one_features" = {
      friendly_name     = "CeFi Delta-One Features"
      description       = "CeFi delta-one feature parquets from features-service. Hive-partitioned by asset_group/data_type/timeframe/day. READ-ONLY external table. Full partition key chain includes feature_group/feature_group_version — prune by day at minimum."
      bucket            = "features-delta-one-cefi-${var.project_id}"
      source_uri_prefix = "gs://features-delta-one-cefi-${var.project_id}/"
    }

    # TradFi delta-one features (ML-ready parquet)
    "tradfi__delta_one_features" = {
      friendly_name     = "TradFi Delta-One Features"
      description       = "TradFi delta-one feature parquets from features-service. Hive-partitioned by asset_group/data_type/timeframe/day. READ-ONLY external table."
      bucket            = "features-delta-one-tradfi-${var.project_id}"
      source_uri_prefix = "gs://features-delta-one-tradfi-${var.project_id}/"
    }

    # DeFi on-chain features (ML-ready parquet)
    "defi__onchain_features" = {
      friendly_name     = "DeFi On-Chain Features"
      description       = "DeFi on-chain feature parquets from features-service (LST APRs, DEX liquidity, lending rates). Hive-partitioned by asset_group/data_type/timeframe/day. READ-ONLY external table."
      bucket            = "features-onchain-defi-${var.project_id}"
      source_uri_prefix = "gs://features-onchain-defi-${var.project_id}/"
    }
  }
}

# -------------------------------------------------------
# BigQuery external tables — one per (asset_group, data_type) entry
# -------------------------------------------------------
# COST GUARDRAIL: require_partition_filter = true on each table.
# Any query that does NOT specify a WHERE clause on at least one partition key
# (day, asset_group, data_type, or timeframe) will be REJECTED by BQ before
# scanning a single byte. This prevents accidental full-corpus scans.
resource "google_bigquery_table" "feature_external" {
  for_each = local.bq_feature_external_tables

  dataset_id          = google_bigquery_dataset.feature_external.dataset_id
  table_id            = replace(each.key, "__", "_") # e.g. "cefi_trades"
  project             = var.project_id
  friendly_name       = each.value.friendly_name
  description         = each.value.description
  deletion_protection = false # external table — no data at risk on delete

  # COST GUARDRAIL: queries must include a partition filter (day/timeframe/etc.)
  # or BQ rejects them before scanning. Prevents accidental full-corpus scans
  # (10s of TB across years of data without this guard).
  require_partition_filter = true

  external_data_configuration {
    autodetect    = true # BQ infers parquet schema from file footer metadata
    source_format = "PARQUET"

    # Wildcard covers all parquet files under the bucket root, regardless of
    # pipeline_mode or other intermediate path segments. BQ hive partitioning
    # then resolves the key chain from the path template declared below.
    # NOTE: BigQuery REJECTS multiple asterisks in a source URI
    # ("Using multiple asterisks ... is not supported"). For a CUSTOM
    # hive-partitioned external table the source URI must be a SINGLE trailing
    # `*` over the static bucket root — BQ walks every object beneath it and
    # extracts the partition keys from `hive_partitioning_options.source_uri_prefix`.
    source_uris = ["${each.value.source_uri_prefix}*"]

    hive_partitioning_options {
      # CUSTOM mode: BQ reads partition key names from the source_uri_prefix
      # template. The template must name every key that appears between the
      # source_uri_prefix and the parquet files, in path order.
      #
      # Canonical path (Phase 3 layout, SSOT: codex/02-data/pipeline-mode-partition.md):
      #   pipeline_mode={mode}_{source}/asset_group={ag}/data_type={dt}/
      #     timeframe={tf}/day={d}/<instrument>.parquet
      #
      # The source_uri_prefix is the static root (bucket root here); BQ will
      # extract the partition keys listed in the template from the file paths.
      # CUSTOM-mode partition keys MUST be encoded as `{name:TYPE}` (BQ rejects a
      # bare `{name}` with "Custom partition schema encoding must be of the form
      # {name:TYPE}"). All hive keys here are string-valued path segments.
      mode              = "CUSTOM"
      source_uri_prefix = "${each.value.source_uri_prefix}{pipeline_mode:STRING}/{asset_group:STRING}/{data_type:STRING}/{timeframe:STRING}/{day:STRING}"

      # Require at least one partition key in queries (reinforces require_partition_filter).
      require_partition_filter = true
    }
  }

  labels = {
    "asset-group" = split("__", each.key)[0]
    "data-type"   = replace(split("__", each.key)[1], "_", "-")
    "purpose"     = "ml-feature-external"
    "plan"        = "bigquery-feature-ml-2026-06-08"
    "managed-by"  = "terraform"
  }

  depends_on = [google_bigquery_dataset.feature_external]
}

# -------------------------------------------------------
# IAM: BigQuery Data Viewer for the unified_trading SA on the dataset
# -------------------------------------------------------
# The dataset-level access block above grants READER to the SA, but the
# explicit IAM binding below is belt-and-suspenders for external table queries
# that go through the BigQuery REST API directly (e.g. from Cloud Run jobs).
resource "google_bigquery_dataset_iam_member" "feature_external_reader" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.feature_external.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.unified_trading.email}"
}

# -------------------------------------------------------
# Outputs
# -------------------------------------------------------
output "bq_feature_external_dataset" {
  description = "BigQuery dataset ID for hive-partitioned external feature tables."
  value       = google_bigquery_dataset.feature_external.dataset_id
}

output "bq_feature_external_table_ids" {
  description = "Map of seed key → BigQuery table ID for each external feature table. COST GUARDRAIL: all tables require partition filters."
  value = {
    for k, _ in local.bq_feature_external_tables :
    k => "${var.project_id}.${google_bigquery_dataset.feature_external.dataset_id}.${replace(k, "__", "_")}"
  }
}
