# BigQuery Hive-Partitioned External Tables over GCS Parquet
#
# Plan: bigquery_feature_ml_compute_engine_option_2026_06_08.md Phase 1 (infra item, non-gated)
# Pattern source: codex/06-coding-standards/data-engine-selection.md
#
# Background
#   BigQuery external tables with hive-partitioning let the ML compute engine query GCS
#   parquet via SQL with partition pruning — no data copy, no ETL, pay-per-query.
#
#   This file defines a for_each set of BigQuery external tables, one per seed entry, each
#   pointing at ONE real GCS tree. **The partition key set + order is NOT uniform across
#   trees** (fixed 2026-07-13, TF-reconcile finding 2026-06-19): `raw_tick_data/` trees carry
#   `day/pipeline_mode/asset_group/venue/[chain/]instrument_type/data_type`, `processed_candles/`
#   carries `day/pipeline_mode/timeframe/data_type/venue` (no asset_group — per-AG bucket makes
#   it redundant), delta-one feature buckets carry `day/feature_group/timeframe` (no data_type),
#   and on-chain feature buckets carry only `day/feature_group`. Each seed entry below declares
#   its own verified `static_prefix` + ordered `partition_keys` — do not assume one entry's
#   layout applies to another, even within the same asset_group.
#
# Cost-guardrail
#   IMPORTANT: BQ external table queries scan ALL files under the source_uri prefix
#   unless partition filters are applied. Every query MUST include a WHERE clause on at least
#   one of that table's declared `partition_keys` (e.g. `day >= '<date>'`) to enable partition
#   pruning and avoid full-corpus scans (potentially 10s of TB). Enforced at the table level via
#   `require_partition_filter = true` (see below).
#
# AWS note
#   The equivalent AWS option (Athena external tables / Redshift Spectrum over the same
#   hive layout on S3) is a parallel option tracked separately in
#   bigquery_feature_ml_compute_engine_option_2026_06_08.md Phase 1 AWS analogue.
#   Not provisioned here — GCP BQ is the primary compute engine for ML.
#
# Schema note
#   Source format is PARQUET — BQ infers schema from file metadata. CUSTOM hive
#   partitioning mode is used so BQ reads partition keys from the path template declared per
#   entry below, without requiring a `_column_metadata` file.
#
# Expansion
#   Add new entries to `local.bq_feature_external_tables` below — VERIFY the real GCS layout
#   first (`gcloud storage ls -r gs://<bucket>/<prefix>` a few sample days) rather than assuming
#   it matches an existing entry; the for_each loop picks new entries up automatically. The
#   dataset is created once and shared.
#
# Table status (2026-07-13 — empirically verified against BOTH table creation AND real queries,
# not just "did bq mk succeed"; earlier passes on this file only checked creation, which is
# insufficient — a table can be created and still be unqueryable if its underlying tree mixes
# incompatible partition shapes, since BQ validates uniform depth lazily at first-query time
# for non-autodetect tables):
#   - defi__onchain_features: WORKING, verified via a real filtered query returning rows +
#     correct partition-filter enforcement. The only one of the original 6 blocked tables that
#     is genuinely fixed.
#   - defi__dex_swaps, cefi__trades, tradfi__ohlcv_1m, cefi__delta_one_features,
#     tradfi__delta_one_features: REMOVED from this file (not created) after exhausting
#     BigQuery-native remediation options — see the "Why the other 5 stay unbuilt" note below.
#     Re-attempt once the underlying data is reorganized (a real data-engineering task, tracked
#     separately per table — see each note) rather than retrying more Terraform/BQ config tricks.
#
# Why the other 5 stay unbuilt (full empirical trail, so the next attempt doesn't re-derive this)
#   BigQuery's hive-partitioned external tables (CUSTOM and AUTO modes both tested; declaring
#   fewer partition keys down to 1 was also tested and refuted) require an INVARIANT partition
#   path-segment count across EVERY object a table's source_uris match — no fallback that
#   treats extra/missing depth as an unparsed "file" suffix, and no exclusion syntax in
#   source_uris beyond a single literal trailing "*".
#   - defi__dex_swaps: market-data-tick-defi-prd's raw_tick_data/ tree turned out to have
#     partition-depth variance at MULTIPLE nesting levels, not just one. First found: CeFi data
#     misplaced under pipeline_mode=batch_aster (6-key, no chain=) mixed with the real 7-key DeFi
#     shape — tracked as its own migration,
#     plans/active/aster_cefi_data_defi_bucket_migration_2026_07_13.md. An explicit per-day ×
#     per-pipeline_mode source_uris list (excluding batch_aster) was built and DID create a
#     table, but querying it still failed — even WITHIN the "clean" pipeline_mode=
#     batch_onchain_subgraph, some (venue, instrument_type, data_type) combinations lack the
#     chain= key entirely (e.g. venue=EIGENLAYER-ETHEREUM/instrument_type=restaking/
#     data_type=rewards is 6-key, not 7-key) — the shape varies by (venue, instrument_type,
#     data_type), a dimension too fine-grained to enumerate via source_uris at any reasonable
#     URI count across ~2,370 real days. Needs either a genuinely uniform on-disk layout (some
#     data_types may just not have a "chain" concept and should not share a partition scheme
#     with ones that do) or ingestion via a schema-merging ETL/managed-table load instead of a
#     native hive-partitioned external table.
#   - cefi__trades: market-data-tick-cefi-prd's raw_tick_data/ tree mixes a 6-key shape
#     (perpetual/spot_pair/etc.) with a 7+ key shape for instrument_type=futures_chain/
#     options_chain (adds underlying=, sometimes quote=/margin=) — CONFIRMED legitimate,
#     SSOT-documented, load-bearing data (not a bug — DERIBIT's shape in particular is correct
#     and consistent across its whole 2019-2026 history; see
#     plans/active/issues/bybit_futures_chain_write_shape_2026_07_13.md for the one venue,
#     BYBIT, that DOES have a genuine write-shape bug within this same tree). The chain-type
#     rows sit under the SAME high-volume pipeline_mode=batch_tardis as 16+ other clean venues,
#     so isolating them needs venue- or instrument_type-level exclusion, not pipeline_mode-level
#     — day × venue across ~2,500 days would run into the thousands of URIs even before adding
#     the other 4 pipeline_modes. Needs a genuinely different table design (e.g. one table for
#     chain-type instrument_types with their own key set, one for the rest), not a config fix.
#   - tradfi__ohlcv_1m: not empirically re-tested after the above findings, but the original
#     template comment already flagged the same class of issue (CME combo instruments' optional
#     underlying= segment) — very likely the same problem, same remediation path.
#   - cefi__delta_one_features / tradfi__delta_one_features: features-delta-one-cefi's
#     feature_group=technical_indicators on day=2026-05-03 has BOTH a versioned
#     (feature_group_version=1/, 4-key) and unversioned (3-key) shape for the SAME (day,
#     feature_group) — and the versioned one is NOT junk: it is the validated fix for a real bug
#     (a missing `close` column that broke downstream cross_instrument ingestion), an
#     intentionally-incomplete migration (1 of ~38 instruments done) tracked in an existing plan.
#     Deleting it would regress that bug. A confirmed-safe, fully isolated OTHER artifact
#     (day=2020-02-19, 4 objects, a stale test-verification artifact) WAS deleted 2026-07-13 —
#     see git history — but that alone doesn't unblock the table; the 2026-05-03 mixed shape is
#     the real, untouchable blocker. Needs features-service to either finish the FINDING-F
#     migration to all instruments (making the shape uniform) or a split-table design.

# -------------------------------------------------------
# BigQuery dataset — one shared dataset for all external feature tables
# -------------------------------------------------------
# Partition filter is enforced at the dataset level via table-level
# require_partition_filter on each table resource (see below).
resource "google_bigquery_dataset" "feature_external" {
  dataset_id  = "uts_feature_external"
  project     = var.project_id
  location    = var.region
  description = "BigQuery external tables over canonical GCS parquet feature data. Hive-partitioned per-table (key set varies by tree — see bigquery_feature_external_tables.tf header). READ-ONLY — no data copy. Plan: bigquery_feature_ml_compute_engine_option_2026_06_08.md Phase 1."

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
# Seed set: table key → GCS bucket + verified partition-key chain
# -------------------------------------------------------
# EXPAND this map as more trees stabilise. Every entry below was verified against LIVE GCS
# object paths on 2026-07-13 (`gcloud storage ls -r`), not assumed from the canonical-schema doc
# — the on-disk layout for market-data-tick trees interleaves `day=` FIRST (not last) and the
# key SET differs per tree (see file header). Re-verify against live paths before adding an entry.
#
# Key format: "<asset_group>__<label>" (double-underscore avoids hyphen in TF resource names;
#   the label does not have to equal a single data_type — several entries expose more than one).
# value.bucket: bucket name (no gs:// prefix, no trailing slash).
# value.static_prefix: the literal (non-hive) path segment before the FIRST partition key.
#   Empty string "" for buckets where the first hive key sits at the bucket root. Must end with
#   "/" when non-empty.
# value.partition_keys: ordered list of hive key names as they appear in the real path, e.g.
#   ["day", "pipeline_mode", "asset_group", "venue", "instrument_type", "data_type"] for
#   `<static_prefix>day=D/pipeline_mode=M/asset_group=A/venue=V/instrument_type=I/data_type=T/file`.
#   The resource block below builds BQ's CUSTOM hive_partitioning_options.source_uri_prefix by
#   joining these as `{key:STRING}` segments in order — order MUST match the real path exactly.
locals {
  bq_feature_external_tables = {
    # DeFi on-chain features (ML-ready parquet). Verified path:
    #   by_date/day=D/feature_group=G/features.parquet
    # Single "features.parquet" filename per (day, feature_group) — no timeframe key (unlike
    # the delta-one buckets). feature_group values seen: flash_loan_availability, health_factor,
    # lending_rates, liquidation_events, rewards, risk_params.
    "defi__onchain_features" = {
      friendly_name    = "DeFi On-Chain Features"
      description      = "DeFi on-chain feature parquets from features-service (LST APRs, DEX liquidity, lending rates, health factor, liquidation events, rewards, risk params). Hive-partitioned by day/feature_group only (single features.parquet per feature_group per day — no timeframe key). READ-ONLY external table."
      bucket           = "features-onchain-defi-${var.project_id}"
      static_prefix    = "by_date/"
      source_uris_glob = "*"
      partition_keys   = ["day", "feature_group"]
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

    # Wildcard covers all parquet files under static_prefix, regardless of the hive key
    # values that follow. BQ hive partitioning then resolves the key chain from the path
    # template declared below.
    # NOTE: BigQuery REJECTS multiple asterisks in a source URI
    # ("Using multiple asterisks ... is not supported"). For a CUSTOM
    # hive-partitioned external table the source URI must be a SINGLE trailing
    # `*` over the static prefix — BQ walks every object beneath it and
    # extracts the partition keys from `hive_partitioning_options.source_uri_prefix`.
    source_uris = ["gs://${each.value.bucket}/${each.value.static_prefix}${each.value.source_uris_glob}"]

    hive_partitioning_options {
      # CUSTOM mode: BQ reads partition key names from the source_uri_prefix template. The
      # template must name every key that appears between static_prefix and the parquet
      # files, IN PATH ORDER — this is per-entry (`each.value.partition_keys`) because the
      # real key set + order differs across trees (see file header + each entry's comment).
      # CUSTOM-mode partition keys MUST be encoded as `{name:TYPE}` (BQ rejects a bare
      # `{name}` with "Custom partition schema encoding must be of the form {name:TYPE}").
      # All hive keys here are string-valued path segments.
      mode = "CUSTOM"
      # static_prefix already ends in "/" when non-empty (or is "" for bucket-root trees), so
      # straight concatenation is correct — no extra separator needed either way.
      source_uri_prefix = "gs://${each.value.bucket}/${each.value.static_prefix}${join("/", [for k in each.value.partition_keys : "{${k}:STRING}"])}"

      # Require at least one partition key in queries (reinforces require_partition_filter).
      require_partition_filter = true
    }
  }

  labels = {
    "asset-group" = split("__", each.key)[0]
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
