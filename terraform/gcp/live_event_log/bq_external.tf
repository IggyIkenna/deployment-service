# BigQuery external tables over the warm GCS sink.
# One dataset (live_events) shared by all shards.
# All 52 shards have table=True per SINK_MATRIX.
# Plan: live_persist_03_infra_pubsub_sinks_2026_06_26.md
#
# Cost guardrail: external table queries scan ALL files in the prefix.
# Use partition filters where possible.
# Source format: PARQUET (autodetect schema).
# Warm GCS layout: live-events/warm/{asset_group}/{data_type}/

resource "google_bigquery_dataset" "live_events" {
  dataset_id  = "live_events"
  project     = var.project_id
  location    = "US"
  description = "BigQuery external tables over the warm GCS event-log sink. READ-ONLY — no data copy. Plan: live_persist_03_infra_pubsub_sinks_2026_06_26.md."

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "WRITER"
    special_group = "projectOwners"
  }

  labels = {
    managed_by = "terraform"
    purpose    = "live-event-log-external-tables"
  }
}

resource "google_bigquery_table" "persist_all_book_depth_bands" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_book_depth_bands"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/book_depth_bands/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/book_depth_bands/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "book-depth-bands"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_candle" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_candle"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/candle/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/candle/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "candle"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_cme_gap" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_cme_gap"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/cme_gap/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/cme_gap/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "cme-gap"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_composite_sr" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_composite_sr"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/composite_sr/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/composite_sr/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "composite-sr"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_cross_asset_correlation" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_cross_asset_correlation"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/cross_asset_correlation/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/cross_asset_correlation/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "cross-asset-correlation"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_cross_venue_spreads" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_cross_venue_spreads"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/cross_venue_spreads/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/cross_venue_spreads/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "cross-venue-spreads"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_economic_events" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_economic_events"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/economic_events/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/economic_events/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "economic-events"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_execution_fills" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_execution_fills"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/execution_fills/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/execution_fills/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "stream_only"
    asset_group     = "all"
    data_type       = "execution-fills"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_execution_pnl" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_execution_pnl"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/execution_pnl/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/execution_pnl/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "stream_only"
    asset_group     = "all"
    data_type       = "execution-pnl"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_execution_positions" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_execution_positions"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/execution_positions/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/execution_positions/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "stream_only"
    asset_group     = "all"
    data_type       = "execution-positions"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_flow_interaction" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_flow_interaction"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/flow_interaction/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/flow_interaction/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "flow-interaction"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_futures_basis" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_futures_basis"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/futures_basis/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/futures_basis/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "futures-basis"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_futures_term_structure" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_futures_term_structure"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/futures_term_structure/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/futures_term_structure/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "futures-term-structure"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_liquidation_clusters" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_liquidation_clusters"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/liquidation_clusters/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/liquidation_clusters/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "liquidation-clusters"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_liquidity_walls" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_liquidity_walls"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/liquidity_walls/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/liquidity_walls/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "liquidity-walls"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_microstructure" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_microstructure"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/microstructure/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/microstructure/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "microstructure"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_moving_averages" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_moving_averages"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/moving_averages/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/moving_averages/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "moving-averages"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_options_iv" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_options_iv"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/options_iv/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/options_iv/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "options-iv"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_options_term_structure" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_options_term_structure"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/options_term_structure/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/options_term_structure/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "options-term-structure"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_paper_ledger" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_paper_ledger"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/paper_ledger/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/paper_ledger/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "stream_only"
    asset_group     = "all"
    data_type       = "paper-ledger"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_per_strategy_signal" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_per_strategy_signal"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/per_strategy_signal/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/per_strategy_signal/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "per-strategy-signal"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_realized_implied_vol" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_realized_implied_vol"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/realized_implied_vol/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/realized_implied_vol/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "realized-implied-vol"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_regime_detection" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_regime_detection"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/regime_detection/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/regime_detection/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "regime-detection"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_technical_indicators" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_technical_indicators"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/technical_indicators/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/technical_indicators/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "technical-indicators"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_tf_momentum_alignment" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_tf_momentum_alignment"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/tf_momentum_alignment/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/tf_momentum_alignment/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "tf-momentum-alignment"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_tf_structure_context" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_tf_structure_context"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/tf_structure_context/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/tf_structure_context/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "tf-structure-context"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_tf_vol_compression" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_tf_vol_compression"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/tf_vol_compression/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/tf_vol_compression/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "tf-vol-compression"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_all_time_features" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "all_time_features"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/all/time_features/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/all/time_features/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "time-features"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_cefi_book_snapshot_5" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "cefi_book_snapshot_5"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/cefi/book_snapshot_5/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/cefi/book_snapshot_5/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "cefi"
    data_type       = "book-snapshot-5"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_cefi_derivative_ticker" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "cefi_derivative_ticker"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/cefi/derivative_ticker/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/cefi/derivative_ticker/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "cefi"
    data_type       = "derivative-ticker"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_cefi_liquidations" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "cefi_liquidations"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/cefi/liquidations/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/cefi/liquidations/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "cefi"
    data_type       = "liquidations"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_cefi_trades" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "cefi_trades"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/cefi/trades/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/cefi/trades/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "cefi"
    data_type       = "trades"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_commodity_storage_alpha" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "commodity_storage_alpha"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/commodity/storage_alpha/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/commodity/storage_alpha/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "commodity"
    data_type       = "storage-alpha"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_commodity_weather_delta" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "commodity_weather_delta"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/commodity/weather_delta/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/commodity/weather_delta/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "commodity"
    data_type       = "weather-delta"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_defi_arbitrage_price_dispersion" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "defi_arbitrage_price_dispersion"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/defi/arbitrage_price_dispersion/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/defi/arbitrage_price_dispersion/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "arbitrage-price-dispersion"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_defi_book_snapshot_5" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "defi_book_snapshot_5"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/defi/book_snapshot_5/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/defi/book_snapshot_5/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "book-snapshot-5"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_defi_dex_pools" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "defi_dex_pools"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/defi/dex_pools/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/defi/dex_pools/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "dex-pools"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_defi_dex_swaps" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "defi_dex_swaps"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/defi/dex_swaps/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/defi/dex_swaps/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "dex-swaps"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_defi_lending_indices" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "defi_lending_indices"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/defi/lending_indices/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/defi/lending_indices/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "lending-indices"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_defi_liquidations" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "defi_liquidations"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/defi/liquidations/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/defi/liquidations/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "liquidations"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_defi_lst_native_rates" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "defi_lst_native_rates"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/defi/lst_native_rates/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/defi/lst_native_rates/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "lst-native-rates"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_defi_lst_rates" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "defi_lst_rates"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/defi/lst_rates/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/defi/lst_rates/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "lst-rates"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_defi_lst_yields" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "defi_lst_yields"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/defi/lst_yields/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/defi/lst_yields/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "lst-yields"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_defi_trades" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "defi_trades"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/defi/trades/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/defi/trades/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "trades"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_prediction_book_snapshot" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "prediction_book_snapshot"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/prediction/book_snapshot/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/prediction/book_snapshot/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "prediction"
    data_type       = "book-snapshot"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_prediction_book_snapshot_5" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "prediction_book_snapshot_5"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/prediction/book_snapshot_5/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/prediction/book_snapshot_5/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "prediction"
    data_type       = "book-snapshot-5"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_prediction_trades" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "prediction_trades"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/prediction/trades/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/prediction/trades/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "prediction"
    data_type       = "trades"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_sports_derived_features" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "sports_derived_features"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/sports/derived_features/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/sports/derived_features/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "sports"
    data_type       = "derived-features"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_sports_fixture_features" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "sports_fixture_features"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/sports/fixture_features/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/sports/fixture_features/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "sports"
    data_type       = "fixture-features"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_sports_odds_features" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "sports_odds_features"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/sports/odds_features/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/sports/odds_features/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "sports"
    data_type       = "odds-features"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_sports_trades" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "sports_trades"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/sports/trades/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/sports/trades/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "sports"
    data_type       = "trades"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

resource "google_bigquery_table" "persist_tradfi_trades" {
  count = var.create_bq_external_tables ? 1 : 0

  dataset_id          = google_bigquery_dataset.live_events.dataset_id
  table_id            = "tradfi_trades"
  project             = var.project_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.warm_gcs_bucket}/live-events/warm/tradfi/trades/*"]

    hive_partitioning_options {
      mode = "AUTO"
      source_uri_prefix = "gs://${var.warm_gcs_bucket}/live-events/warm/tradfi/trades/"
    }
  }

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "tradfi"
    data_type       = "trades"
  }

  depends_on = [google_bigquery_dataset.live_events]
}

