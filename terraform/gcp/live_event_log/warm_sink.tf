# Cloud Storage subscriptions (warm sink) for all 52 SINK_MATRIX shards.
# All shards have gcs_warm=True per SINK_MATRIX.
# Plan: live_persist_03_infra_pubsub_sinks_2026_06_26.md
#
# Warm GCS layout: live-events/warm/{asset_group}/{data_type}/
# Each subscription batches up to 5 min or 512 MB before writing a parquet file.
# Message retention: 7 days (so a delayed subscriber can re-pull).

resource "google_pubsub_subscription" "warm_sink_persist_all_book_depth_bands" {
  name    = "warm-sink-persist-all-book-depth-bands"
  topic   = google_pubsub_topic.persist_all_book_depth_bands.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/book_depth_bands/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "book-depth-bands"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_candle" {
  name    = "warm-sink-persist-all-candle"
  topic   = google_pubsub_topic.persist_all_candle.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/candle/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "candle"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_cme_gap" {
  name    = "warm-sink-persist-all-cme-gap"
  topic   = google_pubsub_topic.persist_all_cme_gap.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/cme_gap/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "cme-gap"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_composite_sr" {
  name    = "warm-sink-persist-all-composite-sr"
  topic   = google_pubsub_topic.persist_all_composite_sr.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/composite_sr/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "composite-sr"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_cross_asset_correlation" {
  name    = "warm-sink-persist-all-cross-asset-correlation"
  topic   = google_pubsub_topic.persist_all_cross_asset_correlation.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/cross_asset_correlation/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "cross-asset-correlation"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_cross_venue_spreads" {
  name    = "warm-sink-persist-all-cross-venue-spreads"
  topic   = google_pubsub_topic.persist_all_cross_venue_spreads.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/cross_venue_spreads/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "cross-venue-spreads"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_economic_events" {
  name    = "warm-sink-persist-all-economic-events"
  topic   = google_pubsub_topic.persist_all_economic_events.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/economic_events/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "economic-events"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_execution_fills" {
  name    = "warm-sink-persist-all-execution-fills"
  topic   = google_pubsub_topic.persist_all_execution_fills.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/execution_fills/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "execution-fills"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_execution_pnl" {
  name    = "warm-sink-persist-all-execution-pnl"
  topic   = google_pubsub_topic.persist_all_execution_pnl.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/execution_pnl/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "execution-pnl"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_execution_positions" {
  name    = "warm-sink-persist-all-execution-positions"
  topic   = google_pubsub_topic.persist_all_execution_positions.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/execution_positions/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "execution-positions"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_flow_interaction" {
  name    = "warm-sink-persist-all-flow-interaction"
  topic   = google_pubsub_topic.persist_all_flow_interaction.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/flow_interaction/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "flow-interaction"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_futures_basis" {
  name    = "warm-sink-persist-all-futures-basis"
  topic   = google_pubsub_topic.persist_all_futures_basis.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/futures_basis/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "futures-basis"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_futures_term_structure" {
  name    = "warm-sink-persist-all-futures-term-structure"
  topic   = google_pubsub_topic.persist_all_futures_term_structure.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/futures_term_structure/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "futures-term-structure"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_liquidation_clusters" {
  name    = "warm-sink-persist-all-liquidation-clusters"
  topic   = google_pubsub_topic.persist_all_liquidation_clusters.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/liquidation_clusters/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "liquidation-clusters"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_liquidity_walls" {
  name    = "warm-sink-persist-all-liquidity-walls"
  topic   = google_pubsub_topic.persist_all_liquidity_walls.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/liquidity_walls/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "liquidity-walls"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_microstructure" {
  name    = "warm-sink-persist-all-microstructure"
  topic   = google_pubsub_topic.persist_all_microstructure.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/microstructure/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "microstructure"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_moving_averages" {
  name    = "warm-sink-persist-all-moving-averages"
  topic   = google_pubsub_topic.persist_all_moving_averages.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/moving_averages/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "moving-averages"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_options_iv" {
  name    = "warm-sink-persist-all-options-iv"
  topic   = google_pubsub_topic.persist_all_options_iv.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/options_iv/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "options-iv"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_options_term_structure" {
  name    = "warm-sink-persist-all-options-term-structure"
  topic   = google_pubsub_topic.persist_all_options_term_structure.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/options_term_structure/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "options-term-structure"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_paper_ledger" {
  name    = "warm-sink-persist-all-paper-ledger"
  topic   = google_pubsub_topic.persist_all_paper_ledger.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/paper_ledger/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "paper-ledger"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_per_strategy_signal" {
  name    = "warm-sink-persist-all-per-strategy-signal"
  topic   = google_pubsub_topic.persist_all_per_strategy_signal.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/per_strategy_signal/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "per-strategy-signal"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_realized_implied_vol" {
  name    = "warm-sink-persist-all-realized-implied-vol"
  topic   = google_pubsub_topic.persist_all_realized_implied_vol.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/realized_implied_vol/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "realized-implied-vol"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_regime_detection" {
  name    = "warm-sink-persist-all-regime-detection"
  topic   = google_pubsub_topic.persist_all_regime_detection.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/regime_detection/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "regime-detection"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_technical_indicators" {
  name    = "warm-sink-persist-all-technical-indicators"
  topic   = google_pubsub_topic.persist_all_technical_indicators.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/technical_indicators/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "technical-indicators"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_tf_momentum_alignment" {
  name    = "warm-sink-persist-all-tf-momentum-alignment"
  topic   = google_pubsub_topic.persist_all_tf_momentum_alignment.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/tf_momentum_alignment/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "tf-momentum-alignment"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_tf_structure_context" {
  name    = "warm-sink-persist-all-tf-structure-context"
  topic   = google_pubsub_topic.persist_all_tf_structure_context.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/tf_structure_context/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "tf-structure-context"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_tf_vol_compression" {
  name    = "warm-sink-persist-all-tf-vol-compression"
  topic   = google_pubsub_topic.persist_all_tf_vol_compression.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/tf_vol_compression/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "tf-vol-compression"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_all_time_features" {
  name    = "warm-sink-persist-all-time-features"
  topic   = google_pubsub_topic.persist_all_time_features.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/all/time_features/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "all"
    data_type   = "time-features"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_cefi_book_snapshot_5" {
  name    = "warm-sink-persist-cefi-book-snapshot-5"
  topic   = google_pubsub_topic.persist_cefi_book_snapshot_5.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/cefi/book_snapshot_5/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "cefi"
    data_type   = "book-snapshot-5"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_cefi_derivative_ticker" {
  name    = "warm-sink-persist-cefi-derivative-ticker"
  topic   = google_pubsub_topic.persist_cefi_derivative_ticker.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/cefi/derivative_ticker/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "cefi"
    data_type   = "derivative-ticker"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_cefi_liquidations" {
  name    = "warm-sink-persist-cefi-liquidations"
  topic   = google_pubsub_topic.persist_cefi_liquidations.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/cefi/liquidations/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "cefi"
    data_type   = "liquidations"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_cefi_trades" {
  name    = "warm-sink-persist-cefi-trades"
  topic   = google_pubsub_topic.persist_cefi_trades.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/cefi/trades/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "cefi"
    data_type   = "trades"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_commodity_storage_alpha" {
  name    = "warm-sink-persist-commodity-storage-alpha"
  topic   = google_pubsub_topic.persist_commodity_storage_alpha.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/commodity/storage_alpha/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "commodity"
    data_type   = "storage-alpha"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_commodity_weather_delta" {
  name    = "warm-sink-persist-commodity-weather-delta"
  topic   = google_pubsub_topic.persist_commodity_weather_delta.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/commodity/weather_delta/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "commodity"
    data_type   = "weather-delta"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_defi_arbitrage_price_dispersion" {
  name    = "warm-sink-persist-defi-arbitrage-price-dispersion"
  topic   = google_pubsub_topic.persist_defi_arbitrage_price_dispersion.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/defi/arbitrage_price_dispersion/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "defi"
    data_type   = "arbitrage-price-dispersion"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_defi_book_snapshot_5" {
  name    = "warm-sink-persist-defi-book-snapshot-5"
  topic   = google_pubsub_topic.persist_defi_book_snapshot_5.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/defi/book_snapshot_5/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "defi"
    data_type   = "book-snapshot-5"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_defi_dex_pools" {
  name    = "warm-sink-persist-defi-dex-pools"
  topic   = google_pubsub_topic.persist_defi_dex_pools.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/defi/dex_pools/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "defi"
    data_type   = "dex-pools"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_defi_dex_swaps" {
  name    = "warm-sink-persist-defi-dex-swaps"
  topic   = google_pubsub_topic.persist_defi_dex_swaps.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/defi/dex_swaps/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "defi"
    data_type   = "dex-swaps"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_defi_lending_indices" {
  name    = "warm-sink-persist-defi-lending-indices"
  topic   = google_pubsub_topic.persist_defi_lending_indices.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/defi/lending_indices/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "defi"
    data_type   = "lending-indices"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_defi_liquidations" {
  name    = "warm-sink-persist-defi-liquidations"
  topic   = google_pubsub_topic.persist_defi_liquidations.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/defi/liquidations/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "defi"
    data_type   = "liquidations"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_defi_lst_native_rates" {
  name    = "warm-sink-persist-defi-lst-native-rates"
  topic   = google_pubsub_topic.persist_defi_lst_native_rates.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/defi/lst_native_rates/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "defi"
    data_type   = "lst-native-rates"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_defi_lst_rates" {
  name    = "warm-sink-persist-defi-lst-rates"
  topic   = google_pubsub_topic.persist_defi_lst_rates.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/defi/lst_rates/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "defi"
    data_type   = "lst-rates"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_defi_lst_yields" {
  name    = "warm-sink-persist-defi-lst-yields"
  topic   = google_pubsub_topic.persist_defi_lst_yields.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/defi/lst_yields/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "defi"
    data_type   = "lst-yields"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_defi_trades" {
  name    = "warm-sink-persist-defi-trades"
  topic   = google_pubsub_topic.persist_defi_trades.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/defi/trades/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "defi"
    data_type   = "trades"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_prediction_book_snapshot" {
  name    = "warm-sink-persist-prediction-book-snapshot"
  topic   = google_pubsub_topic.persist_prediction_book_snapshot.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/prediction/book_snapshot/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "prediction"
    data_type   = "book-snapshot"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_prediction_book_snapshot_5" {
  name    = "warm-sink-persist-prediction-book-snapshot-5"
  topic   = google_pubsub_topic.persist_prediction_book_snapshot_5.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/prediction/book_snapshot_5/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "prediction"
    data_type   = "book-snapshot-5"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_prediction_trades" {
  name    = "warm-sink-persist-prediction-trades"
  topic   = google_pubsub_topic.persist_prediction_trades.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/prediction/trades/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "prediction"
    data_type   = "trades"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_sports_derived_features" {
  name    = "warm-sink-persist-sports-derived-features"
  topic   = google_pubsub_topic.persist_sports_derived_features.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/sports/derived_features/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "sports"
    data_type   = "derived-features"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_sports_fixture_features" {
  name    = "warm-sink-persist-sports-fixture-features"
  topic   = google_pubsub_topic.persist_sports_fixture_features.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/sports/fixture_features/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "sports"
    data_type   = "fixture-features"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_sports_odds_features" {
  name    = "warm-sink-persist-sports-odds-features"
  topic   = google_pubsub_topic.persist_sports_odds_features.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/sports/odds_features/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "sports"
    data_type   = "odds-features"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_sports_trades" {
  name    = "warm-sink-persist-sports-trades"
  topic   = google_pubsub_topic.persist_sports_trades.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/sports/trades/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "sports"
    data_type   = "trades"
  }
}

resource "google_pubsub_subscription" "warm_sink_persist_tradfi_trades" {
  name    = "warm-sink-persist-tradfi-trades"
  topic   = google_pubsub_topic.persist_tradfi_trades.id
  project = var.project_id

  cloud_storage_config {
    bucket          = var.warm_gcs_bucket
    filename_prefix = "live-events/warm/tradfi/trades/"
    filename_suffix = ".parquet"

    max_duration = "300s"
    max_bytes    = 536870912
  }

  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    managed_by  = "terraform"
    sink_type   = "warm-gcs"
    asset_group = "tradfi"
    data_type   = "trades"
  }
}
