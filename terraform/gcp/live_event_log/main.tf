# Pub/Sub topics for the central event-log spine.
# Generated from unified_api_contracts.events.sink_matrix.SINK_MATRIX (52 entries).
# Plan: live_persist_03_infra_pubsub_sinks_2026_06_26.md
#
# Topic naming: persist-{asset_group}-{data_type} (hyphens, lowercase).
# Wildcard asset_group "*" uses "all" in the topic name.
# All topics use SHORT retention (1 day) — the GCS warm sink is the durable store.
#
# Provider + backend are inherited from the parent terraform/gcp/ root module.
# This directory is a SEPARATE terraform module (standalone init + apply).

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }

  backend "gcs" {
    bucket = "uts-terraform-state-central-element-323112"
    prefix = "terraform/state/live-event-log"
  }
}

provider "google" {
  project = var.project_id
  region  = "asia-northeast1"
}

resource "google_pubsub_topic" "persist_all_book_depth_bands" {
  name    = "persist-all-book-depth-bands"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "book-depth-bands"
  }
}

resource "google_pubsub_topic" "persist_all_candle" {
  name    = "persist-all-candle"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "candle"
  }
}

resource "google_pubsub_topic" "persist_all_cme_gap" {
  name    = "persist-all-cme-gap"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "cme-gap"
  }
}

resource "google_pubsub_topic" "persist_all_composite_sr" {
  name    = "persist-all-composite-sr"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "composite-sr"
  }
}

resource "google_pubsub_topic" "persist_all_cross_asset_correlation" {
  name    = "persist-all-cross-asset-correlation"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "cross-asset-correlation"
  }
}

resource "google_pubsub_topic" "persist_all_cross_venue_spreads" {
  name    = "persist-all-cross-venue-spreads"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "cross-venue-spreads"
  }
}

resource "google_pubsub_topic" "persist_all_economic_events" {
  name    = "persist-all-economic-events"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "economic-events"
  }
}

resource "google_pubsub_topic" "persist_all_execution_fills" {
  name    = "persist-all-execution-fills"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "stream_only"
    asset_group     = "all"
    data_type       = "execution-fills"
  }
}

resource "google_pubsub_topic" "persist_all_execution_pnl" {
  name    = "persist-all-execution-pnl"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "stream_only"
    asset_group     = "all"
    data_type       = "execution-pnl"
  }
}

resource "google_pubsub_topic" "persist_all_execution_positions" {
  name    = "persist-all-execution-positions"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "stream_only"
    asset_group     = "all"
    data_type       = "execution-positions"
  }
}

resource "google_pubsub_topic" "persist_all_flow_interaction" {
  name    = "persist-all-flow-interaction"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "flow-interaction"
  }
}

resource "google_pubsub_topic" "persist_all_futures_basis" {
  name    = "persist-all-futures-basis"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "futures-basis"
  }
}

resource "google_pubsub_topic" "persist_all_futures_term_structure" {
  name    = "persist-all-futures-term-structure"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "futures-term-structure"
  }
}

resource "google_pubsub_topic" "persist_all_liquidation_clusters" {
  name    = "persist-all-liquidation-clusters"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "liquidation-clusters"
  }
}

resource "google_pubsub_topic" "persist_all_liquidity_walls" {
  name    = "persist-all-liquidity-walls"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "liquidity-walls"
  }
}

resource "google_pubsub_topic" "persist_all_microstructure" {
  name    = "persist-all-microstructure"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "microstructure"
  }
}

resource "google_pubsub_topic" "persist_all_moving_averages" {
  name    = "persist-all-moving-averages"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "moving-averages"
  }
}

resource "google_pubsub_topic" "persist_all_options_iv" {
  name    = "persist-all-options-iv"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "options-iv"
  }
}

resource "google_pubsub_topic" "persist_all_options_term_structure" {
  name    = "persist-all-options-term-structure"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "options-term-structure"
  }
}

resource "google_pubsub_topic" "persist_all_paper_ledger" {
  name    = "persist-all-paper-ledger"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "stream_only"
    asset_group     = "all"
    data_type       = "paper-ledger"
  }
}

resource "google_pubsub_topic" "persist_all_per_strategy_signal" {
  name    = "persist-all-per-strategy-signal"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "per-strategy-signal"
  }
}

resource "google_pubsub_topic" "persist_all_realized_implied_vol" {
  name    = "persist-all-realized-implied-vol"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "realized-implied-vol"
  }
}

resource "google_pubsub_topic" "persist_all_regime_detection" {
  name    = "persist-all-regime-detection"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "regime-detection"
  }
}

resource "google_pubsub_topic" "persist_all_technical_indicators" {
  name    = "persist-all-technical-indicators"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "technical-indicators"
  }
}

resource "google_pubsub_topic" "persist_all_tf_momentum_alignment" {
  name    = "persist-all-tf-momentum-alignment"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "tf-momentum-alignment"
  }
}

resource "google_pubsub_topic" "persist_all_tf_structure_context" {
  name    = "persist-all-tf-structure-context"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "tf-structure-context"
  }
}

resource "google_pubsub_topic" "persist_all_tf_vol_compression" {
  name    = "persist-all-tf-vol-compression"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "tf-vol-compression"
  }
}

resource "google_pubsub_topic" "persist_all_time_features" {
  name    = "persist-all-time-features"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "all"
    data_type       = "time-features"
  }
}

resource "google_pubsub_topic" "persist_cefi_book_snapshot_5" {
  name    = "persist-cefi-book-snapshot-5"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "cefi"
    data_type       = "book-snapshot-5"
  }
}

resource "google_pubsub_topic" "persist_cefi_derivative_ticker" {
  name    = "persist-cefi-derivative-ticker"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "cefi"
    data_type       = "derivative-ticker"
  }
}

resource "google_pubsub_topic" "persist_cefi_liquidations" {
  name    = "persist-cefi-liquidations"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "cefi"
    data_type       = "liquidations"
  }
}

resource "google_pubsub_topic" "persist_cefi_trades" {
  name    = "persist-cefi-trades"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "cefi"
    data_type       = "trades"
  }
}

resource "google_pubsub_topic" "persist_commodity_storage_alpha" {
  name    = "persist-commodity-storage-alpha"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "commodity"
    data_type       = "storage-alpha"
  }
}

resource "google_pubsub_topic" "persist_commodity_weather_delta" {
  name    = "persist-commodity-weather-delta"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "commodity"
    data_type       = "weather-delta"
  }
}

resource "google_pubsub_topic" "persist_defi_arbitrage_price_dispersion" {
  name    = "persist-defi-arbitrage-price-dispersion"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "arbitrage-price-dispersion"
  }
}

resource "google_pubsub_topic" "persist_defi_book_snapshot_5" {
  name    = "persist-defi-book-snapshot-5"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "book-snapshot-5"
  }
}

resource "google_pubsub_topic" "persist_defi_dex_pools" {
  name    = "persist-defi-dex-pools"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "dex-pools"
  }
}

resource "google_pubsub_topic" "persist_defi_dex_swaps" {
  name    = "persist-defi-dex-swaps"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "dex-swaps"
  }
}

resource "google_pubsub_topic" "persist_defi_lending_indices" {
  name    = "persist-defi-lending-indices"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "lending-indices"
  }
}

resource "google_pubsub_topic" "persist_defi_liquidations" {
  name    = "persist-defi-liquidations"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "liquidations"
  }
}

resource "google_pubsub_topic" "persist_defi_lst_native_rates" {
  name    = "persist-defi-lst-native-rates"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "lst-native-rates"
  }
}

resource "google_pubsub_topic" "persist_defi_lst_rates" {
  name    = "persist-defi-lst-rates"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "lst-rates"
  }
}

resource "google_pubsub_topic" "persist_defi_lst_yields" {
  name    = "persist-defi-lst-yields"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "lst-yields"
  }
}

resource "google_pubsub_topic" "persist_defi_trades" {
  name    = "persist-defi-trades"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "defi"
    data_type       = "trades"
  }
}

resource "google_pubsub_topic" "persist_prediction_book_snapshot" {
  name    = "persist-prediction-book-snapshot"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "prediction"
    data_type       = "book-snapshot"
  }
}

resource "google_pubsub_topic" "persist_prediction_book_snapshot_5" {
  name    = "persist-prediction-book-snapshot-5"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "prediction"
    data_type       = "book-snapshot-5"
  }
}

resource "google_pubsub_topic" "persist_prediction_trades" {
  name    = "persist-prediction-trades"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "prediction"
    data_type       = "trades"
  }
}

resource "google_pubsub_topic" "persist_sports_derived_features" {
  name    = "persist-sports-derived-features"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "sports"
    data_type       = "derived-features"
  }
}

resource "google_pubsub_topic" "persist_sports_fixture_features" {
  name    = "persist-sports-fixture-features"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "sports"
    data_type       = "fixture-features"
  }
}

resource "google_pubsub_topic" "persist_sports_odds_features" {
  name    = "persist-sports-odds-features"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "sports"
    data_type       = "odds-features"
  }
}

resource "google_pubsub_topic" "persist_sports_trades" {
  name    = "persist-sports-trades"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "sports"
    data_type       = "trades"
  }
}

resource "google_pubsub_topic" "persist_tradfi_trades" {
  name    = "persist-tradfi-trades"
  project = var.project_id

  message_retention_duration = "86400s"  # 1 day SHORT retention

  labels = {
    managed_by      = "terraform"
    retention_class = "reproducible"
    asset_group     = "tradfi"
    data_type       = "trades"
  }
}

