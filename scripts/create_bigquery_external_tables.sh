#!/bin/bash
# Create BigQuery External Tables for All Services
# Points to future key=value format paths (even if data doesn't exist yet)
# Tables will be empty until data is generated, then auto-populate

set -e

PROJECT="${GCP_PROJECT_ID:?GCP_PROJECT_ID required}"
LOCATION="US"

echo "========================================="
echo "Creating BigQuery External Tables"
echo "Project: $PROJECT"
echo "Location: $LOCATION"
echo "========================================="
echo

# Function to create table
create_external_table() {
    local dataset=$1
    local table=$2
    local uri=$3
    local hive_mode=$4
    local hive_prefix=$5

    echo "Creating $dataset.$table..."

    # Create dataset if not exists
    bq mk --dataset --location=$LOCATION ${PROJECT}:${dataset} 2>/dev/null || echo "  Dataset exists"

    if [ "$hive_mode" = "AUTO" ]; then
        # With hive partitioning
        python3 << EOFPY
from google.cloud import bigquery
client = bigquery.Client(project='$PROJECT', location='$LOCATION')
external_config = bigquery.ExternalConfig('PARQUET')
external_config.source_uris = ['$uri']
external_config.autodetect = True
external_config.hive_partitioning = bigquery.HivePartitioningOptions()
external_config.hive_partitioning.mode = 'AUTO'
external_config.hive_partitioning.source_uri_prefix = '$hive_prefix'
table = bigquery.Table('$PROJECT.$dataset.$table')
table.external_data_configuration = external_config
client.create_table(table, exists_ok=True)
print('  ✅ Created $table')
EOFPY
    else
        # Without hive partitioning
        python3 << EOFPY
from google.cloud import bigquery
client = bigquery.Client(project='$PROJECT', location='$LOCATION')
external_config = bigquery.ExternalConfig('PARQUET')
external_config.source_uris = ['$uri']
external_config.autodetect = True
table = bigquery.Table('$PROJECT.$dataset.$table')
table.external_data_configuration = external_config
client.create_table(table, exists_ok=True)
print('  ✅ Created $table')
EOFPY
    fi
}

# 1. Instruments (day=)
create_external_table \
  "instruments_data" \
  "instruments_cefi_v2" \
  "gs://instruments-store-cefi-${PROJECT}/instrument_definitions/by_date/day=*/*.parquet" \
  "AUTO" \
  "gs://instruments-store-cefi-${PROJECT}/instrument_definitions/by_date/"

# 2. Market-Tick (day=, data_type=, instrument_type=, venue=, symbol=)
create_external_table \
  "market_tick" \
  "trades_cefi_v2" \
  "gs://market-data-tick-cefi-${PROJECT}/raw_tick_data/by_date/day=*/data_type=trades/instrument_type=futures_chain/venue=BINANCE-FUTURES/symbol=BTC-USDT/*.parquet" \
  "AUTO" \
  "gs://market-data-tick-cefi-${PROJECT}/raw_tick_data/by_date/"

# 3. Market-Data-Processing (day=, timeframe=)
create_external_table \
  "candles_data" \
  "candles_1m_cefi_v2" \
  "gs://market-data-candles-cefi-${PROJECT}/processed_candles/by_date/day=*/timeframe=1m/*.parquet" \
  "AUTO" \
  "gs://market-data-candles-cefi-${PROJECT}/processed_candles/by_date/"

# 4. Features-Delta-One (day=, feature_group=, timeframe=)
# Create one per timeframe to avoid multiple wildcards
for TF in 1m 5m 15m 1h 4h 24h; do
    create_external_table \
      "features_data" \
      "features_${TF}_cefi_v2" \
      "gs://features-delta-one-cefi-${PROJECT}/by_date/day=*/feature_group=technical_indicators/timeframe=${TF}/*.parquet" \
      "AUTO" \
      "gs://features-delta-one-cefi-${PROJECT}/by_date/"
done

# 5. Features-Calendar (day=, category=)
create_external_table \
  "features_data" \
  "features_calendar_v2" \
  "gs://features-calendar-${PROJECT}/calendar/by_date/day=*/category=temporal/*.parquet" \
  "AUTO" \
  "gs://features-calendar-${PROJECT}/calendar/by_date/"

echo
echo "========================================="
echo "✅ External Tables Created"
echo "========================================="
echo
echo "Tables point to FUTURE data structure (key=value format)"
echo "Currently empty, will auto-populate when data is generated"
echo
echo "Test when data exists:"
echo "  bq query \"SELECT COUNT(*), day FROM ${PROJECT}.features_data.features_1m_cefi_v2 GROUP BY day\""
echo "  bq query \"SELECT day, feature_group, timeframe, COUNT(*) FROM ${PROJECT}.features_data.features_1m_cefi_v2 GROUP BY 1,2,3\""
echo
echo "Hive columns automatically extracted:"
echo "  • day (from day=YYYY-MM-DD folder)"
echo "  • feature_group (from feature_group={group} folder)"
echo "  • timeframe (from timeframe={tf} folder)"
