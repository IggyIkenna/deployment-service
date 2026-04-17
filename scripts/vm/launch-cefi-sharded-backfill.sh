#!/usr/bin/env bash
# Launch sharded CeFi VMs for 12h completion target
set -e

PROJECT=central-element-323112
ZONE=asia-northeast1-b
STARTUP=gs://deployment-scripts-central-element-323112/vm/setup-data-pipeline-vm.sh

launch_shard() {
  local venue="$1"
  local year="$2"
  local group="$3"

  local start_date end_date data_types machine
  if [[ "$year" == "2026" ]]; then
    start_date="${year}-01-01"
    end_date="2026-04-17"
  else
    start_date="${year}-01-01"
    end_date="${year}-12-31"
  fi

  if [[ "$group" == "heavy" ]]; then
    data_types="trades,book_snapshot_5"
    machine="e2-standard-4"
  else
    data_types="derivative_ticker,liquidations,futures_chain,options_chain"
    machine="e2-standard-2"
  fi

  local venue_lower
  venue_lower=$(echo "$venue" | tr '[:upper:]-' '[:lower:]-')
  local vm_name="cefi-${venue_lower}-${year}-${group}"

  echo "Launching $vm_name ($venue $year $group data_types=$data_types)"
  gcloud compute instances create "$vm_name" \
    --zone=$ZONE \
    --machine-type=$machine \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --scopes=cloud-platform \
    --metadata=startup-script-url=$STARTUP,VM_TASK=cefi-backfill,VM_SERVICE=market_tick_data_service,VM_OPERATION=download,VM_CATEGORY=CEFI,VM_VENUE=$venue,VM_START_DATE=$start_date,VM_END_DATE=$end_date,VM_DATA_TYPES=$data_types \
    --project=$PROJECT --async 2>&1 | tail -1 &
}

# Binance-Futures: 2020-2026
for year in 2020 2021 2022 2023 2024 2025 2026; do
  launch_shard "BINANCE-FUTURES" "$year" "heavy"
  launch_shard "BINANCE-FUTURES" "$year" "light"
done

# Binance-Spot: 2020-2026
for year in 2020 2021 2022 2023 2024 2025 2026; do
  launch_shard "BINANCE-SPOT" "$year" "heavy"
  launch_shard "BINANCE-SPOT" "$year" "light"
done

# Bybit: 2021-2026
for year in 2021 2022 2023 2024 2025 2026; do
  launch_shard "BYBIT" "$year" "heavy"
  launch_shard "BYBIT" "$year" "light"
done

wait
echo "All VMs launched"
