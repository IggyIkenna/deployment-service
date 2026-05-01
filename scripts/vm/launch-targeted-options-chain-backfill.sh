#!/usr/bin/env bash
# Targeted options_chain backfill — DERIBIT (CEFI) + CME-OPTIONS / CBOE-VIX-OPTIONS (TRADFI).
#
# Why a separate script: launch-cefi-sharded-backfill.sh re-captures the FULL
# CeFi matrix (95 VMs, all venues × years × heavy/light) which is huge
# overkill when we just need options_chain. This script spawns ONLY the
# year-shards for the chain venues, with DATA_TYPES limited to options_chain.
#
# Cost shape (per shard): e2-standard-2 + Tardis options_chain pull
#   DERIBIT: 7 year shards (2020-2026) × 1 = 7 VMs (BTC+ETH chain symbols)
#   CME-OPTIONS / CBOE-VIX-OPTIONS: skip if you've verified manifest claims
#       (run reconcile_market_tick_manifest.py first)
#
# Usage:
#   bash launch-targeted-options-chain-backfill.sh --dry
#   bash launch-targeted-options-chain-backfill.sh --commit                # all 3 venues × all years
#   bash launch-targeted-options-chain-backfill.sh --venue DERIBIT --year 2024 --commit
#
# The MTDS adapter honours the ``--skip-existing`` semantics already (manifest-
# guided pre-flight). With --force absent, days already in the manifest as
# captured will be skipped. So re-running this is idempotent and only spends
# Tardis credits for the actual gaps.

set -euo pipefail

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"
STARTUP="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
# 2026-05-01: bumped from e2-standard-2 (8GB) to e2-standard-4 (16GB) after
# DERIBIT 2024-2026 options_chain OOM-killed at peak RSS. Tardis options_chain
# has thousands of strikes/expiries per underlying — much heavier than the
# perp/spot data_types the previous heavy-profile fix targeted.
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"

DRY=1
SELECTED_VENUE=""
SELECTED_YEAR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry)    DRY=1; shift ;;
        --commit) DRY=0; shift ;;
        --venue)  SELECTED_VENUE="$2"; shift 2 ;;
        --year)   SELECTED_YEAR="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 2 ;;
    esac
done

# Per-venue chain symbol sets (only the bases — Tardis chain glob expands server-side).
SYMBOLS_DERIBIT="BTC;ETH"
SYMBOLS_CME_OPTIONS="ES"
SYMBOLS_CBOE_VIX_OPTIONS="VX"

_launch_shard() {
    local asset_group="$1"   # CEFI | TRADFI
    local venue="$2"
    local year="$3"
    local symbols="$4"

    if [[ -n "$SELECTED_VENUE" && "$SELECTED_VENUE" != "$venue" ]]; then return 0; fi
    if [[ -n "$SELECTED_YEAR" && "$SELECTED_YEAR" != "$year" ]]; then return 0; fi

    local start_date end_date
    if [[ "$year" == "2026" ]]; then
        start_date="${year}-01-01"; end_date="2026-04-17"
    else
        start_date="${year}-01-01"; end_date="${year}-12-31"
    fi

    local venue_lower vm_name
    venue_lower=$(echo "$venue" | tr '[:upper:]' '[:lower:]')
    vm_name="opt-${venue_lower}-${year}"

    local meta="startup-script-url=${STARTUP}"
    meta+=",VM_TASK=cefi-backfill"
    meta+=",VM_SERVICE=market_tick_data_service"
    meta+=",VM_OPERATION=download"
    meta+=",VM_ASSET_GROUP=${asset_group}"
    meta+=",VM_VENUE=${venue}"
    meta+=",VM_START_DATE=${start_date}"
    meta+=",VM_END_DATE=${end_date}"
    meta+=",VM_DATA_TYPES=options_chain"
    meta+=",VM_INSTRUMENT_IDS=${symbols}"
    # 2026-05-01: opt-in auto-delete after task completion (read by
    # vm-exec-with-gcs-tee.sh:253). Without this, one-shot backfill VMs sat
    # RUNNING idle after rc!=0 (or even rc==0) until manually killed — cost leak.
    meta+=",VM_SHUTDOWN_ON_COMPLETION=true"

    if [[ "$DRY" == "1" ]]; then
        echo "[DRY] ${vm_name} venue=${venue} year=${year} ${start_date}..${end_date}"
        echo "      data_types=options_chain symbols=${symbols}"
    else
        echo "Launching ${vm_name}"
        gcloud compute instances create "${vm_name}" \
            --zone="${ZONE}" --machine-type="${MACHINE_TYPE}" \
            --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
            --scopes=cloud-platform --metadata="${meta}" \
            --project="${PROJECT}" --async 2>&1 | tail -1
        sleep 2
    fi
}

# DERIBIT options_chain: 2020-2026
for y in 2020 2021 2022 2023 2024 2025 2026; do
    _launch_shard "CEFI" "DERIBIT" "${y}" "${SYMBOLS_DERIBIT}"
done

# CME-OPTIONS (ES futures options): 2020-2026
for y in 2020 2021 2022 2023 2024 2025 2026; do
    _launch_shard "TRADFI" "CME-OPTIONS" "${y}" "${SYMBOLS_CME_OPTIONS}"
done

# CBOE-VIX-OPTIONS: 2020-2026
for y in 2020 2021 2022 2023 2024 2025 2026; do
    _launch_shard "TRADFI" "CBOE-VIX-OPTIONS" "${y}" "${SYMBOLS_CBOE_VIX_OPTIONS}"
done

if [[ "$DRY" == "1" ]]; then
    echo
    echo "(dry — pass --commit to launch)"
fi
