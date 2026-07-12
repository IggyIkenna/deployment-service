#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Targeted options_chain backfill — DERIBIT + OKX (CEFI) + CME-OPTIONS / CBOE-VIX-OPTIONS (TRADFI).
#
# Why a separate script: launch-cefi-sharded-backfill.sh re-captures the FULL
# CeFi matrix (95 VMs, all venues × years × heavy/light) which is huge
# overkill when we just need options_chain. This script spawns ONLY the
# year-shards for the chain venues, with DATA_TYPES limited to options_chain.
#
# OKX added 2026-07-12 (cefi_deribit_combo_and_okx_bare_venue_gaps_2026_07_12.md
# todo 2) once market-tick-data-service's Tardis exchange resolution became
# instrument-type-aware for options_chain/futures_chain requests — before that
# fix, an OKX options_chain request silently resolved to the wrong Tardis
# exchange slug (venue-only lookup, not the itype-specific "okex-options").
# Start year 2020 matches Tardis okex-options `availableSince: 2020-02-01`.
#
# Cost shape (per shard): e2-standard-2/4 + Tardis options_chain pull
#   DERIBIT: 7 year shards (2020-2026) × 1 = 7 VMs (BTC+ETH chain symbols)
#   DERIBIT-COMBO: 7 year shards (2020-2026) × 1 = 7 VMs (BTC+ETH chain symbols)
#   OKX: 7 year shards (2020-2026) × 1 = 7 VMs (BTC+ETH chain symbols)
#   CME-OPTIONS / CBOE-VIX-OPTIONS: skip if you've verified manifest claims
#       (run reconcile_market_tick_manifest.py first)
#
# Usage:
#   bash launch-targeted-options-chain-backfill.sh --dry
#   bash launch-targeted-options-chain-backfill.sh --commit                # all 4 venues × all years
#   bash launch-targeted-options-chain-backfill.sh --venue OKX --year 2024 --commit
#
# The MTDS adapter honours the ``--skip-existing`` semantics already (manifest-
# guided pre-flight). With --force absent, days already in the manifest as
# captured will be skipped. So re-running this is idempotent and only spends
# Tardis credits for the actual gaps.

set -euo pipefail

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
STARTUP="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
# 2026-05-01: bumped from e2-standard-2 (8GB) to e2-standard-4 (16GB) after
# DERIBIT 2024-2026 options_chain OOM-killed at peak RSS. Tardis options_chain
# has thousands of strikes/expiries per underlying — much heavier than the
# perp/spot data_types the previous heavy-profile fix targeted.
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"

DRY=1
SELECTED_VENUE=""
SELECTED_YEAR=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)   DRY_RUN=true; shift ;;
        --dry)       DRY=1; shift ;;
        --commit)    DRY=0; shift ;;
        --venue)     SELECTED_VENUE="$2"; shift 2 ;;
        --year)      SELECTED_YEAR="$2"; shift 2 ;;
        --env)       DEPLOYMENT_ENV="$2"; shift 2 ;;
        --on-demand) ON_DEMAND=true; shift ;;
        *) echo "Unknown arg: $1"; exit 2 ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# Per-venue chain symbol sets (only the bases — Tardis chain glob expands server-side).
SYMBOLS_DERIBIT="BTC;ETH"
SYMBOLS_DERIBIT_COMBO="BTC;ETH"
SYMBOLS_OKX="BTC;ETH"
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
    meta+=",DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    # 2026-05-01: opt-in auto-delete after task completion (read by
    # vm-exec-with-gcs-tee.sh:253). Without this, one-shot backfill VMs sat
    # RUNNING idle after rc!=0 (or even rc==0) until manually killed — cost leak.
    meta+=",VM_SHUTDOWN_ON_COMPLETION=true"

    # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
    PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
    if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

    if [[ "$DRY" == "1" ]]; then
        echo "[DRY] ${vm_name} venue=${venue} year=${year} ${start_date}..${end_date}"
        echo "      data_types=options_chain symbols=${symbols}"
    else
        echo "Launching ${vm_name} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
        if [[ "${DRY_RUN:-false}" == "true" ]]; then
          echo "[DRY-RUN] Would create VM: "${vm_name}""
          echo "[DRY-RUN] (gcloud compute instances create skipped)"
        else
          # shellcheck disable=SC2086
          gcloud compute instances create "${vm_name}" \
              --zone="${ZONE}" --machine-type="${MACHINE_TYPE}" \
              ${PROVISIONING_FLAGS} \
              --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
              --scopes=cloud-platform --metadata="${meta}" \
              --labels=purpose=targeted-options-chain-backfill,env="${DEPLOYMENT_ENV}" \
              --project="${PROJECT}" --async 2>&1 | tail -1
        fi
        sleep 2
    fi
}

# DERIBIT options_chain: 2020-2026
for y in 2020 2021 2022 2023 2024 2025 2026; do
    _launch_shard "CEFI" "DERIBIT" "${y}" "${SYMBOLS_DERIBIT}"
done

# DERIBIT-COMBO options_chain: 2020-2026. Added 2026-07-12
# (cefi_deribit_combo_and_okx_bare_venue_gaps_2026_07_12.md todo 1) once the
# venue's Tardis routing + canonical_venue propagation + combo row
# classification 3 fixes landed (market-tick-data-service@7dbd19f4,
# @1bc4e000, unified-api-contracts@f0dc61a2). Shares the DERIBIT exchange
# slug (Tardis "deribit"), filtered server-side to type=='combo' symbols by
# the adapter; pre-coverage dates are expected to self-classify honest-empty
# via EXPECTED_SOURCE_DOES_NOT_OFFER_DATA_TYPE per the issue doc's design.
for y in 2020 2021 2022 2023 2024 2025 2026; do
    _launch_shard "CEFI" "DERIBIT-COMBO" "${y}" "${SYMBOLS_DERIBIT_COMBO}"
done

# OKX options_chain: 2020-2026 (Tardis okex-options availableSince 2020-02-01)
for y in 2020 2021 2022 2023 2024 2025 2026; do
    _launch_shard "CEFI" "OKX" "${y}" "${SYMBOLS_OKX}"
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
