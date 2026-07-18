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

# Tardis concurrent-VM cap (HARD RULE, at most 1). Enforced before the shard loops.
# shellcheck source=./tardis-concurrency-guard.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tardis-concurrency-guard.sh"
CODE_BUCKET="deployment-scripts-${PROJECT}"
STARTUP="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
# 2026-05-01: bumped from e2-standard-2 (8GB) to e2-standard-4 (16GB) after
# DERIBIT 2024-2026 options_chain OOM-killed at peak RSS. Tardis options_chain
# has thousands of strikes/expiries per underlying — much heavier than the
# perp/spot data_types the previous heavy-profile fix targeted.
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
# 2026-07-13: DERIBIT-COMBO specifically still OOM-kills at e2-standard-4
# (rc=137) even after the catalog-reader-once-per-process fix
# (market-tick-data-service@f8cab3f0) — live re-verify showed RSS already at
# 80.5% of 15GB from the SINGLE (correctly-deduped) ~1.6M-row catalog load
# alone, before DERIBIT-COMBO's own bulk-stream/Tier-3-fan-out overhead is
# even added. Root cause of why a single load costs that much is still open
# (cefi_deribit_combo_and_okx_bare_venue_gaps_2026_07_12.md); this bump is the
# todo's own explicitly-sanctioned faster mitigation, matching the RAM tier
# launch-cefi-sharded-backfill.sh already uses for the same catalog-loading
# pattern (e2-highmem-8, 64GB) — DERIBIT-COMBO only, other venues on this
# launcher (DERIBIT/OKX/CME-OPTIONS/CBOE-VIX-OPTIONS) proved fine at 15GB.
MACHINE_TYPE_DERIBIT_COMBO="${MACHINE_TYPE_DERIBIT_COMBO:-e2-highmem-8}"
# 2026-07-12: this launcher had NO --boot-disk-size (image default, ~10GB),
# the one outlier among cefi Tardis backfill launchers — every sibling
# (launch-cefi-sharded-backfill.sh, launch-cefi-hl-aster-historical-
# backfill.sh) already sets 50GB. Confirmed live: OKX options_chain hit
# "[Errno 28] No space left on device" mid-stream after a 58M+ row Tardis
# fetch (cefi_deribit_combo_and_okx_bare_venue_gaps_2026_07_12.md round 5).

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

    local shard_machine_type="$MACHINE_TYPE"
    if [[ "$venue" == "DERIBIT-COMBO" ]]; then
        shard_machine_type="$MACHINE_TYPE_DERIBIT_COMBO"
    fi

    local venue_lower vm_name
    venue_lower=$(echo "$venue" | tr '[:upper:]' '[:lower:]')
    vm_name="opt-${venue_lower}-${year}"

    local meta="startup-script-url=${STARTUP}"
    meta+=",VM_TASK=cefi-backfill"
    meta+=",VM_SERVICE=market_tick_data_service"
    meta+=",VM_OPERATION=download"
    # Countable by every other Tardis launcher's guard (union: name pattern OR this tag).
    # CEFI options_chain venues resolve to the tardis adapter; TRADFI ones do not.
    [[ "${asset_group}" == "CEFI" ]] && meta+=",VM_TARDIS_CONSUMER=1"
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
    # 2026-07-12: relax the manifest-consolidator freshness preflight (default
    # 120s) — the large cefi bucket's real Cloud Run consolidator cadence
    # regularly exceeds 120s, so every launch of this VM was hard-failing at
    # bootstrap (assert_consolidator_healthy -> ManifestConsolidatorStaleError)
    # with 0 rows captured. Every other cefi/large-bucket backfill launcher
    # already sets this same 86400s (24h) budget
    # (launch-cefi-sharded-backfill.sh, launch-cefi-hl-aster-historical-
    # backfill.sh, etc.) — this script was the one outlier missing it.
    meta+=",MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
    # Tardis single-concurrent-IP lease (option (a) stopgap, DEFAULT-OFF —
    # tardis_concurrent_ip_lockout_2026_07_12). Opt-in ONLY: set
    # TARDIS_CONCURRENCY_LEASE=1 + TARDIS_CONCURRENCY_LEASE_BUCKET=<control-bucket>
    # on the launcher to serialise every Tardis-calling VM through one GCS TTL lease
    # (fixes the concurrent-IP 403 lockout at the cost of ~20-80x slower waves). Left
    # unstamped by default so MTDS's default-False flag keeps waves fully parallel.
    # The bucket MUST be a coordination bucket, NOT a data/manifest-walked bucket.
    # Mirrors launch-cefi-sharded-backfill.sh — this launcher was the one outlier
    # missing this plumbing (cefi_deribit_combo_and_okx_bare_venue_gaps_2026_07_12.md,
    # 2026-07-13T09:03Z finding: DERIBIT-COMBO options_chain backfills through this
    # script cannot opt into the lease without it).
    [[ "${TARDIS_CONCURRENCY_LEASE:-}" == "1" ]] && meta+=",TARDIS_CONCURRENCY_LEASE=1"
    [[ -n "${TARDIS_CONCURRENCY_LEASE_BUCKET:-}" ]] && meta+=",TARDIS_CONCURRENCY_LEASE_BUCKET=${TARDIS_CONCURRENCY_LEASE_BUCKET}"
    [[ -n "${TARDIS_MAX_CONCURRENT_DOWNLOADS:-}" ]] && meta+=",TARDIS_MAX_CONCURRENT_DOWNLOADS=${TARDIS_MAX_CONCURRENT_DOWNLOADS}"

    # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
    PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
    if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

    if [[ "$DRY" == "1" ]]; then
        echo "[DRY] ${vm_name} venue=${venue} year=${year} ${start_date}..${end_date} machine=${shard_machine_type}"
        echo "      data_types=options_chain symbols=${symbols}"
    else
        echo "Launching ${vm_name} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)] machine=${shard_machine_type}"
        if [[ "${DRY_RUN:-false}" == "true" ]]; then
          echo "[DRY-RUN] Would create VM: "${vm_name}""
          echo "[DRY-RUN] (gcloud compute instances create skipped)"
        else
          # shellcheck disable=SC2086
          gcloud compute instances create "${vm_name}" \
              --zone="${ZONE}" --machine-type="${shard_machine_type}" \
              ${PROVISIONING_FLAGS} \
              --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
              # Tardis-consumer boot disk (measured 2026-07-18): pd-standard 50GB sustains only
              # ~6 MB/s of writes and its burst credits deplete by CUMULATIVE BYTES WRITTEN, which
              # throttled the cefi backfill to ~2.4 MB/s after ~7.5GB (iostat: %util 99.94,
              # w_await 1015ms, CPU 93.5% idle, RAM 115GB free — pure disk starvation, misread for
              # hours as a Tardis quota). Tardis serves .csv.gz so RX is ~5x-amplified on write.
              # 250GB pd-balanced = ~70 MB/s. Enforced by
              # scripts/quality_gates/check_tardis_vm_disk_provisioning.py — do NOT drop back.
              --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
              --scopes=cloud-platform --metadata="${meta}" \
              --labels=purpose=targeted-options-chain-backfill,env="${DEPLOYMENT_ENV}" \
              --project="${PROJECT}" --async 2>&1 | tail -1
        fi
        sleep 2
    fi
}

# ─── Tardis concurrent-VM cap (HARD RULE: operator 2026-07-16, at most 1) ─────────────
# Only the CEFI groups are Tardis consumers: VENUE_TO_ADAPTER_KEY (the SSOT, in
# unified-api-contracts/registry/venue_adapter_keys.py) maps DERIBIT / DERIBIT-COMBO /
# OKX -> "tardis", while CME-OPTIONS and CBOE-VIX-OPTIONS are not Tardis venues at all
# (TradFi is Databento-sourced) — counting those would make every OTHER Tardis launcher
# refuse for no reason. So: 3 CEFI venues x 7 years (2020..2026) = 21 planned.
# This fan-out asks for 21 >> the cap, so the guard will REFUSE — intended, not a bug.
# N>1 on the one Tardis key measured ~94% 403s + 37,212 FALSE attempted_failed rows
# (manifest CORRUPTION) + coverage going BACKWARD. Use launch-cefi-sharded-backfill.sh
# with SINGLE_VM_QUEUE=1 to bundle these into ONE VM; FORCE=1 is the operator override.
PLANNED_TARDIS_VMS=21
tardis_concurrency_guard "$PLANNED_TARDIS_VMS" "$ZONE" "$PROJECT" || exit 1

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
