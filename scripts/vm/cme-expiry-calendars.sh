#!/usr/bin/env bash
# CME futures expiry calendars (last-trading-date) — sourced by tradfi launchers.
#
# Symbol-id format: CME:FUTURE:<ROOT>-YYYYMMDD where YYYYMMDD = official CME
# last-trade-date per the contract spec.
#
# LTD conventions per CME spec:
#   ES   — 3rd Friday of contract month (E-mini S&P 500 quarterlies H/M/U/Z)
#   BTC  — last Friday of contract month (Bitcoin futures, all 12 months)
#   ETH  — last Friday of contract month (Ether futures, all 12 months)
#
# Generated 2026-04-30. Dates pre-computed and embedded as static literals so
# the launcher works on VMs that may lack Python.
#
# Sourced by: launch-tradfi-backfill-vm.sh

# shellcheck disable=SC2034  # arrays are read by sourcing scripts via indirection

# ── ES quarterlies (3rd Friday) — preserved from prior single-tier launcher ──
declare -A ES_EXPIRIES_BY_YEAR=(
    ["2022"]="CME:FUTURE:ES-20220318;CME:FUTURE:ES-20220617;CME:FUTURE:ES-20220916;CME:FUTURE:ES-20221216"
    ["2023"]="CME:FUTURE:ES-20230317;CME:FUTURE:ES-20230616;CME:FUTURE:ES-20230915;CME:FUTURE:ES-20231215"
    ["2024"]="CME:FUTURE:ES-20240315;CME:FUTURE:ES-20240621;CME:FUTURE:ES-20240920;CME:FUTURE:ES-20241220"
    ["2025"]="CME:FUTURE:ES-20250321;CME:FUTURE:ES-20250620;CME:FUTURE:ES-20250919;CME:FUTURE:ES-20251219"
    ["2026"]="CME:FUTURE:ES-20260320;CME:FUTURE:ES-20260619"
)

# ── BTC monthlies (last Friday) — all 12/year ──
declare -A BTC_EXPIRIES_BY_YEAR=(
    ["2022"]="CME:FUTURE:BTC-20220128;CME:FUTURE:BTC-20220225;CME:FUTURE:BTC-20220325;CME:FUTURE:BTC-20220429;CME:FUTURE:BTC-20220527;CME:FUTURE:BTC-20220624;CME:FUTURE:BTC-20220729;CME:FUTURE:BTC-20220826;CME:FUTURE:BTC-20220930;CME:FUTURE:BTC-20221028;CME:FUTURE:BTC-20221125;CME:FUTURE:BTC-20221230"
    ["2023"]="CME:FUTURE:BTC-20230127;CME:FUTURE:BTC-20230224;CME:FUTURE:BTC-20230331;CME:FUTURE:BTC-20230428;CME:FUTURE:BTC-20230526;CME:FUTURE:BTC-20230630;CME:FUTURE:BTC-20230728;CME:FUTURE:BTC-20230825;CME:FUTURE:BTC-20230929;CME:FUTURE:BTC-20231027;CME:FUTURE:BTC-20231124;CME:FUTURE:BTC-20231229"
    ["2024"]="CME:FUTURE:BTC-20240126;CME:FUTURE:BTC-20240223;CME:FUTURE:BTC-20240329;CME:FUTURE:BTC-20240426;CME:FUTURE:BTC-20240531;CME:FUTURE:BTC-20240628;CME:FUTURE:BTC-20240726;CME:FUTURE:BTC-20240830;CME:FUTURE:BTC-20240927;CME:FUTURE:BTC-20241025;CME:FUTURE:BTC-20241129;CME:FUTURE:BTC-20241227"
    ["2025"]="CME:FUTURE:BTC-20250131;CME:FUTURE:BTC-20250228;CME:FUTURE:BTC-20250328;CME:FUTURE:BTC-20250425;CME:FUTURE:BTC-20250530;CME:FUTURE:BTC-20250627;CME:FUTURE:BTC-20250725;CME:FUTURE:BTC-20250829;CME:FUTURE:BTC-20250926;CME:FUTURE:BTC-20251031;CME:FUTURE:BTC-20251128;CME:FUTURE:BTC-20251226"
    ["2026"]="CME:FUTURE:BTC-20260130;CME:FUTURE:BTC-20260227;CME:FUTURE:BTC-20260327;CME:FUTURE:BTC-20260424;CME:FUTURE:BTC-20260529;CME:FUTURE:BTC-20260626;CME:FUTURE:BTC-20260731;CME:FUTURE:BTC-20260828;CME:FUTURE:BTC-20260925;CME:FUTURE:BTC-20261030;CME:FUTURE:BTC-20261127;CME:FUTURE:BTC-20261225"
)

# ── ETH monthlies (last Friday) — all 12/year ──
declare -A ETH_EXPIRIES_BY_YEAR=(
    ["2022"]="CME:FUTURE:ETH-20220128;CME:FUTURE:ETH-20220225;CME:FUTURE:ETH-20220325;CME:FUTURE:ETH-20220429;CME:FUTURE:ETH-20220527;CME:FUTURE:ETH-20220624;CME:FUTURE:ETH-20220729;CME:FUTURE:ETH-20220826;CME:FUTURE:ETH-20220930;CME:FUTURE:ETH-20221028;CME:FUTURE:ETH-20221125;CME:FUTURE:ETH-20221230"
    ["2023"]="CME:FUTURE:ETH-20230127;CME:FUTURE:ETH-20230224;CME:FUTURE:ETH-20230331;CME:FUTURE:ETH-20230428;CME:FUTURE:ETH-20230526;CME:FUTURE:ETH-20230630;CME:FUTURE:ETH-20230728;CME:FUTURE:ETH-20230825;CME:FUTURE:ETH-20230929;CME:FUTURE:ETH-20231027;CME:FUTURE:ETH-20231124;CME:FUTURE:ETH-20231229"
    ["2024"]="CME:FUTURE:ETH-20240126;CME:FUTURE:ETH-20240223;CME:FUTURE:ETH-20240329;CME:FUTURE:ETH-20240426;CME:FUTURE:ETH-20240531;CME:FUTURE:ETH-20240628;CME:FUTURE:ETH-20240726;CME:FUTURE:ETH-20240830;CME:FUTURE:ETH-20240927;CME:FUTURE:ETH-20241025;CME:FUTURE:ETH-20241129;CME:FUTURE:ETH-20241227"
    ["2025"]="CME:FUTURE:ETH-20250131;CME:FUTURE:ETH-20250228;CME:FUTURE:ETH-20250328;CME:FUTURE:ETH-20250425;CME:FUTURE:ETH-20250530;CME:FUTURE:ETH-20250627;CME:FUTURE:ETH-20250725;CME:FUTURE:ETH-20250829;CME:FUTURE:ETH-20250926;CME:FUTURE:ETH-20251031;CME:FUTURE:ETH-20251128;CME:FUTURE:ETH-20251226"
    ["2026"]="CME:FUTURE:ETH-20260130;CME:FUTURE:ETH-20260227;CME:FUTURE:ETH-20260327;CME:FUTURE:ETH-20260424;CME:FUTURE:ETH-20260529;CME:FUTURE:ETH-20260626;CME:FUTURE:ETH-20260731;CME:FUTURE:ETH-20260828;CME:FUTURE:ETH-20260925;CME:FUTURE:ETH-20261030;CME:FUTURE:ETH-20261127;CME:FUTURE:ETH-20261225"
)

# Active-contract sets for the heavy-tier crypto-basis windows.
# CME BTC/ETH list ~6 months out at any time; a contract is tradeable from listing
# (~6mo before LTD) through LTD. For a one-month heavy window we include every
# contract whose [listing, LTD] interval overlaps the window. Curated explicitly
# (rather than computed at runtime) so the active set is auditable and the
# launcher has zero date arithmetic.

# Active during 2023-05 (May 2023): contracts with LTD between May 2023 and Nov 2023.
BTC_ACTIVE_2023_05="CME:FUTURE:BTC-20230526;CME:FUTURE:BTC-20230630;CME:FUTURE:BTC-20230728;CME:FUTURE:BTC-20230825;CME:FUTURE:BTC-20230929;CME:FUTURE:BTC-20231027;CME:FUTURE:BTC-20231124"
ETH_ACTIVE_2023_05="CME:FUTURE:ETH-20230526;CME:FUTURE:ETH-20230630;CME:FUTURE:ETH-20230728;CME:FUTURE:ETH-20230825;CME:FUTURE:ETH-20230929;CME:FUTURE:ETH-20231027;CME:FUTURE:ETH-20231124"

# Active during 2024-06 (June 2024): contracts with LTD between June 2024 and Dec 2024.
BTC_ACTIVE_2024_06="CME:FUTURE:BTC-20240628;CME:FUTURE:BTC-20240726;CME:FUTURE:BTC-20240830;CME:FUTURE:BTC-20240927;CME:FUTURE:BTC-20241025;CME:FUTURE:BTC-20241129;CME:FUTURE:BTC-20241227"
ETH_ACTIVE_2024_06="CME:FUTURE:ETH-20240628;CME:FUTURE:ETH-20240726;CME:FUTURE:ETH-20240830;CME:FUTURE:ETH-20240927;CME:FUTURE:ETH-20241025;CME:FUTURE:ETH-20241129;CME:FUTURE:ETH-20241227"

# Helper: emit semicolon-delimited list of contracts active in a heavy month for a root.
# Usage: cme_active_contracts BTC 2024-06
cme_active_contracts() {
    local root="$1"
    local month="$2"
    local var="${root}_ACTIVE_${month//-/_}"
    printf '%s' "${!var:-}"
}

# Helper: emit semicolon-delimited contracts for a (root, year) covered by the calendars.
# Usage: cme_year_contracts BTC 2024
cme_year_contracts() {
    local root="$1"
    local year="$2"
    local map_name="${root}_EXPIRIES_BY_YEAR"
    declare -n _ref="$map_name"
    printf '%s' "${_ref[$year]:-}"
}

# Databento parent-symbol set per root. Databento parent symbology
# (stype_in=parent) returns every active contract in the chain for the
# requested date window — so the adapter receives ONE symbol like
# "BTC.FUT" and pulls all BTC contracts that traded in the window. This
# is the correct shape for the MTDS DatabentoAdapter, which keys
# stype_in selection on the .FUT / .OPT suffix
# (databento_adapter.py:151).
#
# Per-contract canonical IDs (CME:FUTURE:BTC-YYYYMMDD) DO NOT work with
# the adapter — they are treated as raw_symbols and Databento does not
# recognise them, returning 0 records. The contract-list calendars
# above remain as documentation / audit reference for which contracts
# are expected to land in any given window.
cme_parent_symbols() {
    local root="$1"
    case "$root" in
        BTC)     printf '%s' "BTC.FUT" ;;
        ETH)     printf '%s' "ETH.FUT" ;;
        ES)      printf '%s' "ES.FUT" ;;
        MES)     printf '%s' "MES.FUT" ;;
        ES_OPT)  printf '%s' "ES.OPT;EW.OPT;EW1.OPT;EW2.OPT;EW4.OPT;E1A.OPT;E2A.OPT;E3A.OPT;E4A.OPT;E5A.OPT;EOM.OPT" ;;
        # Spot ETFs — single ticker per root, raw_symbol stype, DBEQ.BASIC.
        # Pre-listing years yield empty Databento responses (empty_confirmed
        # in the manifest) so launching beyond the listing year is safe but
        # wasteful; the etf_default_years_for_root helper clips this.
        IBIT|FBTC|GBTC|BITO|ARKB|ETHA|FETH|ETHE)
                 printf '%s' "$root" ;;
        *)       printf '' ;;
    esac
}

# Year-shard set for a root. CME futures + ES_OPT span 2022..2026; spot
# crypto ETFs are clipped to their listing year onward.
#   IBIT, FBTC, ARKB, GBTC (post-uplisting): 2024-01-11+
#   ETHA, FETH, ETHE (post-uplisting):       2024-07-23+ — but we run
#       full 2024 + onwards; pre-listing months return empty_confirmed
#       which is harmless and cheap.
#   BITO: 2021-10-19+, full 2022..today.
default_years_for_root() {
    case "$1" in
        ES|ES_OPT|MES|BTC|ETH)         echo "2022 2023 2024 2025 2026" ;;
        BITO)                          echo "2022 2023 2024 2025 2026" ;;
        IBIT|FBTC|GBTC|ARKB)           echo "2024 2025 2026" ;;
        ETHA|FETH|ETHE)                echo "2024 2025 2026" ;;
        *)                             echo "" ;;
    esac
}

# Per-ticker listing date (ISO YYYY-MM-DD) for backfill clipping. Pre-listing
# dates within a year-shard return 0 records and write empty_confirmed
# manifest rows — wasteful, correct-but-noisy. Clip to the listing date
# inside _launch_light_year so the start_date never precedes listing.
listing_date_for_root() {
    case "$1" in
        IBIT|FBTC|GBTC|ARKB)           echo "2024-01-11" ;;  # US BTC spot ETF launch
        ETHA|FETH|ETHE)                echo "2024-07-23" ;;  # US ETH spot ETF launch
        BITO)                          echo "2021-10-19" ;;  # ProShares futures-based BTC
        *)                             echo "" ;;            # No clip
    esac
}

# ── ES options — parent symbols (stable across years, parent symbology) ──
# Databento GLBX.MDP3, stype_in="parent" — fetching with these parent symbols
# returns the full options chain (every strike + expiry) for the requested
# date window. UAC declares these in tradfi_instrument_universe.py:155-171.
#   ES   = quarterly options (3rd Friday of quarter month)
#   EW   = weekly options (Friday)
#   EW1/EW2/EW4 = Mon/Tue/Wed weekly options
#   E1A-E5A = daily options (Mon..Fri 0DTE)
#   EOM  = end-of-month options (last business day)
# Together: full quarterly + weekly + daily volatility surface.
ES_OPT_PARENTS="CME:OPTION:ES;CME:OPTION:EW;CME:OPTION:EW1;CME:OPTION:EW2;CME:OPTION:EW4;CME:OPTION:E1A;CME:OPTION:E2A;CME:OPTION:E3A;CME:OPTION:E4A;CME:OPTION:E5A;CME:OPTION:EOM"

# Per-year list is just the parent set — options use parent symbology, not
# dated expiries; one fetch per year-shard returns all strikes/expiries
# active in that window. Declared per-year for symmetry with the futures
# calendar (lookup by ${ROOT}_EXPIRIES_BY_YEAR pattern).
declare -A ES_OPT_EXPIRIES_BY_YEAR=(
    ["2022"]="$ES_OPT_PARENTS"
    ["2023"]="$ES_OPT_PARENTS"
    ["2024"]="$ES_OPT_PARENTS"
    ["2025"]="$ES_OPT_PARENTS"
    ["2026"]="$ES_OPT_PARENTS"
)
