#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch sharded TRADFI backfill VMs — Databento CME futures (ES + crypto:
# BTC, ETH) via the unified MTDS download CLI.
#
# Writes to:
#   gs://<tick-data-bucket>/raw_tick_data/
#     by_date/day={D}/category=tradfi/venue=CME/instrument_type=future/
#     data_type={ohlcv_1m,trades,mbp_10}/<ROOT>-<EXPIRY>.parquet
#
# Manifest:
#   gs://<tick-data-bucket>/_index/availability_index.parquet
#
# ── Roots + tiers ────────────────────────────────────────────────────────────
# ES  — quarterlies, single-tier `ohlcv_1m;trades` for the year (Tier 1 MVP
#       directional-signal calendar, unchanged from the prior launcher).
# BTC — 12 monthlies/year. Crypto-basis tier-plan splits the window into
#       "heavy" (full L2 book: trades + mbp_10) and "light" (ohlcv_1m only).
# ETH — same shape as BTC.
#
# Tier plan `crypto-basis-2023-05+2024-06`:
#     - heavy VM for 2023-05 (active contracts only) -> trades;mbp_10
#     - heavy VM for 2024-06 (active contracts only) -> trades;mbp_10
#     - light VMs for 2022..today, splitting around the heavy months -> ohlcv_1m
# Per root: 9 VMs (2 heavy + 7 light). For BTC + ETH: 18 VMs.
#
# ── Sharding ─────────────────────────────────────────────────────────────────
# One VM per (root, tier, contiguous date-range). Singleton lock matches
# `^tradfi-bf-` so all 18 VMs serialize across the shared Databento account.
# --force bypasses the lock for legitimate parallel investigation.
#
# ── Usage ────────────────────────────────────────────────────────────────────
#   # Existing ES behavior (unchanged):
#   bash launch-tradfi-backfill-vm.sh --dry-run
#   bash launch-tradfi-backfill-vm.sh --year 2026
#
#   # Crypto-basis tier-plan (full BTC fan-out):
#   bash launch-tradfi-backfill-vm.sh --root-symbol BTC --tier-plan crypto-basis-2023-05+2024-06 --dry-run
#   bash launch-tradfi-backfill-vm.sh --root-symbol BTC --tier-plan crypto-basis-2023-05+2024-06
#
#   # Single heavy month (smoke):
#   bash launch-tradfi-backfill-vm.sh --root-symbol BTC --tier heavy --month 2024-06
#
#   # Single light year (smoke):
#   bash launch-tradfi-backfill-vm.sh --root-symbol BTC --tier light --year 2024 --skip-months 2024-06
#
#   # Skip already-captured shards (manifest pre-flight on, no force):
#   bash launch-tradfi-backfill-vm.sh --root-symbol ES_OPT --year 2020 --no-force-window
#
# Prerequisites:
#   - Tarballs uploaded to gs://deployment-scripts-central-element-323112/code/
#     (run `bash create-code-tarballs.sh --asset-group TRADFI` first).
#   - databento-api-key in Secret Manager.
#
# Cost: e2-standard-4 per shard. ohlcv_1m year-shards complete in ~30-90 min;
# mbp_10 month-shards similar (less calendar coverage but ~10x book volume).
#
# Singleton lock: refuses to launch if any tradfi-bf-* VM is RUNNING in the
# zone. Databento per-IP rate-limits are looser than RapidAPI but the account
# is shared across the team; concurrent backfills risk contract-exceeded
# errors on aggressive windows. --force bypasses for legitimate parallel
# investigations. Mirrors the launch-mtds-prediction-backfill-vm.sh +
# launch-sfi-forward-poll.sh pattern (2026-04-20 SSOT).
#
# Observability: inherits the STALL_TIMEOUT_SEC=600 log-mtime watchdog +
# `timeout 30s` run_heartbeat + py-spy stack dump + pkill-by-name fallback
# from vm-exec-with-gcs-tee.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=cme-expiry-calendars.sh
source "${SCRIPT_DIR}/cme-expiry-calendars.sh"
# shellcheck source=lib/launcher_common.sh
source "${SCRIPT_DIR}/lib/launcher_common.sh"

# ── Defaults + arg parsing ──
FORCE=false
DRY_RUN=false
ROOT_SYMBOL="ES"
YEAR=""
MONTH=""
TIER=""
TIER_PLAN=""
INSTRUMENT_IDS_OVERRIDE=""
DATA_TYPES_OVERRIDE=""
START_DATE_OVERRIDE=""
END_DATE_OVERRIDE=""
SKIP_MONTHS=""
FORCE_WINDOW=true
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force)            FORCE=true; shift ;;
        --dry-run)          DRY_RUN=true; shift ;;
        --no-force-window)  FORCE_WINDOW=false; shift ;;
        --root-symbol)      ROOT_SYMBOL="$2"; shift 2 ;;
        --year)             YEAR="$2"; shift 2 ;;
        --month)            MONTH="$2"; shift 2 ;;
        --tier)             TIER="$2"; shift 2 ;;
        --tier-plan)        TIER_PLAN="$2"; shift 2 ;;
        --instrument-ids)   INSTRUMENT_IDS_OVERRIDE="$2"; shift 2 ;;
        --data-types)       DATA_TYPES_OVERRIDE="$2"; shift 2 ;;
        --start-date)       START_DATE_OVERRIDE="$2"; shift 2 ;;
        --end-date)         END_DATE_OVERRIDE="$2"; shift 2 ;;
        --skip-months)      SKIP_MONTHS="$2"; shift 2 ;;
        --env)              DEPLOYMENT_ENV="$2"; shift 2 ;;
        --on-demand)        ON_DEMAND=true; shift ;;
        --help|-h)
            grep '^#' "$0" | head -70
            exit 0
            ;;
        *)
            echo "Unknown arg: $1" >&2
            echo "Usage: $0 [--root-symbol ES|BTC|ETH] [--tier-plan crypto-basis-2023-05+2024-06] [--tier light|heavy] [--year YYYY] [--month YYYY-MM] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--instrument-ids 'ID;ID'] [--data-types 'ohlcv_1m;trades'] [--skip-months 'YYYY-MM,YYYY-MM'] [--dry-run] [--force] [--no-force-window]" >&2
            exit 1
            ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
STARTUP="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
MACHINE_TYPE="e2-standard-4"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

# ── Validate root ──
# MVP TradFi (2026-05-05 scope): CME futures + ES options + BlackRock spot ETFs
# (IBIT, ETHA on NASDAQ). FBTC/ARKB/FETH (BATS) and GBTC/ETHE/BITO (NYSE Arca)
# dropped from scope — date-futures arb only needs CME futures + Deribit
# futures (same expiry day), and the BlackRock ETFs alone cover spot exposure.
case "$ROOT_SYMBOL" in
    ES|ES_OPT|MES|BTC|ETH|IBIT|ETHA) ;;
    *)
        echo "ERROR: --root-symbol must be one of ES|ES_OPT|MES|BTC|ETH|IBIT|ETHA (got '$ROOT_SYMBOL')" >&2
        exit 1
        ;;
esac

# ── Singleton lock (skipped on dry-run) ──
_check_singleton_lock() {
    if $FORCE || $DRY_RUN; then return 0; fi
    local existing
    existing="$(gcloud compute instances list \
        --filter='name~"^tradfi-bf-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$existing" ]]; then
        cat >&2 <<EOF
ERROR: TRADFI backfill VM already running in $ZONE: $existing
Refusing to launch a duplicate — the Databento account is shared; concurrent
VMs on wide windows risk contract-exceeded errors. Adding more shards in
parallel does not speed up a bounded quota.

Options:
  Inspect:   gcloud compute ssh $existing --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${existing}/run.log
  Force:     bash $0 --force [args...]

CAUTION — do NOT delete $existing unless you have confirmed via Inspect/Tail
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If confirmed stale:
  gcloud compute instances delete $existing --zone=$ZONE --quiet
EOF
        exit 1
    fi
}

# ── VM creator: builds METADATA, invokes gcloud (or dry-run echo) ──
# Args: vm_name root tier start_date end_date data_types instrument_ids
_create_vm() {
    local vm_name="$1"
    local root="$2"
    local tier="$3"
    local start_date="$4"
    local end_date="$5"
    local data_types="$6"
    local instrument_ids="$7"

    local metadata
    # Venue routing (2026-04-30 fix): VM_VENUE must be a canonical venue
    # name from _DATABENTO_VENUES = {CME, ICE, NYSE, NASDAQ, CBOE} so the
    # orchestrator's per-date active-venue filter accepts it. The previous
    # `VM_VENUE=DATABENTO` was silently filtered out → "No active venues"
    # warnings + zero rows captured. CME serves BTC/ETH/ES/MES futures +
    # ES options; NASDAQ serves the BlackRock spot ETFs (IBIT, ETHA via
    # XNAS.ITCH); CBOE serves VIX. NYSE-Arca + Cboe-BZX-listed ETFs were
    # part of an earlier scope and are deliberately not handled here.
    local vm_venue="CME"
    case "$root" in
        IBIT|ETHA)                               vm_venue="NASDAQ" ;;     # XNAS.ITCH
        VIX|VX)                                  vm_venue="CBOE" ;;
        *)                                       vm_venue="CME" ;;
    esac

    # VM_FORCE_WINDOW=true (default) triggers --force-window in
    # setup-data-pipeline-vm.sh which the rolling_window helper translates to
    # --force in the MTDS CLI. --force bypasses the pre-flight skip that reads
    # availability_index.parquet. Default kept true for backward compat with
    # the migration-era phantom captured-row period; pass --no-force-window
    # to disable once the tradfi phantom audit (master plan
    # unified-trading-pm/plans/active/sp500_ml_readiness_master_2026_05_05.plan
    # Phase 1) has reconciled the manifest. Then narrow-window VMs skip
    # already-captured shards instead of refetching the entire window.
    metadata="VM_TASK=cefi-backfill"
    metadata="${metadata},VM_SERVICE=market_tick_data_service"
    metadata="${metadata},VM_OPERATION=download"
    metadata="${metadata},VM_ASSET_GROUP=TRADFI"
    metadata="${metadata},VM_VENUE=${vm_venue}"
    metadata="${metadata},VM_FORCE_WINDOW=${FORCE_WINDOW}"
    metadata="${metadata},VM_START_DATE=${start_date}"
    metadata="${metadata},VM_END_DATE=${end_date}"
    metadata="${metadata},VM_DATA_TYPES=${data_types}"
    metadata="${metadata},VM_INSTRUMENT_IDS=${instrument_ids}"
    metadata="${metadata},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    metadata="${metadata},VM_SHUTDOWN_ON_COMPLETION=true"

    local run_ts
    run_ts="$(date +%Y%m%d-%H%M%S)"

    # GCP VM names must match [a-z](?:[-a-z0-9]{0,61}[a-z0-9])? — underscores
    # are not valid. Normalise root by replacing _ with - for the name.
    local vm_name_safe="${vm_name//_/-}"

    if $DRY_RUN; then
        echo "[DRY-RUN] $vm_name_safe"
        echo "          root=$root tier=$tier ${start_date}..${end_date}"
        echo "          data_types=${data_types}"
        echo "          instruments=${instrument_ids}"
        echo "          machine=${MACHINE_TYPE} zone=${ZONE}"
    else
        # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
        PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
        if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi
        echo "Launching $vm_name_safe (root=$root tier=$tier ${start_date}..${end_date}) [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
        # shellcheck disable=SC2086
        gcloud compute instances create "$vm_name_safe" \
            --project="$PROJECT" \
            --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
            --zone="$ZONE" \
            --machine-type="$MACHINE_TYPE" \
            ${PROVISIONING_FLAGS} \
            --image-family=ubuntu-2404-lts-amd64 \
            --image-project=ubuntu-os-cloud \
            --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
            --scopes=cloud-platform \
            --metadata="startup-script-url=${STARTUP},${metadata}" \
            --labels=purpose=tradfi-backfill,env="${DEPLOYMENT_ENV}",run-ts="${run_ts}",root="${root,,}",tier="${tier}",managed-by=deployment-service
        echo "  VM launched."
        # Brief stagger so RUN_TS timestamps stay unique + gcloud create calls
        # don't race quota prechecks.
        sleep 3
    fi
}

# ── Helpers for crypto-basis tier-plan windowing ──

# Filter a year's full monthly contract list to those whose LTD falls within
# [window_start, window_end] inclusive.
# Args: contracts_semicolon window_start_yyyymmdd window_end_yyyymmdd
_filter_contracts_by_ltd() {
    local contracts="$1"
    local win_start="$2"
    local win_end="$3"
    local out=""
    local IFS=';'
    for contract in $contracts; do
        # Extract YYYYMMDD from CME:FUTURE:ROOT-YYYYMMDD
        local ltd="${contract##*-}"
        if [[ "$ltd" -ge "$win_start" && "$ltd" -le "$win_end" ]]; then
            out="${out}${out:+;}${contract}"
        fi
    done
    printf '%s' "$out"
}

# Emit the union of contracts active during a date range. For a backfill window
# we want any contract whose [listing, LTD] interval overlaps the window. CME
# BTC/ETH list 7 months out; we approximate as: LTD in [window_start - 7mo,
# window_end + 0]. Practically: pull contracts with LTD between window_start
# (anything earlier already settled) and window_end + 7mo (so the back-month
# contracts trading during the window are included).
# Args: root year window_start window_end (all yyyymmdd)
_contracts_active_in_window() {
    local root="$1"
    local year="$2"
    local win_start="$3"
    local win_end="$4"

    # Years to consider: previous, current, next (since window can straddle).
    local prev_y=$((year - 1))
    local next_y=$((year + 1))
    local map_name="${root}_EXPIRIES_BY_YEAR"
    declare -n _ref="$map_name"

    local pool=""
    for y in "$prev_y" "$year" "$next_y"; do
        local entry="${_ref[$y]:-}"
        [[ -n "$entry" ]] && pool="${pool}${pool:+;}${entry}"
    done

    # Active = LTD in [win_start, win_start + 7mo]. Compute win_start_plus_7mo
    # crudely: same date 7 months later; if month overflow, roll year.
    local ws_year ws_mon ws_day
    ws_year="${win_start:0:4}"
    ws_mon="${win_start:4:2}"
    ws_day="${win_start:6:2}"
    local plus_mon=$((10#$ws_mon + 7))
    local plus_year="$ws_year"
    if (( plus_mon > 12 )); then
        plus_mon=$(( plus_mon - 12 ))
        plus_year=$(( plus_year + 1 ))
    fi
    local cap
    cap=$(printf '%04d%02d%02d' "$plus_year" "$plus_mon" "10#$ws_day")

    _filter_contracts_by_ltd "$pool" "$win_start" "$cap"
}

# Last day of a YYYY-MM string. Args: yyyy mm. Echoes YYYYMMDD.
_last_day_of_month() {
    local y="$1" m="$2"
    # date(1) gnu vs bsd; use a portable trick.
    local first_of_next
    if (( 10#$m == 12 )); then
        first_of_next=$(printf '%04d%02d01' $((y + 1)) 1)
    else
        first_of_next=$(printf '%04d%02d01' "$y" $((10#$m + 1)))
    fi
    # Subtract 1 day: parse YYYYMMDD, decrement.
    # Simpler: use /usr/bin/date if available, else portable computation.
    # We compute by counting backward from the 1st of next month.
    local d_y d_m d_d
    d_y="${first_of_next:0:4}"
    d_m="${first_of_next:4:2}"
    d_d="${first_of_next:6:2}"
    local prev_d=$((10#$d_d - 1))
    if (( prev_d == 0 )); then
        if (( 10#$d_m == 1 )); then
            d_y=$((d_y - 1)); d_m=12
        else
            d_m=$((10#$d_m - 1))
        fi
        # Last day of previous month — fall back to per-month table.
        case "$d_m" in
            1|3|5|7|8|10|12) prev_d=31 ;;
            4|6|9|11)        prev_d=30 ;;
            2)
                # leap-year check
                if (( (d_y % 4 == 0 && d_y % 100 != 0) || d_y % 400 == 0 )); then
                    prev_d=29
                else
                    prev_d=28
                fi
                ;;
        esac
    fi
    printf '%04d%02d%02d' "$d_y" "$d_m" "$prev_d"
}

# ── Mode dispatch ──

# Build an array LIGHT_WINDOWS=(start1:end1 start2:end2 ...) for a year given a
# list of skip-months (comma-separated YYYY-MM). Output ISO YYYY-MM-DD.
_build_light_windows() {
    local year="$1"
    local skip_csv="$2"
    LIGHT_WINDOWS=()

    local year_start year_end
    year_start="${year}-01-01"
    if [[ "$year" == "$(date +%Y)" ]]; then
        year_end="$(date +%Y-%m-%d)"
    else
        year_end="${year}-12-31"
    fi

    # Per-ticker listing-date clip — avoid attempting pre-listing dates
    # which return 0 records and write empty_confirmed manifest noise.
    local listing
    listing="$(listing_date_for_root "${ROOT_SYMBOL}")"
    if [[ -n "$listing" && "$year_start" < "$listing" ]]; then
        # If listing is after year-end, the whole year is pre-listing — skip.
        if [[ "$listing" > "$year_end" ]]; then
            LIGHT_WINDOWS=()
            return
        fi
        year_start="$listing"
    fi

    # Sort skip months ascending and clip to current year.
    local -a skips=()
    if [[ -n "$skip_csv" ]]; then
        local IFS=','
        for sm in $skip_csv; do
            if [[ "${sm:0:4}" == "$year" ]]; then
                skips+=("$sm")
            fi
        done
        # Sort.
        IFS=$'\n' read -r -d '' -a skips < <(printf '%s\n' "${skips[@]}" | sort && printf '\0')
    fi

    if (( ${#skips[@]} == 0 )); then
        LIGHT_WINDOWS=("${year_start}:${year_end}")
        return
    fi

    # Build contiguous sub-windows.
    local cursor="$year_start"
    for sm in "${skips[@]}"; do
        local sm_y sm_m skip_start skip_end_yyyymmdd
        sm_y="${sm:0:4}"
        sm_m="${sm:5:2}"
        skip_start="${sm}-01"
        skip_end_yyyymmdd=$(_last_day_of_month "$sm_y" "$sm_m")

        # Sub-window before this skip: cursor..(skip_start-1day)
        # We compute "skip_start minus 1 day" via the same _last_day helper trick
        # but more directly: if skip_start is the 1st, prev is last day of prior month.
        local prev_y prev_m prev_d
        prev_y="$sm_y"
        prev_m=$((10#$sm_m - 1))
        if (( prev_m == 0 )); then
            prev_y=$((prev_y - 1)); prev_m=12
        fi
        prev_d=$(_last_day_of_month "$prev_y" "$(printf '%02d' "$prev_m")")
        local before_end="${prev_d:0:4}-${prev_d:4:2}-${prev_d:6:2}"

        if [[ "$cursor" < "$skip_start" ]]; then
            LIGHT_WINDOWS+=("${cursor}:${before_end}")
        fi

        # Advance cursor to day-after skip end.
        local next_y next_m next_d
        next_y="${skip_end_yyyymmdd:0:4}"
        next_m="${skip_end_yyyymmdd:4:2}"
        next_d="${skip_end_yyyymmdd:6:2}"
        local next_d_int=$((10#$next_d + 1))
        local last_d_of_skip
        last_d_of_skip=$(_last_day_of_month "$next_y" "$next_m")
        if (( next_d_int > 10#${last_d_of_skip:6:2} )); then
            next_d_int=1
            local nm=$((10#$next_m + 1))
            if (( nm > 12 )); then nm=1; next_y=$((next_y + 1)); fi
            next_m=$(printf '%02d' "$nm")
        fi
        cursor=$(printf '%04d-%02d-%02d' "$next_y" "$next_m" "$next_d_int")
    done

    # Tail window (cursor..year_end) if cursor still <= year_end
    if [[ "$cursor" < "$year_end" || "$cursor" == "$year_end" ]]; then
        LIGHT_WINDOWS+=("${cursor}:${year_end}")
    fi
}

# Launch one heavy month VM. Args: root yyyy-mm
_launch_heavy_month() {
    local root="$1"
    local yyyy_mm="$2"
    local sm_y sm_m
    sm_y="${yyyy_mm:0:4}"
    sm_m="${yyyy_mm:5:2}"
    local last_yyyymmdd
    last_yyyymmdd=$(_last_day_of_month "$sm_y" "$sm_m")
    local start="${yyyy_mm}-01"
    local end="${last_yyyymmdd:0:4}-${last_yyyymmdd:4:2}-${last_yyyymmdd:6:2}"

    # Databento parent symbology: pass "BTC.FUT" / "ETH.FUT" / etc. The
    # adapter's stype_in selector keys on the .FUT/.OPT suffix
    # (databento_adapter.py:151). Date window narrows to the heavy month;
    # Databento returns whichever contracts in the chain traded during
    # that month. The previous per-contract enumeration (BTC_ACTIVE_2023_05
    # etc.) was incompatible with the adapter and silently produced 0 rows.
    local instruments
    instruments=$(cme_parent_symbols "$root")

    if [[ -z "$instruments" ]]; then
        echo "ERROR: no Databento parent symbol mapping for root=$root" >&2
        return 1
    fi

    # Heavy = trades + tbbo (top-of-book NBBO + each trade). Satisfies the
    # universal "trades + tbbo overlap on reference months" rule.
    # mbp_10 (10-deep book) is NOT in the MTDS DatabentoAdapter supported set
    # ({ohlcv_1m, trades, quotes, tbbo}); adding mbp_10 requires extending
    # the adapter (db.Schema.MBP_10 mapping + writer columns). Tracked as
    # a separate follow-up.
    local data_types="${DATA_TYPES_OVERRIDE:-trades;tbbo}"
    local run_ts
    run_ts="$(date +%Y%m%d-%H%M%S)"
    local vm_name="tradfi-bf-${root,,}-heavy-${yyyy_mm}-${run_ts}"
    _create_vm "$vm_name" "$root" "heavy" "$start" "$end" "$data_types" "$instruments"
}

# Launch light VMs for a year. Args: root year skip_months_csv
_launch_light_year() {
    local root="$1"
    local year="$2"
    local skip_csv="$3"

    # Databento parent symbology — see comment in _launch_heavy_month.
    local instruments
    instruments=$(cme_parent_symbols "$root")
    if [[ -z "$instruments" ]]; then
        echo "ERROR: no Databento parent symbol mapping for root=$root" >&2
        return 1
    fi

    local data_types="${DATA_TYPES_OVERRIDE:-ohlcv_1m}"
    if [[ "$root" == "ES" ]]; then
        # Preserve historical ES single-tier default.
        data_types="${DATA_TYPES_OVERRIDE:-ohlcv_1m;trades}"
    fi
    if [[ "$root" == "ES_OPT" ]]; then
        # Options chains: ohlcv_1m only (volume coverage). Trades + tbbo
        # across thousands of strikes per chain × 11 chains is prohibitively
        # expensive; option microstructure goes through dedicated runs if
        # needed (separate plan).
        data_types="${DATA_TYPES_OVERRIDE:-ohlcv_1m}"
    fi
    case "$root" in
        IBIT|ETHA)
            # Spot ETFs (NASDAQ-listed BlackRock funds): ohlcv_1m only for
            # the year-shard window. Trades and tbbo land in dedicated
            # heavy-month runs (universal reference months 2024-08 + 2025-03
            # — see _launch_etf_default).
            data_types="${DATA_TYPES_OVERRIDE:-ohlcv_1m}"
            ;;
    esac

    _build_light_windows "$year" "$skip_csv"

    local idx=0
    for win in "${LIGHT_WINDOWS[@]}"; do
        local win_start="${win%%:*}"
        local win_end="${win##*:}"
        local run_ts
        run_ts="$(date +%Y%m%d-%H%M%S)"
        local tag="${year}"
        if (( ${#LIGHT_WINDOWS[@]} > 1 )); then
            idx=$((idx + 1))
            tag="${year}-w${idx}"
        fi
        local vm_name="tradfi-bf-${root,,}-light-${tag}-${run_ts}"
        _create_vm "$vm_name" "$root" "light" "$win_start" "$win_end" "$data_types" "$instruments"
    done
}

# Launch a tier plan for a root.
_launch_tier_plan() {
    local root="$1"
    local plan="$2"

    if [[ "$plan" != "crypto-basis-2023-05+2024-06" ]]; then
        echo "ERROR: unknown --tier-plan '$plan'. Known: crypto-basis-2023-05+2024-06" >&2
        exit 1
    fi
    if [[ "$root" != "BTC" && "$root" != "ETH" ]]; then
        echo "ERROR: --tier-plan crypto-basis-* requires --root-symbol BTC|ETH (got '$root')" >&2
        exit 1
    fi

    # Heavy month VMs (one per heavy month).
    _launch_heavy_month "$root" "2023-05"
    _launch_heavy_month "$root" "2024-06"

    # Light year VMs for 2022..current year, skipping the heavy months in their year.
    local current_year
    current_year="$(date +%Y)"
    for y in 2022 2023 2024 2025 2026; do
        if (( y > current_year )); then break; fi
        local skip=""
        if (( y == 2023 )); then skip="2023-05"; fi
        if (( y == 2024 )); then skip="2024-06"; fi
        _launch_light_year "$root" "$y" "$skip"
    done
}

# ── Generic year-shard default (ES / ES_OPT / ETF tickers / etc.) ──
_legacy_es_default() {
    local root="${1:-ES}"
    local years_to_run=()
    if [[ -n "$YEAR" ]]; then
        years_to_run=("$YEAR")
    else
        # shellcheck disable=SC2207
        years_to_run=($(default_years_for_root "$root"))
    fi
    if (( ${#years_to_run[@]} == 0 )); then
        echo "ERROR: no default year set for root=$root" >&2
        exit 1
    fi
    for year in "${years_to_run[@]}"; do
        local skip=""
        _launch_light_year "$root" "$year" "$skip"
    done
}

# ── Top-level dispatch ──
_check_singleton_lock

# Explicit --start-date/--end-date override (single-VM ad-hoc): caller-provided
# instrument list, single window, single VM. Useful for retries.
if [[ -n "$START_DATE_OVERRIDE" || -n "$END_DATE_OVERRIDE" ]]; then
    if [[ -z "$START_DATE_OVERRIDE" || -z "$END_DATE_OVERRIDE" ]]; then
        echo "ERROR: --start-date and --end-date must be used together" >&2
        exit 1
    fi
    if [[ -z "$INSTRUMENT_IDS_OVERRIDE" ]]; then
        echo "ERROR: ad-hoc --start-date/--end-date mode requires --instrument-ids" >&2
        exit 1
    fi
    DT="${DATA_TYPES_OVERRIDE:-ohlcv_1m;trades}"
    TIER_TAG="${TIER:-adhoc}"
    RTS="$(date +%Y%m%d-%H%M%S)"
    VN="tradfi-bf-${ROOT_SYMBOL,,}-${TIER_TAG}-adhoc-${RTS}"
    _create_vm "$VN" "$ROOT_SYMBOL" "$TIER_TAG" "$START_DATE_OVERRIDE" "$END_DATE_OVERRIDE" "$DT" "$INSTRUMENT_IDS_OVERRIDE"
    exit 0
fi

# Tier plan (multi-VM fan-out).
if [[ -n "$TIER_PLAN" ]]; then
    _launch_tier_plan "$ROOT_SYMBOL" "$TIER_PLAN"
    echo ""
    if $DRY_RUN; then
        echo "=========================================="
        echo "DRY-RUN: tier-plan '$TIER_PLAN' for root=$ROOT_SYMBOL"
        echo "=========================================="
    else
        echo "Tier-plan '$TIER_PLAN' VMs launched in ${ZONE} for root=$ROOT_SYMBOL"
        echo ""
        echo "Manifest check:"
        echo "  gsutil cp gs://market-data-tick-tradfi-${PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
        echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); m=(df.venue=='CME')&(df.instrument_type=='future')&(df.symbol.str.startswith('${ROOT_SYMBOL}-')); print(df[m].groupby(['data_type','capture_status']).size())\""
    fi
    exit 0
fi

# Single-tier-explicit modes.
if [[ "$TIER" == "heavy" ]]; then
    if [[ -z "$MONTH" ]]; then
        echo "ERROR: --tier heavy requires --month YYYY-MM" >&2
        exit 1
    fi
    _launch_heavy_month "$ROOT_SYMBOL" "$MONTH"
    exit 0
fi

if [[ "$TIER" == "light" ]]; then
    if [[ -z "$YEAR" ]]; then
        echo "ERROR: --tier light requires --year YYYY" >&2
        exit 1
    fi
    _launch_light_year "$ROOT_SYMBOL" "$YEAR" "$SKIP_MONTHS"
    exit 0
fi

# No tier-plan, no explicit tier: year-shard default for ES / ES_OPT / ETFs.
case "$ROOT_SYMBOL" in
    ES|ES_OPT|MES|IBIT|ETHA)
        _legacy_es_default "$ROOT_SYMBOL"
    echo ""
    if $DRY_RUN; then
        echo "=========================================="
        echo "DRY-RUN: ${ROOT_SYMBOL} year-shards"
        echo "=========================================="
    else
        echo "${ROOT_SYMBOL} year-shards launched in ${ZONE}"
        echo ""
        echo "Manifest check:"
        echo "  gsutil cp gs://market-data-tick-tradfi-${PROJECT}/_index/availability_index.parquet /tmp/t.parquet"
        echo "  python -c \"import pandas as pd; df=pd.read_parquet('/tmp/t.parquet'); print(df[df.venue=='CME'].capture_status.value_counts())\""
    fi
    exit 0
    ;;
esac

echo "ERROR: --root-symbol ${ROOT_SYMBOL} requires --tier-plan or explicit --tier light|heavy" >&2
echo "       For BTC/ETH crypto-basis backfill: --tier-plan crypto-basis-2023-05+2024-06" >&2
exit 1
