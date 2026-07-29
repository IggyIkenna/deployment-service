#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Shared date-range-sharding helper for the CeFi funding-timestamp-fix VM
# launcher family (`launch-cefi-funding-timestamp-fix-vm.sh` and siblings that
# scale the same one-VM-per-<VENUE,START,END> pattern).
#
# Why this exists: BINANCE-FUTURES's full remaining range took ~24 real
# wall-clock hours single-threaded at the measured ~1.2 days/minute processing
# rate (2026-07-28 session). The operator worked around it by hand -- computing
# per-venue midpoint dates and launching N separate invocations of the same
# launcher with disjoint <START> <END> windows. This lib promotes that proven
# math into a reusable, testable function so a launcher can do it for an
# arbitrary N instead of a hand-computed midpoint.
#
# Safety precondition this relies on (established, not re-derived here): the
# underlying reprocessing script's own idempotency guard
# (`skipped_next_funding_timestamp_already_present` in
# market-tick-data-service/scripts/one_offs/
# reprocess_bulk_tardis_derivative_ticker_funding_timestamp_2026_07_28.py`)
# makes accidental date-range overlap between shards a safe no-op, not a
# correctness risk -- so exact-boundary contiguity below is a quality goal
# (no gap, no doubly-processed range), not a hard safety requirement.
#
# SSOT: plans/active/issues/cefi_migration_vm_launcher_no_sharding_and_spot_preemption_churn_2026_07_28.md
set -euo pipefail

# _cefi_fts_epoch_day <YYYY-MM-DD>
# ---------------------------------------------------------------------------
# UTC midnight epoch seconds for a calendar date. GNU date first (`-d`), BSD
# date fallback (`-j -f`) -- same portability pattern as
# `_tradfi-ohlcv-launcher-lib.sh`'s `_ohlcv_epoch_day` (this is a deliberate,
# minimal fork of that proven pattern for THIS launcher family, not a shared
# import -- see the issue doc's scope note on staying file-local).
_cefi_fts_epoch_day() {
    date -u -d "$1" +%s 2>/dev/null || date -u -j -f '%Y-%m-%d' "$1" +%s
}

# _cefi_fts_iso_from_epoch <epoch_seconds>
# ---------------------------------------------------------------------------
_cefi_fts_iso_from_epoch() {
    date -u -d "@$1" +%Y-%m-%d 2>/dev/null || date -u -r "$1" +%Y-%m-%d
}

# cefi_fts_split_date_shards <start_iso> <end_iso> <n>
# ---------------------------------------------------------------------------
# Split one inclusive [start_iso, end_iso] calendar-day window into N
# contiguous, exhaustive, non-overlapping sub-windows and echo one
# "start_iso:end_iso" per line, shard 1 first.
#
# Math (mirrors the exact midpoint-split arithmetic proven this session,
# generalised from 2-way to N-way): base = total_days / N (integer division),
# shards 1..N-1 each get exactly `base` days, and shard N (the LAST shard)
# absorbs whatever remains (`total_days - base*(N-1)`) -- so a
# not-evenly-divisible range never loses or double-counts a boundary day, and
# never produces a "fat tail" beyond the one extra remainder day.
#
# Returns 1 (and prints an error to stderr) on an inverted range or a
# non-positive-integer N. Clamps N down to total_days if N exceeds the number
# of days in the range (a 1-day shard is the smallest meaningful unit).
cefi_fts_split_date_shards() {
    local start_iso="${1:?cefi_fts_split_date_shards: start_iso required}"
    local end_iso="${2:?cefi_fts_split_date_shards: end_iso required}"
    local n="${3:?cefi_fts_split_date_shards: n required}"

    if [[ ! "$n" =~ ^[0-9]+$ ]] || (( n < 1 )); then
        echo "ERROR: cefi_fts_split_date_shards: shard count must be a positive integer (got: ${n})" >&2
        return 1
    fi

    local start_epoch end_epoch total_days
    start_epoch="$(_cefi_fts_epoch_day "$start_iso")"
    end_epoch="$(_cefi_fts_epoch_day "$end_iso")"
    total_days=$(( (end_epoch - start_epoch) / 86400 + 1 ))
    if (( total_days <= 0 )); then
        echo "ERROR: cefi_fts_split_date_shards: end_date must be >= start_date (got ${start_iso}..${end_iso})" >&2
        return 1
    fi
    (( n > total_days )) && n="$total_days"

    local base=$(( total_days / n ))
    local cursor_epoch="$start_epoch"
    local i shard_days shard_start shard_end shard_end_epoch
    for (( i = 1; i <= n; i++ )); do
        if (( i == n )); then
            # Last shard absorbs the remainder -- see docstring math above.
            shard_days=$(( total_days - base * (n - 1) ))
        else
            shard_days="$base"
        fi
        shard_start="$(_cefi_fts_iso_from_epoch "$cursor_epoch")"
        shard_end_epoch=$(( cursor_epoch + (shard_days - 1) * 86400 ))
        shard_end="$(_cefi_fts_iso_from_epoch "$shard_end_epoch")"
        printf '%s:%s\n' "$shard_start" "$shard_end"
        cursor_epoch=$(( shard_end_epoch + 86400 ))
    done
}
