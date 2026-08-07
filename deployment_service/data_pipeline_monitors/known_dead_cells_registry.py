"""Registry of KNOWN-DEAD ``(asset_group, data_type)`` cells for DP-FETCH-009.

A cell lands here when its ``attempted_failed`` population is DELIBERATELY-DEFERRED,
frozen residue from BEFORE its UAC ``expected_coverage``/``VENUE_DATA_TYPE_CAPABILITIES``
entry was narrowed to stop new attempts — not a live, ongoing failure. Without this
registry, ``check_high_attempted_failed`` (``meta_watchers.py``) counts the WHOLE
manifest history with no recency window, so a dead cell whose count stays above
``ATTEMPTED_FAILED_ABS_THRESHOLD`` re-pages ``DP_RUN_MOSTLY_EMPTY`` forever via the
30-min re-nag cooldown, even though nothing is newly broken.

Shared mechanism (not a CBOE-only special case) per
``issues/tradfi_ohlcv_attempted_failed_cluster_2026_07_23.md`` /
``issues/tradfi_unreachable_databento_data_types_mbp10_ohlcv_coarse_calendar_2026_07_15.md``
— ``mbp_10`` is also registered here (2026-07-15 operator decision);
``corporate_action_confirmed``/``earnings_result`` historical rows were cleaned up
outright (``market-tick-data-service@c24db4cf``, 2026-07-28) so they don't need a
registry entry.

SAFETY: an entry only suppresses while the cell's ``attempted_failed`` population has
ZERO activity newer than ``narrowed_at`` — see :func:`is_known_dead`. Any row with
``attempted_at > narrowed_at`` means new attempts are STILL happening (the narrowing
didn't take, or a different failure mode reappeared), which is a genuinely new signal
and MUST page — the registry never blindly silences a cell forever.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd


@dataclass(frozen=True)
class KnownDeadCell:
    """One registered known-dead ``(asset_group, data_type)`` cell.

    ``venue`` is informational provenance only — ``check_high_attempted_failed``
    aggregates at ``(asset_group, data_type)``, matching its existing alert
    granularity (no per-venue dimension exists in that check today).
    """

    asset_group: str
    data_type: str
    narrowed_at: date
    venue: str
    narrowed_by: str
    note: str


KNOWN_DEAD_CELLS: dict[tuple[str, str], KnownDeadCell] = {
    ("tradfi", "ohlcv_15m"): KnownDeadCell(
        asset_group="tradfi",
        data_type="ohlcv_15m",
        narrowed_at=date(2026, 7, 15),
        venue="CBOE",
        narrowed_by="unified-api-contracts@78b9e899",
        note=(
            "CBOE ohlcv_15m expected-coverage entry was stale drift from a removed "
            "Yahoo VIX-cash-index path; narrowed 2026-07-15 to stop new attempts. "
            "1,242 historical rows are frozen at a single 2026-07-07 batch timestamp — "
            "deliberately left un-purged. "
            "issues/tradfi_ohlcv_attempted_failed_cluster_2026_07_23.md"
        ),
    ),
    ("tradfi", "mbp_10"): KnownDeadCell(
        asset_group="tradfi",
        data_type="mbp_10",
        narrowed_at=date(2026, 7, 15),
        venue="CME",
        narrowed_by="operator-decision (2026-07-15 interactive reconciliation)",
        note=(
            "CME mbp_10 UAC VENUE_DATA_TYPE_CAPABILITIES restriction is a "
            "confirmed-still-intentional operator MVP-scope decision (2026-05-15 "
            "OHLCV-only MVP narrowing, reconfirmed 2026-07-15). Adapter-layer fix "
            "shipped (market-tick-data-service@e2018167) but live capture is "
            "explicitly NOT activated by operator choice. 1,186 historical rows are "
            "frozen at a single 2026-07-07 batch timestamp per "
            "issues/tradfi_unreachable_databento_data_types_mbp10_ohlcv_coarse_calendar_2026_07_15.md."
        ),
    ),
}


def is_known_dead(asset_group: str, data_type: str, *, max_attempted_at: str | None) -> bool:
    """True when ``(asset_group, data_type)`` is registered AND has zero attempted_failed
    activity newer than its registered ``narrowed_at`` date.

    ``max_attempted_at`` is the MAX ``attempted_at`` (ISO-8601 string) among the cell's
    CURRENT ``attempted_failed`` rows. Empty/None/unparseable → treated as "no known
    recent activity" (matches the manifest's own convention: ``attempted_at=""`` means
    unknown/legacy, not recent) rather than refusing to suppress on a data quirk.
    """
    entry = KNOWN_DEAD_CELLS.get((asset_group, data_type))
    if entry is None:
        return False
    if not max_attempted_at:
        return True
    ts = pd.to_datetime(max_attempted_at, utc=True, errors="coerce")
    if pd.isna(ts):
        return True
    return ts.date() <= entry.narrowed_at


def is_known_dead_for_series(asset_group: str, data_type: str, failed_attempted_at: pd.Series) -> bool:
    """Convenience wrapper: reduce a cell's ``attempted_failed`` rows' ``attempted_at``
    values to their max, then delegate to :func:`is_known_dead`."""
    max_attempted_at = max(failed_attempted_at, default="")
    return is_known_dead(asset_group, data_type, max_attempted_at=max_attempted_at)
