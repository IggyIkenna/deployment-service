"""Unit tests for :mod:`deployment_service.data_pipeline_monitors.known_dead_cells_registry`.

Pure-function coverage of the boundary/safety logic ``check_high_attempted_failed``
(``meta_watchers.py``) relies on for DP-FETCH-009 known-dead suppression. Integration
coverage (the full manifest-read → suppression → no-page path) lives in
``test_data_pipeline_monitors.py``.
"""

from __future__ import annotations

from deployment_service.data_pipeline_monitors import known_dead_cells_registry as m


def test_registered_cell_with_no_activity_is_known_dead() -> None:
    assert m.is_known_dead("tradfi", "ohlcv_15m", max_attempted_at="2026-07-07T06:40:00Z") is True


def test_registered_cell_exactly_on_narrowing_date_is_known_dead() -> None:
    # same-day activity as the narrowing itself is not "new" — inclusive boundary.
    assert m.is_known_dead("tradfi", "ohlcv_15m", max_attempted_at="2026-07-15T23:59:59Z") is True


def test_registered_cell_with_activity_after_narrowing_is_not_known_dead() -> None:
    assert m.is_known_dead("tradfi", "ohlcv_15m", max_attempted_at="2026-07-20T00:00:00Z") is False


def test_registered_cell_with_no_rows_at_all_is_known_dead() -> None:
    # Empty/None max_attempted_at (no attempted_failed rows for the cell) — no known
    # recent activity, matches the manifest's own "" == unknown/legacy convention.
    assert m.is_known_dead("tradfi", "ohlcv_15m", max_attempted_at="") is True
    assert m.is_known_dead("tradfi", "ohlcv_15m", max_attempted_at=None) is True


def test_registered_cell_with_unparseable_timestamp_is_known_dead() -> None:
    # Fail toward "no known recent activity" rather than crashing on a data quirk.
    assert m.is_known_dead("tradfi", "ohlcv_15m", max_attempted_at="not-a-timestamp") is True


def test_unregistered_cell_is_never_known_dead() -> None:
    assert m.is_known_dead("tradfi", "trades", max_attempted_at="2026-07-07T06:40:00Z") is False
    assert m.is_known_dead("sports", "ohlcv_15m", max_attempted_at="2026-07-07T06:40:00Z") is False


def test_is_known_dead_for_series_reduces_to_max() -> None:
    import pandas as pd

    old_only = pd.Series(["2026-07-01T00:00:00Z", "2026-07-07T06:40:00Z"])
    assert m.is_known_dead_for_series("tradfi", "ohlcv_15m", old_only) is True

    with_new = pd.Series(["2026-07-01T00:00:00Z", "2026-07-20T00:00:00Z"])
    assert m.is_known_dead_for_series("tradfi", "ohlcv_15m", with_new) is False

    empty = pd.Series([], dtype=str)
    assert m.is_known_dead_for_series("tradfi", "ohlcv_15m", empty) is True
