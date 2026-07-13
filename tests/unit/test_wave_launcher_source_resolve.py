"""
Unit tests for wave_launcher.compute_dispatch_candidates — source-resolution logic.

Tests added 2026-06-24 for the SOURCE-RESOLVE fix: a logical cell is a gap only if
NO source row is captured (multi-source union rule). Covers:
(a) EU(massive) + captured(databento) → NOT a gap
(b) EU(databento) only → IS a gap
(c) attempted_failed + captured(another-source) → NOT a gap
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import types
from types import ModuleType
from unittest.mock import MagicMock

import pandas as pd
import pytest


def _load_wave_launcher() -> ModuleType:
    """Load scripts/wave_launcher.py with UTL mocked out.

    wave_launcher imports unified_trading_library at module level.  We stub it
    in sys.modules before exec so the import resolves to a lightweight mock.
    This is the same isolation approach used by test_deployments_registry.py.
    """
    # Build a minimal unified_trading_library stub.
    utl_stub = types.ModuleType("unified_trading_library")
    utl_stub.get_storage_client = MagicMock()  # type: ignore[attr-defined]
    utl_stub.log_event = MagicMock()  # type: ignore[attr-defined]
    utl_stub.setup_events = MagicMock()  # type: ignore[attr-defined]

    _orig = sys.modules.get("unified_trading_library")
    sys.modules["unified_trading_library"] = utl_stub
    try:
        path = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "wave_launcher.py"
        module_name = "_wave_launcher_under_test"
        spec = importlib.util.spec_from_file_location(module_name, str(path))
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = mod
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod
    finally:
        # Restore original (or remove stub) after loading.
        if _orig is None:
            sys.modules.pop("unified_trading_library", None)
        else:
            sys.modules["unified_trading_library"] = _orig


# Load once at module scope — subsequent imports of the same module_name are cached.
_wl = _load_wave_launcher()
compute_dispatch_candidates = _wl.compute_dispatch_candidates


def _make_df(rows: list[dict]) -> pd.DataFrame:
    """Build a manifest-like DataFrame from row dicts."""
    return pd.DataFrame(rows)


def _base_row(
    venue: str = "NASDAQ",
    date: str = "2025-01-15",
    data_type: str = "ohlcv_1m",
    instrument_id: str = "NASDAQ:EQUITY:AAPL",
    capture_status: str = "expected_unattempted",
    source: str = "databento",
) -> dict:
    return {
        "venue": venue,
        "date": date,
        "data_type": data_type,
        "instrument_id": instrument_id,
        "capture_status": capture_status,
        "source": source,
        "underlying": "",
    }


# ── Source-resolve: multi-source union rule ──────────────────────────────────


def test_eu_plus_captured_other_source_is_not_a_gap() -> None:
    """(a) EU(massive) + captured(databento) → NOT a gap.

    A logical cell that has source=massive expected_unattempted AND source=databento
    captured must NOT be dispatched — another source already captured it.
    """
    df = _make_df(
        [
            # EU row from massive (the orphan after a source-priority flip)
            _base_row(
                venue="NASDAQ",
                date="2025-01-15",
                data_type="ohlcv_1m",
                instrument_id="NASDAQ:EQUITY:AAPL",
                capture_status="expected_unattempted",
                source="massive",
            ),
            # Captured row from databento for the SAME logical cell
            _base_row(
                venue="NASDAQ",
                date="2025-01-15",
                data_type="ohlcv_1m",
                instrument_id="NASDAQ:EQUITY:AAPL",
                capture_status="captured",
                source="databento",
            ),
        ]
    )
    dispatches, oos = compute_dispatch_candidates(df)
    # The cell is covered — no dispatch must be issued for it.
    assert dispatches == [], f"Expected no dispatches but got: {dispatches}"


def test_eu_only_no_captured_source_is_a_gap() -> None:
    """(b) EU(databento) only, no captured source → IS a gap → dispatch issued.

    When no source has captured the logical cell, an EU row counts as a gap.
    """
    df = _make_df(
        [
            _base_row(
                venue="NASDAQ",
                date="2025-01-15",
                data_type="ohlcv_1m",
                instrument_id="NASDAQ:EQUITY:AAPL",
                capture_status="expected_unattempted",
                source="databento",
            ),
        ]
    )
    dispatches, oos = compute_dispatch_candidates(df)
    # Should produce exactly one dispatch for NASDAQ 2025.
    assert len(dispatches) == 1, f"Expected 1 dispatch but got: {dispatches}"
    dispatch = dispatches[0]
    assert dispatch.venue == "NASDAQ"
    assert dispatch.year == 2025


def test_attempted_failed_plus_captured_other_source_is_not_a_gap() -> None:
    """(c) attempted_failed(massive) + captured(databento) → NOT a gap.

    An attempted_failed row is a retry candidate — but if another source already
    captured the cell, the failure is irrelevant (union rule covers it).
    """
    df = _make_df(
        [
            _base_row(
                venue="NASDAQ",
                date="2025-02-10",
                data_type="ohlcv_1m",
                instrument_id="NASDAQ:EQUITY:MSFT",
                capture_status="attempted_failed",
                source="massive",
            ),
            _base_row(
                venue="NASDAQ",
                date="2025-02-10",
                data_type="ohlcv_1m",
                instrument_id="NASDAQ:EQUITY:MSFT",
                capture_status="captured",
                source="databento",
            ),
        ]
    )
    dispatches, oos = compute_dispatch_candidates(df)
    assert dispatches == [], f"Expected no dispatches but got: {dispatches}"


# ── Regression: single-source cells unaffected ───────────────────────────────


def test_single_source_eu_still_gaps() -> None:
    """Multiple EU rows from DIFFERENT logical cells both remain gaps."""
    df = _make_df(
        [
            _base_row(
                venue="NYSE",
                date="2025-03-01",
                data_type="ohlcv_1m",
                instrument_id="NYSE:EQUITY:BRK",
                capture_status="expected_unattempted",
                source="databento",
            ),
            _base_row(
                venue="NYSE",
                date="2025-03-02",
                data_type="ohlcv_1m",
                instrument_id="NYSE:EQUITY:BRK",
                capture_status="expected_unattempted",
                source="databento",
            ),
        ]
    )
    dispatches, oos = compute_dispatch_candidates(df)
    # Both dates are gaps — one dispatch per (venue, root, year); NYSE is PER_YEAR so
    # they collapse to a single (NYSE, None, 2025) dispatch atom.
    assert len(dispatches) == 1
    assert dispatches[0].venue == "NYSE"
    assert dispatches[0].year == 2025


def test_mixed_batch_only_uncovered_cells_dispatched() -> None:
    """Multiple cells: some covered, some not.  Only uncovered cells dispatch."""
    df = _make_df(
        [
            # Cell A: covered (EU massive + captured databento) — should NOT dispatch
            _base_row(
                venue="NASDAQ",
                date="2025-04-01",
                data_type="ohlcv_1m",
                instrument_id="NASDAQ:EQUITY:GOOG",
                capture_status="expected_unattempted",
                source="massive",
            ),
            _base_row(
                venue="NASDAQ",
                date="2025-04-01",
                data_type="ohlcv_1m",
                instrument_id="NASDAQ:EQUITY:GOOG",
                capture_status="captured",
                source="databento",
            ),
            # Cell B: not covered (EU only) — SHOULD dispatch
            _base_row(
                venue="NASDAQ",
                date="2025-04-02",
                data_type="ohlcv_1m",
                instrument_id="NASDAQ:EQUITY:NVDA",
                capture_status="expected_unattempted",
                source="databento",
            ),
        ]
    )
    dispatches, oos = compute_dispatch_candidates(df)
    # Only cell B is a gap; both collapse to NASDAQ 2025 since PER_YEAR, so 1 dispatch.
    assert len(dispatches) == 1
    assert dispatches[0].venue == "NASDAQ"
    assert dispatches[0].year == 2025
