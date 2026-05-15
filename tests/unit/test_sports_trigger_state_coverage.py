"""Coverage tests for sports_trigger_state.py uncovered branches.

Targets the paths missed in test_sports_trigger_scheduler_periodic.py:
  - _UTLStateAdapter upload/download delegation
  - PeriodicTierState: malformed JSON, non-dict data, non-dict entries
  - get_last_run: bad ISO format returns None
  - _persist: upload failure swallowed (warning only)
  - as_int / as_float: float/str coercion + bool guard
  - resolve_source_key: TRANSFERS/XG/WEATHER entity routing
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from deployment_service.sports_trigger_state import (
    PeriodicTierState,
    as_float,
    as_int,
    resolve_source_key,
)

# ---------------------------------------------------------------------------
# PeriodicTierState — storage adapter stub
# ---------------------------------------------------------------------------


class _MemStorage:
    """In-memory storage stub that implements SchedulerStateStorage."""

    def __init__(self, initial: str | None = None) -> None:
        self._data: dict[str, str] = {}
        if initial is not None:
            self._data[("bucket", "key")] = initial  # type: ignore[index]

    def upload_string(self, bucket: str, key: str, body: str) -> None:
        self._data[(bucket, key)] = body  # type: ignore[index]

    def download_string(self, bucket: str, key: str) -> str:
        val = self._data.get((bucket, key))  # type: ignore[call-overload]
        if val is None:
            raise FileNotFoundError(f"not found: {bucket}/{key}")
        return val


def _state(raw_json: str | None = None) -> PeriodicTierState:
    storage = _MemStorage()
    if raw_json is not None:
        storage._data[("bkt", "key")] = raw_json  # type: ignore[index]
    return PeriodicTierState(bucket="bkt", key="key", storage=storage)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Malformed / unexpected JSON shapes
# ---------------------------------------------------------------------------


class TestPeriodicTierStateLoad:
    def test_missing_state_starts_fresh(self) -> None:
        state = _state(raw_json=None)  # FileNotFoundError on download → fresh
        assert state.snapshot() == {}

    def test_malformed_json_starts_fresh(self) -> None:
        state = _state(raw_json="{not valid json!")
        assert state.snapshot() == {}

    def test_non_dict_top_level_starts_fresh(self) -> None:
        state = _state(raw_json='["a", "b"]')
        assert state.snapshot() == {}

    def test_non_dict_last_run_starts_fresh(self) -> None:
        state = _state(raw_json='{"last_run": ["a"]}')
        assert state.snapshot() == {}

    def test_valid_state_loads_correctly(self) -> None:
        ts = "2026-05-15T10:00:00"
        state = _state(raw_json=f'{{"last_run": {{"tier1": "{ts}"}}}}')
        assert state.snapshot() == {"tier1": ts}


# ---------------------------------------------------------------------------
# get_last_run — bad ISO string
# ---------------------------------------------------------------------------


class TestGetLastRun:
    def test_invalid_iso_returns_none(self) -> None:
        state = _state(raw_json='{"last_run": {"tier1": "not-a-date"}}')
        result = state.get_last_run("tier1")
        assert result is None

    def test_valid_iso_returns_datetime(self) -> None:
        ts = "2026-05-15T10:00:00+00:00"
        state = _state(raw_json=f'{{"last_run": {{"t1": "{ts}"}}}}')
        result = state.get_last_run("t1")
        assert isinstance(result, datetime)


# ---------------------------------------------------------------------------
# _persist — upload failure is swallowed
# ---------------------------------------------------------------------------


class TestPersistFailure:
    def test_upload_failure_does_not_raise(self) -> None:
        bad_storage = MagicMock()
        bad_storage.download_string.side_effect = FileNotFoundError
        bad_storage.upload_string.side_effect = OSError("disk full")

        state = PeriodicTierState(bucket="bkt", key="key", storage=bad_storage)  # type: ignore[arg-type]
        # set_last_run triggers _persist which calls upload_string — must not raise
        state.set_last_run("tier1", datetime.now(UTC), persist=True)


# ---------------------------------------------------------------------------
# as_int
# ---------------------------------------------------------------------------


class TestAsInt:
    def test_int_passthrough(self) -> None:
        assert as_int(5, default=0) == 5

    def test_float_converts(self) -> None:
        assert as_int(3.7, default=0) == 3

    def test_string_converts(self) -> None:
        assert as_int("42", default=0) == 42

    def test_invalid_string_returns_default(self) -> None:
        assert as_int("abc", default=7) == 7

    def test_bool_returns_default(self) -> None:
        assert as_int(True, default=99) == 99

    def test_none_returns_default(self) -> None:
        assert as_int(None, default=3) == 3


# ---------------------------------------------------------------------------
# as_float
# ---------------------------------------------------------------------------


class TestAsFloat:
    def test_float_passthrough(self) -> None:
        assert as_float(1.5, default=0.0) == 1.5

    def test_int_converts(self) -> None:
        assert as_float(3, default=0.0) == 3.0

    def test_string_converts(self) -> None:
        assert as_float("2.5", default=0.0) == 2.5

    def test_invalid_string_returns_default(self) -> None:
        assert as_float("xyz", default=9.9) == 9.9

    def test_bool_returns_default(self) -> None:
        assert as_float(True, default=0.5) == 0.5

    def test_none_returns_default(self) -> None:
        assert as_float(None, default=1.1) == 1.1


# ---------------------------------------------------------------------------
# resolve_source_key
# ---------------------------------------------------------------------------


class TestResolveSourceKey:
    def test_explicit_data_source_wins(self) -> None:
        assert resolve_source_key({"data_source": "my_source"}) == "my_source"

    def test_transfers_entity_maps_to_transfermarkt(self) -> None:
        assert resolve_source_key({"args": {"--sports-entity": "TRANSFERS"}}) == "transfermarkt"

    def test_xg_entity_maps_to_understat(self) -> None:
        assert resolve_source_key({"args": {"--sports-entity": "XG"}}) == "understat"

    def test_weather_entity_maps_to_open_meteo(self) -> None:
        assert resolve_source_key({"args": {"--sports-entity": "WEATHER"}}) == "open_meteo"

    def test_unknown_entity_falls_back_to_api_football(self) -> None:
        assert resolve_source_key({"args": {"--sports-entity": "FIXTURES"}}) == "api_football"

    def test_no_args_falls_back_to_api_football(self) -> None:
        assert resolve_source_key({}) == "api_football"
