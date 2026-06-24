"""Unit tests for the sports source-publish-lag observation recorder.

Covers:
  - compute_lag_seconds: parse + diff, unparseable → None, negative lag passthrough.
  - LatencyObservationRecorder.record: writes a per-(day, run) parquet, append
    semantics (second record() concatenates), day-partitioning by match_end.
  - SportsTriggerScheduler._record_latency_observations: emits one obs per
    OBSERVABLE post-match entity, skips pre-match / non-observable entities,
    NEVER touches available_at (writes only the observation parquet), degrades
    to no-op when the recorder is None.
  - _register_first_success_pending: registers pending entries after fire.
  - _poll_first_success: writes first_success=True obs on rc==0, advances
    attempt on rc!=0, prunes expired entries.

No real GCS — an in-memory bytes-storage stub matching the UTL StorageClient
bytes interface. Follows shard-level failure isolation (no network/subprocess).
"""

from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pandas as pd
import pytest

from deployment_service.sports_latency_observation import (
    ENTITY_TO_OBSERVATION_TARGET,
    FIRST_SUCCESS_EARLY_INTERVAL_MIN,
    FIRST_SUCCESS_MAX_WINDOW_H,
    FirstSuccessPendingEntry,
    LatencyObservation,
    LatencyObservationRecorder,
    compute_lag_seconds,
    next_poll_utc_for_attempt,
)


class _InMemoryBytesStorage:
    """Matches the StorageClient bytes interface the recorder uses."""

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}

    def upload_bytes(self, *, bucket: str, blob_path: str, data: bytes, content_type: str = "") -> None:
        self.store[f"{bucket}/{blob_path}"] = data

    def download_bytes(self, *, bucket: str, blob_path: str) -> bytes:
        key = f"{bucket}/{blob_path}"
        if key not in self.store:
            raise FileNotFoundError(key)
        return self.store[key]


def _obs(
    match_end: str, first_fetch: str, *, data_type: str = "FIXTURE_STATS", source: str = "api_football"
) -> LatencyObservation:
    lag = compute_lag_seconds(match_end, first_fetch)
    assert lag is not None
    return LatencyObservation(
        fixture_id="fx1",
        league_id="EPL",
        data_type=data_type,
        source=source,
        match_end_utc=match_end,
        first_fetch_utc=first_fetch,
        observed_publish_lag_s=lag,
        fetched_rows=-1,
        first_success=False,
        trigger_name="stats_immediate",
        assumed_lag_constant_s=1800,
    )


# --------------------------------------------------------------------------- #
# compute_lag_seconds
# --------------------------------------------------------------------------- #


def test_compute_lag_seconds_basic() -> None:
    me = "2026-06-22T16:45:00+00:00"
    ff = "2026-06-22T17:15:00+00:00"  # +30 min
    assert compute_lag_seconds(me, ff) == 1800.0


def test_compute_lag_seconds_z_suffix() -> None:
    assert compute_lag_seconds("2026-06-22T16:45:00Z", "2026-06-22T16:50:00Z") == 300.0


def test_compute_lag_seconds_unparseable_returns_none() -> None:
    assert compute_lag_seconds("not-a-date", "2026-06-22T17:00:00Z") is None
    assert compute_lag_seconds("2026-06-22T17:00:00Z", "") is None


def test_compute_lag_seconds_negative_passthrough() -> None:
    # first_fetch BEFORE match_end (match ran short of the kickoff+105 estimate)
    lag = compute_lag_seconds("2026-06-22T17:00:00Z", "2026-06-22T16:50:00Z")
    assert lag == -600.0


# --------------------------------------------------------------------------- #
# LatencyObservationRecorder.record
# --------------------------------------------------------------------------- #


def test_recorder_writes_parquet_partitioned_by_day() -> None:
    storage = _InMemoryBytesStorage()
    rec = LatencyObservationRecorder(bucket="b", run_tag="run1", storage=storage, enabled=True)
    n = rec.record([_obs("2026-06-22T16:45:00Z", "2026-06-22T17:15:00Z")])
    assert n == 1
    key = "b/_index/latency_observations/day=2026-06-22/run1.parquet"
    assert key in storage.store
    df = pd.read_parquet(io.BytesIO(storage.store[key]))
    assert len(df) == 1
    assert df.iloc[0]["observed_publish_lag_s"] == 1800.0
    assert df.iloc[0]["source"] == "api_football"
    assert df.iloc[0]["assumed_lag_constant_s"] == 1800


def test_recorder_append_concatenates() -> None:
    storage = _InMemoryBytesStorage()
    rec = LatencyObservationRecorder(bucket="b", run_tag="run1", storage=storage, enabled=True)
    rec.record([_obs("2026-06-22T16:45:00Z", "2026-06-22T17:15:00Z")])
    rec.record([_obs("2026-06-22T18:45:00Z", "2026-06-22T19:30:00Z", source="api_football")])
    key = "b/_index/latency_observations/day=2026-06-22/run1.parquet"
    df = pd.read_parquet(io.BytesIO(storage.store[key]))
    assert len(df) == 2  # appended, not overwritten


def test_recorder_disabled_is_noop() -> None:
    storage = _InMemoryBytesStorage()
    rec = LatencyObservationRecorder(bucket="b", run_tag="run1", storage=storage, enabled=False)
    assert rec.record([_obs("2026-06-22T16:45:00Z", "2026-06-22T17:15:00Z")]) == 0
    assert storage.store == {}


def test_recorder_empty_observations_noop() -> None:
    storage = _InMemoryBytesStorage()
    rec = LatencyObservationRecorder(bucket="b", run_tag="run1", storage=storage, enabled=True)
    assert rec.record([]) == 0


# --------------------------------------------------------------------------- #
# Scheduler emit
# --------------------------------------------------------------------------- #


def _make_scheduler_with_recorder(storage: _InMemoryBytesStorage):
    from deployment_service.sports_trigger_scheduler import SportsTriggerScheduler

    rec = LatencyObservationRecorder(bucket="b", run_tag="run1", storage=storage, enabled=True)
    sched = SportsTriggerScheduler(
        config_path="configs/sports-trigger-tiers.yaml",
        dry_run=False,
        periodic_state=None,
        latency_recorder=rec,
    )
    return sched


def test_scheduler_emits_obs_for_observable_postmatch_entity() -> None:
    storage = _InMemoryBytesStorage()
    sched = _make_scheduler_with_recorder(storage)
    kickoff = datetime.now(UTC) - timedelta(minutes=200)  # match ended ~95 min ago
    event = {
        "trigger_name": "stats_immediate",
        "fixture_id": "fx42",
        "league_id": "MLS",
        "kickoff_utc": kickoff.isoformat(),
        "fire_at_utc": datetime.now(UTC).isoformat(),
        "services": [{"args": {"--sports-entity": "FIXTURE_STATS"}}],
    }
    sched._record_latency_observations(event)  # pyright: ignore[reportPrivateUsage]
    # one obs landed on the match-end day
    keys = [k for k in storage.store if "latency_observations" in k]
    assert len(keys) == 1
    df = pd.read_parquet(io.BytesIO(storage.store[keys[0]]))
    assert df.iloc[0]["data_type"] == "FIXTURE_STATS"
    assert df.iloc[0]["source"] == "api_football"
    assert df.iloc[0]["fixture_id"] == "fx42"
    assert df.iloc[0]["observed_publish_lag_s"] > 0


def test_scheduler_skips_nonobservable_entity() -> None:
    storage = _InMemoryBytesStorage()
    sched = _make_scheduler_with_recorder(storage)
    event = {
        "trigger_name": "odds_t1h",
        "fixture_id": "fx99",
        "league_id": "MLS",
        "kickoff_utc": datetime.now(UTC).isoformat(),
        "fire_at_utc": datetime.now(UTC).isoformat(),
        "services": [{"args": {"--sports-entity": "LINEUPS"}}],  # pre-match, not observable
    }
    sched._record_latency_observations(event)  # pyright: ignore[reportPrivateUsage]
    assert [k for k in storage.store if "latency_observations" in k] == []


def test_scheduler_no_recorder_is_noop() -> None:
    from deployment_service.sports_trigger_scheduler import SportsTriggerScheduler

    sched = SportsTriggerScheduler(
        config_path="configs/sports-trigger-tiers.yaml",
        dry_run=False,
        periodic_state=None,
        record_latency=False,
    )
    assert sched._latency_recorder is None  # pyright: ignore[reportPrivateUsage]
    # should not raise
    sched._record_latency_observations(  # pyright: ignore[reportPrivateUsage]
        {
            "trigger_name": "stats_immediate",
            "fixture_id": "x",
            "league_id": "EPL",
            "kickoff_utc": datetime.now(UTC).isoformat(),
            "fire_at_utc": datetime.now(UTC).isoformat(),
            "services": [{"args": {"--sports-entity": "FIXTURE_STATS"}}],
        }
    )


def test_observation_target_map_covers_postmatch_entities() -> None:
    # The observable set is exactly the post-match data_types whose lag we calibrate.
    assert set(ENTITY_TO_OBSERVATION_TARGET) == {
        "FIXTURE_STATS",
        "FIXTURE_EVENTS",
        "PLAYER_STATS",
        "XG",
        "SFI_PROGRESSIVE_STATS",
    }
    # XG → understat @ 7200s, SFI → sfi @ 300s (matches source_data_latency.py)
    assert ENTITY_TO_OBSERVATION_TARGET["XG"] == ("XG", "understat", 7200)
    assert ENTITY_TO_OBSERVATION_TARGET["SFI_PROGRESSIVE_STATS"] == ("SFI_PROGRESSIVE_STATS", "sfi", 300)


# --------------------------------------------------------------------------- #
# next_poll_utc_for_attempt
# --------------------------------------------------------------------------- #


def test_next_poll_utc_early_cadence() -> None:
    base = datetime(2026, 6, 22, 16, 0, 0, tzinfo=UTC)
    result = next_poll_utc_for_attempt(0, base)
    assert result == base + timedelta(minutes=FIRST_SUCCESS_EARLY_INTERVAL_MIN)


def test_next_poll_utc_late_cadence() -> None:
    from deployment_service.sports_latency_observation import FIRST_SUCCESS_LATE_INTERVAL_H

    base = datetime(2026, 6, 22, 16, 0, 0, tzinfo=UTC)
    result = next_poll_utc_for_attempt(3, base)
    assert result == base + timedelta(hours=FIRST_SUCCESS_LATE_INTERVAL_H)
    result2 = next_poll_utc_for_attempt(10, base)
    assert result2 == base + timedelta(hours=FIRST_SUCCESS_LATE_INTERVAL_H)


# --------------------------------------------------------------------------- #
# _register_first_success_pending
# --------------------------------------------------------------------------- #


def test_scheduler_registers_pending_for_observable_entity() -> None:
    storage = _InMemoryBytesStorage()
    sched = _make_scheduler_with_recorder(storage)
    kickoff = datetime.now(UTC) - timedelta(minutes=200)
    event = {
        "trigger_name": "stats_immediate",
        "fixture_id": "fx77",
        "league_id": "EPL",
        "kickoff_utc": kickoff.isoformat(),
        "fire_at_utc": datetime.now(UTC).isoformat(),
        "services": [{"args": {"--sports-entity": "FIXTURE_STATS"}}],
    }
    # Patch _dispatch_services to return 1 so fire_trigger calls register_from_event.
    with patch.object(sched, "_dispatch_services", return_value=1):
        sched.fire_trigger(event)  # pyright: ignore[reportArgumentType]
    key = "fx77:FIXTURE_STATS:api_football"
    assert key in sched._first_success_poller.pending  # pyright: ignore[reportPrivateUsage]
    entry = sched._first_success_poller.pending[key]  # pyright: ignore[reportPrivateUsage]
    assert entry.data_type == "FIXTURE_STATS"
    assert entry.source == "api_football"
    assert entry.assumed_lag_constant_s == 1800
    assert entry.attempt == 0
    assert entry.fixture_id == "fx77"


def test_scheduler_skips_pending_for_nonobservable_entity() -> None:
    storage = _InMemoryBytesStorage()
    sched = _make_scheduler_with_recorder(storage)
    event = {
        "trigger_name": "odds_t1h",
        "fixture_id": "fx88",
        "league_id": "EPL",
        "kickoff_utc": datetime.now(UTC).isoformat(),
        "fire_at_utc": datetime.now(UTC).isoformat(),
        "services": [{"args": {"--sports-entity": "LINEUPS"}}],
    }
    with patch.object(sched, "_dispatch_services", return_value=1):
        sched.fire_trigger(event)  # pyright: ignore[reportArgumentType]
    assert sched._first_success_poller.pending == {}  # pyright: ignore[reportPrivateUsage]


# --------------------------------------------------------------------------- #
# _poll_first_success
# --------------------------------------------------------------------------- #


def _make_pending_entry(
    *,
    fixture_id: str = "fx1",
    data_type: str = "FIXTURE_STATS",
    source: str = "api_football",
    attempt: int = 0,
    next_poll_offset_s: float = -1.0,
    expires_offset_h: float = 47.0,
) -> FirstSuccessPendingEntry:
    now = datetime.now(UTC)
    match_end = (now - timedelta(hours=2)).isoformat()
    return FirstSuccessPendingEntry(
        fixture_id=fixture_id,
        league_id="EPL",
        data_type=data_type,
        source=source,
        match_end_utc=match_end,
        trigger_name="stats_immediate",
        assumed_lag_constant_s=1800,
        service="features-service",
        cmd="python -m features_service compute --asset-group SPORTS",
        attempt=attempt,
        next_poll_utc=(now + timedelta(seconds=next_poll_offset_s)).isoformat(),
        expires_utc=(now + timedelta(hours=expires_offset_h)).isoformat(),
    )


def test_poll_first_success_writes_obs_on_rc0() -> None:
    storage = _InMemoryBytesStorage()
    sched = _make_scheduler_with_recorder(storage)
    key = "fx1:FIXTURE_STATS:api_football"
    sched._first_success_poller.pending[key] = _make_pending_entry()  # pyright: ignore[reportPrivateUsage]

    with patch.object(sched, "_dispatch_local", return_value=True):
        sched._first_success_poller.poll()  # pyright: ignore[reportPrivateUsage]

    assert key not in sched._first_success_poller.pending  # pyright: ignore[reportPrivateUsage]
    keys = [k for k in storage.store if "latency_observations" in k]
    assert len(keys) == 1
    df = pd.read_parquet(io.BytesIO(storage.store[keys[0]]))
    assert len(df) == 1
    assert df.iloc[0]["first_success"] is True or df.iloc[0]["first_success"] == True  # noqa: E712
    assert df.iloc[0]["fetched_rows"] == 1
    assert df.iloc[0]["data_type"] == "FIXTURE_STATS"
    assert df.iloc[0]["source"] == "api_football"
    assert df.iloc[0]["observed_publish_lag_s"] > 0


def test_poll_first_success_advances_attempt_on_failure() -> None:
    storage = _InMemoryBytesStorage()
    sched = _make_scheduler_with_recorder(storage)
    key = "fx1:FIXTURE_STATS:api_football"
    sched._first_success_poller.pending[key] = _make_pending_entry(attempt=0)  # pyright: ignore[reportPrivateUsage]

    with patch.object(sched, "_dispatch_local", return_value=False):
        sched._first_success_poller.poll()  # pyright: ignore[reportPrivateUsage]

    assert key in sched._first_success_poller.pending  # pyright: ignore[reportPrivateUsage]
    updated = sched._first_success_poller.pending[key]  # pyright: ignore[reportPrivateUsage]
    assert updated.attempt == 1
    # attempt 1 < 3 → early cadence (15 min)
    expected_next = datetime.now(UTC) + timedelta(minutes=FIRST_SUCCESS_EARLY_INTERVAL_MIN)
    actual_next = datetime.fromisoformat(updated.next_poll_utc)
    if actual_next.tzinfo is None:
        actual_next = actual_next.replace(tzinfo=UTC)
    assert abs((actual_next - expected_next).total_seconds()) < 5
    # No observation written
    assert storage.store == {}


def test_poll_first_success_skips_not_yet_due() -> None:
    storage = _InMemoryBytesStorage()
    sched = _make_scheduler_with_recorder(storage)
    key = "fx1:FIXTURE_STATS:api_football"
    # next_poll_offset_s=+60 → due in 60 s, not yet
    sched._first_success_poller.pending[key] = _make_pending_entry(next_poll_offset_s=60.0)  # pyright: ignore[reportPrivateUsage]

    with patch.object(sched, "_dispatch_local", return_value=True) as mock_dispatch:
        sched._first_success_poller.poll()  # pyright: ignore[reportPrivateUsage]
        mock_dispatch.assert_not_called()

    assert key in sched._first_success_poller.pending  # pyright: ignore[reportPrivateUsage]
    assert storage.store == {}


def test_poll_first_success_prunes_expired() -> None:
    storage = _InMemoryBytesStorage()
    sched = _make_scheduler_with_recorder(storage)
    key = "fx1:FIXTURE_STATS:api_football"
    # expires_offset_h=-1 → already expired
    sched._first_success_poller.pending[key] = _make_pending_entry(expires_offset_h=-1.0)  # pyright: ignore[reportPrivateUsage]

    with patch.object(sched, "_dispatch_local", return_value=True) as mock_dispatch:
        sched._first_success_poller.poll()  # pyright: ignore[reportPrivateUsage]
        mock_dispatch.assert_not_called()

    assert key not in sched._first_success_poller.pending  # pyright: ignore[reportPrivateUsage]
    assert storage.store == {}


def test_poll_first_success_noop_without_recorder() -> None:
    from deployment_service.sports_trigger_scheduler import SportsTriggerScheduler

    sched = SportsTriggerScheduler(
        config_path="configs/sports-trigger-tiers.yaml",
        dry_run=False,
        periodic_state=None,
        record_latency=False,
    )
    key = "fx1:FIXTURE_STATS:api_football"
    sched._first_success_poller.pending[key] = _make_pending_entry()  # pyright: ignore[reportPrivateUsage]
    # Should not raise and should not dispatch
    with patch.object(sched, "_dispatch_local") as mock_dispatch:
        sched._first_success_poller.poll()  # pyright: ignore[reportPrivateUsage]
        mock_dispatch.assert_not_called()
    # Entry untouched (no recorder → early return before processing)
    assert key in sched._first_success_poller.pending  # pyright: ignore[reportPrivateUsage]
