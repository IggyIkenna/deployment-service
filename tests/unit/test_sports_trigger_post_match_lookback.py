"""Regression coverage for the post-match fixture-lookback bug.

unified-trading-pm/plans/active/issues/
sports_post_match_trigger_24h_lookback_bug_2026_07_27.md: `get_upcoming_fixtures()`
only keeps a fixture visible until `kickoff + 2h`, so post-match triggers that
fire well past that (`stats_delayed` at ~kickoff+25.75h, `features_post_match`
at ~kickoff+26.75h) could never see their own target fixture — unconditional
dead code for every fixture, always. Confirmed live-impacting via the prod
manifest (0 XG captures in 30 days) before this fix shipped.

Covers:
  - `get_recently_completed_fixtures` returns a fixture whose kickoff was well
    past `get_upcoming_fixtures`' ~2h cutoff (the actual bug fix).
  - `get_upcoming_fixtures` still excludes that same fixture — the pre-match/
    discovery window is untouched by this fix, exactly as intended.
  - `post_match_lookback_hours` derives a >2h lookback from a 24h-offset
    trigger config (the config-driven sizing, not a hardcoded constant).
  - `_build_post_match_fixture_pool` (storage mocked, not the fetch function)
    round-trips a 25h-old fixture into the post-match evaluation pool.
  - `evaluate_post_match_triggers` fires a 25h-offset trigger for that fixture
    once it's in the pool — the full regression the issue doc's todo asks for.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from unified_trading_library import BlobMetadata

from deployment_service.sports_trigger_scheduler import (
    MATCH_END_OFFSET_MIN,
    SportsTriggerScheduler,
    TriggerState,
)
from deployment_service.sports_trigger_state import (
    get_recently_completed_fixtures,
    get_upcoming_fixtures,
    merge_post_match_fixture_pool,
    post_match_lookback_hours,
)

_STATS_DELAYED_TRIGGER = {
    "name": "stats_delayed",
    "offset_hours": 24,
    "services": [{"service": "instruments-service", "args": {"--sports-entity": "XG"}}],
}


def _fixture_parquet_bytes(fixture_id: str, kickoff: datetime) -> bytes:
    df = pd.DataFrame(
        [
            {
                "fixture_id": fixture_id,
                "league_id": "EPL",
                "kickoff_utc": kickoff.isoformat(),
                "home_team": "Arsenal",
                "away_team": "Chelsea",
            }
        ]
    )
    buf = io.BytesIO()
    df.to_parquet(buf)
    return buf.getvalue()


def _mock_storage_for(kickoff: datetime, fixture_id: str = "fx-late-001") -> MagicMock:
    parquet_bytes = _fixture_parquet_bytes(fixture_id, kickoff)
    blob = BlobMetadata(
        name="sports_reference/fixtures/day=2026-06-01/fixtures.parquet",
        bucket="test-bucket",
        size=len(parquet_bytes),
        last_modified=None,
    )
    mock_storage = MagicMock()
    mock_storage.list_blobs.return_value = [blob]
    mock_storage.download_bytes.return_value = parquet_bytes
    return mock_storage


# ---------------------------------------------------------------------------
# get_recently_completed_fixtures — the actual fetch-layer fix
# ---------------------------------------------------------------------------


def test_get_recently_completed_fixtures_returns_fixture_25h_in_the_past() -> None:
    now = datetime.now(UTC)
    kickoff = now - timedelta(hours=25)
    mock_storage = _mock_storage_for(kickoff)

    with (
        patch("deployment_service.sports_trigger_state.get_storage_client", return_value=mock_storage),
        patch("deployment_service.sports_trigger_state.DeploymentConfig") as mock_cfg,
        patch("deployment_service.sports_trigger_state.get_bucket_name", return_value="test-bucket"),
    ):
        mock_cfg.return_value.project_id = "test-project"
        fixtures = get_recently_completed_fixtures(lookback_hours=30)

    assert any(f["fixture_id"] == "fx-late-001" for f in fixtures)


def test_get_upcoming_fixtures_still_excludes_the_same_fixture() -> None:
    """The pre-match/discovery window is untouched — proves this fix is additive."""
    now = datetime.now(UTC)
    kickoff = now - timedelta(hours=25)
    mock_storage = _mock_storage_for(kickoff)

    with (
        patch("deployment_service.sports_trigger_state.get_storage_client", return_value=mock_storage),
        patch("deployment_service.sports_trigger_state.DeploymentConfig") as mock_cfg,
        patch("deployment_service.sports_trigger_state.get_bucket_name", return_value="test-bucket"),
    ):
        mock_cfg.return_value.project_id = "test-project"
        fixtures = get_upcoming_fixtures(horizon_hours=48)

    assert not any(f["fixture_id"] == "fx-late-001" for f in fixtures)


# ---------------------------------------------------------------------------
# post_match_lookback_hours — config-driven sizing
# ---------------------------------------------------------------------------


def test_post_match_lookback_hours_widens_for_24h_offset_trigger() -> None:
    post_match_config = {"triggers": [_STATS_DELAYED_TRIGGER]}
    lookback = post_match_lookback_hours(post_match_config, MATCH_END_OFFSET_MIN)
    # 105 (match end) + 24*60 (offset) + 30 (default tolerance) = 1575 min = 26.25h
    assert lookback == pytest.approx(26.25)


def test_post_match_lookback_hours_floors_at_2h_with_no_triggers() -> None:
    assert post_match_lookback_hours({"triggers": []}, MATCH_END_OFFSET_MIN) == 2.0
    assert post_match_lookback_hours({}, MATCH_END_OFFSET_MIN) == 2.0


def test_merge_post_match_fixture_pool_dedupes_by_fixture_id() -> None:
    near = [{"fixture_id": "fx-1", "league_id": "EPL", "kickoff_utc": "x", "home_team": "a", "away_team": "b"}]
    late = [
        {"fixture_id": "fx-1", "league_id": "EPL", "kickoff_utc": "STALE", "home_team": "a", "away_team": "b"},
        {"fixture_id": "fx-2", "league_id": "EPL", "kickoff_utc": "y", "home_team": "c", "away_team": "d"},
    ]
    merged = merge_post_match_fixture_pool(near, late)  # pyright: ignore[reportArgumentType]
    assert {f["fixture_id"] for f in merged} == {"fx-1", "fx-2"}
    # near-term copy wins on duplicate id
    assert next(f for f in merged if f["fixture_id"] == "fx-1")["kickoff_utc"] == "x"


# ---------------------------------------------------------------------------
# End-to-end: fetch (mocked storage) -> pool merge -> trigger fires
# ---------------------------------------------------------------------------


def test_build_post_match_fixture_pool_includes_late_fixture_via_mocked_gcs() -> None:
    """`_build_post_match_fixture_pool` — storage mocked, not the fetch fn — proves the real wiring."""
    now = datetime.now(UTC)
    kickoff = now - timedelta(hours=25)
    mock_storage = _mock_storage_for(kickoff)

    sched = SportsTriggerScheduler.__new__(SportsTriggerScheduler)
    sched._config = {"post_match": {"triggers": [_STATS_DELAYED_TRIGGER]}}  # pyright: ignore[reportAttributeAccessIssue]

    with (
        patch("deployment_service.sports_trigger_state.get_storage_client", return_value=mock_storage),
        patch("deployment_service.sports_trigger_state.DeploymentConfig") as mock_cfg,
        patch("deployment_service.sports_trigger_state.get_bucket_name", return_value="test-bucket"),
    ):
        mock_cfg.return_value.project_id = "test-project"
        pool = sched._build_post_match_fixture_pool([])  # pyright: ignore[reportPrivateUsage]

    assert any(f["fixture_id"] == "fx-late-001" for f in pool)


def test_evaluate_post_match_triggers_fires_25h_offset_trigger_for_late_fixture() -> None:
    """The regression the issue doc's fix todo asks for: a synthetic 25h-offset
    trigger now fires for a fixture whose kickoff was >24h in the past."""
    sched = SportsTriggerScheduler.__new__(SportsTriggerScheduler)
    sched._config = {"post_match": {"triggers": [_STATS_DELAYED_TRIGGER]}}  # pyright: ignore[reportAttributeAccessIssue]
    sched._state = TriggerState()  # pyright: ignore[reportAttributeAccessIssue]

    # Pin kickoff so fire_at lands exactly at `now` — zero timing slop.
    now = datetime.now(UTC)
    total_offset_minutes = 24 * 60  # offset_hours=24
    kickoff = now - timedelta(minutes=MATCH_END_OFFSET_MIN + total_offset_minutes)
    assert (now - kickoff) > timedelta(hours=24)  # the scenario the todo names

    fixture = {
        "fixture_id": "fx-late-001",
        "league_id": "EPL",
        "kickoff_utc": kickoff.isoformat(),
        "home_team": "Arsenal",
        "away_team": "Chelsea",
    }

    events = sched.evaluate_post_match_triggers([fixture])  # pyright: ignore[reportArgumentType]

    assert len(events) == 1
    assert events[0]["trigger_name"] == "stats_delayed"
    assert events[0]["fixture_id"] == "fx-late-001"


def test_evaluate_post_match_triggers_respects_configured_tolerance_minutes() -> None:
    """Adjacent fix: tolerance_minutes was read for pre-match triggers but
    silently ignored (hardcoded 30) for post-match triggers — features_post_match
    configures tolerance_minutes=60 in prod and needs it honored."""
    sched = SportsTriggerScheduler.__new__(SportsTriggerScheduler)
    trigger = {"name": "features_post_match", "offset_hours": 25, "tolerance_minutes": 60, "services": []}
    sched._config = {"post_match": {"triggers": [trigger]}}  # pyright: ignore[reportAttributeAccessIssue]
    sched._state = TriggerState()  # pyright: ignore[reportAttributeAccessIssue]

    now = datetime.now(UTC)
    # 45 min past fire_at — inside the configured 60min tolerance, outside the
    # old hardcoded 30min one.
    total_offset_minutes = 25 * 60
    fire_at = now - timedelta(minutes=45)
    kickoff = fire_at - timedelta(minutes=MATCH_END_OFFSET_MIN + total_offset_minutes)

    fixture = {
        "fixture_id": "fx-late-002",
        "league_id": "EPL",
        "kickoff_utc": kickoff.isoformat(),
        "home_team": "Arsenal",
        "away_team": "Chelsea",
    }

    events = sched.evaluate_post_match_triggers([fixture])  # pyright: ignore[reportArgumentType]

    assert len(events) == 1
    assert events[0]["trigger_name"] == "features_post_match"
