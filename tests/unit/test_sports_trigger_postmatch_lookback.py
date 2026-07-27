"""Regression tests for sports_post_match_trigger_24h_lookback_bug_2026_07_27.md.

Root cause: ``get_upcoming_fixtures``'s fixture-visibility filter closed at
``kickoff + 2h`` for every caller, so a post-match trigger whose fire window
sits beyond that (``stats_delayed`` @ ~kickoff+26h, ``features_post_match`` @
~kickoff+28h) could never see its own target fixture — those triggers never
fired live. The fix widens the lookback specifically for post-match
evaluation (via ``SportsTriggerScheduler._max_post_match_lookback_hours`` +
a second ``get_upcoming_fixtures`` call in ``run_once``) without touching the
default 2h cutoff every other caller (pre-match, Tier-1/2 periodic) relies on.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd

from deployment_service.sports_trigger_scheduler import MATCH_END_OFFSET_MIN, SportsTriggerScheduler
from deployment_service.sports_trigger_state import FixtureInfo, get_upcoming_fixtures


def _make_scheduler() -> SportsTriggerScheduler:
    return SportsTriggerScheduler(
        config_path="configs/sports-trigger-tiers.yaml",
        dry_run=False,
        periodic_state=None,
        record_latency=False,
    )


def _make_fixture_parquet(kickoff_utc: str, fixture_id: str = "fx-stale") -> bytes:
    df = pd.DataFrame(
        [
            {
                "fixture_id": fixture_id,
                "league_id": "EPL",
                "kickoff_utc": kickoff_utc,
                "home_team": "Arsenal",
                "away_team": "Chelsea",
            }
        ]
    )
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


class TestMaxPostMatchLookbackHours:
    def test_computed_from_real_config_covers_features_post_match(self) -> None:
        """features_post_match (offset_hours=25, tolerance_minutes=60) is the
        widest configured post-match trigger — the lookback must cover its
        fire-window edge: MATCH_END_OFFSET_MIN + 25h + 60min tolerance."""
        scheduler = _make_scheduler()
        lookback = scheduler._max_post_match_lookback_hours()  # pyright: ignore[reportPrivateUsage]
        expected_edge_minutes = MATCH_END_OFFSET_MIN + (25 * 60) + 60
        assert lookback == expected_edge_minutes / 60.0
        assert lookback > 24.0  # strictly beyond stats_delayed's own 24h offset
        assert lookback > 2.0  # strictly beyond the old broken default cutoff

    def test_no_post_match_triggers_falls_back_to_default(self) -> None:
        scheduler = _make_scheduler()
        scheduler._config = {}  # pyright: ignore[reportPrivateUsage]
        assert scheduler._max_post_match_lookback_hours() == 2.0  # pyright: ignore[reportPrivateUsage]


class TestGetUpcomingFixturesLookback:
    def _patched(self, kickoff_iso: str):
        mock_blob = MagicMock()
        mock_blob.__str__ = lambda self: "sports_reference/fixtures/day=2026-07-26/all.parquet"
        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = [mock_blob]
        mock_storage.download_bytes.return_value = _make_fixture_parquet(kickoff_iso)
        mock_config = MagicMock()
        mock_config.project_id = "test-project"
        return (
            patch("deployment_service.sports_trigger_state.DeploymentConfig", return_value=mock_config),
            patch("deployment_service.sports_trigger_state.get_storage_client", return_value=mock_storage),
            patch("deployment_service.sports_trigger_state.get_bucket_name", return_value="test-bucket"),
        )

    def test_default_lookback_excludes_stale_kickoff(self) -> None:
        """Reproduces the ORIGINAL bug shape: with the untouched default (2h)
        lookback, a fixture that kicked off 25h45m ago is invisible."""
        stale_kickoff = (datetime.now(UTC) - timedelta(hours=25, minutes=45)).isoformat()
        p1, p2, p3 = self._patched(stale_kickoff)
        with p1, p2, p3:
            fixtures = get_upcoming_fixtures(horizon_hours=48)
        assert fixtures == []

    def test_widened_lookback_includes_stale_kickoff(self) -> None:
        """With a lookback wide enough (as computed by
        ``_max_post_match_lookback_hours``), the same stale fixture IS found —
        this is the fix."""
        stale_kickoff = (datetime.now(UTC) - timedelta(hours=25, minutes=45)).isoformat()
        p1, p2, p3 = self._patched(stale_kickoff)
        with p1, p2, p3:
            fixtures = get_upcoming_fixtures(horizon_hours=48, lookback_hours=27.75)
        assert len(fixtures) >= 1
        assert fixtures[0]["fixture_id"] == "fx-stale"


class TestRunOnceFiresStalePostMatchTrigger:
    def test_stats_delayed_fires_for_fixture_kicked_off_over_24h_ago(self) -> None:
        """End-to-end regression proof: a fixture whose kickoff was >24h in
        the past now reaches ``evaluate_post_match_triggers`` and fires
        ``stats_delayed`` — this could never happen before the fix, because
        ``run_once`` fetched exactly one (2h-lookback) fixture list and fed
        it to both pre- and post-match evaluation.
        """
        scheduler = _make_scheduler()
        now = datetime.now(UTC)
        # fire_at for stats_delayed (offset_hours=24) = kickoff + 105min + 24h.
        # Setting kickoff so fire_at == now puts this dead-center of the
        # trigger's tolerance window.
        stale_kickoff = now - timedelta(minutes=MATCH_END_OFFSET_MIN + 24 * 60)
        stale_fixture = FixtureInfo(
            fixture_id="fx-stale",
            league_id="EPL",
            kickoff_utc=stale_kickoff.isoformat(),
            home_team="Arsenal",
            away_team="Chelsea",
        )

        def _fake_get_upcoming_fixtures(horizon_hours: int = 48, lookback_hours: float = 2.0):
            hours_until = (stale_kickoff - now).total_seconds() / 3600
            if -lookback_hours <= hours_until <= horizon_hours:
                return [stale_fixture]
            return []

        with (
            patch.object(scheduler, "get_upcoming_fixtures", side_effect=_fake_get_upcoming_fixtures),
            patch.object(scheduler, "_dispatch_local", return_value=True),
            patch.object(scheduler._first_success_poller, "poll", return_value=None),  # pyright: ignore[reportPrivateUsage]
            patch.object(scheduler, "_check_discovery", return_value=0),
            patch.object(scheduler, "_check_reference", return_value=0),
        ):
            fired = scheduler.run_once()

        assert scheduler._state.has_fired("stats_delayed", "fx-stale")  # pyright: ignore[reportPrivateUsage]
        assert fired >= 1
