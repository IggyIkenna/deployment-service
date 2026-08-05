"""Unit tests for pre-match trigger league odds-coverage filter.

Covers the fix for sports_fast_t1_recon_oom_live_capture_outage_2026_08_01.md:
evaluate_pre_match_triggers must not produce market-tick-data-service trigger
events for fixtures whose league has no odds_api coverage per the UAC registry
(e.g. REF_API_ONLY leagues like SLOVAKIA_SUPER_LIGA, CANADA_PREMIER_LEAGUE,
POLAND_I_LIGA). Other service entries (instruments-service, features-service)
must still be included.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from deployment_service.sports_trigger_evaluation import (
    TriggerState,
    _league_has_odds_coverage,
    evaluate_pre_match_triggers,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(triggers: list[dict[str, object]]) -> dict[str, object]:
    return {"pre_match": {"triggers": triggers}}


def _make_fixture(
    fixture_id: str = "fx1",
    league_id: str = "EPL",
    kickoff_offset_hours: float = -1.0,
) -> dict[str, str]:
    """Build a FixtureInfo-like dict with a kickoff relative to now."""
    kickoff = datetime.now(UTC) + timedelta(hours=kickoff_offset_hours)
    return {
        "fixture_id": fixture_id,
        "league_id": league_id,
        "kickoff_utc": kickoff.isoformat(),
        "home_team": "Home FC",
        "away_team": "Away FC",
    }


# ---------------------------------------------------------------------------
# _league_has_odds_coverage — unit tests (direct, no mocking)
# ---------------------------------------------------------------------------


def test_league_has_odds_coverage_true_for_known_odds_league() -> None:
    """A league registered with data_sources including odds_api returns True."""
    # EPL is in the canonical UAC registry with odds_api coverage.
    assert _league_has_odds_coverage("EPL") is True


def test_league_has_odds_coverage_false_for_ref_api_only_league() -> None:
    """A REF_API_ONLY league (no odds_api) returns False."""
    # CANADA_PREMIER_LEAGUE has data_sources=frozenset({"REF_API_ONLY"}).
    assert _league_has_odds_coverage("CANADA_PREMIER_LEAGUE") is False


def test_league_has_odds_coverage_false_for_unknown_league() -> None:
    """A league not in the registry at all returns False."""
    assert _league_has_odds_coverage("NONEXISTENT_LEAGUE_XYZ123") is False


# ---------------------------------------------------------------------------
# evaluate_pre_match_triggers — odds-coverage filter
# ---------------------------------------------------------------------------


def test_no_mtds_event_for_non_odds_league() -> None:
    """A fixture from a REF_API_ONLY league produces NO trigger event when every
    trigger's services entry is market-tick-data-service only (e.g. odds_t6h).
    """
    config = _make_config(
        [
            {
                "name": "odds_t6h",
                "offset_hours": -6,
                "tolerance_minutes": 30,
                "services": [
                    {"service": "market-tick-data-service", "operation": "download", "category": "SPORTS"},
                ],
            },
        ]
    )
    state = TriggerState()
    fixture = _make_fixture(league_id="CANADA_PREMIER_LEAGUE", kickoff_offset_hours=6.0)

    events = evaluate_pre_match_triggers(config, state, [fixture])
    assert events == []


def test_mtds_event_still_produced_for_odds_league() -> None:
    """A fixture from an odds_api-covered league (e.g. EPL) DOES get a
    market-tick-data-service trigger event.
    """
    config = _make_config(
        [
            {
                "name": "odds_t6h",
                "offset_hours": -6,
                "tolerance_minutes": 30,
                "services": [
                    {"service": "market-tick-data-service", "operation": "download", "category": "SPORTS"},
                ],
            },
        ]
    )
    state = TriggerState()
    fixture = _make_fixture(league_id="EPL", kickoff_offset_hours=6.0)

    events = evaluate_pre_match_triggers(config, state, [fixture])
    assert len(events) == 1
    assert events[0]["trigger_name"] == "odds_t6h"
    assert events[0]["league_id"] == "EPL"
    assert len(events[0]["services"]) == 1
    assert events[0]["services"][0]["service"] == "market-tick-data-service"


def test_non_mtds_services_preserved_for_non_odds_league() -> None:
    """A REF_API_ONLY league still gets instruments-service and features-service
    entries — only market-tick-data-service is stripped.
    """
    config = _make_config(
        [
            {
                "name": "odds_t1h",
                "offset_hours": -1,
                "tolerance_minutes": 15,
                "services": [
                    {"service": "market-tick-data-service", "operation": "download", "category": "SPORTS"},
                    {
                        "service": "instruments-service",
                        "operation": "instruments",
                        "args": {"--sports-entity": "LINEUPS"},
                    },
                    {"service": "features-service", "operation": "compute", "category": "SPORTS"},
                ],
            },
        ]
    )
    state = TriggerState()
    fixture = _make_fixture(league_id="SLOVAKIA_SUPER_LIGA", kickoff_offset_hours=1.0)

    events = evaluate_pre_match_triggers(config, state, [fixture])
    assert len(events) == 1
    services = [s["service"] for s in events[0]["services"]]
    assert "market-tick-data-service" not in services
    assert "instruments-service" in services
    assert "features-service" in services


def test_odds_t24h_mixed_services_filtered_for_non_odds_league() -> None:
    """odds_t24h has both market-tick-data-service AND instruments-service
    (PREDICTIONS). For a non-odds league the event still fires with only the
    instruments-service entry.
    """
    config = _make_config(
        [
            {
                "name": "odds_t24h",
                "offset_hours": -24,
                "tolerance_minutes": 30,
                "services": [
                    {"service": "market-tick-data-service", "operation": "download", "category": "SPORTS"},
                    {
                        "service": "instruments-service",
                        "operation": "instruments",
                        "args": {"--sports-entity": "PREDICTIONS"},
                    },
                ],
            },
        ]
    )
    state = TriggerState()
    fixture = _make_fixture(league_id="POLAND_I_LIGA", kickoff_offset_hours=24.0)

    events = evaluate_pre_match_triggers(config, state, [fixture])
    assert len(events) == 1
    services = [s["service"] for s in events[0]["services"]]
    assert "market-tick-data-service" not in services
    assert "instruments-service" in services


def test_already_fired_not_re_evaluated() -> None:
    """Already-fired triggers are skipped before reaching the odds-coverage
    filter — the existing dedup still works.
    """
    config = _make_config(
        [
            {
                "name": "odds_t6h",
                "offset_hours": -6,
                "tolerance_minutes": 30,
                "services": [
                    {"service": "market-tick-data-service", "operation": "download"},
                ],
            },
        ]
    )
    state = TriggerState()
    state.mark_fired("odds_t6h", "fx1")
    fixture = _make_fixture(league_id="EPL", kickoff_offset_hours=6.0)

    events = evaluate_pre_match_triggers(config, state, [fixture])
    assert events == []
