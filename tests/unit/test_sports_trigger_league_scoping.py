"""Unit tests for fire_trigger's per-fixture --league scoping.

Covers the fix for sports_fast_t1_recon_oom_live_capture_outage_2026_08_01.md:
market-tick-data-service dispatches previously carried no --league flag, so
OddsApiAdapter fetched all ~30 Prediction-tier leagues per single-fixture
trigger instead of the ONE triggering league -- the confirmed root cause of
the SPORTS fast-t1-recon Cloud Run Job OOM. fire_trigger must now inject
--league=<event league_id> into every market-tick-data-service service entry
while leaving every other service's args (e.g. instruments-service's
--sports-entity) untouched.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

from deployment_service.sports_trigger_scheduler import SportsTriggerScheduler


def _make_scheduler() -> SportsTriggerScheduler:
    return SportsTriggerScheduler(
        config_path="configs/sports-trigger-tiers.yaml",
        dry_run=False,
        periodic_state=None,
        record_latency=False,
    )


def _event(services: list[dict[str, object]]) -> dict[str, object]:
    return {
        "trigger_name": "odds_t1h",
        "fixture_id": "fx1",
        "league_id": "UCL",
        "kickoff_utc": datetime.now(UTC).isoformat(),
        "fire_at_utc": datetime.now(UTC).isoformat(),
        "services": services,
    }


def test_fire_trigger_injects_league_into_market_tick_data_service() -> None:
    sched = _make_scheduler()
    event = _event(
        [
            {
                "service": "market-tick-data-service",
                "operation": "download",
                "category": "SPORTS",
                "cloud_run_job_name": "uts-prod-market-tick-data-service-fast-t1-recon",
            }
        ]
    )
    with patch.object(sched, "_dispatch_services", return_value=1) as mock_dispatch:
        sched.fire_trigger(event)  # pyright: ignore[reportArgumentType]
    dispatched_services = mock_dispatch.call_args.kwargs["services"]
    assert len(dispatched_services) == 1
    assert dispatched_services[0]["args"] == {"--league": "UCL"}
    # the original service dict passed into fire_trigger must be untouched (no
    # in-place mutation of the trigger-tiers config's own service entries).
    assert "args" not in event["services"][0]  # pyright: ignore[reportIndexIssue]


def test_fire_trigger_leaves_other_services_args_untouched() -> None:
    sched = _make_scheduler()
    event = _event(
        [
            {
                "service": "market-tick-data-service",
                "operation": "download",
                "category": "SPORTS",
            },
            {
                "service": "instruments-service",
                "operation": "instruments",
                "category": "SPORTS",
                "args": {"--sports-entity": "PREDICTIONS"},
            },
        ]
    )
    with patch.object(sched, "_dispatch_services", return_value=1) as mock_dispatch:
        sched.fire_trigger(event)  # pyright: ignore[reportArgumentType]
    dispatched_services = mock_dispatch.call_args.kwargs["services"]
    mtds_entry = next(s for s in dispatched_services if s["service"] == "market-tick-data-service")
    is_entry = next(s for s in dispatched_services if s["service"] == "instruments-service")
    assert mtds_entry["args"] == {"--league": "UCL"}
    # instruments-service keeps its own --sports-entity arg, no --league injected.
    assert is_entry["args"] == {"--sports-entity": "PREDICTIONS"}


def test_fire_trigger_scopes_multiple_market_tick_data_service_entries() -> None:
    sched = _make_scheduler()
    event = _event(
        [
            {"service": "market-tick-data-service", "operation": "download"},
            {"service": "market-tick-data-service", "operation": "download", "category": "SPORTS_LIVE"},
        ]
    )
    with patch.object(sched, "_dispatch_services", return_value=1) as mock_dispatch:
        sched.fire_trigger(event)  # pyright: ignore[reportArgumentType]
    dispatched_services = mock_dispatch.call_args.kwargs["services"]
    assert len(dispatched_services) == 2
    assert all(s["args"] == {"--league": "UCL"} for s in dispatched_services)
