# SCHEMA_PROVENANCE_EXEMPT: Service-internal types — not cross-repo contracts.
"""Pre-match / post-match trigger evaluation for the sports trigger scheduler.

Extracted from ``sports_trigger_scheduler.py`` to keep that module under the
900-line codex cap (mirrors the earlier ``PeriodicTierDispatcher`` /
``FirstSuccessPoller`` extractions). Pure evaluation: given fixtures, the
trigger-tier YAML config, and already-fired state, determine which trigger
events are due right now. No dispatch, no IO.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TypedDict, cast

from .sports_trigger_state import FixtureInfo, as_int

logger = logging.getLogger(__name__)

try:
    from unified_api_contracts.sports import get_league as _uac_get_league
except ImportError:  # pragma: no cover - UAC unavailable in bare-minimal env
    _uac_get_league = None  # type: ignore[assignment]


def _league_has_odds_coverage(league_id: str) -> bool:
    """Check whether *league_id* has odds_api coverage per the UAC registry.

    Returns ``False`` when the league is not found in the registry at all,
    or when its ``data_sources`` frozenset does not include ``"odds_api"``.
    Gracefully returns ``True`` (pass-through) when UAC is unavailable so a
    missing-dependency env doesn't silently suppress dispatches.
    """
    if _uac_get_league is None:
        return True  # no UAC → pass-through (existing behaviour)
    league_def = _uac_get_league(league_id)
    if league_def is None:
        logger.debug("League %s not found in UAC registry — treating as no odds coverage", league_id)
        return False
    return "odds_api" in league_def.data_sources


class TriggerEvent(TypedDict):  # CORRECT-LOCAL: scheduler-internal event dict, never exported
    """A trigger that should fire for a specific fixture."""

    trigger_name: str
    fixture_id: str
    league_id: str
    kickoff_utc: str
    fire_at_utc: str
    services: list[dict[str, str]]


@dataclass
class TriggerState:
    """Tracks which triggers have already fired to avoid duplicates."""

    fired: set[str] = field(default_factory=set)

    def key(self, trigger_name: str, fixture_id: str) -> str:
        """Generate a unique key for a trigger+fixture combination."""
        return f"{trigger_name}:{fixture_id}"

    def has_fired(self, trigger_name: str, fixture_id: str) -> bool:
        return self.key(trigger_name, fixture_id) in self.fired

    def mark_fired(self, trigger_name: str, fixture_id: str) -> None:
        self.fired.add(self.key(trigger_name, fixture_id))


def evaluate_pre_match_triggers(
    config: dict[str, object],
    state: TriggerState,
    fixtures: list[FixtureInfo],
) -> list[TriggerEvent]:
    """Determine which pre-match triggers should fire now."""
    now = datetime.now(UTC)
    events: list[TriggerEvent] = []

    pre_match = config.get("pre_match", {})
    if not isinstance(pre_match, dict):
        return events

    triggers = pre_match.get("triggers", [])
    if not isinstance(triggers, list):
        return events

    for trigger in triggers:
        if not isinstance(trigger, dict):
            continue

        name = str(trigger.get("name", ""))
        offset_hours = float(trigger.get("offset_hours", 0))
        tolerance_minutes = int(trigger.get("tolerance_minutes", 30))

        for fixture in fixtures:
            fixture_id = fixture["fixture_id"]

            # Skip if already fired
            if state.has_fired(name, fixture_id):
                continue

            try:
                kickoff = datetime.fromisoformat(fixture["kickoff_utc"].replace("Z", "+00:00"))
            except ValueError:
                continue

            # Calculate when this trigger should fire
            fire_at = kickoff + timedelta(hours=offset_hours)
            delta_minutes = abs((now - fire_at).total_seconds()) / 60

            # Fire if we're within tolerance window
            if delta_minutes <= tolerance_minutes:
                services = trigger.get("services", [])
                if not isinstance(services, list):
                    services = []

                # ---- league odds-coverage filter ----
                # sports_fast_t1_recon_oom_live_capture_outage_2026_08_01.md
                # identified that the pre-match trigger scheduler fires
                # odds-fetch (market-tick-data-service) dispatches for
                # EVERY scheduled fixture regardless of whether the
                # fixture's league has odds_api coverage per the UAC
                # registry.  Filter out market-tick-data-service entries
                # when the triggering fixture's league has no odds_api
                # coverage, so we don't waste Cloud Run executions on
                # leagues that structurally can never produce odds rows.
                # Other service entries (instruments-service,
                # features-service, ml-service) are left untouched.
                if not _league_has_odds_coverage(fixture["league_id"]):
                    services = [
                        s for s in services if isinstance(s, dict) and s.get("service") != "market-tick-data-service"
                    ]

                if services:
                    events.append(
                        TriggerEvent(
                            trigger_name=name,
                            fixture_id=fixture_id,
                            league_id=fixture["league_id"],
                            kickoff_utc=fixture["kickoff_utc"],
                            fire_at_utc=fire_at.isoformat(),
                            services=[s for s in services if isinstance(s, dict)],
                        )
                    )

    return events


def evaluate_post_match_triggers(
    config: dict[str, object],
    state: TriggerState,
    fixtures: list[FixtureInfo],
    match_end_offset_min: int,
) -> list[TriggerEvent]:
    """Determine which post-match triggers should fire now."""
    now = datetime.now(UTC)
    events: list[TriggerEvent] = []

    post_match = config.get("post_match", {})
    if not isinstance(post_match, dict):
        return events

    triggers = post_match.get("triggers", [])
    if not isinstance(triggers, list):
        return events

    for trigger_raw in triggers:
        if not isinstance(trigger_raw, dict):
            continue
        trigger = cast("dict[str, object]", trigger_raw)

        name = str(trigger.get("name", ""))
        # Post-match offsets can be minutes or hours
        offset_minutes = int(trigger.get("offset_minutes", 0))
        offset_hours = int(trigger.get("offset_hours", 0))
        total_offset_minutes = offset_minutes + (offset_hours * 60)
        tolerance_minutes = as_int(trigger.get("tolerance_minutes"), default=30)

        for fixture in fixtures:
            fixture_id = fixture["fixture_id"]

            if state.has_fired(name, fixture_id):
                continue

            try:
                kickoff = datetime.fromisoformat(fixture["kickoff_utc"].replace("Z", "+00:00"))
            except ValueError:
                continue

            # Estimate match end: kickoff + match_end_offset_min (90 min play + 15 min HT + ~stoppage)
            match_end = kickoff + timedelta(minutes=match_end_offset_min)
            fire_at = match_end + timedelta(minutes=total_offset_minutes)

            delta_minutes = abs((now - fire_at).total_seconds()) / 60

            if delta_minutes <= tolerance_minutes:
                services = trigger.get("services", [])
                if not isinstance(services, list):
                    services = []

                events.append(
                    TriggerEvent(
                        trigger_name=name,
                        fixture_id=fixture_id,
                        league_id=fixture["league_id"],
                        kickoff_utc=fixture["kickoff_utc"],
                        fire_at_utc=fire_at.isoformat(),
                        services=[s for s in services if isinstance(s, dict)],
                    )
                )

    return events


__all__ = ["TriggerEvent", "TriggerState", "evaluate_post_match_triggers", "evaluate_pre_match_triggers"]
