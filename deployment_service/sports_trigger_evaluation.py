# SCHEMA_PROVENANCE_EXEMPT: Service-internal types — not cross-repo contracts.
"""Pre-/post-match trigger evaluation for the sports trigger scheduler.

Extracted from ``sports_trigger_scheduler.py`` (2026-07-30, deployment-service
``ldr_qg_failure`` fix — the scheduler had grown to 945L against the 930L
codex file-size cap) to mirror the existing extraction pattern
(``sports_trigger_periodic.py``, ``sports_latency_observation.py``): pure
evaluation logic operating on trigger-tier config + a fixture list, with no
scheduler instance state beyond ``TriggerState`` (already a plain dataclass
with no scheduler back-reference).

Contract SSOT: ``unified-trading-pm/codex/02-data/sports-scheduling-and-
sharding.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TypedDict, cast

from .sports_trigger_state import FixtureInfo, as_int

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


def max_post_match_lookback_hours(config: dict[str, object], match_end_offset_min: int) -> float:
    """How far in the past a fixture's kickoff may fall and still be due
    for a post-match trigger, derived from ``configs/sports-trigger-tiers.yaml``.

    A post-match trigger's fire window closes at
    ``kickoff + match_end_offset_min + total_offset_minutes + tolerance``
    (mirrors :func:`evaluate_post_match_triggers`). The widest of these
    across all configured post-match triggers is how far back
    ``get_upcoming_fixtures`` must look so the fixture is still visible when
    its trigger becomes due — computed from config, not hardcoded, so a
    future trigger with an even later offset doesn't silently repeat the
    ``sports_post_match_trigger_24h_lookback_bug`` (a fixture-visibility
    cutoff of ``kickoff + 2h`` that structurally prevented
    ``stats_delayed``/``features_post_match`` from ever firing). Returns 2.0
    (matching the default lookback) if no post-match trigger needs more.
    """
    post_match_raw = config.get("post_match", {})
    if not isinstance(post_match_raw, dict):
        return 2.0
    post_match = cast("dict[str, object]", post_match_raw)
    triggers_raw = post_match.get("triggers", [])
    if not isinstance(triggers_raw, list):
        return 2.0
    triggers = cast("list[object]", triggers_raw)

    max_minutes = 0
    for trigger_raw in triggers:
        if not isinstance(trigger_raw, dict):
            continue
        trigger = cast("dict[str, object]", trigger_raw)
        offset_minutes = as_int(trigger.get("offset_minutes"), default=0)
        offset_hours = as_int(trigger.get("offset_hours"), default=0)
        tolerance_minutes = as_int(trigger.get("tolerance_minutes"), default=30)
        fire_edge_minutes = match_end_offset_min + offset_minutes + (offset_hours * 60) + tolerance_minutes
        max_minutes = max(max_minutes, fire_edge_minutes)

    if max_minutes == 0:
        return 2.0
    return max_minutes / 60.0


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

            # Estimate match end: kickoff + match_end_offset_min (90 + 15 HT + stoppage)
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
