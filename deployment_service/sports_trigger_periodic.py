# SCHEMA_PROVENANCE_EXEMPT: Service-internal dispatcher — not a cross-repo contract.
"""Periodic-tier dispatcher for sports trigger scheduler.

Wires Tier-1 discovery (rolling-window, force-overwrite) and Tier-2 reference
(window-condition gating: ``transfer_window_open`` / ``season_boundary``)
into the ``SportsTriggerScheduler`` polling loop without growing the scheduler
module past the 900-line codex cap.

The dispatcher is intentionally thin. It owns cadence + gating + rolling
window math. It reaches back into the scheduler for the actual CLI build and
subprocess / Cloud Run dispatch — so every tier shares the same backend stub
(switching per-fixture + periodic to Cloud Run is one code change).

Contract SSOT: ``unified-trading-pm/codex/02-data/sports-scheduling-and-
sharding.md`` (§2 providers, §4 rolling window, §7 transfer-window triggers,
§8 Cloud Run vs VM).
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from typing import Protocol

from unified_api_contracts.sports import (  # noqa: UAC sports facade (domain-level) is the canonical surface — not a deep import
    get_expected_leagues_for_source,
    get_season_end,
    get_season_start,
    is_transfer_window_open,
)

from .sports_trigger_state import PeriodicTierState, as_float, as_int, resolve_source_key

logger = logging.getLogger(__name__)


class SchedulerDispatchAdapter(Protocol):
    """Minimal surface the dispatcher needs from the scheduler.

    Keeps the module acyclic — dispatcher does not import the scheduler class.
    """

    @property
    def dry_run(self) -> bool: ...

    def build_cli_cmd(
        self,
        *,
        service: str,
        operation: str,
        category: str,
        start_date: str,
        end_date: str,
        extra_args: dict[str, object],
        force: bool,
    ) -> str: ...

    def dispatch_services(
        self,
        *,
        services: list[dict[str, object]],
        start_date: str,
        end_date: str,
        trigger_name: str,
        dispatch_id: str,
        force: bool,
    ) -> int: ...


class PeriodicTierDispatcher:
    """Owns Tier-1 + Tier-2 cadence, gating, and rolling-window math."""

    def __init__(
        self,
        *,
        config: dict[str, object],
        state: PeriodicTierState | None,
        adapter: SchedulerDispatchAdapter,
    ) -> None:
        self._config = config
        self._state = state
        self._adapter = adapter

    # ------------------------------------------------------------------
    # Cadence + rolling window
    # ------------------------------------------------------------------

    def _rolling_window(self, section: dict[str, object]) -> tuple[str, str]:
        """Resolve ``[today - lookback_days, today + lookahead_days]``.

        Falls back to single-day ``today..today`` if ``rolling_window`` is
        absent. Contract: codex/02-data/sports-scheduling-and-sharding.md §4.
        """
        window_raw = section.get("rolling_window")
        window: dict[str, object] = window_raw if isinstance(window_raw, dict) else {}
        lookback = as_int(window.get("lookback_days"), default=0)
        lookahead = as_int(window.get("lookahead_days"), default=0)
        today = datetime.now(UTC).date()
        start = (today - timedelta(days=lookback)).strftime("%Y-%m-%d")
        end = (today + timedelta(days=lookahead)).strftime("%Y-%m-%d")
        return start, end

    def _force_overwrite(self, section: dict[str, object]) -> bool:
        window_raw = section.get("rolling_window")
        window: dict[str, object] = window_raw if isinstance(window_raw, dict) else {}
        return bool(window.get("force_overwrite", False))

    def _is_cadence_elapsed(self, tier_name: str, frequency_hours: float, now: datetime) -> bool:
        """True if this tier has never fired OR ``now - last_run >= frequency_hours``."""
        if self._state is None:
            # No persistent state — fire on every poll, but log so operators
            # can see cadence isn't being enforced across restarts.
            logger.info("Tier %s: no persistent state — firing on this poll", tier_name)
            return True
        last = self._state.get_last_run(tier_name)
        if last is None:
            return True
        return (now - last) >= timedelta(hours=frequency_hours)

    # ------------------------------------------------------------------
    # Tier-1 discovery
    # ------------------------------------------------------------------

    def check_discovery(self) -> int:
        """Fire Tier-1 discovery services if cadence has elapsed."""
        section_raw = self._config.get("discovery")
        if not isinstance(section_raw, dict):
            return 0
        section: dict[str, object] = section_raw
        tier_name = "discovery"

        frequency_hours = as_float(section.get("frequency_hours"), default=6.0)
        now = datetime.now(UTC)
        if not self._is_cadence_elapsed(tier_name, frequency_hours, now):
            return 0

        services = _services_from(section)
        if not services:
            return 0

        start_date, end_date = self._rolling_window(section)
        force = self._force_overwrite(section)

        logger.info(
            "TIER-1 DISCOVERY firing: window=[%s..%s] force=%s services=%d",
            start_date,
            end_date,
            force,
            len(services),
        )

        if self._adapter.dry_run:
            for svc in services:
                cmd = self._adapter.build_cli_cmd(
                    service=str(svc.get("service", "")),
                    operation=str(svc.get("operation", "")),
                    category=str(svc.get("category", "SPORTS")),
                    start_date=start_date,
                    end_date=end_date,
                    extra_args=_args_of(svc),
                    force=force,
                )
                logger.info("DRY RUN  -> %s (%s)", cmd, svc.get("description", ""))
            return len(services)

        dispatched = self._adapter.dispatch_services(
            services=services,
            start_date=start_date,
            end_date=end_date,
            trigger_name=tier_name,
            dispatch_id="periodic",
            force=force,
        )
        if dispatched and self._state is not None:
            self._state.set_last_run(tier_name, now)
        return dispatched

    # ------------------------------------------------------------------
    # Tier-2 reference
    # ------------------------------------------------------------------

    def check_reference(self) -> int:
        """Fire Tier-2 reference services if cadence has elapsed.

        Respects per-service ``run_always`` / ``window_condition``:
          - ``run_always: true`` — fire when cadence elapsed.
          - ``window_condition: transfer_window_open`` — fire for leagues
            whose transfer window is open today; passes ``--league L1,L2``.
          - ``window_condition: season_boundary`` — fire if today is within
            tolerance of any expected-league season boundary.
        """
        section_raw = self._config.get("reference")
        if not isinstance(section_raw, dict):
            return 0
        section: dict[str, object] = section_raw
        tier_name = "reference"

        frequency_hours = as_float(section.get("frequency_hours"), default=24.0)
        now = datetime.now(UTC)
        if not self._is_cadence_elapsed(tier_name, frequency_hours, now):
            return 0

        services = _services_from(section)
        if not services:
            return 0

        today = now.date()
        today_str = today.strftime("%Y-%m-%d")
        total_dispatched = 0

        logger.info(
            "TIER-2 REFERENCE evaluating: frequency=%.1fh services=%d today=%s",
            frequency_hours,
            len(services),
            today_str,
        )

        for svc in services:
            gated_services, gate_reason = self._apply_window_condition(svc, today)
            if gated_services is None:
                logger.info(
                    "  SKIP %s — %s",
                    svc.get("description", svc.get("service")),
                    gate_reason,
                )
                continue

            if self._adapter.dry_run:
                for gsvc in gated_services:
                    cmd = self._adapter.build_cli_cmd(
                        service=str(gsvc.get("service", "")),
                        operation=str(gsvc.get("operation", "")),
                        category=str(gsvc.get("category", "SPORTS")),
                        start_date=today_str,
                        end_date=today_str,
                        extra_args=_args_of(gsvc),
                        force=False,
                    )
                    logger.info("DRY RUN  -> %s (%s)", cmd, gsvc.get("description", ""))
                total_dispatched += len(gated_services)
                continue

            total_dispatched += self._adapter.dispatch_services(
                services=gated_services,
                start_date=today_str,
                end_date=today_str,
                trigger_name=tier_name,
                dispatch_id="periodic",
                force=False,
            )

        if total_dispatched and self._state is not None and not self._adapter.dry_run:
            self._state.set_last_run(tier_name, now)
        return total_dispatched

    # ------------------------------------------------------------------
    # Window-condition gating
    # ------------------------------------------------------------------

    def _apply_window_condition(
        self,
        svc: dict[str, object],
        today: date,
    ) -> tuple[list[dict[str, object]] | None, str]:
        """Apply ``run_always`` / ``window_condition`` gating."""
        if bool(svc.get("run_always", True)):
            return [svc], "run_always"

        condition = str(svc.get("window_condition", ""))
        if condition == "transfer_window_open":
            return self._gate_by_transfer_window(svc, today)
        if condition == "season_boundary":
            return self._gate_by_season_boundary(svc, today)

        logger.warning("Unknown window_condition %r for service %s", condition, svc.get("service"))
        return None, f"unknown window_condition={condition}"

    def _gate_by_transfer_window(
        self,
        svc: dict[str, object],
        today: date,
    ) -> tuple[list[dict[str, object]] | None, str]:
        """Scope to leagues whose transfer window is open today."""
        source_key = resolve_source_key(svc)
        leagues = get_expected_leagues_for_source(source_key)
        open_leagues = [
            lg.league_id for lg in leagues if is_transfer_window_open(lg.league_id, today)
        ]
        if not open_leagues:
            return None, "no transfer windows open"
        return [
            _scope_to_leagues(svc, open_leagues)
        ], f"transfer windows open for {len(open_leagues)} leagues"

    def _gate_by_season_boundary(
        self,
        svc: dict[str, object],
        today: date,
        *,
        tolerance_days: int = 3,
    ) -> tuple[list[dict[str, object]] | None, str]:
        """Fire only if today is within tolerance of any expected-league season boundary."""
        source_key = resolve_source_key(svc)
        leagues = get_expected_leagues_for_source(source_key)
        triggering: list[str] = []
        for lg in leagues:
            for year in (today.year - 1, today.year, today.year + 1):
                try:
                    season_open = get_season_start(lg.league_id, year)
                    season_close = get_season_end(lg.league_id, year)
                except (ValueError, KeyError):
                    continue
                if (
                    abs((today - season_open).days) <= tolerance_days
                    or abs((today - season_close).days) <= tolerance_days
                ):
                    triggering.append(lg.league_id)
                    break
        if not triggering:
            return None, "no season boundary within tolerance"
        return [
            _scope_to_leagues(svc, triggering)
        ], f"season boundary near for {len(triggering)} leagues"


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _services_from(section: dict[str, object]) -> list[dict[str, object]]:
    services_raw = section.get("services", [])
    if not isinstance(services_raw, list):
        return []
    return [s for s in services_raw if isinstance(s, dict)]


def _args_of(svc: dict[str, object]) -> dict[str, object]:
    args_raw = svc.get("args", {})
    return args_raw if isinstance(args_raw, dict) else {}


def _scope_to_leagues(svc: dict[str, object], leagues: list[str]) -> dict[str, object]:
    """Return a copy of ``svc`` with ``--league`` populated from ``leagues``."""
    scoped = dict(svc)
    args = dict(_args_of(scoped))
    args["--league"] = ",".join(sorted(leagues))
    scoped["args"] = args
    return scoped


__all__ = ["PeriodicTierDispatcher", "SchedulerDispatchAdapter"]
