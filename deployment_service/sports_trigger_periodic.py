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
        asset_group: str,
        start_date: str | None = None,
        end_date: str | None = None,
        extra_args: dict[str, object],
        force: bool = False,
        rolling_window: tuple[int, int, bool] | None = None,
    ) -> str: ...

    def dispatch_services(
        self,
        *,
        services: list[dict[str, object]],
        start_date: str | None = None,
        end_date: str | None = None,
        trigger_name: str,
        dispatch_id: str,
        force: bool = False,
        rolling_window: tuple[int, int, bool] | None = None,
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

    def _rolling_window_triple(self, section: dict[str, object]) -> tuple[int, int, bool]:
        """Read `(lookback_days, lookahead_days, force_window)` from the YAML.

        Passed raw to instruments-service, which owns the date math per CLI
        contract ``70517b2`` (codex/02-data/sports-scheduling-and-
        sharding.md §4). Falls back to `(0, 0, False)` if `rolling_window`
        is absent — effectively single-day ``today..today``.
        """
        window_raw = section.get("rolling_window")
        window: dict[str, object] = window_raw if isinstance(window_raw, dict) else {}
        lookback = as_int(window.get("lookback_days"), default=0)
        lookahead = as_int(window.get("lookahead_days"), default=0)
        force_window = bool(window.get("force_overwrite", False))
        return lookback, lookahead, force_window

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
        """Fire Tier-1 discovery services if cadence has elapsed.

        Passes the raw ``(lookback, lookahead, force_window)`` triple through
        to instruments-service which resolves it to concrete dates at parse
        time (CLI SSOT: ``70517b2``). Keeping the date math in one place
        avoids clock-drift between scheduler and CLI and matches the manual
        launcher shape (deployment-service ``b0eb874``).
        """
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

        rolling = self._rolling_window_triple(section)

        logger.info(
            "TIER-1 DISCOVERY firing: rolling_window=(lookback=%d, lookahead=%d, force_window=%s) services=%d",
            rolling[0],
            rolling[1],
            rolling[2],
            len(services),
        )

        if self._adapter.dry_run:
            for svc in services:
                ag = str(svc.get("asset_group") or svc.get("category", "SPORTS"))
                cmd = self._adapter.build_cli_cmd(
                    service=str(svc.get("service", "")),
                    operation=str(svc.get("operation", "")),
                    asset_group=ag,
                    extra_args=_args_of(svc),
                    rolling_window=rolling,
                )
                logger.info("DRY RUN  -> %s (%s)", cmd, svc.get("description", ""))
            return len(services)

        dispatched = self._adapter.dispatch_services(
            services=services,
            trigger_name=tier_name,
            dispatch_id="periodic",
            rolling_window=rolling,
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
                    gag = str(gsvc.get("asset_group") or gsvc.get("category", "SPORTS"))
                    cmd = self._adapter.build_cli_cmd(
                        service=str(gsvc.get("service", "")),
                        operation=str(gsvc.get("operation", "")),
                        asset_group=gag,
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
        open_leagues = [lg.league_id for lg in leagues if is_transfer_window_open(lg.league_id, today)]
        if not open_leagues:
            return None, "no transfer windows open"
        return [_scope_to_leagues(svc, open_leagues)], f"transfer windows open for {len(open_leagues)} leagues"

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
        return [_scope_to_leagues(svc, triggering)], f"season boundary near for {len(triggering)} leagues"


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
