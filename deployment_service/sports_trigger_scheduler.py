# SCHEMA_PROVENANCE_EXEMPT: Service-internal types — not cross-repo contracts.
"""Sports Fixture-Aware Trigger Scheduler.

Reads the fixture calendar from GCS, determines what's due based on
trigger tier config, and fires standard batch CLI invocations at
fixture-proximate times.

Two operational layers:
  1. Discovery loop — runs every N hours to refresh the fixture calendar
  2. Fixture-proximate triggers — scheduled relative to kickoff times

Sports "live" = batch with ``--date today``, fired at fixture-proximate
times.  Same CLI, same service, just triggered by fixture proximity
instead of daily cron.

Usage:
    scheduler = SportsTriggerScheduler(config_path="configs/sports-trigger-tiers.yaml")
    scheduler.run()  # blocking loop — scans fixtures, fires triggers
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypedDict, cast

import yaml
from unified_trading_library import get_bucket_name

from .backends.cloud_run import CloudRunBackend
from .sports_latency_observation import (
    FirstSuccessPoller,
    LatencyObservationRecorder,
    build_observations_for_fire,
)
from .sports_trigger_dispatch_backends import (
    dispatch_local,
    get_cloud_run_backend,
    strip_python_module_prefix,
)
from .sports_trigger_periodic import PeriodicTierDispatcher
from .sports_trigger_state import (
    FixtureInfo,
    PeriodicTierState,
    as_int,
    resolve_state_bucket,
)
from .sports_trigger_state import (
    get_upcoming_fixtures as _get_upcoming_fixtures,
)

logger = logging.getLogger(__name__)

# Match-end estimate offset from kickoff (90 min play + 15 min HT + ~stoppage).
# Single source for both post-match trigger firing AND latency observation, so
# the ``observed_publish_lag_s`` baseline matches the firing baseline.
MATCH_END_OFFSET_MIN: int = 105

# Consolidated multi-family services whose baked container ENTRYPOINT is the
# ``python -m <service>`` dispatcher (``features_service/cli/main.py``), which
# REQUIRES a leading ``--feature-family <family>`` before the family's own flags
# and errors ("--feature-family is required") without it. Single-family images
# (instruments-service, market-tick-data-service) parse ``--operation``/
# ``--mode``/... directly and must NOT receive this prefix. ``features-service``
# is the only consolidated image this (sports) scheduler dispatches — after the
# 2026-05-08 features-repo consolidation the legacy per-family
# ``features-sports-service`` image (``command: None``) was retired for
# ``features-service-sports-job`` running the multi-family dispatcher. SSOT:
# unified-trading-pm/plans/active/features_sports_service_consolidation_deploy_2026_07_15.md
_MULTI_FAMILY_DISPATCH_SERVICES: frozenset[str] = frozenset({"features-service"})

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
# Scheduler
# ---------------------------------------------------------------------------


class SportsTriggerScheduler:
    """Fixture-aware trigger scheduler for sports data pipelines.

    Reads trigger tier config from YAML, reads fixture calendar from GCS,
    and determines which CLI invocations to fire based on fixture kickoff
    times.
    """

    def __init__(
        self,
        config_path: str = "configs/sports-trigger-tiers.yaml",
        poll_interval_seconds: int = 300,
        dry_run: bool = False,
        backend: str = "local",
        workspace_root: str = "",
        periodic_state: PeriodicTierState | None = None,
        state_bucket: str | None = None,
        cloud_run_config: dict[str, str] | None = None,
        latency_recorder: LatencyObservationRecorder | None = None,
        record_latency: bool = True,
    ) -> None:
        self._config_path = config_path
        self._poll_interval = poll_interval_seconds
        self._dry_run = dry_run
        self._backend = backend
        self._workspace_root = workspace_root
        self._state = TriggerState()
        self._config = self._load_config()
        self._running = False
        self._periodic_state = (
            periodic_state if periodic_state is not None else self._build_periodic_state(state_bucket)
        )
        self._cloud_run_config: dict[str, str] = cloud_run_config or {}
        self._cloud_run_backends: dict[str, CloudRunBackend] = {}
        self._latency_recorder = (
            latency_recorder if latency_recorder is not None else self._build_latency_recorder(enabled=record_latency)
        )
        dispatch_fn = (lambda **kw: self._dispatch_local(**kw)) if self._backend == "local" else None
        self._first_success_poller = self._build_first_success_poller(state_bucket, dispatch_fn)

    def _build_first_success_poller(
        self, state_bucket: str | None, dispatch_fn: Callable[..., bool] | None
    ) -> FirstSuccessPoller:
        """GCS-backed first-success poller; falls back to in-memory-only on failure.

        Same resolution + resilience shape as ``_build_periodic_state``: reuse the
        explicit ``state_bucket`` override if given, else ``resolve_state_bucket()``
        (the shared ``deployment-scripts-<project>`` ops bucket ``PeriodicTierState``
        already writes to — a sibling JSON key, not the same one). The WHOLE
        construction (bucket resolution + storage-client creation, mirroring
        ``PeriodicTierState``'s own client resolution) is wrapped — any failure
        (creds-less context) falls back to ``bucket=None`` (in-memory-only,
        pending polls reset on restart) rather than raising out of the
        scheduler's constructor
        (sports_stats_delayed_live_capture_still_dead_post_fix_2026_07_29.md).
        """
        try:
            bucket = state_bucket or resolve_state_bucket()
            return FirstSuccessPoller(self._latency_recorder, dispatch_fn, MATCH_END_OFFSET_MIN, bucket=bucket)
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning("First-success poll state unavailable (%s) — pending polls reset on restart", exc)
            return FirstSuccessPoller(self._latency_recorder, dispatch_fn, MATCH_END_OFFSET_MIN)

    def _build_latency_recorder(self, *, enabled: bool) -> LatencyObservationRecorder | None:
        """GCS-backed lag recorder; None if the bucket can't resolve (creds-less ctx)."""
        if not enabled:
            return None
        try:
            bucket = get_bucket_name("instruments", "SPORTS")
        except Exception as exc:  # pragma: no cover - creds-less context
            logger.warning("Latency recorder: cannot resolve sports bucket (%s) — latency obs disabled", exc)
            return None
        run_tag = f"sports-scheduler-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        return LatencyObservationRecorder(bucket=bucket, run_tag=run_tag, enabled=True)

    def _build_periodic_state(self, state_bucket: str | None) -> PeriodicTierState | None:
        """GCS-backed periodic state; None on failure (creds-less ctx)."""
        try:
            bucket = state_bucket or resolve_state_bucket()
            return PeriodicTierState(bucket=bucket)
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning("Periodic-tier state unavailable (%s) — cadence will reset on restart", exc)
            return None

    def _load_config(self) -> dict[str, object]:
        """Load trigger tier configuration from YAML."""
        path = Path(self._config_path)
        if not path.exists():
            # Try relative to deployment-service root
            alt = Path(__file__).parent.parent / self._config_path
            if alt.exists():
                path = alt
            else:
                logger.error("Trigger config not found: %s", self._config_path)
                return {}

        with path.open() as f:
            config_raw = yaml.safe_load(f)  # pyright: ignore[reportAny]
            config: dict[str, object] = cast(dict[str, object], config_raw)
        logger.info("Loaded sports trigger config from %s", path)
        return config

    # ------------------------------------------------------------------
    # Fixture calendar
    # ------------------------------------------------------------------

    def get_upcoming_fixtures(self, horizon_hours: int = 48, lookback_hours: float = 2.0) -> list[FixtureInfo]:
        """Read upcoming fixtures from GCS.

        Thin instance wrapper around the module-level
        :func:`sports_trigger_state.get_upcoming_fixtures` (extracted to keep
        this module under the 900-line codex cap; the logic uses no instance
        state).
        """
        return _get_upcoming_fixtures(horizon_hours, lookback_hours)

    def _max_post_match_lookback_hours(self) -> float:
        """How far in the past a fixture's kickoff may fall and still be due
        for a post-match trigger, derived from ``configs/sports-trigger-tiers.yaml``.

        A post-match trigger's fire window closes at
        ``kickoff + MATCH_END_OFFSET_MIN + total_offset_minutes + tolerance``
        (mirrors :meth:`evaluate_post_match_triggers`). The widest of these
        across all configured post-match triggers is how far back
        :meth:`get_upcoming_fixtures` must look so the fixture is still
        visible when its trigger becomes due — computed from config, not
        hardcoded, so a future trigger with an even later offset doesn't
        silently repeat the ``sports_post_match_trigger_24h_lookback_bug``
        (a fixture-visibility cutoff of ``kickoff + 2h`` that structurally
        prevented ``stats_delayed``/``features_post_match`` from ever firing).
        Returns 2.0 (matching the default lookback) if no post-match trigger
        needs more.
        """
        post_match_raw = self._config.get("post_match", {})
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
            fire_edge_minutes = MATCH_END_OFFSET_MIN + offset_minutes + (offset_hours * 60) + tolerance_minutes
            max_minutes = max(max_minutes, fire_edge_minutes)

        if max_minutes == 0:
            return 2.0
        return max_minutes / 60.0

    # ------------------------------------------------------------------
    # Trigger evaluation
    # ------------------------------------------------------------------

    def evaluate_pre_match_triggers(
        self,
        fixtures: list[FixtureInfo],
    ) -> list[TriggerEvent]:
        """Determine which pre-match triggers should fire now."""
        now = datetime.now(UTC)
        events: list[TriggerEvent] = []

        pre_match = self._config.get("pre_match", {})
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
                if self._state.has_fired(name, fixture_id):
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
        self,
        fixtures: list[FixtureInfo],
    ) -> list[TriggerEvent]:
        """Determine which post-match triggers should fire now."""
        now = datetime.now(UTC)
        events: list[TriggerEvent] = []

        post_match = self._config.get("post_match", {})
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

                if self._state.has_fired(name, fixture_id):
                    continue

                try:
                    kickoff = datetime.fromisoformat(fixture["kickoff_utc"].replace("Z", "+00:00"))
                except ValueError:
                    continue

                # Estimate match end: kickoff + 105 min (90 + 15 HT + stoppage)
                match_end = kickoff + timedelta(minutes=MATCH_END_OFFSET_MIN)
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

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def fire_trigger(self, event: TriggerEvent) -> bool:
        """Fire a trigger by invoking the configured service CLIs.

        Dispatches to the configured backend:
        - ``local``: runs the CLI command as a subprocess via ``Popen``
        - ``cloud``: placeholder for Cloud Run / VM dispatch (not yet wired)

        Shard-level failure isolation: individual service failures are logged
        but do not prevent other services in the same trigger from running.
        """
        fixture_id = event["fixture_id"]
        trigger_name = event["trigger_name"]
        league_id = event["league_id"]

        logger.info(
            "TRIGGER [%s] fixture=%s league=%s kickoff=%s fire_at=%s services=%d",
            trigger_name,
            fixture_id,
            league_id,
            event["kickoff_utc"],
            event["fire_at_utc"],
            len(event["services"]),
        )

        if self._dry_run:
            logger.info("DRY RUN — skipping execution for %s:%s", trigger_name, fixture_id)
            self._state.mark_fired(trigger_name, fixture_id)
            return True

        kickoff_iso = str(event.get("kickoff_utc", "")).replace("Z", "+00:00")
        try:
            kickoff_dt = datetime.fromisoformat(kickoff_iso)
            if kickoff_dt.tzinfo is None:
                kickoff_dt = kickoff_dt.replace(tzinfo=UTC)
            fixture_date = kickoff_dt.astimezone(UTC).strftime("%Y-%m-%d")
        except (TypeError, ValueError):
            fixture_date = datetime.now(UTC).strftime("%Y-%m-%d")

        dispatched = self._dispatch_services(
            services=list(event["services"]),
            start_date=fixture_date,
            end_date=fixture_date,
            trigger_name=trigger_name,
            dispatch_id=fixture_id,
        )
        self._state.mark_fired(trigger_name, fixture_id)
        if dispatched > 0:
            self._record_latency_observations(event)
            self._first_success_poller.register_from_event(event, fixture_date, self._build_cli_cmd)
        return True

    def _record_latency_observations(self, event: TriggerEvent) -> None:
        """Record first-attempt source-publish-lag observations on a post-match fire.

        On the FIRST fire of a ``(trigger_name, fixture_id)`` (``mark_fired``
        dedupes) the scheduler dispatches the fetch — so this fire IS the first
        attempt. Delegates to the pure ``build_observations_for_fire``; records
        the first-ATTEMPT lag (CEILING on the true publish lag). NEVER touches
        ``available_at``. No-op if no recorder / no observable entity.
        """
        recorder = self._latency_recorder
        if recorder is None:
            return
        observations = build_observations_for_fire(
            services=[cast("dict[str, object]", s) for s in event["services"]],
            fixture_id=event["fixture_id"],
            league_id=event["league_id"],
            trigger_name=event["trigger_name"],
            kickoff_utc=event["kickoff_utc"],
            match_end_offset_min=MATCH_END_OFFSET_MIN,
        )
        if observations:
            recorder.record(observations)

    def _build_cli_cmd(
        self,
        *,
        service: str,
        operation: str,
        asset_group: str,
        start_date: str | None = None,
        end_date: str | None = None,
        extra_args: dict[str, object],
        force: bool = False,
        run_tag: str = "live",
        rolling_window: tuple[int, int, bool] | None = None,
    ) -> str:
        """Assemble the standard batch-CLI invocation string.

        Shared by per-fixture dispatch (start=end=today) and periodic-tier
        dispatch. Two mutually-exclusive shapes, matching the
        instruments-service CLI contract (``70517b2``, codex/02-data/
        sports-scheduling-and-sharding.md §4):

        - Explicit dates: ``--start-date X --end-date Y [--force]``. Used by
          per-fixture triggers and Tier-2 reference (today/today).
        - Rolling window: ``--lookback-days N --lookahead-days M [--force-
          window]``. Used by Tier-1 discovery so instruments-service owns
          the date math (single source of truth; avoids clock-drift
          between scheduler and CLI).

        Consolidated multi-family services (``_MULTI_FAMILY_DISPATCH_SERVICES``,
        i.e. ``features-service``) additionally get a leading
        ``--feature-family <asset_group.lower()>`` right after the module —
        their baked ``python -m features_service`` dispatcher requires it before
        the family's own flags. Single-family services (instruments-service,
        market-tick-data-service) are unaffected.
        """
        parts = [f"python -m {service.replace('-', '_')}"]
        if service in _MULTI_FAMILY_DISPATCH_SERVICES:
            # Consolidated features-service: the image's baked entrypoint is the
            # multi-family dispatcher, which needs the family selector BEFORE the
            # family's own flags. asset_group SPORTS -> feature-family "sports"
            # (the ``features_service/sports`` sub-package). Stays first so
            # ``_strip_python_module_prefix`` (drops only "python -m <module>")
            # preserves it as the leading arg the dispatcher parses.
            parts.append(f"--feature-family {asset_group.lower()}")
        parts += [
            f"--operation {operation}",
            "--mode batch",
            f"--asset-group {asset_group}",
        ]
        if rolling_window is not None:
            lookback, lookahead, force_window = rolling_window
            parts.append(f"--lookback-days {lookback}")
            parts.append(f"--lookahead-days {lookahead}")
            if force_window:
                parts.append("--force-window")
        else:
            parts.append(f"--start-date {start_date}")
            parts.append(f"--end-date {end_date}")
            if force:
                parts.append("--force")
        parts.append(f"--run-tag {run_tag}")
        for arg_name, arg_val in extra_args.items():
            parts.append(f"{arg_name} {arg_val}")
        return " ".join(parts)

    def _dispatch_services(
        self,
        *,
        services: list[dict[str, object]],
        start_date: str | None = None,
        end_date: str | None = None,
        trigger_name: str,
        dispatch_id: str,
        force: bool = False,
        rolling_window: tuple[int, int, bool] | None = None,
    ) -> int:
        """Dispatch a list of service configs through the active backend.

        Shared by per-fixture triggers (`fire_trigger`) and periodic tiers
        (`_check_discovery` / `_check_reference`). Accepts EITHER explicit
        start/end dates (+ ``force``) OR a ``rolling_window`` tuple of
        ``(lookback_days, lookahead_days, force_window)`` — mirrors the
        instruments-service CLI contract. Per-service failures log but do
        not raise (shard-level failure isolation). Returns the number of
        services that dispatched successfully.
        """
        dispatched = 0
        for svc_config in services:
            service = str(svc_config.get("service", ""))
            operation = str(svc_config.get("operation", ""))
            ag = str(svc_config.get("asset_group") or svc_config.get("category", "SPORTS"))
            description = str(svc_config.get("description", ""))
            extra_args_raw = svc_config.get("args", {})
            extra_args: dict[str, object] = extra_args_raw if isinstance(extra_args_raw, dict) else {}

            cmd = self._build_cli_cmd(
                service=service,
                operation=operation,
                asset_group=ag,
                start_date=start_date,
                end_date=end_date,
                extra_args=extra_args,
                force=force,
                rolling_window=rolling_window,
            )
            logger.info("  -> %s (%s)", cmd, description)

            if self._backend == "local":
                if self._dispatch_local(
                    cmd=cmd,
                    service=service,
                    trigger_name=trigger_name,
                    fixture_id=dispatch_id,
                ):
                    dispatched += 1
            elif self._backend == "cloud":
                cloud_run_job_name = str(svc_config.get("cloud_run_job_name", ""))
                if not cloud_run_job_name:
                    logger.warning(
                        "No cloud_run_job_name for %s (trigger %s:%s) — skipping cloud dispatch",
                        service,
                        trigger_name,
                        dispatch_id,
                    )
                elif not self._cloud_run_config:
                    logger.warning(
                        "cloud_run_config not set — cannot dispatch %s cloud (trigger %s:%s)",
                        service,
                        trigger_name,
                        dispatch_id,
                    )
                else:
                    cr_backend = self._get_cloud_run_backend(cloud_run_job_name)
                    if cr_backend is not None and not self._dry_run:
                        try:
                            shard_id = f"{service}-{trigger_name}-{dispatch_id}"
                            cr_backend.deploy_shard(
                                shard_id=shard_id,
                                docker_image="",
                                args=self._strip_python_module_prefix(cmd),
                                environment_variables={},
                                compute_config={},
                                labels={"trigger": trigger_name, "dispatch_id": dispatch_id},
                            )
                            dispatched += 1
                        except Exception as exc:
                            logger.warning(
                                "Cloud Run dispatch failed for %s (trigger %s:%s): %s",
                                service,
                                trigger_name,
                                dispatch_id,
                                exc,
                            )
            else:
                logger.warning(
                    "Unknown backend %s — skipping %s for trigger %s:%s",
                    self._backend,
                    service,
                    trigger_name,
                    dispatch_id,
                )
        return dispatched

    def _get_cloud_run_backend(self, job_name: str) -> CloudRunBackend | None:
        """Return a cached CloudRunBackend for the given Cloud Run job name.

        Delegates to :func:`sports_trigger_dispatch_backends.get_cloud_run_backend`
        (extracted to keep this module under the 930-line codex cap; the
        scheduler still owns the backend cache dict and config, passed
        through explicitly).
        """
        return get_cloud_run_backend(job_name, self._cloud_run_config, self._cloud_run_backends)

    @staticmethod
    def _strip_python_module_prefix(cmd: str) -> list[str]:
        """Strip the ``python -m <module>`` prefix ``_build_cli_cmd`` always emits.

        Delegates to :func:`sports_trigger_dispatch_backends.strip_python_module_prefix`.
        """
        return strip_python_module_prefix(cmd)

    def _dispatch_local(
        self,
        cmd: str,
        service: str,
        trigger_name: str,
        fixture_id: str,
    ) -> bool:
        """Dispatch a CLI command as a local subprocess.

        Delegates to :func:`sports_trigger_dispatch_backends.dispatch_local`
        (extracted to keep this module under the 930-line codex cap).
        """
        return dispatch_local(cmd, service, trigger_name, fixture_id, self._workspace_root)

    # ------------------------------------------------------------------
    # Periodic tiers — delegated to PeriodicTierDispatcher
    # ------------------------------------------------------------------

    @property
    def dry_run(self) -> bool:
        """Expose dry-run flag for the periodic dispatcher's adapter surface."""
        return self._dry_run

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
    ) -> str:
        """Adapter alias for PeriodicTierDispatcher — delegates to `_build_cli_cmd`."""
        return self._build_cli_cmd(
            service=service,
            operation=operation,
            asset_group=asset_group,
            start_date=start_date,
            end_date=end_date,
            extra_args=extra_args,
            force=force,
            rolling_window=rolling_window,
        )

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
    ) -> int:
        """Adapter alias for PeriodicTierDispatcher — delegates to `_dispatch_services`."""
        return self._dispatch_services(
            services=services,
            start_date=start_date,
            end_date=end_date,
            trigger_name=trigger_name,
            dispatch_id=dispatch_id,
            force=force,
            rolling_window=rolling_window,
        )

    def _periodic_dispatcher(self) -> PeriodicTierDispatcher:
        """Lazy-build the dispatcher so config reloads pick up fresh state."""
        return PeriodicTierDispatcher(
            config=self._config,
            state=self._periodic_state,
            adapter=self,
        )

    # Thin delegators so callers (and tests) can invoke tiers directly.
    def _check_discovery(self) -> int:
        return self._periodic_dispatcher().check_discovery()

    def _check_reference(self) -> int:
        return self._periodic_dispatcher().check_reference()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run_once(self) -> int:
        """Run a single evaluation cycle. Returns number of triggers fired."""
        fired = 0
        self._first_success_poller.poll()
        # Tier-1 discovery + Tier-2 reference — periodic, not fixture-proximate.
        dispatcher = self._periodic_dispatcher()
        fired += dispatcher.check_discovery()
        fired += dispatcher.check_reference()

        fixtures = self.get_upcoming_fixtures(horizon_hours=48)

        # Post-match triggers can be due long after the standard 2h
        # fixture-visibility cutoff (e.g. stats_delayed/features_post_match
        # fire ~25-27h after kickoff) — fetch a SEPARATE, wider-lookback
        # fixture list for them rather than widening the shared cutoff (that
        # would also inflate the pre-match/discovery scan cost). See
        # unified-trading-pm/plans/active/issues/
        # sports_post_match_trigger_24h_lookback_bug_2026_07_27.md.
        post_match_lookback_hours = self._max_post_match_lookback_hours()
        post_match_fixtures = (
            fixtures
            if post_match_lookback_hours <= 2.0
            else self.get_upcoming_fixtures(horizon_hours=48, lookback_hours=post_match_lookback_hours)
        )

        if not fixtures and not post_match_fixtures:
            logger.info("No upcoming fixtures — periodic-only cycle fired=%d", fired)
            return fired

        # Tier-3 pre-match + Tier-4 post-match — fixture-proximate.
        pre_match_events = self.evaluate_pre_match_triggers(fixtures)
        post_match_events = self.evaluate_post_match_triggers(post_match_fixtures)
        all_events = pre_match_events + post_match_events

        if not all_events:
            logger.info(
                "No fixture-proximate triggers due — %d fixtures checked, %d already fired, periodic fired=%d",
                len(fixtures),
                len(self._state.fired),
                fired,
            )
            return fired

        for event in all_events:
            if self.fire_trigger(event):
                fired += 1

        logger.info(
            "Fired %d triggers (%d pre-match, %d post-match, periodic included)",
            fired,
            len(pre_match_events),
            len(post_match_events),
        )
        return fired

    def run(self) -> None:
        """Run the trigger scheduler in a polling loop.

        Polls every ``poll_interval_seconds`` (default 5 min), evaluates
        which triggers are due, and fires them.  Designed to run as a
        long-lived process on a VM or as a Cloud Run service.
        """
        self._running = True
        logger.info(
            "Sports trigger scheduler started (poll=%ds, dry_run=%s)",
            self._poll_interval,
            self._dry_run,
        )

        while self._running:
            try:
                self.run_once()
            except Exception as exc:
                logger.exception("Trigger evaluation failed: %s", exc)

            time.sleep(self._poll_interval)

    def stop(self) -> None:
        """Stop the polling loop."""
        self._running = False
        logger.info("Sports trigger scheduler stopped")
