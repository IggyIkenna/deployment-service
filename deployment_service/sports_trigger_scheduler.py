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

import io
import logging
import shlex
import subprocess
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict, cast

if TYPE_CHECKING:
    from .backends.cloud_run import CloudRunBackend

import pandas as pd
import yaml
from unified_trading_library import get_bucket_name, get_storage_client

from .deployment_config import DeploymentConfig
from .sports_trigger_periodic import PeriodicTierDispatcher
from .sports_trigger_state import PeriodicTierState, resolve_state_bucket

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


class FixtureInfo(TypedDict):  # CORRECT-LOCAL: scheduler-local fixture-parquet shape, never exported
    """Minimal fixture data read from GCS parquets."""

    fixture_id: str
    league_id: str
    kickoff_utc: str  # ISO 8601
    home_team: str
    away_team: str


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

    def _build_periodic_state(self, state_bucket: str | None) -> PeriodicTierState | None:
        """Construct the GCS-backed periodic state. Returns None on failure.

        Failure to construct (e.g. no GCP creds in a unit-test-like context)
        must NOT crash the scheduler — periodic tiers degrade to in-memory
        state for that process, per shard-level failure isolation.
        """
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

    def get_upcoming_fixtures(self, horizon_hours: int = 48) -> list[FixtureInfo]:
        """Read upcoming fixtures from GCS.

        Looks at the fixture calendar for today and the next few days,
        returning fixtures with kickoff within ``horizon_hours`` from now.
        """
        try:
            config = DeploymentConfig()
            storage = get_storage_client(project_id=config.project_id)

            now = datetime.now(UTC)
            fixtures: list[FixtureInfo] = []

            # Scan today and next 3 days for fixtures.
            # Try multiple GCS path patterns — the fixture calendar may be at
            # the legacy path or the new entity-partitioned path.
            _fixture_path_patterns = [
                "sports_reference/fixtures/day={date}/",
                "sports_reference/by_date/day={date}/entity=fixtures/",
            ]
            for day_offset in range(4):
                scan_date = (now + timedelta(days=day_offset)).strftime("%Y-%m-%d")
                bucket_name = get_bucket_name("instruments", "SPORTS", project_id=config.project_id)

                for path_pattern in _fixture_path_patterns:
                    prefix = path_pattern.format(date=scan_date)

                    try:
                        blobs = list(
                            storage.list_blobs(
                                bucket=bucket_name,
                                prefix=prefix,
                                max_results=100,
                            )
                        )
                    except Exception as exc:
                        logger.debug("No fixtures at %s: %s", prefix, exc)
                        continue

                    for blob in blobs:
                        if not str(blob).endswith(".parquet"):
                            continue

                        # Read parquet to get fixture details
                        try:
                            raw = storage.download_bytes(bucket=bucket_name, blob_path=str(blob))
                            df = pd.read_parquet(io.BytesIO(raw))

                            for _, row in df.iterrows():
                                kickoff_str = str(row.get("kickoff_utc", ""))  # pyright: ignore[reportAny]
                                if not kickoff_str:
                                    continue

                                try:
                                    kickoff = datetime.fromisoformat(kickoff_str.replace("Z", "+00:00"))
                                except ValueError:
                                    continue

                                # Only include fixtures within horizon
                                hours_until = (kickoff - now).total_seconds() / 3600
                                if -2 <= hours_until <= horizon_hours:
                                    fixtures.append(
                                        FixtureInfo(
                                            fixture_id=str(cast(object, row.get("fixture_id", ""))),
                                            league_id=str(cast(object, row.get("league_id", ""))),
                                            kickoff_utc=kickoff_str,
                                            home_team=str(cast(object, row.get("home_team", ""))),
                                            away_team=str(cast(object, row.get("away_team", ""))),
                                        )
                                    )
                        except Exception as exc:
                            logger.warning("Failed to read fixture parquet %s: %s", blob, exc)

            # end of path_pattern loop for this scan_date

            logger.info(
                "Found %d upcoming fixtures within %dh horizon",
                len(fixtures),
                horizon_hours,
            )
            return fixtures

        except Exception as exc:
            logger.error("Failed to read fixture calendar from GCS: %s", exc)
            return []

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

        for trigger in triggers:
            if not isinstance(trigger, dict):
                continue

            name = str(trigger.get("name", ""))
            # Post-match offsets can be minutes or hours
            offset_minutes = int(trigger.get("offset_minutes", 0))
            offset_hours = int(trigger.get("offset_hours", 0))
            total_offset_minutes = offset_minutes + (offset_hours * 60)

            for fixture in fixtures:
                fixture_id = fixture["fixture_id"]

                if self._state.has_fired(name, fixture_id):
                    continue

                try:
                    kickoff = datetime.fromisoformat(fixture["kickoff_utc"].replace("Z", "+00:00"))
                except ValueError:
                    continue

                # Estimate match end: kickoff + 105 min (90 + 15 HT + stoppage)
                match_end = kickoff + timedelta(minutes=105)
                fire_at = match_end + timedelta(minutes=total_offset_minutes)

                delta_minutes = abs((now - fire_at).total_seconds()) / 60

                if delta_minutes <= 30:  # 30 min tolerance for post-match
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

        self._dispatch_services(
            services=list(event["services"]),
            start_date=fixture_date,
            end_date=fixture_date,
            trigger_name=trigger_name,
            dispatch_id=fixture_id,
        )
        self._state.mark_fired(trigger_name, fixture_id)
        return True

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
        """
        parts = [
            f"python -m {service.replace('-', '_')}",
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
                                args=shlex.split(cmd),
                                environment_variables={},
                                compute_config={},
                                labels={"trigger": trigger_name, "dispatch_id": dispatch_id},
                            )
                            dispatched += 1
                        except Exception as exc:  # noqa: BLE001
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

        Returns None and logs a warning if cloud_run_config is missing required keys.
        Backends are cached per job_name so repeated calls share one client instance.
        """
        if job_name in self._cloud_run_backends:
            return self._cloud_run_backends[job_name]
        required = ("project_id", "region", "service_account_email")
        missing = [k for k in required if not self._cloud_run_config.get(k)]
        if missing:
            logger.warning(
                "cloud_run_config missing keys %s — cannot create backend for job %s",
                missing,
                job_name,
            )
            return None
        try:
            from .backends.cloud_run import CloudRunBackend as _CloudRunBackend

            backend = _CloudRunBackend(
                project_id=self._cloud_run_config["project_id"],
                region=self._cloud_run_config["region"],
                service_account_email=self._cloud_run_config["service_account_email"],
                job_name=job_name,
            )
            self._cloud_run_backends[job_name] = backend
            return backend
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to create CloudRunBackend for job %s: %s", job_name, exc)
            return None

    def _dispatch_local(
        self,
        cmd: str,
        service: str,
        trigger_name: str,
        fixture_id: str,
    ) -> bool:
        """Dispatch a CLI command as a local subprocess.

        Uses the service repo's .venv python if ``workspace_root`` is set,
        otherwise falls back to the system python on PATH.

        Returns True on success, False on failure. Never raises — shard-level
        failure isolation ensures one service failure does not block others.
        """
        # Resolve service repo directory and venv python
        if self._workspace_root:
            service_dir = Path(self._workspace_root) / service
            venv_python = service_dir / ".venv" / "bin" / "python"

            if not service_dir.is_dir():
                logger.warning(
                    "Service repo not found at %s — skipping %s for %s:%s",
                    service_dir,
                    service,
                    trigger_name,
                    fixture_id,
                )
                return False

            # Use repo venv python instead of generic "python -m ..."
            # shlex.split the full command, then replace python path
            raw_tokens = shlex.split(cmd)
            # raw_tokens[0] is "python", replace with venv python
            raw_tokens[0] = str(venv_python)
            cmd_tokens = raw_tokens
            cwd = str(service_dir)
        else:
            cmd_tokens = shlex.split(cmd)
            cwd = None

        logger.info(
            "Dispatching local subprocess: %s (cwd=%s)",
            " ".join(cmd_tokens),
            cwd or "<inherited>",
        )

        process: subprocess.Popen[bytes] | None = None
        try:
            process = subprocess.Popen(
                cmd_tokens,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            _, stderr_bytes = process.communicate(timeout=3600)

            if process.returncode != 0:
                stderr_text = stderr_bytes.decode("utf-8", errors="replace")[:500]
                logger.warning(
                    "Trigger dispatch failed for %s (trigger=%s fixture=%s) exit_code=%d stderr=%s",
                    service,
                    trigger_name,
                    fixture_id,
                    process.returncode,
                    stderr_text,
                )
                return False

            logger.info(
                "Trigger dispatch succeeded for %s (trigger=%s fixture=%s pid=%d)",
                service,
                trigger_name,
                fixture_id,
                process.pid,
            )
            return True

        except subprocess.TimeoutExpired:
            logger.warning(
                "Trigger dispatch timed out for %s (trigger=%s fixture=%s) — killing",
                service,
                trigger_name,
                fixture_id,
            )
            if process is not None:
                process.kill()
                process.wait(timeout=10)
            return False
        except (FileNotFoundError, PermissionError, OSError):
            logger.warning(
                "Executable not found for %s (trigger=%s fixture=%s) — cmd=%s",
                service,
                trigger_name,
                fixture_id,
                " ".join(cmd_tokens),
            )
            return False

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

        # Tier-1 discovery + Tier-2 reference — periodic, not fixture-proximate.
        dispatcher = self._periodic_dispatcher()
        fired += dispatcher.check_discovery()
        fired += dispatcher.check_reference()

        fixtures = self.get_upcoming_fixtures(horizon_hours=48)
        if not fixtures:
            logger.info("No upcoming fixtures — periodic-only cycle fired=%d", fired)
            return fired

        # Tier-3 pre-match + Tier-4 post-match — fixture-proximate.
        pre_match_events = self.evaluate_pre_match_triggers(fixtures)
        post_match_events = self.evaluate_post_match_triggers(fixtures)
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
