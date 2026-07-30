# SCHEMA_PROVENANCE_EXEMPT: Service-internal types — not cross-repo contracts.
"""Periodic-tier cadence state + parsing helpers for sports trigger scheduler.

Extracted from ``sports_trigger_scheduler`` to keep that module under the
900-line codex cap. Two concerns live here:

  1. ``PeriodicTierState`` — GCS-backed `last_run` map for periodic tiers.
     Single JSON blob, overwrite on every persist. Chosen for parity with
     ``DeploymentsRegistry`` — keeps ops surface at one backend (no Firestore
     dependency added). SSOT: codex/02-data/sports-scheduling-and-sharding.md §8.

  2. YAML coercion / source-key resolution helpers used by the periodic
     dispatcher to parse strings out of ``configs/sports-trigger-tiers.yaml``.
"""

from __future__ import annotations

import io
import json
import logging
import math
from datetime import UTC, datetime, timedelta
from typing import Protocol, TypedDict, cast

import pandas as pd
from unified_trading_library import StorageClient, get_bucket_name, get_storage_client

from .deployment_config import DeploymentConfig

logger = logging.getLogger(__name__)

DEFAULT_STATE_KEY = "sports_scheduler_state/scheduler.json"


class FixtureInfo(TypedDict):  # CORRECT-LOCAL: scheduler-local fixture-parquet shape, never exported
    """Minimal fixture data read from GCS parquets."""

    fixture_id: str
    league_id: str
    kickoff_utc: str  # ISO 8601
    home_team: str
    away_team: str


def get_upcoming_fixtures(horizon_hours: int = 48, lookback_hours: float = 2.0) -> list[FixtureInfo]:
    """Read upcoming fixtures from GCS.

    Looks at the fixture calendar for today and the next few days,
    returning fixtures with kickoff within ``horizon_hours`` from now AND no
    more than ``lookback_hours`` in the past.

    ``lookback_hours`` defaults to 2h — the historical fixture-visibility
    cutoff every existing caller (pre-match evaluation, Tier-1/2 periodic
    dispatch) relies on; passing the default keeps the day-scan and result
    set byte-identical to before this parameter existed, so the common
    discovery/pre-match call path's GCS scan cost is unchanged. A caller that
    needs to see fixtures further in the past — e.g.
    ``sports_trigger_scheduler.evaluate_post_match_triggers``, whose fire
    window can sit >24h after kickoff — passes a larger value explicitly,
    which ALSO widens the backward day-scan so a fixture written to a prior
    day's GCS partition is still found (see
    ``sports_trigger_scheduler._max_post_match_lookback_hours``). Root cause:
    unified-trading-pm/plans/active/issues/
    sports_post_match_trigger_24h_lookback_bug_2026_07_27.md.
    """
    try:
        config = DeploymentConfig()
        storage = get_storage_client(project_id=config.project_id)

        now = datetime.now(UTC)
        fixtures: list[FixtureInfo] = []
        total_parquets_scanned = 0

        # Scan today and next 3 days for fixtures.
        # Try multiple GCS path patterns — the fixture calendar may be at
        # the legacy path, the (never-populated) pre-pipeline_mode entity
        # path, or the CURRENT canonical instruments-service writer shape
        # (confirmed live via GCS listing 2026-07-14: instruments-service's
        # sports fixtures writer emits under a ``pipeline_mode=`` segment
        # with entity name ``fixtures_schedule``, e.g.
        # ``.../day=2026-07-14/pipeline_mode=batch_api_football/
        # entity=fixtures_schedule/league=UCL/fixtures_schedule.parquet`` —
        # NEITHER prior pattern ever matches this, which silently zeroed out
        # Tier-3/4 fixture-proximate triggers indefinitely; see
        # unified-trading-pm/plans/active/
        # sports_data_sources_canonical_completion_2026_07_13.md).
        _fixture_path_patterns = [
            "sports_reference/fixtures/day={date}/",
            "sports_reference/by_date/day={date}/entity=fixtures/",
            "sports_reference/by_date/day={date}/pipeline_mode=batch_api_football/entity=fixtures_schedule/",
        ]
        # Only widen the backward scan when a caller asks for more than the
        # default 2h lookback — keeps the default call's day range (and GCS
        # scan cost) exactly as before.
        days_back = math.ceil(lookback_hours / 24) if lookback_hours > 2.0 else 0
        for day_offset in range(-days_back, 4):
            scan_date = (now + timedelta(days=day_offset)).strftime("%Y-%m-%d")
            # No explicit project_id → delegates to cloud-providers.yaml SSOT
            # → env-tiered ``instruments-store-sports-prd-{pid}`` canonical form
            # (never the legacy no-env bucket decommissioned at sports cutover).
            bucket_name = get_bucket_name("instruments", "SPORTS")

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
                    if not blob.name.endswith(".parquet"):
                        continue

                    total_parquets_scanned += 1

                    # Read parquet to get fixture details
                    try:
                        raw = storage.download_bytes(bucket=bucket_name, blob_path=blob.name)
                        df = pd.read_parquet(io.BytesIO(raw))

                        # league_id: prefer the canonical string league key carried
                        # in the blob's own ``league={KEY}`` path segment (matches
                        # the manifest-wide league_id convention, e.g. "UCL") over
                        # api_football's numeric ``af_league_id`` — only used as a
                        # last-resort fallback when the path carries no such segment
                        # (e.g. the legacy non-league-partitioned path shape).
                        _path_league_id = ""
                        for _seg in blob.name.split("/"):
                            if _seg.startswith("league="):
                                _path_league_id = _seg[len("league=") :]
                                break

                        for _, row in df.iterrows():
                            # kickoff_utc: legacy writer shape. timestamp: the
                            # current instruments-service fixtures_schedule shape
                            # (see the path-pattern comment above).
                            kickoff_str = str(row.get("kickoff_utc") or row.get("timestamp") or "")  # pyright: ignore[reportAny]
                            if not kickoff_str:
                                continue

                            try:
                                kickoff = datetime.fromisoformat(kickoff_str.replace("Z", "+00:00"))
                            except ValueError:
                                continue

                            # Only include fixtures within horizon
                            hours_until = (kickoff - now).total_seconds() / 3600
                            if -lookback_hours <= hours_until <= horizon_hours:
                                _fixture_id = row.get("fixture_id") or row.get("af_fixture_id") or ""
                                _league_id = _path_league_id or row.get("league_id") or row.get("af_league_id") or ""
                                _home_team = row.get("home_team") or row.get("af_home_name") or ""
                                _away_team = row.get("away_team") or row.get("af_away_name") or ""
                                fixtures.append(
                                    FixtureInfo(
                                        fixture_id=str(cast(object, _fixture_id)),
                                        league_id=str(cast(object, _league_id)),
                                        kickoff_utc=kickoff_str,
                                        home_team=str(cast(object, _home_team)),
                                        away_team=str(cast(object, _away_team)),
                                    )
                                )
                    except Exception as exc:
                        logger.warning("Failed to read fixture parquet %s: %s", blob.name, exc)

        # end of path_pattern loop for this scan_date

        if total_parquets_scanned == 0:
            # No fixture parquets found at all — instruments-service has not yet written
            # fixture data to GCS. Tier-3/4 triggers will remain silent until
            # instruments-service runs successfully. Check that the sports-scheduler
            # venv includes instruments_service and that IS has been dispatched.
            logger.warning(
                "Fixture calendar is empty — 0 parquets found across all scanned paths "
                "(instruments-service may not have written fixture data yet). "
                "Tier-3/4 pre/post-match triggers will not fire until fixture data is present."
            )
        else:
            logger.info(
                "Found %d upcoming fixtures within %dh horizon (scanned %d parquets)",
                len(fixtures),
                horizon_hours,
                total_parquets_scanned,
            )
        return fixtures

    except Exception as exc:
        logger.error("Failed to read fixture calendar from GCS: %s", exc)
        return []


class SchedulerStateStorage(Protocol):
    """Tiny storage shim — matches ``InMemoryStorageClient`` from unified_trading_library.deployment_registry."""

    def upload_string(self, bucket: str, key: str, body: str) -> None: ...

    def download_string(self, bucket: str, key: str) -> str: ...


class _UTLStateAdapter:
    """Adapts UTL ``StorageClient`` to the ``SchedulerStateStorage`` Protocol."""

    def __init__(self, client: StorageClient) -> None:
        self._client = client

    def upload_string(self, bucket: str, key: str, body: str) -> None:
        self._client.upload_bytes(
            bucket=bucket,
            blob_path=key,
            data=body.encode("utf-8"),
            content_type="application/json",
        )

    def download_string(self, bucket: str, key: str) -> str:
        data = self._client.download_bytes(bucket=bucket, blob_path=key)
        return data.decode("utf-8")


def default_state_storage() -> SchedulerStateStorage:
    """Default GCS-backed storage adapter — shared by any scheduler-state consumer.

    Public (no longer module-private): ``FirstSuccessPoller``
    (``sports_latency_observation.py``) also uses this to persist its
    ``_pending`` map in the same ``deployment-scripts-<project>`` bucket
    ``PeriodicTierState`` already uses (sibling JSON key, not the same one) —
    sports_stats_delayed_live_capture_still_dead_post_fix_2026_07_29.md.
    """
    return _UTLStateAdapter(get_storage_client())


def resolve_state_bucket() -> str:
    """Default scheduler-state bucket — ``deployment-scripts-<project_id>``.

    Mirrors ``DeploymentsRegistry.DEFAULT_BUCKET`` convention. Derived from
    ``DeploymentConfig.project_id`` so it tracks the active environment.
    """
    return f"deployment-scripts-{DeploymentConfig().project_id}"


class PeriodicTierState:
    """GCS-backed ``last_run`` map for periodic tiers."""

    def __init__(
        self,
        *,
        bucket: str,
        key: str = DEFAULT_STATE_KEY,
        storage: SchedulerStateStorage | None = None,
    ) -> None:
        self._bucket = bucket
        self._key = key
        self._storage: SchedulerStateStorage = storage if storage is not None else default_state_storage()
        self._last_run: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        try:
            raw = self._storage.download_string(self._bucket, self._key)
        except Exception as exc:
            # First-boot case: scheduler.json doesn't exist yet → GCS raises
            # google.cloud.storage.exceptions.InvalidResponse (404), UTL's
            # local provider raises FileNotFoundError, in-memory raises
            # KeyError. Treat ANY download failure as "no state yet" so the
            # daemon can start and write the first state file on its first
            # persist. Logging is INFO — this is the happy-path bootstrap.
            logger.info(
                "No existing scheduler state at gs://%s/%s (%s) — starting fresh",
                self._bucket,
                self._key,
                type(exc).__name__,
            )
            self._last_run = {}
            return
        try:
            data: object = cast(object, json.loads(raw))
        except json.JSONDecodeError as exc:
            logger.warning(
                "Malformed scheduler state at gs://%s/%s: %s — starting fresh",
                self._bucket,
                self._key,
                exc,
            )
            self._last_run = {}
            return
        if not isinstance(data, dict):
            self._last_run = {}
            return
        entries = data.get("last_run", {})
        if not isinstance(entries, dict):
            self._last_run = {}
            return
        self._last_run = {str(k): str(v) for k, v in entries.items()}

    def get_last_run(self, tier_name: str) -> datetime | None:
        raw = self._last_run.get(tier_name)
        if raw is None:
            return None
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None

    def set_last_run(self, tier_name: str, when: datetime, *, persist: bool = True) -> None:
        self._last_run[tier_name] = when.isoformat()
        if persist:
            self._persist()

    def _persist(self) -> None:
        body = json.dumps({"last_run": dict(self._last_run)}, sort_keys=True)
        try:
            self._storage.upload_string(self._bucket, self._key, body)
        except (OSError, ValueError) as exc:
            logger.warning("Failed to persist scheduler state to gs://%s/%s: %s", self._bucket, self._key, exc)

    def snapshot(self) -> dict[str, str]:
        """Return a copy of the internal last-run map (for tests / introspection)."""
        return dict(self._last_run)


# ---------------------------------------------------------------------------
# YAML value coercion + source-key resolution used by the periodic dispatcher
# ---------------------------------------------------------------------------


def as_int(value: object, *, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, (float, str)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    return default


def as_float(value: object, *, default: float) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    return default


def resolve_source_key(svc: dict[str, object]) -> str:
    """Pick the UAC data-source key for a reference-tier service.

    Preference order:
      1. explicit ``data_source`` field on the service config
      2. ``--sports-entity`` → adapter mapping
         (TRANSFERS→transfermarkt; XG→understat; WEATHER→open_meteo;
         others default to api_football)
      3. fallback: "api_football" (broadest denominator)
    """
    explicit = svc.get("data_source")
    if isinstance(explicit, str) and explicit:
        return explicit
    args_raw = svc.get("args", {})
    args: dict[str, object] = args_raw if isinstance(args_raw, dict) else {}
    entity = args.get("--sports-entity")
    if isinstance(entity, str):
        if entity == "TRANSFERS":
            return "transfermarkt"
        if entity == "XG":
            return "understat"
        if entity == "WEATHER":
            return "open_meteo"
    return "api_football"


__all__ = [
    "DEFAULT_STATE_KEY",
    "PeriodicTierState",
    "SchedulerStateStorage",
    "as_float",
    "as_int",
    "default_state_storage",
    "resolve_source_key",
    "resolve_state_bucket",
]
