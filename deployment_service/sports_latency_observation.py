# SCHEMA_PROVENANCE_EXEMPT: Service-internal observability record — not a cross-repo contract.
"""Empirical source-publish-lag observation recorder for the sports forward path.

**Why this exists.** The per-source p95 lag constants in
``unified-api-contracts/.../registry/source_data_latency.py`` (SFI 300 s,
api_football 1800 s, footystats 3600 s, understat 7200 s, open_meteo 3600 s)
feed ``CanonicalFixture.report_time = match_end_time + lag`` — but they are
**UNVALIDATABLE FROM BACKFILL** (every captured ``available_at`` is either the
lag arithmetic itself, i.e. ``match_end + lag``, or the backfill write
wall-clock days-to-weeks after the match). The ONLY way to learn the REAL
source-publish lag is to instrument the **forward/live path** and record, per
fixture per data_type, the **wall-clock of the FIRST successful fetch** and
difference it against that fixture's real ``match_end_time``.

**Mechanism.** The :class:`SportsTriggerScheduler` fires post-match triggers at
``match_end + offset`` and dispatches the fetch CLIs. On the FIRST fire for a
``(trigger_name, fixture_id)`` it dispatches and ``mark_fired``s — so the first
fire IS the first attempt. This recorder is invoked from ``fire_trigger`` AFTER
a successful dispatch: it computes ``observed_publish_lag_s = now_utc -
match_end`` per ``(fixture_id, data_type, source)`` and **appends** one row to a
dedicated GCS parquet under
``instruments-store-sports-<env>/_index/latency_observations/day=<D>/<vm_or_run>.parquet``.

**It NEVER touches ``available_at``** — it writes its own observation file, so
the circular ``available_at = match_end + constant`` arithmetic is left intact.
After ~1-2 weeks of forward accrual over the in-season leagues (matches land
daily), the observations are aggregated (p50/p95/max per source) and the
constants are re-pinned from REAL data.

**Source-publish lag, not dispatch lag.** The post-match scheduler tolerance is
±30 min and the trigger ``offset`` is itself the *assumed* lag, so a single
fire's ``now - match_end`` would just recover the assumed offset. To measure the
TRUE publish time the scheduler must POLL: re-attempt the fetch on a tightening
cadence from ``match_end`` until the source actually returns data, and record
the wall-clock of the first NON-EMPTY return. That ``fetched_rows`` /
``first_success`` flag is carried on each observation row so the aggregator can
filter to genuine first-success observations (``first_success=True AND
fetched_rows>0``), discarding the empties-before-publish polls.

The recorder degrades to a no-op (logs + continues) if GCS is unavailable —
shard-level failure isolation; a missing observation never blocks a fetch.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

import pandas as pd
from unified_trading_library import StorageClient, get_storage_client

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

LATENCY_OBSERVATIONS_PREFIX: str = "_index/latency_observations"
"""GCS prefix under the sports instruments-store bucket where observation
parquets land. Hive-partitioned by ``day=`` so the aggregator lists paths
rather than scanning every file. Deliberately UNDER ``_index/`` (a sibling of
``availability_index.parquet``) so it never collides with captured-data paths
and is never mistaken for a captured cell."""

# Source-publish-lag observation schema (per (fixture, data_type, source) row).
# Kept flat + string/number-only so a downstream DuckDB/pandas aggregation is
# trivial and schema-stable across the accrual window.
OBSERVATION_COLUMNS: tuple[str, ...] = (
    "fixture_id",
    "league_id",
    "data_type",
    "source",
    "match_end_utc",
    "first_fetch_utc",
    "observed_publish_lag_s",
    "fetched_rows",
    "first_success",
    "trigger_name",
    "assumed_lag_constant_s",
    "recorded_at_utc",
)


@dataclass(frozen=True)
class LatencyObservation:
    """One source-publish-lag observation for a single (fixture, data_type, source)."""

    fixture_id: str
    league_id: str
    data_type: str
    source: str
    match_end_utc: str
    first_fetch_utc: str
    observed_publish_lag_s: float
    fetched_rows: int
    first_success: bool
    trigger_name: str
    assumed_lag_constant_s: int

    def to_row(self) -> dict[str, object]:
        """Flatten to the stable parquet row shape (``OBSERVATION_COLUMNS``)."""
        return {
            "fixture_id": self.fixture_id,
            "league_id": self.league_id,
            "data_type": self.data_type,
            "source": self.source,
            "match_end_utc": self.match_end_utc,
            "first_fetch_utc": self.first_fetch_utc,
            "observed_publish_lag_s": self.observed_publish_lag_s,
            "fetched_rows": self.fetched_rows,
            "first_success": self.first_success,
            "trigger_name": self.trigger_name,
            "assumed_lag_constant_s": self.assumed_lag_constant_s,
            "recorded_at_utc": datetime.now(UTC).isoformat(),
        }


def compute_lag_seconds(match_end_utc: str, first_fetch_utc: str) -> float | None:
    """``observed_publish_lag_s = first_fetch - match_end`` in seconds.

    Returns ``None`` if either timestamp is unparseable (caller skips the
    observation rather than emitting a poisoned row). Negative lags (a fetch
    timestamp BEFORE the estimated match end — possible when ``match_end`` is the
    kickoff+105min ESTIMATE and the real match ran short) are returned as-is so
    the aggregator can see + clip them; they are not silently zeroed.
    """
    try:
        me = datetime.fromisoformat(match_end_utc.replace("Z", "+00:00"))
        ff = datetime.fromisoformat(first_fetch_utc.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if me.tzinfo is None:
        me = me.replace(tzinfo=UTC)
    if ff.tzinfo is None:
        ff = ff.replace(tzinfo=UTC)
    return (ff - me).total_seconds()


class LatencyObservationRecorder:
    """Appends source-publish-lag observations to a per-run GCS parquet.

    One recorder per scheduler process; ``run_tag`` makes the output file unique
    per VM/run (per-VM-shard isolation pattern) so concurrent schedulers never
    clobber each other's observation file. The aggregator unions all
    ``day=*/*.parquet`` under the prefix.
    """

    def __init__(
        self,
        *,
        bucket: str,
        run_tag: str,
        storage: StorageClient | None = None,
        enabled: bool = True,
    ) -> None:
        self._bucket = bucket
        self._run_tag = run_tag
        self._enabled = enabled
        self._storage = storage

    def _client(self) -> StorageClient | None:
        if not self._enabled:
            return None
        if self._storage is None:
            try:
                self._storage = get_storage_client()
            except Exception as exc:  # pragma: no cover - creds-less context
                logger.warning("LatencyObservationRecorder: no storage client (%s) — degrading to no-op", exc)
                self._enabled = False
                return None
        return self._storage

    def _blob_path(self, day: str) -> str:
        return f"{LATENCY_OBSERVATIONS_PREFIX}/day={day}/{self._run_tag}.parquet"

    def record(self, observations: Sequence[LatencyObservation]) -> int:
        """Append observations to the per-day per-run parquet. Returns rows written.

        APPEND semantics: reads the existing per-(day, run) parquet if present,
        concatenates the new rows, and re-writes. The (day, run_tag) grain keeps
        each file small (one VM's observations for one day), so the read-modify-
        write is cheap. Per shard-level failure isolation a GCS error logs +
        returns 0; it never raises into the fetch loop.
        """
        client = self._client()
        if client is None or not observations:
            return 0

        # Group by day so each observation lands in its match-day partition.
        by_day: dict[str, list[dict[str, object]]] = {}
        for obs in observations:
            day = obs.match_end_utc[:10] if obs.match_end_utc else datetime.now(UTC).strftime("%Y-%m-%d")
            by_day.setdefault(day, []).append(obs.to_row())

        total_written = 0
        for day, rows in by_day.items():
            blob_path = self._blob_path(day)
            try:
                frames: list[pd.DataFrame] = [pd.DataFrame(rows, columns=list(OBSERVATION_COLUMNS))]
                existing_count = 0
                try:
                    existing_bytes = client.download_bytes(bucket=self._bucket, blob_path=blob_path)
                    existing_df = pd.read_parquet(io.BytesIO(existing_bytes))
                    existing_count = len(existing_df)
                    # Prepend existing so the file accumulates (append semantics).
                    frames.insert(0, existing_df)
                except Exception as _probe_exc:
                    # First-write probe: GCS 404 (google NotFound, not OSError) or
                    # any read failure → no prior (day, run) file; treat as first write.
                    logger.debug("Latency obs: no prior file at %s (%s) — first write", blob_path, _probe_exc)
                    existing_count = 0

                df = pd.concat(frames, ignore_index=True)
                buf = io.BytesIO()
                df.to_parquet(buf, index=False)
                client.upload_bytes(
                    bucket=self._bucket,
                    blob_path=blob_path,
                    data=buf.getvalue(),
                    content_type="application/octet-stream",
                )
                total_written += len(rows)
                logger.info(
                    "LatencyObservationRecorder: wrote %d obs to gs://%s/%s (total in file=%d)",
                    len(rows),
                    self._bucket,
                    blob_path,
                    existing_count + len(rows),
                )
            except Exception as exc:
                logger.warning(
                    "LatencyObservationRecorder: failed to append %d obs to gs://%s/%s: %s",
                    len(rows),
                    self._bucket,
                    blob_path,
                    exc,
                )
        return total_written


# Mapping from a post-match trigger's ``--sports-entity`` arg to the
# (data_type, source, assumed_lag_constant_s) it observes. The assumed constant
# is transcribed from source_data_latency.py so the observation row carries the
# yardstick alongside the measurement (the aggregator compares observed vs
# assumed directly). Kept as a closed dict here (scheduler-internal) rather than
# importing UAC so the recorder has zero UAC coupling.
ENTITY_TO_OBSERVATION_TARGET: dict[str, tuple[str, str, int]] = {
    "FIXTURE_STATS": ("FIXTURE_STATS", "api_football", 1800),
    "FIXTURE_EVENTS": ("FIXTURE_EVENTS", "api_football", 1800),
    "PLAYER_STATS": ("PLAYER_STATS", "api_football", 1800),
    "XG": ("XG", "understat", 7200),
    "SFI_PROGRESSIVE_STATS": ("SFI_PROGRESSIVE_STATS", "sfi", 300),
}
"""Closed map: post-match ``--sports-entity`` → (data_type, source, assumed p95
constant in seconds). Only POST-MATCH data_types are observable for publish lag
(pre-match / forward-determinable entities have no ``match_end``-relative lag to
measure). Keys mirror Tier-4 ``post_match`` trigger entities in
``sports-trigger-tiers.yaml``."""


def build_observations_for_fire(
    *,
    services: Sequence[dict[str, object]],
    fixture_id: str,
    league_id: str,
    trigger_name: str,
    kickoff_utc: str,
    match_end_offset_min: int,
) -> list[LatencyObservation]:
    """Build first-attempt lag observations for one post-match trigger fire.

    Pure (no I/O): the scheduler calls this on a successful post-match dispatch,
    then hands the result to :meth:`LatencyObservationRecorder.record`. Emits ONE
    observation per service whose ``--sports-entity`` is in
    ``ENTITY_TO_OBSERVATION_TARGET`` (the calibratable post-match data_types).

    ``match_end = kickoff + match_end_offset_min`` (the same estimate the
    scheduler fires on); ``observed_publish_lag_s = now - match_end``. Records the
    first-ATTEMPT wall-clock (``fetched_rows=-1`` / ``first_success=False``
    sentinel) because the scheduler dispatches asynchronously and does not see
    the fetch row count — the aggregator treats a single attempt's lag as a
    CEILING on the true publish lag (the source HAD published by
    ``first_fetch_utc``). The polling-retry enhancement (follow-up todo) stamps
    the genuine first-success row. Returns ``[]`` on an unparseable kickoff or no
    observable entity (caller no-ops).
    """
    try:
        kickoff = datetime.fromisoformat(str(kickoff_utc).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return []
    if kickoff.tzinfo is None:
        kickoff = kickoff.replace(tzinfo=UTC)
    match_end_iso = (kickoff + timedelta(minutes=match_end_offset_min)).isoformat()
    now_iso = datetime.now(UTC).isoformat()

    out: list[LatencyObservation] = []
    for svc in services:
        args_obj = svc.get("args", {})
        entity = ""
        if isinstance(args_obj, dict):
            entity = str(cast("dict[str, object]", args_obj).get("--sports-entity", "")).upper()
        target = ENTITY_TO_OBSERVATION_TARGET.get(entity)
        if target is None:
            continue
        data_type, source, assumed = target
        lag = compute_lag_seconds(match_end_iso, now_iso)
        if lag is None:
            continue
        out.append(
            LatencyObservation(
                fixture_id=fixture_id,
                league_id=league_id,
                data_type=data_type,
                source=source,
                match_end_utc=match_end_iso,
                first_fetch_utc=now_iso,
                observed_publish_lag_s=lag,
                fetched_rows=-1,
                first_success=False,
                trigger_name=trigger_name,
                assumed_lag_constant_s=assumed,
            )
        )
    return out
