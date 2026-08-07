# Epic: batch_live_symmetry_master | Lifecycle: permanent | Delete-when: central-event-log-spine decommissioned
"""Daily cold compaction job for the central event-log warm GCS sink.

Cloud Run Job entry-point (invoked once per day by Cloud Scheduler at 2 AM UTC).

Workflow
--------
1. Identify yesterday's date (UTC).
2. For each ``(asset_group, data_type)`` shard in :data:`SINK_MATRIX`:
   a. List all warm-sink objects under the shard's warm prefix for that date
      (``live-events/warm/{asset_group}/{data_type}/``). Each object is a raw
      Pub/Sub ``CanonicalPersistEnvelope`` (JSON) written verbatim by the Cloud
      Storage subscription — despite the ``.parquet`` filename suffix, the
      subscription has no ``parquet_config``/``avro_config`` set, so the bytes
      on the wire are exactly the JSON message data, never real Parquet.
   b. Parse each envelope, extract ``payload_inline`` (JSON-encoded row or list
      of rows), and concat all rows across the shard's warm files into a single
      in-memory DataFrame — this is where the JSON->Parquet conversion actually
      happens (the warm tier itself is never Parquet).
   c. Write a daily cold parquet file to the cold prefix
      (``live-events/cold/{asset_group}/{data_type}/date={YYYY-MM-DD}/data.parquet``).
3. For REPRODUCIBLE shards: the cold-tier GCS lifecycle rule (set via terraform
   ``google_storage_bucket`` ``lifecycle_rule``) handles TTL automatically. The
   compactor logs the ``cold_ttl_days`` for operator visibility.
4. For STREAM_ONLY shards: no TTL — files are kept forever (system of record).

Skips shards with zero warm files (normal for shards not yet producing data).
Skips (with a warning, never a crash) any individual warm file that fails to
parse as a ``CanonicalPersistEnvelope`` or that carries ``payload_pointer``
instead of ``payload_inline`` (no SINK_MATRIX shard sets ``hot=False`` today,
so the pointer path is unexercised — logged, not silently dropped, so it is
visible the moment a producer starts using it).
Never deletes warm files — TTL is managed by the warm bucket lifecycle rule.

Plan: ``live_data_persistence_central_event_log_2026_06_25.md`` Plan 03.
Terraform: ``terraform/gcp/live_event_log/compaction_job.tf``.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
from datetime import UTC, date, datetime, timedelta
from typing import cast

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import AliasChoices, Field, ValidationError
from unified_api_contracts.events.persist import CanonicalPersistEnvelope, RetentionClass  # noqa: qg-deep-import
from unified_api_contracts.events.sink_matrix import SINK_MATRIX, SinkConfig  # noqa: qg-deep-import
from unified_trading_library import UnifiedCloudConfig, get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)


class CompactorConfig(UnifiedCloudConfig):
    """Job-local config for the live-event-log compactor Cloud Run Job.

    Adds two compactor-specific env var overrides on top of UnifiedCloudConfig:

    ``DRY_RUN=1``
        Skip GCS writes; log what would have been written.  Useful for smoke-testing
        the compaction loop against a live warm bucket without materialising cold files.

    ``COMPACTION_DATE=YYYY-MM-DD``
        Override the target date instead of using yesterday (UTC).  Used for
        single-date backfill: trigger the Cloud Run Job with this env var set to
        the missed date.  When empty (default), the job compacts yesterday as usual.
    """

    dry_run: bool = Field(
        default=False,
        validation_alias=AliasChoices("DRY_RUN"),
        description="Skip GCS writes (dry-run mode). Set DRY_RUN=1 to enable.",
    )

    compaction_date: str = Field(
        default="",
        validation_alias=AliasChoices("COMPACTION_DATE"),
        description=(
            "ISO-8601 date (YYYY-MM-DD) to compact instead of yesterday. "
            "Used for backfill: trigger the Cloud Run Job with COMPACTION_DATE=2026-08-01 "
            "to recompact a specific missed date. Empty = use yesterday (normal daily run)."
        ),
    )


# ---------------------------------------------------------------------------
# Bucket resolution helpers
# ---------------------------------------------------------------------------


def _warm_bucket(cfg: UnifiedCloudConfig) -> str:
    """Resolve the warm-sink GCS bucket name via the canonical resolver.

    ``events`` is a flat cross-asset_group template (``${GCP_PROJECT_ID}-events``,
    one shared bucket for every asset_group) — passing an ``asset_group=`` here
    raises (only ``cefi``/``defi``/``prediction``/``sports``/``tradfi`` are valid
    values; "warm" is not one).
    """
    del cfg
    name: str = resolve_bucket_name(cloud="gcp", kind="events")
    return name


def _cold_bucket(cfg: UnifiedCloudConfig) -> str:
    """Resolve the cold-compacted GCS bucket name via the canonical resolver.

    Same bucket as the warm sink (single ``events`` bucket, split by the
    ``live-events/{warm,cold}/`` prefix) — see :func:`_warm_bucket`.
    """
    del cfg
    name: str = resolve_bucket_name(cloud="gcp", kind="events")
    return name


# ---------------------------------------------------------------------------
# GCS path helpers
# ---------------------------------------------------------------------------


def warm_prefix(asset_group: str, data_type: str) -> str:
    """Return the warm-sink GCS object prefix for a shard.

    Layout: ``live-events/warm/{asset_group}/{data_type}/``
    """
    ag = "all" if asset_group == "*" else asset_group
    return f"live-events/warm/{ag}/{data_type}/"


def cold_object_path(asset_group: str, data_type: str, target_date: date) -> str:
    """Return the cold-tier GCS object path for a daily compacted file.

    Layout: ``live-events/cold/{asset_group}/{data_type}/date={YYYY-MM-DD}/data.parquet``
    """
    ag = "all" if asset_group == "*" else asset_group
    return f"live-events/cold/{ag}/{data_type}/date={target_date.isoformat()}/data.parquet"


# ---------------------------------------------------------------------------
# Shard compaction
# ---------------------------------------------------------------------------


def _warm_blob_matches_date(blob_name: str, prefix: str, target_date: date) -> bool:
    """True if a warm-sink object's embedded timestamp falls on ``target_date`` (UTC).

    The Pub/Sub Cloud Storage subscription's default object naming is
    ``{filename_prefix}{ISO8601_timestamp}_{uuid}{filename_suffix}`` — a flat
    listing under the prefix, NOT a ``{YYYY}/{MM}/{DD}/`` subfolder layout
    (verified live: e.g. ``live-events/warm/prediction/trades/2026-07-30T06:00:14+00:00_59fc0c.parquet``).
    So filtering by date means checking the leading ``YYYY-MM-DD`` of the
    filename remainder after the prefix, not a path-segment prefix.
    """
    remainder = blob_name[len(prefix) :] if blob_name.startswith(prefix) else blob_name
    return remainder.startswith(target_date.isoformat())


def _extract_rows(blob_name: str, raw_bytes: bytes) -> list[dict[str, object]]:
    """Parse one warm-sink object's bytes as a ``CanonicalPersistEnvelope`` and return its rows.

    Every warm-sink object is the raw Pub/Sub message the ``google_pubsub_subscription``
    ``cloud_storage_config`` wrote verbatim (no ``parquet_config``/``avro_config`` is set on
    any of the 52 subscriptions in ``warm_sink.tf``) — i.e. JSON, never Parquet, despite the
    ``.parquet`` filename suffix. ``payload_inline`` carries either a single row dict or a
    list of row dicts (the compactor is the actual JSON->Parquet conversion boundary).

    Returns ``[]`` (never raises) on a malformed envelope, invalid JSON payload, a
    payload whose JSON shape isn't a dict/list-of-dicts, or a ``payload_pointer``-only
    envelope (no SINK_MATRIX shard sets ``hot=False`` today, so this path is unexercised
    in production) — logs a warning so the gap stays visible instead of silently dropping
    data or crashing the whole shard on one bad object.
    """
    try:
        envelope = CanonicalPersistEnvelope.model_validate_json(raw_bytes)
    except ValidationError:
        logger.warning("_extract_rows: blob=%s failed CanonicalPersistEnvelope validation — skipping", blob_name)
        return []

    if envelope.payload_inline is None:
        logger.warning(
            "_extract_rows: blob=%s carries payload_pointer (%s), not payload_inline — "
            "pointer dereferencing is not yet implemented, skipping",
            blob_name,
            envelope.payload_pointer,
        )
        return []

    try:
        payload = cast(object, json.loads(envelope.payload_inline))
    except json.JSONDecodeError:
        logger.warning("_extract_rows: blob=%s payload_inline is not valid JSON — skipping", blob_name)
        return []

    if isinstance(payload, dict):
        return [cast(dict[str, object], payload)]
    if isinstance(payload, list):
        rows: list[dict[str, object]] = []
        dropped = 0
        for item in cast(list[object], payload):
            if isinstance(item, dict):
                rows.append(cast(dict[str, object], item))
            else:
                dropped += 1
        if dropped:
            logger.warning(
                "_extract_rows: blob=%s payload_inline list has %d/%d non-dict entries — dropping them",
                blob_name,
                dropped,
                dropped + len(rows),
            )
        return rows

    logger.warning(
        "_extract_rows: blob=%s payload_inline JSON is neither a dict nor a list (%s) — skipping",
        blob_name,
        type(payload).__name__,
    )
    return []


async def compact_shard(
    *,
    asset_group: str,
    data_type: str,
    sink_cfg: SinkConfig,
    target_date: date,
    warm_bucket_name: str,
    cold_bucket_name: str,
    dry_run: bool = False,
) -> bool:
    """Compact all warm-sink parquet files for one shard into a single daily cold file.

    Returns ``True`` if the shard was compacted (files found + written), ``False``
    if the shard had no warm files for ``target_date`` (skip, not an error).

    Uses UTL ``get_storage_client()`` (``list_blobs`` / ``download_bytes`` /
    ``upload_bytes``), never ``subprocess gcloud``.
    """
    prefix = warm_prefix(asset_group, data_type)

    logger.info(
        "compact_shard: shard=(%s, %s) date=%s warm_prefix=%s",
        asset_group,
        data_type,
        target_date,
        prefix,
    )

    storage_client = get_storage_client()
    warm_blob_names = [
        blob.name
        for blob in storage_client.list_blobs(warm_bucket_name, prefix=prefix)
        if _warm_blob_matches_date(blob.name, prefix, target_date)
    ]
    if not warm_blob_names:
        logger.info(
            "compact_shard: no warm files for shard=(%s, %s) date=%s — skipping",
            asset_group,
            data_type,
            target_date,
        )
        return False

    cold_path = cold_object_path(asset_group, data_type, target_date)
    retention_label = sink_cfg.retention_class.value
    ttl_note = (
        f"cold_ttl={sink_cfg.cold_ttl_days}d (REPRODUCIBLE, bucket lifecycle handles TTL)"
        if sink_cfg.retention_class == RetentionClass.REPRODUCIBLE
        else "no TTL (STREAM_ONLY — system of record, keep forever)"
    )

    logger.info(
        "compact_shard: writing cold file shard=(%s, %s) date=%s files=%d path=%s retention=%s %s",
        asset_group,
        data_type,
        target_date,
        len(warm_blob_names),
        cold_path,
        retention_label,
        ttl_note,
    )

    if dry_run:
        logger.info("compact_shard: DRY RUN — not writing cold file")
        return True

    # Streaming write: process one warm file at a time into a ParquetWriter so the
    # full row-set is never materialised in memory simultaneously. The previous
    # approach (accumulate all rows as dicts, then pd.DataFrame.from_records) OOM-killed
    # the 512Mi container on the (cefi, book_snapshot_5) shard (~1497 warm files/day).
    # Each file's rows are written as a separate row group; the established schema from
    # the first non-empty file is used for all subsequent writes (minor null-type drift
    # is resolved via cast — type-incompatible schema divergence raises explicitly).
    cold_buf = io.BytesIO()
    pq_writer: pq.ParquetWriter | None = None
    pq_schema: pa.Schema | None = None
    unusable = 0
    for name in warm_blob_names:
        rows = _extract_rows(name, storage_client.download_bytes(warm_bucket_name, name))
        if not rows:
            unusable += 1
            continue
        batch_table = pa.Table.from_pandas(pd.DataFrame.from_records(rows), preserve_index=False)
        if pq_schema is not None and batch_table.schema != pq_schema:
            batch_table = batch_table.cast(pq_schema)
        if pq_writer is None:
            pq_schema = batch_table.schema
            pq_writer = pq.ParquetWriter(cold_buf, pq_schema)
        pq_writer.write_table(batch_table)

    if pq_writer is None:
        logger.warning(
            "compact_shard: shard=(%s, %s) date=%s — all %d warm files yielded 0 usable rows, nothing to write",
            asset_group,
            data_type,
            target_date,
            len(warm_blob_names),
        )
        return False

    pq_writer.close()
    storage_client.upload_bytes(cold_bucket_name, cold_path, cold_buf.getvalue())
    if unusable:
        logger.warning(
            "compact_shard: shard=(%s, %s) date=%s — skipped %d/%d unusable warm files (see prior warnings)",
            asset_group,
            data_type,
            target_date,
            unusable,
            len(warm_blob_names),
        )
    return True


# ---------------------------------------------------------------------------
# Main compaction loop
# ---------------------------------------------------------------------------


async def run_compaction(*, target_date: date, dry_run: bool = False) -> None:
    """Run daily cold compaction over all SINK_MATRIX shards for ``target_date``.

    Called once per Cloud Run Job execution (invoked by Cloud Scheduler at 2 AM UTC).
    """
    cfg = UnifiedCloudConfig()
    warm_bucket_name = _warm_bucket(cfg)
    cold_bucket_name = _cold_bucket(cfg)

    logger.info(
        "run_compaction: START date=%s dry_run=%s warm_bucket=%s cold_bucket=%s",
        target_date,
        dry_run,
        warm_bucket_name,
        cold_bucket_name,
    )

    compacted = 0
    skipped = 0
    for (asset_group, data_type), sink_cfg in sorted(SINK_MATRIX.items()):
        ok = await compact_shard(
            asset_group=asset_group,
            data_type=data_type,
            sink_cfg=sink_cfg,
            target_date=target_date,
            warm_bucket_name=warm_bucket_name,
            cold_bucket_name=cold_bucket_name,
            dry_run=dry_run,
        )
        if ok:
            compacted += 1
        else:
            skipped += 1

    logger.info(
        "run_compaction: DONE date=%s compacted=%d skipped=%d",
        target_date,
        compacted,
        skipped,
    )


# ---------------------------------------------------------------------------
# Cloud Run Job entry-point
# ---------------------------------------------------------------------------


def main() -> None:
    """Cloud Run Job entry-point.

    Cloud Scheduler invokes the job daily at 2 AM UTC.  By default compacts
    yesterday's warm files (UTC).

    Env var overrides (see :class:`CompactorConfig`):
    - ``DRY_RUN=1`` — skip GCS writes.
    - ``COMPACTION_DATE=YYYY-MM-DD`` — compact a specific date instead of yesterday;
      used for backfill after the OOM fix (see plan issue
      ``cefi_live_event_cold_compactor_oom_and_legacy_path_check_2026_08_07.md`` todo 2).
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    cfg = CompactorConfig()

    if cfg.compaction_date:
        try:
            target_date = date.fromisoformat(cfg.compaction_date)
        except ValueError:
            raise ValueError(
                f"COMPACTION_DATE={cfg.compaction_date!r} is not a valid ISO-8601 date (YYYY-MM-DD)"
            ) from None
        logger.info("main: COMPACTION_DATE override active — compacting %s (not yesterday)", target_date)
    else:
        target_date = datetime.now(UTC).date() - timedelta(days=1)

    asyncio.run(run_compaction(target_date=target_date, dry_run=cfg.dry_run))


if __name__ == "__main__":
    main()
