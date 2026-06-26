# Epic: batch_live_symmetry_master | Lifecycle: permanent | Delete-when: central-event-log-spine decommissioned
"""Daily cold compaction job for the central event-log warm GCS sink.

Cloud Run Job entry-point (invoked once per day by Cloud Scheduler at 2 AM UTC).

Workflow
--------
1. Identify yesterday's date (UTC).
2. For each ``(asset_group, data_type)`` shard in :data:`SINK_MATRIX`:
   a. List all warm-sink 5-minute parquet files under the shard's warm prefix
      for that date (``live-events/warm/{asset_group}/{data_type}/``).
   b. Read and concat them into a single in-memory DataFrame.
   c. Write a daily cold parquet file to the cold prefix
      (``live-events/cold/{asset_group}/{data_type}/date={YYYY-MM-DD}/data.parquet``).
3. For REPRODUCIBLE shards: the cold-tier GCS lifecycle rule (set via terraform
   ``google_storage_bucket`` ``lifecycle_rule``) handles TTL automatically. The
   compactor logs the ``cold_ttl_days`` for operator visibility.
4. For STREAM_ONLY shards: no TTL — files are kept forever (system of record).

Skips shards with zero warm files (normal for shards not yet producing data).
Never deletes warm files — TTL is managed by the warm bucket lifecycle rule.

Plan: ``live_data_persistence_central_event_log_2026_06_25.md`` Plan 03.
Terraform: ``terraform/gcp/live_event_log/compaction_job.tf``.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, date, datetime, timedelta

from unified_api_contracts.events.persist import RetentionClass  # noqa: qg-deep-import
from unified_api_contracts.events.sink_matrix import SINK_MATRIX, SinkConfig  # noqa: qg-deep-import
from unified_trading_library import UnifiedCloudConfig, resolve_bucket_name

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bucket resolution helpers
# ---------------------------------------------------------------------------


def _warm_bucket(cfg: UnifiedCloudConfig) -> str:
    """Resolve the warm-sink GCS bucket name via the canonical resolver."""
    name: str = resolve_bucket_name(cloud="gcp", kind="events", asset_group="warm")
    return name


def _cold_bucket(cfg: UnifiedCloudConfig) -> str:
    """Resolve the cold-compacted GCS bucket name via the canonical resolver."""
    name: str = resolve_bucket_name(cloud="gcp", kind="events", asset_group="cold")
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
# Shard compaction (scaffold — IO wired via UTL cloud_interface)
# ---------------------------------------------------------------------------


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
    """Compact all warm-sink 5-minute parquet files for one shard into a daily cold file.

    Returns ``True`` if the shard was compacted (files found + written), ``False``
    if the shard had no warm files for ``target_date`` (skip, not an error).

    Uses UTL ``gcs_copy_object`` / ``gcs_describe_object`` (never ``subprocess gcloud``).
    """
    prefix = warm_prefix(asset_group, data_type)
    date_infix = target_date.strftime("%Y/%m/%d")
    date_prefix = f"{prefix}{date_infix}/"

    logger.info(
        "compact_shard: shard=(%s, %s) date=%s warm_prefix=%s",
        asset_group,
        data_type,
        target_date,
        date_prefix,
    )

    # --- scaffold: list warm files for target_date ---
    # Real implementation: use UTL gcs_describe_object / storage_client.list_objects
    # to enumerate all objects under date_prefix. Concat parquet bytes with pyarrow.
    # Then write the concatenated bytes to the cold path via UTL gcs_copy_object.
    #
    # Skeleton left here to be wired in Plan 04 (compactor implementation).
    warm_files: list[str] = []  # TODO(Plan 04): populate via UTL storage_client.list_objects
    if not warm_files:
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
        len(warm_files),
        cold_path,
        retention_label,
        ttl_note,
    )

    if dry_run:
        logger.info("compact_shard: DRY RUN — not writing cold file")
        return True

    # TODO(Plan 04): write concatenated parquet to cold_bucket_name/cold_path
    # via UTL gcs_copy_object or storage_client.write_bytes.
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

    Cloud Scheduler invokes the job daily at 2 AM UTC. By default compacts
    yesterday's warm files (UTC). Set ``DRY_RUN=1`` in the job env to skip writes.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    cfg = UnifiedCloudConfig()
    # UnifiedCloudConfig exposes config via typed fields.
    # dry_run reads from the typed config field if available; defaults False.
    dry_run: bool = getattr(cfg, "dry_run", False)

    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    asyncio.run(run_compaction(target_date=yesterday, dry_run=dry_run))


if __name__ == "__main__":
    main()
