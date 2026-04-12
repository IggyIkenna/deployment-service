"""Rebuild sports manifest entries with league_id from league-partitioned GCS data.

Scans league-partitioned parquets in GCS and builds manifest entries with
league_id populated and schema_version=3.  Replaces old entries for the
same (date, venue) combinations.

Actual GCS layout (after migration):
  instruments: sports_reference/fixtures/day={date}/league={id}/fixtures.parquet
               sports_reference/by_date/day={date}/entity={type}/league={id}/{type}.parquet
               instrument_availability/by_date/day={date}/venue={venue}/league={id}/instruments.parquet
  MTDS: raw_tick_data/by_date/day={date}/category=sports/venue=ODDS_API/.../league={id}/ticks.parquet

Usage:
  python scripts/rebuild_sports_manifest.py \\
    --service instruments-service \\
    --date-range 2025-01-01 2025-12-31 \\
    --dry-run
"""

from __future__ import annotations

import argparse
import io
import logging
import re
from datetime import UTC, datetime

import pyarrow.parquet as pq
from unified_trading_library import (
    ManifestWriter,
    get_project_id,
    get_storage_client,
)

logger = logging.getLogger(__name__)

_BUCKET_TEMPLATES: dict[str, str] = {
    "instruments-service": "instruments-store-sports-{project_id}",
    "market-tick-data-service": "market-data-tick-sports-{project_id}",
}

# Regex to parse partitions from blob path
_LEAGUE_PATTERN = re.compile(r"/league=([^/]+)/")
_VENUE_PATTERN = re.compile(r"/venue=([^/]+)/")
_DAY_PATTERN = re.compile(r"/day=([^/]+)/")
_ENTITY_PATTERN = re.compile(r"/entity=([^/]+)/")

# Map GCS path patterns to logical venue names for manifest
_INSTRUMENTS_VENUE_MAPS = {
    "sports_reference/fixtures/": "API_FOOTBALL_FIXTURES",
    "sports_reference/by_date/": None,  # derives from entity= partition
    "instrument_availability/by_date/": None,  # derives from venue= partition
}


def _derive_venue_from_path(path_str: str) -> str:
    """Derive logical venue name from GCS blob path."""
    if "sports_reference/fixtures/" in path_str:
        return "API_FOOTBALL_FIXTURES"

    venue_match = _VENUE_PATTERN.search(path_str)
    if venue_match:
        return venue_match.group(1)

    entity_match = _ENTITY_PATTERN.search(path_str)
    if entity_match:
        return entity_match.group(1).upper()

    return "UNKNOWN"


def rebuild_manifest(
    service: str,
    start_date: str,
    end_date: str,
    dry_run: bool = True,
) -> int:
    """Scan league-partitioned parquets and rebuild manifest entries.

    Returns count of manifest entries written.
    """
    project_id = get_project_id()
    bucket = _BUCKET_TEMPLATES[service].format(project_id=project_id)
    storage_client = get_storage_client()

    # Scan for all league-partitioned blobs
    prefixes = []
    if service == "instruments-service":
        prefixes = [
            "sports_reference/fixtures/",
            "sports_reference/by_date/",
            "instrument_availability/by_date/",
        ]
    elif service == "market-tick-data-service":
        prefixes = [
            "raw_tick_data/by_date/",
        ]

    league_blobs: list[str] = []
    for prefix in prefixes:
        blobs = _list_blobs(storage_client, bucket, prefix)
        league_blobs.extend(b for b in blobs if "/league=" in b)

    logger.info("Found %d league-partitioned blobs in %s", len(league_blobs), bucket)

    # Group by (date, venue, league_id)
    entries: dict[tuple[str, str, str], int] = {}
    for path_str in league_blobs:
        if not path_str.endswith(".parquet"):
            continue

        day_match = _DAY_PATTERN.search(path_str)
        league_match = _LEAGUE_PATTERN.search(path_str)

        if not (day_match and league_match):
            continue

        date_str = day_match.group(1)
        league_id = league_match.group(1)
        venue = _derive_venue_from_path(path_str)

        # Filter to date range
        if date_str < start_date or date_str > end_date:
            continue

        # Read row count from parquet
        try:
            raw_data = storage_client.download_bytes(bucket, path_str)  # type: ignore[union-attr]
            table = pq.read_table(io.BytesIO(raw_data))
            row_count = table.num_rows
        except Exception as exc:
            logger.warning("Failed to read %s: %s", path_str, exc)
            row_count = 0

        key = (date_str, venue, league_id)
        entries[key] = entries.get(key, 0) + row_count

    logger.info("Found %d unique (date, venue, league) entries", len(entries))

    if dry_run:
        for (date_str, venue, league_id), count in sorted(entries.items()):
            logger.info(
                "[DRY RUN] Would write manifest: date=%s venue=%s league=%s rows=%d",
                date_str,
                venue,
                league_id,
                count,
            )
        return len(entries)

    # Write manifest entries using ManifestWriter
    writer = ManifestWriter(
        service_name=service,
        catalogue_bucket=bucket,
    )

    now = datetime.now(UTC).isoformat()
    for (date_str, venue, league_id), count in sorted(entries.items()):
        writer.add(
            processing_date=now,
            row_count=count,
            venue=venue,
            league_id=league_id,
            date=date_str,
        )

    writer.flush()
    logger.info("Wrote %d manifest entries to %s", len(entries), bucket)
    return len(entries)


def _list_blobs(
    storage_client: object,
    bucket: str,
    prefix: str,
) -> list[str]:
    """List all blobs under a prefix, returning path strings."""
    try:
        blobs = storage_client.list_blobs(bucket, prefix=prefix)  # type: ignore[union-attr]
        return [b.name if hasattr(b, "name") else str(b) for b in blobs]
    except Exception:
        return []


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild sports manifest with league_id")
    parser.add_argument(
        "--service",
        choices=list(_BUCKET_TEMPLATES.keys()),
        required=True,
        help="Service to rebuild manifest for",
    )
    parser.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        required=True,
        help="Date range (YYYY-MM-DD YYYY-MM-DD)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print what would be done without writing",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    count = rebuild_manifest(
        service=args.service,
        start_date=args.date_range[0],
        end_date=args.date_range[1],
        dry_run=args.dry_run,
    )

    logger.info("Done — %d entries processed", count)


if __name__ == "__main__":
    main()
