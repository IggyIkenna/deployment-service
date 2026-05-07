"""Re-partition existing sports GCS parquets by league.

Reads single-file parquets from GCS (old format: one file per date/venue),
extracts league_id from the data, and writes per-league parquets to the
new partition layout.

Actual GCS layout:
  instruments bucket:
    sports_reference/fixtures/day={date}/fixtures.parquet
    sports_reference/by_date/day={date}/entity={type}/{type}.parquet
    instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet
  MTDS bucket:
    raw_tick_data/by_date/day={date}/category=sports/venue=ODDS_API/instrument_type=/data_type=odds/ticks.parquet

New layout adds league partition:
    .../league={league_id}/original_filename.parquet

Usage:
  python scripts/migrate_sports_league_sharding.py \\
    --service instruments \\
    --date-range 2025-01-01 2025-12-31 \\
    --dry-run
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from unified_api_contracts.sports import get_league_by_api_football_id
from unified_trading_library import get_project_id, get_storage_client

logger = logging.getLogger(__name__)

# Bucket templates per service
_BUCKET_TEMPLATES: dict[str, str] = {
    "instruments": "instruments-store-sports-{project_id}",
    "mtds": "market-data-tick-sports-{project_id}",
}

# GCS path patterns per service
_INSTRUMENTS_PREFIXES = [
    # Fixtures: day-partitioned
    "sports_reference/fixtures/day={date}/",
    # Entity data: day-partitioned
    "sports_reference/by_date/day={date}/",
    # Instrument availability: day+venue partitioned
    "instrument_availability/by_date/day={date}/",
]

_MTDS_PREFIX = "raw_tick_data/by_date/day={date}/category=sports/venue=ODDS_API/"


def _extract_league_from_instrument_id(instrument_id: str) -> str:
    """Extract league from odds instrument_id.

    Format: FOOTBALL:{BK}:{MKT}:{LEAGUE}:{SEASON}:{H}-{A}::{SEL}
    Segment 3 (0-indexed) is the league.
    """
    parts = instrument_id.split(":")
    if len(parts) >= 4:
        return parts[3]
    return ""


def _resolve_af_league_id(af_id: object) -> str:
    """Resolve API Football numeric league ID to canonical name."""
    try:
        numeric_id = int(af_id)  # type: ignore[arg-type]
        league = get_league_by_api_football_id(numeric_id)
        if league:
            return league.league_id
    except (ValueError, TypeError):
        pass
    return str(af_id)


def _extract_league_fixtures(df: pd.DataFrame) -> pd.DataFrame:
    """Extract league_id from fixtures/reference DataFrame.

    Uses canonical_league_id (preferred), or resolves af_league_id/league_id
    from numeric API Football IDs to canonical names.
    """
    if "canonical_league_id" in df.columns:
        df["league_id"] = df["canonical_league_id"].astype(str)
        return df

    # If league_id exists but contains numeric AF IDs, resolve them
    if "league_id" in df.columns and df["league_id"].astype(str).str.len().sum() > 0:
        df["league_id"] = df["league_id"].apply(_resolve_af_league_id)
        return df

    if "af_league_id" in df.columns:
        df["league_id"] = df["af_league_id"].apply(_resolve_af_league_id)
        return df

    if "league" in df.columns:
        try:
            df["league_id"] = df["league"].apply(
                lambda x: str(x.get("league_id", "")) if isinstance(x, dict) else ""
            )
        except (AttributeError, TypeError):
            df["league_id"] = ""
    else:
        df["league_id"] = ""
    return df


def _extract_league_odds(df: pd.DataFrame) -> pd.DataFrame:
    """Extract league_id from odds DataFrame via instrument_id parsing."""
    if "league_id" in df.columns and df["league_id"].astype(str).str.len().sum() > 0:
        return df
    if "instrument_id" in df.columns:
        df["league_id"] = df["instrument_id"].apply(_extract_league_from_instrument_id)
    else:
        df["league_id"] = ""
    return df


def _list_blobs(
    storage_client: object,
    bucket: str,
    prefix: str,
) -> list[str]:
    """List all blob paths under a prefix."""
    try:
        blobs = storage_client.list_blobs(bucket, prefix=prefix)  # type: ignore[union-attr]
        return [str(b.name) if hasattr(b, "name") else str(b) for b in blobs]
    except Exception:
        return []


def _migrate_blob(
    storage_client: object,
    bucket: str,
    blob_path: str,
    extractor: object,
    dry_run: bool,
) -> int:
    """Migrate a single blob by splitting it into per-league files.

    Returns total rows processed.
    """
    try:
        raw_data = storage_client.download_bytes(bucket, blob_path)  # type: ignore[union-attr]
        table = pq.read_table(io.BytesIO(raw_data))
        df = table.to_pandas()

        if df.empty:
            return 0

        df = extractor(df)  # type: ignore[operator]

        total_rows = 0
        for league_id, league_df in df.groupby("league_id"):
            lid = str(league_id) if league_id else "UNKNOWN"
            if not lid or lid == "" or lid == "nan":
                lid = "UNKNOWN"

            # Build new path: insert league={lid}/ before the filename
            parts = blob_path.rsplit("/", 1)
            directory = parts[0]
            filename = parts[1] if len(parts) > 1 else blob_path
            new_path = f"{directory}/league={lid}/{filename}"

            row_count = len(league_df)
            total_rows += row_count

            if dry_run:
                logger.info(
                    "[DRY RUN] Would write %d rows to %s/%s",
                    row_count,
                    bucket,
                    new_path,
                )
            else:
                out_table = pa.Table.from_pandas(league_df)
                buf = io.BytesIO()
                pq.write_table(out_table, buf)
                storage_client.upload_bytes(  # type: ignore[union-attr]
                    bucket, new_path, buf.getvalue()
                )
                logger.info(
                    "Wrote %d rows to %s/%s",
                    row_count,
                    bucket,
                    new_path,
                )

        return total_rows
    except Exception as exc:
        logger.warning("Failed to process %s/%s: %s", bucket, blob_path, exc)
        return 0


def migrate_service(
    service: str,
    start_date: str,
    end_date: str,
    dry_run: bool = True,
) -> dict[str, int]:
    """Re-partition existing parquets by league for one service.

    Lists all blobs once per prefix (efficient), then filters by date range.
    Returns dict of {date: rows_migrated}.
    """
    import re

    project_id = get_project_id()
    bucket = _BUCKET_TEMPLATES[service].format(project_id=project_id)
    storage_client = get_storage_client()
    stats: dict[str, int] = defaultdict(int)
    day_re = re.compile(r"/day=([^/]+)/")

    extractor = _extract_league_fixtures if service == "instruments" else _extract_league_odds

    # Determine which prefixes to scan
    if service == "instruments":
        scan_prefixes = [
            "sports_reference/fixtures/",
            "sports_reference/by_date/",
            "instrument_availability/by_date/",
        ]
    else:
        scan_prefixes = ["raw_tick_data/by_date/"]

    for prefix in scan_prefixes:
        logger.info("Scanning %s/%s ...", bucket, prefix)
        all_blobs = _list_blobs(storage_client, bucket, prefix)
        logger.info("  Found %d total blobs under %s", len(all_blobs), prefix)

        # Filter to parquets in date range, not already league-partitioned
        candidates = []
        for blob_path in all_blobs:
            if not blob_path.endswith(".parquet"):
                continue
            if "/league=" in blob_path:
                continue
            m = day_re.search(blob_path)
            if not m:
                continue
            date_str = m.group(1)
            if date_str < start_date or date_str > end_date:
                continue
            candidates.append((date_str, blob_path))

        logger.info(
            "  %d candidates to migrate in date range [%s, %s]",
            len(candidates),
            start_date,
            end_date,
        )

        for i, (date_str, blob_path) in enumerate(candidates):
            if i > 0 and i % 50 == 0:
                logger.info("  Progress: %d/%d blobs processed", i, len(candidates))

            rows = _migrate_blob(storage_client, bucket, blob_path, extractor, dry_run)
            if rows > 0:
                stats[date_str] += rows

    return dict(stats)


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-partition sports parquets by league")
    parser.add_argument(
        "--service",
        choices=["instruments", "mtds"],
        required=True,
        help="Service to migrate (fss bucket does not exist yet)",
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

    stats = migrate_service(
        service=args.service,
        start_date=args.date_range[0],
        end_date=args.date_range[1],
        dry_run=args.dry_run,
    )

    total = sum(stats.values())
    logger.info("Migration complete: %d dates, %d total rows", len(stats), total)
    if not stats:
        logger.info("No data found to migrate")
        sys.exit(0)

    for date_str, count in sorted(stats.items()):
        logger.info("  %s: %d rows", date_str, count)


if __name__ == "__main__":
    main()
