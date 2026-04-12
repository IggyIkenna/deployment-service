"""Re-partition existing sports GCS parquets by league.

Reads single-file parquets from GCS (old format: one file per date/venue),
extracts league_id from the data, and writes per-league parquets to the
new partition layout: .../venue=X/league={id}/instruments.parquet

Supports:
  - instruments-service: fixtures (nested league.league_id struct), reference data (flat league_id)
  - market-tick-data-service: odds (FOOTBALL:{BK}:{MKT}:{LG}:... instrument_id)
  - features-sports-service: features (join with fixtures on event_id to recover league)

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
from unified_trading_library import get_project_id, get_storage_client

logger = logging.getLogger(__name__)

# Bucket templates per service
_BUCKET_TEMPLATES: dict[str, str] = {
    "instruments": "instruments-store-sports-{project_id}",
    "mtds": "market-data-tick-sports-{project_id}",
    "fss": "features-sports-sports-{project_id}",
}

# Sports venues that carry league data
_SPORTS_VENUES = {
    "API_FOOTBALL_FIXTURES",
    "API_FOOTBALL_STANDINGS",
    "API_FOOTBALL_TEAMS",
    "FOOTYSTATS_MATCH_STATS",
    "FOOTYSTATS_LEAGUE_STATS",
    "TRANSFERMARKT_PLAYERS",
    "SFI_FIXTURES",
    "SFI_MATCH_STATS",
    "UNDERSTAT_MATCH_STATS",
    "ODDS_API",
}


def _extract_league_from_instrument_id(instrument_id: str) -> str:
    """Extract league from odds instrument_id.

    Format: FOOTBALL:{BK}:{MKT}:{LG}:{SN}:{H}-{A}::{SEL}
    Segment 3 (0-indexed) is the league.
    """
    parts = instrument_id.split(":")
    if len(parts) >= 4:
        return parts[3]
    return ""


def _extract_league_fixtures(df: pd.DataFrame) -> pd.DataFrame:
    """Extract league_id from fixtures DataFrame.

    Fixtures have a nested 'league' struct with 'league_id' field,
    or a flat 'league_id' column in newer data.
    """
    if "league_id" in df.columns:
        return df
    if "league" in df.columns:
        # Nested struct — extract league_id
        try:
            df["league_id"] = df["league"].apply(
                lambda x: x.get("league_id", "") if isinstance(x, dict) else ""
            )
        except (AttributeError, TypeError):
            df["league_id"] = ""
    else:
        df["league_id"] = ""
    return df


def _extract_league_odds(df: pd.DataFrame) -> pd.DataFrame:
    """Extract league_id from odds DataFrame via instrument_id parsing."""
    if "league_id" in df.columns and df["league_id"].str.len().sum() > 0:
        return df
    if "instrument_id" in df.columns:
        df["league_id"] = df["instrument_id"].apply(_extract_league_from_instrument_id)
    else:
        df["league_id"] = ""
    return df


def _extract_league_reference(df: pd.DataFrame) -> pd.DataFrame:
    """Reference data (teams, standings) already has flat league_id."""
    if "league_id" not in df.columns:
        df["league_id"] = ""
    return df


def _list_date_blobs(
    storage_client: object,
    bucket: str,
    prefix: str,
) -> list[str]:
    """List all blobs under a prefix."""
    try:
        return list(storage_client.list_blobs(bucket, prefix=prefix))  # type: ignore[union-attr]
    except Exception:
        return []


def migrate_service(
    service: str,
    start_date: str,
    end_date: str,
    dry_run: bool = True,
) -> dict[str, int]:
    """Re-partition existing parquets by league for one service.

    Returns dict of {date: rows_migrated}.
    """
    project_id = get_project_id()
    bucket = _BUCKET_TEMPLATES[service].format(project_id=project_id)
    storage_client = get_storage_client()

    dates = pd.date_range(start_date, end_date, freq="D")
    stats: dict[str, int] = defaultdict(int)

    extractor = {
        "instruments": _extract_league_fixtures,
        "mtds": _extract_league_odds,
        "fss": _extract_league_reference,
    }[service]

    for dt in dates:
        date_str = dt.strftime("%Y-%m-%d")

        # List blobs for this date across all sports venues
        for venue in _SPORTS_VENUES:
            old_prefix = f"instrument_availability/by_date/day={date_str}/venue={venue}/"
            if service == "mtds":
                old_prefix = f"category=sports/venue={venue}/data_type=odds/day={date_str}/"
            elif service == "fss":
                old_prefix = f"sports_features/by_date/day={date_str}/"

            blobs = _list_date_blobs(storage_client, bucket, old_prefix)
            if not blobs:
                continue

            for blob_path in blobs:
                # Skip already-league-partitioned data
                if "/league=" in str(blob_path):
                    continue

                try:
                    raw_data = storage_client.download_bytes(bucket, str(blob_path))
                    table = pq.read_table(io.BytesIO(raw_data))
                    df = table.to_pandas()

                    if df.empty:
                        continue

                    # Extract league_id
                    df = extractor(df)

                    # Group by league and write per-league
                    for league_id, league_df in df.groupby("league_id"):
                        lid = str(league_id) if league_id else "UNKNOWN"
                        if not lid or lid == "":
                            lid = "UNKNOWN"

                        # Build new path with league partition
                        blob_name = str(blob_path).rsplit("/", 1)[-1]
                        parts = str(blob_path).rsplit("/", 1)[0]
                        new_path = f"{parts}/league={lid}/{blob_name}"

                        row_count = len(league_df)
                        stats[date_str] += row_count

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
                except Exception as exc:
                    logger.warning(
                        "Failed to process %s/%s: %s",
                        bucket,
                        blob_path,
                        exc,
                    )

    return dict(stats)


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-partition sports parquets by league")
    parser.add_argument(
        "--service",
        choices=["instruments", "mtds", "fss"],
        required=True,
        help="Service to migrate",
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
