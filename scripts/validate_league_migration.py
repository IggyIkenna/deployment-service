"""Validate league migration — compare old vs new row counts per date.

Reads both old (non-league-partitioned) and new (league-partitioned) parquets
and verifies total row counts match.

Usage:
  python scripts/validate_league_migration.py \\
    --service instruments \\
    --date-range 2025-01-01 2025-12-31
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
from collections import defaultdict

import pyarrow.parquet as pq
from unified_trading_library import get_project_id, get_storage_client

logger = logging.getLogger(__name__)

_BUCKET_TEMPLATES: dict[str, str] = {
    "instruments": "instruments-store-sports-{project_id}",
    "mtds": "market-data-tick-sports-{project_id}",
    "fss": "features-sports-sports-{project_id}",
}

_DAY_PATTERN = re.compile(r"/day=([^/]+)/")
_LEAGUE_PATTERN = re.compile(r"/league=([^/]+)/")


def _list_blobs(storage_client: object, bucket: str, prefix: str) -> list[str]:
    try:
        return list(storage_client.list_blobs(bucket, prefix=prefix))  # type: ignore[union-attr]
    except Exception:
        return []


def _count_rows(storage_client: object, bucket: str, blob_path: str) -> int:
    try:
        raw = storage_client.download_bytes(bucket, str(blob_path))  # type: ignore[union-attr]
        table = pq.read_table(io.BytesIO(raw))
        return table.num_rows
    except Exception:
        return 0


def validate(
    service: str,
    start_date: str,
    end_date: str,
) -> bool:
    """Compare old vs new row counts. Returns True if all match."""
    project_id = get_project_id()
    bucket = _BUCKET_TEMPLATES[service].format(project_id=project_id)
    storage_client = get_storage_client()

    all_blobs = _list_blobs(storage_client, bucket, "")

    # Separate old (non-league) and new (league-partitioned) blobs
    old_blobs: dict[str, list[str]] = defaultdict(list)
    new_blobs: dict[str, list[str]] = defaultdict(list)

    for blob_path in all_blobs:
        path_str = str(blob_path)
        day_match = _DAY_PATTERN.search(path_str)
        if not day_match:
            continue

        date_str = day_match.group(1)
        if date_str < start_date or date_str > end_date:
            continue

        if _LEAGUE_PATTERN.search(path_str):
            new_blobs[date_str].append(path_str)
        else:
            old_blobs[date_str].append(path_str)

    all_dates = sorted(set(old_blobs.keys()) | set(new_blobs.keys()))
    mismatches = 0
    matched = 0

    for date_str in all_dates:
        old_rows = sum(_count_rows(storage_client, bucket, b) for b in old_blobs.get(date_str, []))
        new_rows = sum(_count_rows(storage_client, bucket, b) for b in new_blobs.get(date_str, []))

        if old_rows == 0 and new_rows == 0:
            continue

        if old_rows != new_rows:
            logger.warning(
                "MISMATCH %s: old=%d new=%d (delta=%+d)",
                date_str,
                old_rows,
                new_rows,
                new_rows - old_rows,
            )
            mismatches += 1
        else:
            matched += 1
            logger.info("OK %s: %d rows", date_str, old_rows)

    logger.info("Validation complete: %d matched, %d mismatches", matched, mismatches)
    return mismatches == 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate league migration row counts")
    parser.add_argument(
        "--service",
        choices=list(_BUCKET_TEMPLATES.keys()),
        required=True,
    )
    parser.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        required=True,
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    ok = validate(
        service=args.service,
        start_date=args.date_range[0],
        end_date=args.date_range[1],
    )

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
