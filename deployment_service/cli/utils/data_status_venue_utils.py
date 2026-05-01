"""Venue coverage utilities for data status commands."""

import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import cast as _cast

import click
import gcsfs
from unified_api_contracts import VenueMapping
from unified_api_contracts.internal import MarketCategory

from ...config_loader import ConfigLoader
from .data_status_formatters import format_venue_coverage_header, format_venue_coverage_results

# Canonical market categories for data status commands (excludes SPORTS/PREDICTION)
_DATA_MARKET_CATEGORIES: list[str] = [
    MarketCategory.CEFI.value,
    MarketCategory.TRADFI.value,
    MarketCategory.DEFI.value,
]

# Earliest date for pre-launch data; used when a category has no explicit start date configured
DEFAULT_CATEGORY_START_DATE = "2020-01-01"


def check_instruments_venue_coverage(
    start_date: datetime,
    end_date: datetime,
    asset_group: tuple[str, ...],
    output: str,
    config_dir: str,
):
    """
    Check venue coverage within instruments parquet files.

    OPTIMIZED: Uses parallel parquet column reads (venue column only) to efficiently
    verify that each date's instruments file contains data for all expected venues.

    This is a smoke test to catch API failures early - if a venue is missing, it
    indicates an issue with that venue's adapter (rate limits, API errors, etc.)
    rather than waiting for market-tick-data-handler to fail.
    """
    scan_start = time.time()
    loader = ConfigLoader(config_dir)

    # Create shared GCS filesystem instance
    gcs_fs = gcsfs.GCSFileSystem()

    # YAML config for category-level start dates and bucket config only.
    # Venue start dates come exclusively from UAC VenueMapping (SSOT).
    expected_dates = loader.load_expected_start_dates()
    instruments_config = expected_dates.get("instruments-service") or {}
    _venue_mapping = VenueMapping()

    asset_groups = list(asset_group) if asset_group else list(_DATA_MARKET_CATEGORIES)

    format_venue_coverage_header(start_date, end_date)
    click.echo(
        click.style("Scanning parquet files for venue coverage (venue column only)...", dim=True)
    )

    # Generate date list
    all_dates = []
    current = start_date
    while current <= end_date:
        all_dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    # Results: cat -> date -> {found_venues: set, expected_venues: set, missing_venues: set}
    results = defaultdict(lambda: defaultdict(dict))
    total_files = 0
    files_checked = 0
    missing_files = 0

    def check_parquet_venues(cat: str, date_str: str, gcs_path: str, fs) -> dict[str, object]:
        """Discover venues from the per-venue directory structure.

        Instruments are stored at: {bucket}/instrument_availability/by_date/
        day={date}/venue={venue}/instruments.parquet

        gcs_path is the day-level prefix (without trailing slash).
        We list venue= subdirectories to discover which venues are present.
        """
        try:
            # List venue=* subdirectories under the day prefix
            entries = fs.ls(gcs_path, detail=False)
            found_venues: set[str] = set()
            for entry in entries:
                # entry is like: bucket/instrument_availability/by_date/day=X/venue=AAVEV3-ETHEREUM
                basename = entry.rstrip("/").rsplit("/", 1)[-1]
                if basename.startswith("venue="):
                    found_venues.add(basename[len("venue=") :])
            if not found_venues:
                return {
                    "date": date_str,
                    "cat": cat,
                    "found_venues": set(),
                    "error": "file_not_found",
                }
            return {
                "date": date_str,
                "cat": cat,
                "found_venues": found_venues,
                "error": None,
            }
        except (OSError, ValueError, RuntimeError) as e:
            error_msg = str(e)
            if (
                "404" in error_msg
                or "NotFound" in error_msg
                or "FileNotFoundError" in error_msg.lower()
                or "not found" in error_msg.lower()
            ):
                return {
                    "date": date_str,
                    "cat": cat,
                    "found_venues": set(),
                    "error": "file_not_found",
                }
            return {
                "date": date_str,
                "cat": cat,
                "found_venues": set(),
                "error": error_msg,
            }

    # Build tasks for parallel execution
    tasks = []
    for cat in asset_groups:
        cat_config = instruments_config.get(cat, {})
        cat_start = cat_config.get("category_start", DEFAULT_CATEGORY_START_DATE)
        expected_venues = set(cat_config.get("venues") or {}.keys())

        for date_str in all_dates:
            # Skip dates before category start
            if date_str < cat_start:
                continue

            bucket = loader.get_bucket_name("instruments-store", cat)
            # Day-level prefix (no trailing slash) — venue= subdirs live beneath this.
            # Structure: {bucket}/instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet
            gcs_path = f"{bucket}/instrument_availability/by_date/day={date_str}"

            tasks.append((cat, date_str, gcs_path, expected_venues))
            total_files += 1

    click.echo(f"  Checking {total_files} parquet files across {len(asset_groups)} asset groups...")

    # Execute in parallel
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {
            executor.submit(check_parquet_venues, cat, date_str, gcs_path, gcs_fs): (
                cat,
                date_str,
                expected_venues,
            )
            for cat, date_str, gcs_path, expected_venues in tasks
        }

        for future in as_completed(futures):
            cat, date_str, expected_venues = futures[future]
            result = future.result()
            files_checked += 1

            if result["error"] == "file_not_found":
                missing_files += 1
                results[cat][date_str] = {
                    "found_venues": set(),
                    "expected_venues": expected_venues,
                    "missing_venues": expected_venues,
                    "file_exists": False,
                }
            elif result["error"]:
                results[cat][date_str] = {
                    "found_venues": set(),
                    "expected_venues": expected_venues,
                    "missing_venues": expected_venues,
                    "file_exists": False,
                    "error": result["error"],
                }
            else:
                found = result["found_venues"]

                # UAC VenueMapping is the SSOT for venue start dates.
                # For each found venue, check if it should exist on this date.
                expected_for_date = set()
                for venue in found:
                    uac_start = _venue_mapping.get_venue_start_date(venue)
                    if uac_start and date_str >= uac_start:
                        expected_for_date.add(venue)

                missing = expected_for_date - found
                results[cat][date_str] = {
                    "found_venues": found,
                    "expected_venues": expected_for_date,
                    "missing_venues": missing,
                    "file_exists": True,
                }

            # Progress indicator every 100 files
            if files_checked % 100 == 0:
                click.echo(f"  [{files_checked}/{total_files}] files checked...")

    scan_time = time.time() - scan_start
    click.echo(
        click.style(f"Scan complete in {scan_time:.1f}s ({files_checked} files)", fg="green")
    )
    click.echo()

    # Display results
    format_venue_coverage_results(
        _cast(dict[str, dict[str, object]], results), output, start_date, end_date
    )
