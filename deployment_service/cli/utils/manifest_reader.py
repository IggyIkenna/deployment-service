"""ManifestReader — thin wrapper for data-status CLI.

Provides availability-check and completion-query APIs on top of UTL's
ManifestWriter / read_availability_index functions.  This is deployment-service
CLI tooling (not a shared library type), so it lives here rather than in UTL.
"""

from __future__ import annotations

import io
import logging
from typing import cast

import pandas as pd
import pyarrow.parquet as pq
from unified_api_contracts import VenueMapping
from unified_api_contracts.internal import MarketCategory  # noqa: qg-deep-import
from unified_api_contracts.sports import (
    get_all_prediction_league_ids,
    get_league_fixture_calendar,
)
from unified_trading_library import (
    AssetGroup,
    get_bucket_name,
    get_cloud_provider,
    get_project_id,
    get_storage_client,
    read_availability_index,
    resolve_bucket_name,
)
from unified_trading_library.cloud_interface.bucket_naming import Cloud  # noqa: qg-deep-import

_ALL_CATEGORIES = [str(c) for c in MarketCategory]

logger = logging.getLogger(__name__)

# Global pipeline start date — nothing before this is counted as "expected".
# The trading system pipeline did not exist before this date; any data gaps
# prior to it are not actionable.
_PIPELINE_START_DATE = "2019-01-01"

# ── Bucket resolution: uses same templates as catalog.py SERVICE_GCS_CONFIGS ──
# Maps service → bucket template. Templates use {asset_group_lower} and {project_id}.
# Services without {asset_group_lower} use a shared bucket (no per-category split).
#
# NOTE (A11, 2026-06-02): services whose buckets have a canonical
# `cloud-providers.yaml` kind are resolved via `resolve_bucket_name()` instead of
# these legacy templates (see `_SERVICE_TO_CANONICAL_KIND` below) so data-status
# survives migrations that delete the legacy non-env-tiered buckets. The remaining
# template strings are the still-pre-canonical services (kept verbatim until their
# own canonicalisation sweep).
BUCKET_TEMPLATES: dict[str, str] = {
    "instruments-service": "instruments-store-{asset_group_lower}-{project_id}",
    "corporate-actions": "instruments-store-{asset_group_lower}-{project_id}",
    "market-tick-data-service": "market-data-tick-{asset_group_lower}-{project_id}",
    "market-data-processing-service": "market-data-tick-{asset_group_lower}-{project_id}",
    "features-delta-one-service": "features-delta-one-{asset_group_lower}-{project_id}",
    "features-volatility-service": "features-volatility-{asset_group_lower}-{project_id}",
    "features-onchain-service": "features-onchain-{project_id}",
    "features-calendar-service": "features-calendar-{project_id}",
    "features-sports-service": "features-sports-{asset_group_lower}-{project_id}",
    "features-multi-timeframe-service": "features-multi-timeframe-{asset_group_lower}-{project_id}",
    "features-cross-instrument-service": "features-cross-instrument-{asset_group_lower}-{project_id}",
    "features-commodity-service": "features-commodity-{asset_group_lower}-{project_id}",
    # CORRECT-LOCAL — legacy local template dict; canonical SSOT is
    # `cloud-providers.yaml` kind="ml-models-store"/"ml-predictions-store". This
    # dict is consumed only by deployment-service CLI tooling for ad-hoc manifest
    # reads and consolidates via `resolve_bucket_name()` in a follow-up sweep.
    # Consolidated from ml-training-service + ml-inference-service (2026-05-20).
    "ml-service": "ml-models-store-{project_id}",
    "strategy-service": "strategy-store-{project_id}",
    "execution-service": "execution-store-{domain}-{project_id}",
    # risk-and-exposure-service + pnl-attribution-service removed 2026-05-21:
    # consolidated into strategy-service (strategy_repo_consolidation_2026_05_19.md)
    "alerting-service": "alerting-service-{project_id}",
}

# Services that use a shared bucket (no category dimension in bucket name)
_SHARED_BUCKET_SERVICES = {
    "features-onchain-service",
    "features-calendar-service",
    "ml-service",
    "strategy-service",
    # risk-and-exposure-service + pnl-attribution-service removed (consolidated into strategy-service)
    "alerting-service",
}

# Services whose primary bucket has a canonical `cloud-providers.yaml` kind.
# Resolved via `resolve_bucket_name(cloud=..., kind=..., asset_group=...)` so the
# env-tiered canonical name (e.g. market-data-tick-defi-${DEPLOYMENT_ENV_SHORT}-{pid})
# is always used and the reader survives legacy-bucket deletion (A11, 2026-06-02).
_SERVICE_TO_CANONICAL_KIND: dict[str, str] = {
    "market-tick-data-service": "market-data",
    "market-data-processing-service": "market-data",
}

# MTDS DeFi operations write to additional per-type buckets beyond the main
# category bucket. These are scanned alongside the primary bucket when querying
# MTDS defi data. Each entry is a canonical `cloud-providers.yaml` kind resolved
# via `resolve_bucket_name()` (A11, 2026-06-02) — never a hardcoded f-string —
# so they pick up the env tier and survive migrations that delete legacy buckets.
# evm-defi/solana-defi/lending-indices/liquidations/dex-swaps/oracle-prices/gas-fees REMOVED
# (GCS bucket-estate cleanup plan dated 2026-07-10 + 2026-07-12 follow-up in
# unified-trading-pm/plans/active/) — confirmed zero writers for these cloud-providers.yaml
# kinds (real DeFi data lands in the shared market-data-tick-defi bucket, resolved separately
# as the primary bucket above), and the dedicated buckets were empty/migrated and deleted.
# dex-pools/lst-rates/perp-funding REMOVED 2026-07-13
# (defi_dedicated_bucket_shared_migration_2026_07_13) — every reader/scanner migrated to the
# shared bucket, all (venue, data_type, day) shards backfilled there, kinds retired from
# cloud-providers.yaml.
# Left in place unguarded here they'd raise BucketNamingError on every data-status
# resolve_all_buckets() call for this service/asset_group.
_EXTRA_BUCKET_KINDS: dict[str, dict[str, list[str]]] = {
    "market-tick-data-service": {
        "defi": [
            "eigenlayer-rewards",
        ],
    },
}

# Venue aliases — instruments-service sometimes writes bare exchange names
# instead of the canonical EXCHANGE-PRODUCT format.  Fold them at read time.
_VENUE_ALIASES: dict[str, str] = {
    "OKX": "OKX-SPOT",
    "COINBASE": "COINBASE-SPOT",
}

# Sub-dimension key used in manifest venue column per service
_SUB_DIMENSION_KEY: dict[str, str] = {
    "instruments-service": "venue",
    "market-tick-data-service": "data_type",
    "market-data-processing-service": "data_type",
    "features-delta-one-service": "feature_group",
    "features-volatility-service": "feature_group",
    "features-onchain-service": "feature_group",
    "features-calendar-service": "feature_group",
    "features-sports-service": "feature_group",
    "features-multi-timeframe-service": "feature_group",
    "features-cross-instrument-service": "feature_group",
    "features-commodity-service": "feature_group",
    "execution-service": "domain",
    "strategy-service": "strategy_id",
    "ml-service": "model_id",
    # risk-and-exposure-service + pnl-attribution-service removed 2026-05-21 (consolidated into strategy-service)
    "alerting-service": "alert_type",
}


class ManifestReader:
    """Read-side companion to UTL ManifestWriter.

    Used by the ``data-status`` CLI command to query data-availability indices
    stored as parquet in cloud storage (GCS/S3) category buckets.
    """

    def __init__(self) -> None:
        self._available: bool | None = None
        self._project_id: str | None = None
        self._venue_mapping = VenueMapping()

    def _get_project_id(self) -> str:
        """Resolve cloud project ID (GCP project or AWS account)."""
        if self._project_id is None:
            try:
                self._project_id = get_project_id()
            except Exception:
                self._project_id = "unknown"
        return self._project_id

    # ------------------------------------------------------------------
    # Availability probe
    # ------------------------------------------------------------------

    def is_available(self) -> bool:
        """Return True if we can read at least one availability index."""
        if self._available is not None:
            return self._available
        try:
            # Probe with a real bucket — instruments-store-cefi is the most
            # common and always exists.  An empty string fails silently on
            # some cloud backends, giving a false negative.
            # No explicit project_id → delegates to cloud-providers.yaml SSOT →
            # env-tiered ``instruments-store-cefi-prd-{pid}`` canonical probe bucket
            # (never the legacy no-env form decommissioned at the cefi cutover).
            probe_bucket = get_bucket_name("instruments", "CEFI")
            read_availability_index(probe_bucket)
            self._available = True
        except Exception:
            self._available = False
        return self._available

    # ------------------------------------------------------------------
    # Completion query
    # ------------------------------------------------------------------

    def get_completion(
        self,
        *,
        service: str,
        asset_group: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """Query overall completion for *service* in *asset_group* between dates.

        Returns a dict with at least ``overall_completion`` (float 0-100) and
        ``asset_group``.  On failure the dict contains ``error``.
        """
        try:
            # Clamp start to pipeline start — no data expected before this
            effective_start_date = max(start_date, _PIPELINE_START_DATE)
            all_buckets = self._resolve_all_buckets(service, asset_group)
            frames: list[pd.DataFrame] = []
            for bkt in all_buckets:
                try:
                    idx = read_availability_index(bkt)
                    if not idx.empty:
                        frames.append(idx)
                except Exception:
                    logger.debug("No manifest index in %s — treating as empty", bkt)
            index = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            if index.empty:
                return {"asset_group": asset_group, "overall_completion": 0, "dates": []}

            mask = (index["date"] >= effective_start_date) & (index["date"] <= end_date)
            if "service_name" in index.columns:
                mask = mask & (index["service_name"] == service)
            filtered = index.loc[mask]

            if filtered.empty:
                return {"asset_group": asset_group, "overall_completion": 0, "dates": []}

            dates_present = filtered["date"].nunique()
            total_days = max(
                1,
                (pd.Timestamp(end_date) - pd.Timestamp(effective_start_date)).days + 1,
            )
            completion = round(dates_present / total_days * 100, 2)

            return {
                "asset_group": asset_group,
                "overall_completion": completion,
                "dates_present": dates_present,
                "total_days": total_days,
                "venues": sorted(filtered["venue"].unique().tolist()) if "venue" in filtered.columns else [],
            }
        except Exception as exc:
            logger.debug("ManifestReader.get_completion failed: %s", exc)
            return {"error": str(exc), "asset_group": asset_group}

    def get_manifest_status(
        self,
        *,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None = None,
    ) -> dict[str, object]:
        """Read manifest indices and return structured status for all asset groups.

        This is the fast-path: reads parquet indices instead of listing blobs.
        Works with both GCS and S3 (cloud-agnostic via UCI StorageClient).

        Returns a dict matching the turbo response shape so the UI can consume
        it identically.
        """
        result_categories: dict[str, dict[str, object]] = {}
        total_found = 0
        total_expected = 0
        sub_dim_key = _SUB_DIMENSION_KEY.get(service)

        # For shared-bucket services, query once with category=None
        cat_list = [None] if service in _SHARED_BUCKET_SERVICES else asset_groups or _ALL_CATEGORIES

        for cat in cat_list:
            cat_label = cat or "ALL"
            all_buckets = self._resolve_all_buckets(service, cat or "")
            frames: list[pd.DataFrame] = []
            primary_bucket = all_buckets[0] if all_buckets else ""
            for bkt in all_buckets:
                try:
                    idx = read_availability_index(bkt)
                    if not idx.empty:
                        frames.append(idx)
                except Exception:
                    logger.debug("No manifest index in %s — treating as empty", bkt)
            index = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

            if not frames:
                result_categories[cat_label] = {
                    "asset_group": cat_label,
                    "bucket": primary_bucket,
                    "dates_expected": 0,
                    "dates_found": 0,
                    "dates_missing": 0,
                    "completion_pct": 0,
                    "missing_dates": [],
                }
                continue

            # Normalise venue aliases (e.g. bare "OKX" → "OKX-SPOT")
            if not index.empty and "venue" in index.columns:
                index["venue"] = index["venue"].replace(_VENUE_ALIASES)

            if index.empty:
                result_categories[cat_label] = {
                    "asset_group": cat_label,
                    "bucket": primary_bucket,
                    "dates_expected": 0,
                    "dates_found": 0,
                    "dates_missing": 0,
                    "completion_pct": 0,
                    "missing_dates": [],
                }
                continue

            # Clamp start to pipeline start — no data expected before this
            clamped_start = max(start_date, _PIPELINE_START_DATE)

            # Filter by date range and service name
            mask = (index["date"] >= clamped_start) & (index["date"] <= end_date)
            if "service_name" in index.columns:
                mask = mask & (index["service_name"] == service)
            filtered = index.loc[mask]

            days_total = max(1, (pd.Timestamp(end_date) - pd.Timestamp(clamped_start)).days + 1)
            dates_found = int(filtered["date"].nunique())

            # Build sub-dimension breakdown (venue/feature_group/data_type)
            # Use venue start dates from UAC to compute realistic expected days
            sub_dims: dict[str, dict[str, object]] = {}
            venue_weighted_expected = 0
            venue_weighted_found = 0
            if sub_dim_key and "venue" in filtered.columns:
                for venue_val_raw in sorted(filtered["venue"].unique()):  # pyright: ignore[reportAny]
                    venue_val_typed: object = cast(object, venue_val_raw)
                    venue_str: str = str(venue_val_typed)
                    v_mask = filtered["venue"] == venue_str
                    v_dates = int(filtered.loc[v_mask, "date"].nunique())

                    # Resolve venue-specific expected days using launch date.
                    # For compound keys like "BINANCE-FUTURES:trades", extract
                    # the venue name (first segment before ':'). Use
                    # ``get_instrument_discovery_start`` so HYPERLIQUID-style
                    # discovery-API gaps clip the denominator (matches the
                    # orchestrator's ``is_venue_available`` gate).
                    split_result = venue_str.split(":")[0] if ":" in venue_str else venue_str
                    base_venue: str = str(split_result)
                    venue_start = self._venue_mapping.get_instrument_discovery_start(base_venue)
                    effective_start = max(clamped_start, venue_start) if venue_start else clamped_start
                    v_expected = max(
                        1,
                        len(
                            self._venue_mapping.get_expected_trading_dates(
                                base_venue,
                                effective_start,
                                end_date,
                            )
                        ),
                    )

                    venue_weighted_expected += v_expected
                    venue_weighted_found += v_dates

                    # Per-venue date lists — use venue-specific trading schedule
                    # (e.g. crypto=24/7, tradfi=weekdays-minus-holidays)
                    v_found_set = set(filtered.loc[v_mask, "date"].unique())
                    v_expected_dates = self._venue_mapping.get_expected_trading_dates(
                        base_venue,
                        effective_start,
                        end_date,
                    )
                    v_missing = sorted(d for d in v_expected_dates if d not in v_found_set)
                    v_found_list = sorted(v_found_set)

                    # Truncation + tail for long lists (same pattern as
                    # category-level date lists)
                    max_list = 50
                    tail = 5
                    v_found_truncated = len(v_found_list) > max_list
                    v_missing_truncated = len(v_missing) > max_list

                    venue_entry: dict[str, object] = {
                        "dates_found": v_dates,
                        "dates_expected": v_expected,
                        "dates_expected_venue": v_expected,
                        "completion_pct": round(v_dates / v_expected * 100, 2),
                        "venue_start_date": venue_start,
                        # Available dates (green dropdown)
                        "dates_found_count": len(v_found_list),
                        "dates_found_list": v_found_list[:max_list],
                        "dates_found_truncated": v_found_truncated,
                        "dates_found_list_tail": v_found_list[-tail:] if v_found_truncated else None,
                        # Missing dates (red dropdown)
                        "dates_missing": max(0, v_expected - v_dates),
                        "dates_missing_count": len(v_missing),
                        "dates_missing_list": v_missing[:max_list],
                        "dates_missing_truncated": v_missing_truncated,
                        "dates_missing_list_tail": v_missing[-tail:] if v_missing_truncated else None,
                        "missing_dates": v_missing[:max_list],
                    }

                    # League sub-breakdown: when league_id column has non-empty
                    # values, build per-league stats nested under this venue.
                    if "league_id" in filtered.columns and filtered.loc[v_mask, "league_id"].str.len().sum() > 0:
                        venue_entry["leagues"] = self._build_league_breakdown(
                            filtered.loc[v_mask],
                            clamped_start,
                            end_date,
                        )

                    sub_dims[venue_str] = venue_entry

            # Find missing dates — clamp to earliest venue launch so pre-protocol
            # dates are not flagged as gaps (e.g. DeFi before Uniswap V2 launch).
            if sub_dims:
                earliest_venue = min(
                    (sd["venue_start_date"] for sd in sub_dims.values() if sd.get("venue_start_date")),
                    default=clamped_start,
                )
                cat_effective_start = max(clamped_start, str(earliest_venue))
            else:
                cat_effective_start = clamped_start

            # Use the most restrictive schedule among venues in this category
            # to compute category-level missing dates (weekdays for tradfi, all for crypto)
            _cat_schedule = "24_7"
            if sub_dims:
                schedules = {
                    self._venue_mapping.get_venue_schedule(v.split(":")[0] if ":" in v else v) for v in sub_dims
                }
                if schedules == {"weekdays"} or schedules == {"weekdays", "weekdays_minus_cme"}:
                    _cat_schedule = "weekdays"
            if _cat_schedule == "weekdays":
                # Use a representative venue to get expected dates
                _rep_venue = next(
                    (v.split(":")[0] if ":" in v else v for v in sub_dims),
                    "",
                )
                _cat_expected = self._venue_mapping.get_expected_trading_dates(
                    _rep_venue,
                    cat_effective_start,
                    end_date,
                )
            else:
                _cat_expected = [d.strftime("%Y-%m-%d") for d in pd.date_range(cat_effective_start, end_date, freq="D")]
            found_dates = set(filtered["date"].unique())
            missing_dates = sorted(d for d in _cat_expected if d not in found_dates)

            # Found dates
            found_dates_list = sorted(found_dates)

            # When venue start dates are available, use venue-weighted totals
            # so the category completion % reflects realistic expectations.
            if venue_weighted_expected > 0:
                cat_expected = venue_weighted_expected
                cat_found = venue_weighted_found
            else:
                cat_expected = days_total
                cat_found = dates_found
            cat_missing = max(0, cat_expected - cat_found)
            cat_completion = round(cat_found / cat_expected * 100, 2) if cat_expected > 0 else 0

            cat_result: dict[str, object] = {
                "asset_group": cat_label,
                "bucket": primary_bucket,
                "dates_expected": cat_expected,
                "dates_found": cat_found,
                "dates_missing": cat_missing,
                "completion_pct": cat_completion,
                "missing_dates": missing_dates[:50],
                "dates_found_list": found_dates_list[:50],
                "source": "manifest",
            }
            if sub_dims:
                # Use the appropriate key name for the sub-dimension
                sub_dim_response_key: dict[str, str] = {
                    "venue": "venues",
                    "data_type": "data_types",
                    "feature_group": "feature_groups",
                    "league_id": "leagues",
                    "strategy_id": "strategies",
                    "model_id": "models",
                    "mode": "modes",
                    "domain": "domains",
                    "client_id": "clients",
                    "alert_type": "alert_types",
                }
                response_key = sub_dim_response_key.get(sub_dim_key or "", "venues")
                cat_result[response_key] = sub_dims

                # Override display label for categories where the manifest
                # "venue" column doesn't represent trading venues.
                category_sub_dim_label: dict[str, str] = {
                    "SPORTS": "Data Sources",
                    "PREDICTIONS": "Market Category Shards",
                }
                if service == "instruments-service" and cat_label in category_sub_dim_label:
                    cat_result["sub_dimension_label"] = category_sub_dim_label[cat_label]

            result_categories[cat_label] = cat_result
            total_found += cat_found
            total_expected += cat_expected

        overall_pct = round(total_found / total_expected * 100, 2) if total_expected > 0 else 0

        effective_start = max(start_date, _PIPELINE_START_DATE)
        total_days = max(1, (pd.Timestamp(end_date) - pd.Timestamp(effective_start)).days + 1)

        return {
            "service": service,
            "date_range": {
                "start": start_date,
                "end": end_date,
                "days": total_days,
                "pipeline_start": _PIPELINE_START_DATE,
            },
            "mode": "manifest",
            "source": "manifest",
            "sub_dimension": sub_dim_key,
            "overall_completion_pct": overall_pct,
            "overall_dates_found": total_found,
            "overall_dates_expected": total_expected,
            "total_missing": max(0, total_expected - total_found),
            "asset_groups": result_categories,
        }

    # ------------------------------------------------------------------
    # Venue detail (drill-down)
    # ------------------------------------------------------------------

    def get_venue_detail(
        self,
        *,
        service: str,
        asset_group: str,
        venue: str,
        date: str | None = None,
        instrument_offset: int = 0,
        instrument_limit: int = 200,
    ) -> dict[str, object]:
        """Read a single venue parquet and return instrument_type breakdown.

        If *date* is None, uses the most recent available date for the venue.
        Returns counts per instrument_type and sample instruments.
        """
        try:
            bucket_name = self._resolve_bucket(service, asset_group)
            storage_client = get_storage_client()

            # Find the latest date with data for this venue
            if date:
                target_date = date
            else:
                # Scan for latest date with this venue
                index = read_availability_index(bucket_name)
                if not index.empty and "venue" in index.columns:
                    index["venue"] = index["venue"].replace(_VENUE_ALIASES)
                    v_mask = index["venue"] == venue
                    v_dates = index.loc[v_mask, "date"]
                    if not v_dates.empty:
                        target_date = str(cast(object, v_dates.max()))
                    else:
                        return {"error": f"No data for venue {venue}", "venue": venue}
                else:
                    return {"error": "Empty index", "venue": venue}

            blob_path = f"instrument_availability/by_date/day={target_date}/venue={venue}/instruments.parquet"
            try:
                raw_data = storage_client.download_bytes(bucket_name, blob_path)
            except (FileNotFoundError, OSError):
                return {"error": f"No parquet at {blob_path}", "venue": venue}

            table = pq.read_table(io.BytesIO(raw_data))
            df = table.to_pandas()

            result: dict[str, object] = {
                "venue": venue,
                "asset_group": asset_group,
                "date": target_date,
                "total_instruments": len(df),
                "columns": list(df.columns),
            }

            if "instrument_type" in df.columns:
                type_counts = df["instrument_type"].value_counts().to_dict()
                result["instrument_types"] = {str(k): int(v) for k, v in type_counts.items()}

            if "status" in df.columns:
                status_counts = df["status"].value_counts().to_dict()
                result["statuses"] = {str(k): int(v) for k, v in status_counts.items()}

            # Paginated instruments sample
            result["total_instruments_unfiltered"] = len(df)
            page_df = df.iloc[instrument_offset : instrument_offset + instrument_limit]
            top: list[dict[str, str]] = []
            key_col = "instrument_key" if "instrument_key" in df.columns else "raw_symbol"
            for _, row in page_df.iterrows():
                top.append(
                    {
                        "key": str(row.get(key_col, "")),
                        "type": str(row.get("instrument_type", "")),
                        "base": str(row.get("base_asset", "")),
                        "quote": str(row.get("quote_asset", "")),
                    }
                )
            result["top_instruments"] = top

            return result
        except Exception as exc:
            logger.debug("ManifestReader.get_venue_detail failed: %s", exc)
            return {"error": str(exc), "venue": venue}

    # ------------------------------------------------------------------
    # Coverage summary (manifest totals + latest-day instrument counts)
    # ------------------------------------------------------------------

    def get_coverage_summary(
        self,
        *,
        service: str,
        asset_groups: list[str] | None = None,
    ) -> dict[str, object]:
        """Return instrument coverage summary for the deployment UI.

        For each asset group:
        - Manifest totals: total shards, instrument-rows, unique dates, unique venues
        - Latest-day unique instrument counts by instrument_type (from parquet files)

        This is fast: reads 4 small manifest parquets + latest-day parquets only.
        """
        cat_list = asset_groups or _ALL_CATEGORIES
        result_categories: dict[str, dict[str, object]] = {}
        grand_shards = 0
        grand_rows = 0
        grand_dates = 0
        grand_latest_instruments = 0

        for cat in cat_list:
            # Read primary bucket + any extra buckets (e.g. MTDS defi sub-buckets)
            all_buckets = self._resolve_all_buckets(service, cat)
            frames: list[pd.DataFrame] = []
            for bkt in all_buckets:
                try:
                    idx = read_availability_index(bkt)
                    if not idx.empty:
                        frames.append(idx)
                except Exception:
                    logger.debug("Extra bucket %s not found or empty", bkt)
            index = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

            if index.empty:
                result_categories[cat] = {
                    "total_shards": 0,
                    "total_instrument_rows": 0,
                    "unique_dates": 0,
                    "unique_venues": 0,
                    "date_range": None,
                    "latest_day": None,
                    "latest_day_instruments": {},
                    "latest_day_total": 0,
                }
                continue

            # Normalise venue aliases
            if "venue" in index.columns:
                index["venue"] = index["venue"].replace(_VENUE_ALIASES)

            total_shards = len(index)
            total_rows = int(cast(float, index["instrument_count"].sum())) if "instrument_count" in index.columns else 0
            unique_dates = int(index["date"].nunique())
            unique_venues = int(index["venue"].nunique()) if "venue" in index.columns else 0
            date_min = str(cast(object, index["date"].min()))
            date_max = str(cast(object, index["date"].max()))

            # Latest-day instrument type breakdown from actual parquets
            latest_day_counts: dict[str, int] = {}
            latest_day_total = 0
            latest_date = date_max
            if "venue" in index.columns:
                latest_venues = sorted(index.loc[index["date"] == latest_date, "venue"].unique().tolist())
                for venue_name_raw in latest_venues:  # pyright: ignore[reportAny]
                    venue_name_typed: object = cast(object, venue_name_raw)
                    venue_name = str(venue_name_typed)
                    detail = self.get_venue_detail(
                        service=service,
                        asset_group=cat,
                        venue=venue_name,
                        date=latest_date,
                    )
                    if "error" not in detail:
                        venue_total = int(detail.get("total_instruments", 0))
                        latest_day_total += venue_total
                        for itype, count in (detail.get("instrument_types") or {}).items():
                            latest_day_counts[itype] = latest_day_counts.get(itype, 0) + int(count)

            result_categories[cat] = {
                "total_shards": total_shards,
                "total_instrument_rows": total_rows,
                "unique_dates": unique_dates,
                "unique_venues": unique_venues,
                "sub_dimension_label": {
                    "SPORTS": "Data Sources",
                    "PREDICTIONS": "Market Category Shards",
                }.get(cat, "Venues"),
                "date_range": {"start": date_min, "end": date_max},
                "latest_day": latest_date,
                "latest_day_instruments": latest_day_counts,
                "latest_day_total": latest_day_total,
            }
            grand_shards += total_shards
            grand_rows += total_rows
            grand_dates += unique_dates
            grand_latest_instruments += latest_day_total

        return {
            "service": service,
            "asset_groups": result_categories,
            "totals": {
                "shards": grand_shards,
                "instrument_rows": grand_rows,
                "dates_across_asset_groups": grand_dates,
                "latest_day_instruments": grand_latest_instruments,
            },
        }

    # ------------------------------------------------------------------
    # League sub-breakdown (sports venues)
    # ------------------------------------------------------------------

    def _build_league_breakdown(
        self,
        venue_df: pd.DataFrame,
        start_date: str,
        end_date: str,
    ) -> dict[str, dict[str, object]]:
        """Build per-league stats for a sports venue.

        Uses the UAC league fixture calendar as the per-league denominator
        for expected dates.  Leagues present in UAC's prediction registry
        but absent from the manifest appear at 0%.
        """
        leagues_in_data: set[str] = set()
        if "league_id" in venue_df.columns:
            leagues_in_data = {str(cast(object, lid)) for lid in venue_df["league_id"].unique() if lid}  # pyright: ignore[reportAny]

        # All prediction leagues — ensures newly-added leagues show 0%
        all_league_ids = set(get_all_prediction_league_ids())
        all_leagues = sorted(leagues_in_data | all_league_ids)

        result: dict[str, dict[str, object]] = {}
        for lid in all_leagues:
            expected_dates = get_league_fixture_calendar(lid, start_date, end_date)
            expected_count = max(1, len(expected_dates)) if expected_dates else 1

            if lid in leagues_in_data:
                l_mask = venue_df["league_id"] == lid
                found_set = set(venue_df.loc[l_mask, "date"].unique())
                found_count = len(found_set)
                missing = sorted(d for d in expected_dates if d not in found_set)
            else:
                found_count = 0
                found_set = set()
                missing = sorted(expected_dates) if expected_dates else []

            max_list = 50
            tail = 5
            found_list = sorted(found_set)
            found_truncated = len(found_list) > max_list
            missing_truncated = len(missing) > max_list

            result[lid] = {
                "dates_found": found_count,
                "dates_expected": expected_count,
                "completion_pct": round(found_count / expected_count * 100, 2),
                "dates_found_list": found_list[:max_list],
                "dates_found_truncated": found_truncated,
                "dates_found_list_tail": found_list[-tail:] if found_truncated else None,
                "dates_missing": max(0, expected_count - found_count),
                "dates_missing_count": len(missing),
                "dates_missing_list": missing[:max_list],
                "dates_missing_truncated": missing_truncated,
                "dates_missing_list_tail": missing[-tail:] if missing_truncated else None,
            }

        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_bucket(self, service: str, asset_group: str) -> str:
        """Resolve primary bucket name.

        Services with a canonical ``cloud-providers.yaml`` kind resolve via
        ``resolve_bucket_name()`` (env-tiered SSOT); the rest fall back to the
        legacy ``BUCKET_TEMPLATES`` strings until their own canonicalisation sweep.
        """
        canonical_kind = _SERVICE_TO_CANONICAL_KIND.get(service)
        if canonical_kind is not None:
            ag = cast(AssetGroup, asset_group.lower()) if asset_group else None
            return resolve_bucket_name(
                cloud=cast(Cloud, get_cloud_provider()),
                kind=canonical_kind,
                asset_group=ag,
            )

        template = BUCKET_TEMPLATES.get(service)
        if not template:
            # Fallback for unknown services
            ag_slug = asset_group.lower().replace("_", "-") if asset_group else "default"
            return f"{service}-store-{ag_slug}"

        project_id = self._get_project_id()
        ag_lower = asset_group.lower().replace("_", "-") if asset_group else "default"

        return template.format(
            asset_group_lower=ag_lower,
            project_id=project_id,
            domain=ag_lower,  # execution-service uses {domain}
        )

    def _resolve_all_buckets(self, service: str, asset_group: str) -> list[str]:
        """Resolve primary bucket + any extra buckets for the service/asset_group pair."""
        primary = self._resolve_bucket(service, asset_group)
        buckets = [primary]
        extra_kinds = _EXTRA_BUCKET_KINDS.get(service, {}).get(asset_group.lower(), [])
        if extra_kinds:
            cloud = cast(Cloud, get_cloud_provider())
            ag = cast(AssetGroup, asset_group.lower()) if asset_group else None
            for kind in extra_kinds:
                # Flat-string kinds (dex-pools etc.) ignore asset_group; pass it
                # through for uniformity — the resolver drops it for flat kinds.
                buckets.append(resolve_bucket_name(cloud=cloud, kind=kind, asset_group=ag))
        return buckets

    def resolve_all_buckets(self, service: str, asset_group: str) -> list[str]:
        """Public wrapper: resolve every manifest bucket for a service/asset_group.

        Every name is produced via ``resolve_bucket_name`` (the bucket-name SSOT) —
        never an inline ``gs://`` string. Used by the deployment-API data-status
        route to feed canonical buckets into ``compute_coverage_for_bucket``.
        """
        return self._resolve_all_buckets(service, asset_group)


def all_asset_group_categories() -> list[str]:
    """Public accessor for the full asset-group/category list (MarketCategory)."""
    return list(_ALL_CATEGORIES)
