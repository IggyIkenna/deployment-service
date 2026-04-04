"""ManifestReader — thin wrapper for data-status CLI.

Provides availability-check and completion-query APIs on top of UTL's
ManifestWriter / read_availability_index functions.  This is deployment-service
CLI tooling (not a shared library type), so it lives here rather than in UTL.
"""

from __future__ import annotations

import io
import logging

import pandas as pd
import pyarrow.parquet as pq
from unified_api_contracts import VenueMapping
from unified_api_contracts.internal import MarketCategory  # noqa: qg-deep-import
from unified_trading_library import get_project_id, get_storage_client, read_availability_index

_ALL_CATEGORIES = [str(c) for c in MarketCategory]

logger = logging.getLogger(__name__)

# Global pipeline start date — nothing before this is counted as "expected".
# The trading system pipeline did not exist before this date; any data gaps
# prior to it are not actionable.
_PIPELINE_START_DATE = "2019-01-01"

# ── Bucket resolution: uses same templates as catalog.py SERVICE_GCS_CONFIGS ──
# Maps service → bucket template. Templates use {category_lower} and {project_id}.
# Services without {category_lower} use a shared bucket (no per-category split).
_BUCKET_TEMPLATES: dict[str, str] = {
    "instruments-service": "instruments-store-{category_lower}-{project_id}",
    "corporate-actions": "instruments-store-{category_lower}-{project_id}",
    "market-tick-data-service": "market-data-tick-{category_lower}-{project_id}",
    "market-data-processing-service": "market-data-tick-{category_lower}-{project_id}",
    "features-delta-one-service": "features-delta-one-{category_lower}-{project_id}",
    "features-volatility-service": "features-volatility-{category_lower}-{project_id}",
    "features-onchain-service": "features-onchain-{project_id}",
    "features-calendar-service": "features-calendar-{project_id}",
    "features-sports-service": "features-sports-{category_lower}-{project_id}",
    "features-multi-timeframe-service": "features-multi-timeframe-{category_lower}-{project_id}",
    "features-cross-instrument-service": "features-cross-instrument-{category_lower}-{project_id}",
    "features-commodity-service": "features-commodity-{category_lower}-{project_id}",
    "ml-training-service": "ml-models-store-{project_id}",
    "ml-inference-service": "ml-predictions-store-{project_id}",
    "strategy-service": "strategy-store-{project_id}",
    "execution-service": "execution-store-{domain}-{project_id}",
    "risk-and-exposure-service": "risk-and-exposure-{project_id}",
    "pnl-attribution-service": "pnl-attribution-{project_id}",
    "alerting-service": "alerting-service-{project_id}",
}

# Services that use a shared bucket (no category dimension in bucket name)
_SHARED_BUCKET_SERVICES = {
    "features-onchain-service",
    "features-calendar-service",
    "ml-training-service",
    "ml-inference-service",
    "strategy-service",
    "risk-and-exposure-service",
    "pnl-attribution-service",
    "alerting-service",
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
    "ml-training-service": "model_id",
    "ml-inference-service": "mode",
    "risk-and-exposure-service": "client_id",
    "pnl-attribution-service": "strategy_id",
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
            except Exception:  # noqa: BLE001
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
            project_id = self._get_project_id()
            probe_bucket = f"instruments-store-cefi-{project_id}"
            read_availability_index(probe_bucket)
            self._available = True
        except Exception:  # noqa: BLE001 — broad catch is intentional for probe
            self._available = False
        return self._available

    # ------------------------------------------------------------------
    # Completion query
    # ------------------------------------------------------------------

    def get_completion(
        self,
        *,
        service: str,
        category: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """Query overall completion for *service* in *category* between dates.

        Returns a dict with at least ``overall_completion`` (float 0-100) and
        ``category``.  On failure the dict contains ``error``.
        """
        try:
            # Clamp start to pipeline start — no data expected before this
            effective_start_date = max(start_date, _PIPELINE_START_DATE)
            bucket = self._resolve_bucket(service, category)
            index = read_availability_index(bucket)
            if index.empty:
                return {"category": category, "overall_completion": 0, "dates": []}

            mask = (index["date"] >= effective_start_date) & (index["date"] <= end_date)
            if "service_name" in index.columns:
                mask = mask & (index["service_name"] == service)
            filtered = index.loc[mask]

            if filtered.empty:
                return {"category": category, "overall_completion": 0, "dates": []}

            dates_present = filtered["date"].nunique()
            total_days = max(
                1,
                (pd.Timestamp(end_date) - pd.Timestamp(effective_start_date)).days + 1,
            )
            completion = round(dates_present / total_days * 100, 2)

            return {
                "category": category,
                "overall_completion": completion,
                "dates_present": dates_present,
                "total_days": total_days,
                "venues": sorted(filtered["venue"].unique().tolist())
                if "venue" in filtered.columns
                else [],
            }
        except Exception as exc:  # noqa: BLE001
            logger.debug("ManifestReader.get_completion failed: %s", exc)
            return {"error": str(exc), "category": category}

    def get_manifest_status(
        self,
        *,
        service: str,
        start_date: str,
        end_date: str,
        categories: list[str] | None = None,
    ) -> dict[str, object]:
        """Read manifest indices and return structured status for all categories.

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
        cat_list = [None] if service in _SHARED_BUCKET_SERVICES else categories or _ALL_CATEGORIES

        for cat in cat_list:
            cat_label = cat or "ALL"
            bucket = self._resolve_bucket(service, cat or "")
            index = read_availability_index(bucket)

            # Normalise venue aliases (e.g. bare "OKX" → "OKX-SPOT")
            if not index.empty and "venue" in index.columns:
                index["venue"] = index["venue"].replace(_VENUE_ALIASES)

            if index.empty:
                result_categories[cat_label] = {
                    "category": cat_label,
                    "bucket": bucket,
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
                for venue_val in sorted(filtered["venue"].unique()):
                    v_mask = filtered["venue"] == venue_val
                    v_dates = int(filtered.loc[v_mask, "date"].nunique())

                    # Resolve venue-specific expected days using launch date
                    # For compound keys like "BINANCE-FUTURES:trades", extract
                    # the venue name (first segment before ':')
                    base_venue = venue_val.split(":")[0] if ":" in venue_val else venue_val
                    venue_start = self._venue_mapping.get_venue_start_date(base_venue)
                    if venue_start:
                        effective_start = max(clamped_start, venue_start)
                    else:
                        effective_start = clamped_start
                    v_expected = max(
                        1,
                        (pd.Timestamp(end_date) - pd.Timestamp(effective_start)).days + 1,
                    )

                    venue_weighted_expected += v_expected
                    venue_weighted_found += v_dates

                    # Per-venue date lists
                    v_found_set = set(filtered.loc[v_mask, "date"].unique())
                    v_all_dates = pd.date_range(effective_start, end_date, freq="D")
                    v_missing = sorted(
                        d.strftime("%Y-%m-%d")
                        for d in v_all_dates
                        if d.strftime("%Y-%m-%d") not in v_found_set
                    )
                    v_found_list = sorted(v_found_set)

                    sub_dims[venue_val] = {
                        "dates_found": v_dates,
                        "dates_expected": v_expected,
                        "completion_pct": round(v_dates / v_expected * 100, 2),
                        "venue_start_date": venue_start,
                        "missing_dates": v_missing[:50],
                        "dates_found_list": v_found_list[:50],
                        "dates_missing": max(0, v_expected - v_dates),
                    }

            # Find missing dates (using clamped start so pre-pipeline dates excluded)
            all_dates = pd.date_range(clamped_start, end_date, freq="D")
            found_dates = set(filtered["date"].unique())
            missing_dates = sorted(
                d.strftime("%Y-%m-%d")
                for d in all_dates
                if d.strftime("%Y-%m-%d") not in found_dates
            )

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
                "category": cat_label,
                "bucket": bucket,
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
                _SUB_DIM_RESPONSE_KEY: dict[str, str] = {
                    "venue": "venues",
                    "data_type": "data_types",
                    "feature_group": "feature_groups",
                    "strategy_id": "strategies",
                    "model_id": "models",
                    "mode": "modes",
                    "domain": "domains",
                    "client_id": "clients",
                    "alert_type": "alert_types",
                }
                response_key = _SUB_DIM_RESPONSE_KEY.get(sub_dim_key or "", "venues")
                cat_result[response_key] = sub_dims

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
            "categories": result_categories,
        }

    # ------------------------------------------------------------------
    # Venue detail (drill-down)
    # ------------------------------------------------------------------

    def get_venue_detail(
        self,
        *,
        service: str,
        category: str,
        venue: str,
        date: str | None = None,
    ) -> dict[str, object]:
        """Read a single venue parquet and return instrument_type breakdown.

        If *date* is None, uses the most recent available date for the venue.
        Returns counts per instrument_type and sample instruments.
        """
        try:
            bucket_name = self._resolve_bucket(service, category)
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
                        target_date = str(v_dates.max())
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
                "category": category,
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

            # Top instruments sample
            top: list[dict[str, str]] = []
            key_col = "instrument_key" if "instrument_key" in df.columns else "raw_symbol"
            for _, row in df.head(30).iterrows():
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
        except Exception as exc:  # noqa: BLE001
            logger.debug("ManifestReader.get_venue_detail failed: %s", exc)
            return {"error": str(exc), "venue": venue}

    # ------------------------------------------------------------------
    # Coverage summary (manifest totals + latest-day instrument counts)
    # ------------------------------------------------------------------

    def get_coverage_summary(
        self,
        *,
        service: str,
        categories: list[str] | None = None,
    ) -> dict[str, object]:
        """Return instrument coverage summary for the deployment UI.

        For each category:
        - Manifest totals: total shards, instrument-rows, unique dates, unique venues
        - Latest-day unique instrument counts by instrument_type (from parquet files)

        This is fast: reads 4 small manifest parquets + latest-day parquets only.
        """
        cat_list = categories or _ALL_CATEGORIES
        result_categories: dict[str, dict[str, object]] = {}
        grand_shards = 0
        grand_rows = 0
        grand_dates = 0
        grand_latest_instruments = 0

        for cat in cat_list:
            bucket = self._resolve_bucket(service, cat)
            index = read_availability_index(bucket)

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
            total_rows = (
                int(index["instrument_count"].sum()) if "instrument_count" in index.columns else 0
            )
            unique_dates = int(index["date"].nunique())
            unique_venues = int(index["venue"].nunique()) if "venue" in index.columns else 0
            date_min = str(index["date"].min())
            date_max = str(index["date"].max())

            # Latest-day instrument type breakdown from actual parquets
            latest_day_counts: dict[str, int] = {}
            latest_day_total = 0
            latest_date = date_max
            if "venue" in index.columns:
                latest_venues = sorted(
                    index.loc[index["date"] == latest_date, "venue"].unique().tolist()
                )
                for venue_name in latest_venues:
                    detail = self.get_venue_detail(
                        service=service,
                        category=cat,
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
            "categories": result_categories,
            "totals": {
                "shards": grand_shards,
                "instrument_rows": grand_rows,
                "dates_across_categories": grand_dates,
                "latest_day_instruments": grand_latest_instruments,
            },
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_bucket(self, service: str, category: str) -> str:
        """Resolve bucket name using real templates from SERVICE_GCS_CONFIGS."""
        template = _BUCKET_TEMPLATES.get(service)
        if not template:
            # Fallback for unknown services
            cat_slug = category.lower().replace("_", "-") if category else "default"
            return f"{service}-store-{cat_slug}"

        project_id = self._get_project_id()
        cat_lower = category.lower().replace("_", "-") if category else "default"

        return template.format(
            category_lower=cat_lower,
            project_id=project_id,
            domain=cat_lower,  # execution-service uses {domain}
        )
