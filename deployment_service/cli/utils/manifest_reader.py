"""ManifestReader — thin wrapper for data-status CLI.

Provides availability-check and completion-query APIs on top of UTL's
ManifestWriter / read_availability_index functions.  This is deployment-service
CLI tooling (not a shared library type), so it lives here rather than in UTL.
"""

from __future__ import annotations

import logging

import pandas as pd
from unified_trading_library import read_availability_index

logger = logging.getLogger(__name__)


class ManifestReader:
    """Read-side companion to UTL ManifestWriter.

    Used by the ``data-status`` CLI command to query data-availability indices
    stored as parquet in GCS category buckets.
    """

    def __init__(self) -> None:
        self._available: bool | None = None

    # ------------------------------------------------------------------
    # Availability probe
    # ------------------------------------------------------------------

    def is_available(self) -> bool:
        """Return True if we can read at least one availability index."""
        if self._available is not None:
            return self._available
        try:
            # Probe with a known bucket pattern — if cloud access works the
            # function returns a (possibly empty) DataFrame without raising.
            read_availability_index("")
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
            bucket = self._resolve_bucket(service, category)
            index = read_availability_index(bucket)
            if index.empty:
                return {"category": category, "overall_completion": 0, "dates": []}

            mask = (index["date"] >= start_date) & (index["date"] <= end_date)
            if "service_name" in index.columns:
                mask = mask & (index["service_name"] == service)
            filtered = index.loc[mask]

            if filtered.empty:
                return {"category": category, "overall_completion": 0, "dates": []}

            dates_present = filtered["date"].nunique()
            total_days = max(
                1,
                (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days + 1,
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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_bucket(service: str, category: str) -> str:
        """Best-effort bucket name from service + category."""
        cat_slug = category.lower().replace("_", "-") if category else "default"
        return f"{service}-store-{cat_slug}"
