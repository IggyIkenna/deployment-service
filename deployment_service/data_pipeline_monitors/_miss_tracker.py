"""``MissTracker`` — the cross-sweep consecutive-miss counter for the DP meta-watchers.

Extracted verbatim from ``meta_watchers.py`` on 2026-07-20: that module had grown to 925
lines and breached the 900-line QG cap (crossed at 897 -> 925), which blocked every commit
in this repo. Pure move + re-export — ``meta_watchers`` still exposes ``MissTracker``, so
no caller changes.

The counter is what makes a page mean "SUSTAINED", not "blipped": ``register(key,
stale=...)`` increments on a genuine (unsuppressed) stale probe and resets to 0 on a fresh
or suppressed-by-design one, so a self-resolving blip never pages.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import cast

from unified_trading_library import StorageClient

from deployment_service.data_pipeline_monitors import _gcs

logger = logging.getLogger(__name__)

DP_MISS_COUNTERS_BLOB = "vm-census/dp-miss-counters.json"
DEFAULT_MIN_CONSECUTIVE_MISSES = 2  # meta sweep is */15 -> 2 misses = ~30m sustained before paging


@dataclass(frozen=True)
class MissTracker:
    """Cross-sweep consecutive-miss counter, GCS-persisted. ``register(key, stale=...)``
    increments on a genuine (unsuppressed) stale probe and resets to 0 on a fresh or
    suppressed-by-design probe; an alert fires only once the count reaches the
    threshold. ``load`` at sweep start, ``persist`` at sweep end. Injected so the
    checkers stay pure + credential-free; the cli wires the real GCS-backed instance."""

    storage_client: StorageClient
    log_bucket: str
    blob: str = DP_MISS_COUNTERS_BLOB
    _counts: dict[str, int] = field(default_factory=dict)

    @classmethod
    def load(
        cls,
        *,
        storage_client: StorageClient,
        log_bucket: str,
        blob: str = DP_MISS_COUNTERS_BLOB,
    ) -> MissTracker:
        """Read the persisted counters. Fail toward NO false-suppress: a read/parse
        miss treats every counter as 0 (so a sustained-stale condition still pages
        after ``min_consecutive`` fresh sweeps of accumulation, never silently never)."""
        counts: dict[str, int] = {}
        raw = _gcs.read_text(storage_client, log_bucket, blob)
        if raw:
            try:
                loaded = cast("object", json.loads(raw))
                if isinstance(loaded, dict):
                    parsed = cast("dict[str, object]", loaded)
                    counts = {str(k): int(v) for k, v in parsed.items() if isinstance(v, (int, float))}
            except (ValueError, TypeError):
                counts = {}
        return cls(storage_client=storage_client, log_bucket=log_bucket, blob=blob, _counts=counts)

    def register(self, key: str, *, stale: bool) -> int:
        """Increment ``key``'s consecutive-miss count on a stale probe, reset to 0 on
        a fresh/suppressed one. Returns the new count (0 when reset)."""
        if stale:
            new = self._counts.get(key, 0) + 1
            self._counts[key] = new
            return new
        self._counts.pop(key, None)
        return 0

    def persist(self, *, dry_run: bool = False) -> None:
        """Write the updated counters back to GCS (no-op on dry_run)."""
        if dry_run:
            return
        try:
            self.storage_client.upload_bytes(
                self.log_bucket,
                self.blob,
                json.dumps(self._counts, sort_keys=True).encode("utf-8"),
                content_type="application/json",
            )
        except Exception as exc:
            logger.warning("MissTracker.persist failed: %s", exc)
