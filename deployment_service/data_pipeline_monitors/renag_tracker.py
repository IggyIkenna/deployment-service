"""``RenagTracker`` — cross-sweep RE-NAG cooldown suppressor for meta-watcher alerts.

Split into its own module rather than added to ``meta_watchers.py`` (already at its
900-line file-size cap — mirrors why ``consolidator_scheduler_watcher.py`` exists).

2026-07-15, defense-in-depth for the ``DP_RUN_MOSTLY_EMPTY`` duplicate-alert spam
(see ``plans/active/issues/dp_run_mostly_empty_no_recurring_dedup_2026_07_15.md``):
``meta_watchers.MissTracker`` gates ONSET only (a probe must be stale/high for
``min_consecutive`` sweeps before its FIRST page); it has no ongoing re-fire
suppression, so a detector wired only with ``MissTracker`` re-emits an identical
CRITICAL page every sweep for as long as the underlying condition stays true (the
confirmed root cause: ``check_high_attempted_failed`` re-paged an identical
``DP_RUN_MOSTLY_EMPTY`` every ``*/15`` meta-sweep because nothing re-attempts the
failed manifest cells between sweeps, so the numbers never change). ``RenagTracker``
closes that gap: it persists the UTC epoch-seconds timestamp of a key's LAST ACTUAL
emission (not every sweep the condition was true), so a caller can re-nag on a
cooldown (``fire on change / RESOLVED / re-remind, never every tick`` — the workspace
CI-alerting convention, applied here at the detector source rather than relying
solely on the downstream generic ``AlertDeduplicator``).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast

from unified_trading_library import StorageClient

from deployment_service.data_pipeline_monitors import _gcs

logger = logging.getLogger(__name__)

# RE-NAG TIMESTAMPS: mirrors meta_watchers.DP_MISS_COUNTERS_BLOB's GCS-persisted,
# cross-sweep pattern (each Cloud Run sweep is a fresh process — no in-memory
# carry-over) but stores a per-key UTC ``last_alerted_at`` epoch-seconds timestamp
# instead of a consecutive-miss count.
DP_RENAG_TIMESTAMPS_BLOB = "vm-census/dp-renag-timestamps.json"

# Default cooldown between re-nags of a still-true condition — 30 min, matching the
# alerting-service-side ``DP_RUN_MOSTLY_EMPTY`` cooldown fix
# (``_RECURRING_WARN_COOLDOWN_SEC``) for consistency across both dedup layers.
DEFAULT_RENAG_COOLDOWN_SECONDS = 1800.0


@dataclass(frozen=True)
class RenagTracker:
    """Cross-sweep RE-NAG suppressor, GCS-persisted.

    ``load`` at sweep start, ``persist`` at sweep end — mirrors ``MissTracker``
    exactly (same injected ``StorageClient``, same JSON-blob-in-``vm-census/``
    pattern, same fail-toward-alert direction on a read/parse/write error).
    """

    storage_client: StorageClient
    log_bucket: str
    blob: str = DP_RENAG_TIMESTAMPS_BLOB
    _last_alerted_at: dict[str, float] = field(default_factory=dict)

    @classmethod
    def load(
        cls,
        *,
        storage_client: StorageClient,
        log_bucket: str,
        blob: str = DP_RENAG_TIMESTAMPS_BLOB,
    ) -> RenagTracker:
        """Read the persisted last-alerted timestamps. Fail toward NO false-suppress:
        a read/parse miss treats every key as never-alerted, so the first sweep after a
        state loss re-fires immediately (same fail-open direction as ``MissTracker``)."""
        timestamps: dict[str, float] = {}
        raw = _gcs.read_text(storage_client, log_bucket, blob)
        if raw:
            try:
                loaded = cast("object", json.loads(raw))
                if isinstance(loaded, dict):
                    parsed = cast("dict[str, object]", loaded)
                    timestamps = {str(k): float(v) for k, v in parsed.items() if isinstance(v, (int, float))}
            except (ValueError, TypeError):
                timestamps = {}
        return cls(storage_client=storage_client, log_bucket=log_bucket, blob=blob, _last_alerted_at=timestamps)

    def should_emit(self, key: str, *, cooldown_seconds: float, now: datetime | None = None) -> bool:
        """``True`` when ``key`` has never been recorded (first-ever emission — i.e. a
        fresh onset, including one following a :meth:`clear`) OR ``cooldown_seconds``
        has elapsed since its last RECORDED emission. Read-only — does NOT mutate
        state; the caller records the emission (via :meth:`record`) only once it has
        actually emitted, so a suppressed sweep never advances the cooldown clock."""
        last = self._last_alerted_at.get(key)
        if last is None:
            return True
        moment = now or datetime.now(UTC)
        return (moment.timestamp() - last) >= cooldown_seconds

    def record(self, key: str, *, now: datetime | None = None) -> None:
        """Record ``key``'s emission timestamp. Call ONLY immediately after the caller
        actually emits the alert for ``key`` (never on a cooldown-suppressed sweep, or
        the cooldown clock would advance without a real alert having fired)."""
        moment = now or datetime.now(UTC)
        self._last_alerted_at[key] = moment.timestamp()

    def clear(self, key: str) -> None:
        """Drop ``key``'s state entirely. Called by ``meta_watchers.reconcile_resolved``
        when a key's condition clears (the key no longer appears in the sweep's
        emitted set), so a LATER re-onset of the SAME key is treated as fresh (pages
        immediately) instead of being rate-limited by a stale ``last_alerted_at`` left
        over from the PRIOR incident."""
        self._last_alerted_at.pop(key, None)

    def persist(self, *, dry_run: bool = False) -> None:
        """Write the updated timestamps back to GCS (no-op on dry_run)."""
        if dry_run:
            return
        try:
            self.storage_client.upload_bytes(
                self.log_bucket,
                self.blob,
                json.dumps(self._last_alerted_at, sort_keys=True).encode("utf-8"),
                content_type="application/json",
            )
        except Exception as exc:
            logger.warning("RenagTracker.persist failed: %s", exc)


def apply_cooldown(
    tracker: RenagTracker | None,
    key: str,
    *,
    cooldown_seconds: float,
    active_sweep: dict[str, str],
    event: str,
) -> bool:
    """Re-nag gate for a still-true condition. Returns ``True`` (caller should SKIP
    emitting) when ``tracker`` says the cooldown hasn't elapsed since ``key``'s last
    ACTUAL alert. On suppression it still marks ``key`` ACTIVE in ``active_sweep``
    (the caller's per-sweep emitted-alert tracker, e.g. ``meta_watchers._EMITTED_THIS_SWEEP``)
    so a RESOLVED reconciler doesn't mistake cooldown-suppression for a genuine clear.
    ``tracker=None`` ⇒ never suppress (back-compat: fire every sweep past onset)."""
    if tracker is None or tracker.should_emit(key, cooldown_seconds=cooldown_seconds):
        return False
    active_sweep[key] = event
    logger.info("renag_tracker: %s suppressed for '%s' — cooldown not yet elapsed", event, key)
    return True
