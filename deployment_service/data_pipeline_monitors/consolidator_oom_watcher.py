"""DP-WATCHER-005 — automatic OOM-signature detector for manifest consolidators.

A sibling of ``consolidator_scheduler_watcher.py`` (DP-WATCHER-003) and the
companion to the ``CONSOLIDATOR_DOWN`` auto-recover actuator at
``scripts/recovery/relaunch_consolidator.py`` (which already accepts ``oom=True``
and escalates the Cloud Run Job to the next machine tier before re-executing).

The gap: ``CONSOLIDATOR_DOWN`` fires on index staleness but does not KNOW whether
the root cause is OOM — a human had to pass ``--oom`` manually. This watcher
closes that gap: for each per-AG ``manifest-consolidator-{ag}`` Cloud Run Job,
it reads the latest execution, classifies ``failed_count > 0`` as OOM via the
execution's ``conditions[].message``/``.reason`` (signal 9 / exit 137 /
"out of memory" / "OOMKilled"), ANDs it with the index blob mtime not advancing
(``index_age_reader``), then emits
``PipelineFinding(event=CONSOLIDATOR_DOWN, tier=AUTO_RECOVER, details={"oom":True,...})``
— which the existing escalation hop routes directly to
``RelaunchConsolidator.relaunch(oom=True)`` without a human in the loop.

Plan: ``cefi_hl_aster_batch_data_gaps_2026_06_22.md`` todo "[INFRA] P3. deployment-service
— automatic OOM-signature detector."
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable

from deployment_service.data_pipeline_monitors.escalation import EscalationTier, PipelineFinding
from deployment_service.data_pipeline_monitors.meta_watchers import (
    DEFAULT_MIN_CONSECUTIVE_MISSES,
    MissTracker,
    emit_finding,
)

logger = logging.getLogger(__name__)

# () -> dict mapping asset_group -> the latest failed execution's OOM diagnostics
# (None per ag when the latest execution SUCCEEDED / no executions found / API error).
# Each non-None entry carries ``{"failed_count": int, "oom_reason": str,
# "completion_age_min": float | None}``.
# Injected so the check stays credential-free + testable; the cli wires the real
# GCP SDK via ``_make_consolidator_execution_oom_reader``.
ConsolidatorExecutionOomReader = Callable[[Iterable[str]], dict[str, dict[str, object] | None]]

# ``asset_group -> index blob age in minutes`` (``None`` = blob absent/unreadable).
# Wraps ``_gcs.blob_age_minutes`` against the per-AG ``_index/availability_index.parquet``.
IndexAgeReader = Callable[[str], float | None]


def _oom_miss_key(asset_group: str) -> str:
    return f"CONSOLIDATOR_DOWN::oom::{asset_group}"


# Known OOM-signature substrings in Cloud Run execution condition messages/reasons.
# Case-insensitive match on the concatenated ``reason + " " + message``.
_OOM_SIGNATURE_SUBSTRINGS: tuple[str, ...] = (
    "signal: 9",
    "signal 9",
    "sigkill",
    "exit code 137",
    "exit_code=137",
    "out of memory",
    "oomkill",
    "oom killed",
    "memory limit exceeded",
    "memorylimit exceeded",
    "container killed",
)


def classify_oom_from_conditions(conditions: Iterable[object]) -> str:
    """Return the OOM reason string extracted from execution conditions, or "" if none found.

    Each condition object is expected to have ``type``, ``state``, ``message``,
    and ``reason`` attributes (``google.cloud.run_v2.types.Condition`` proto fields).
    We concatenate ``reason + " " + message`` (lowercased) and match against the
    known OOM-signature substrings.
    """
    for cond in conditions:
        reason = str(getattr(cond, "reason", "") or "")
        message = str(getattr(cond, "message", "") or "")
        combined = f"{reason} {message}".lower()
        for sig in _OOM_SIGNATURE_SUBSTRINGS:
            if sig in combined:
                return sig
    return ""


# The max index staleness (in minutes) that still counts as "not advancing."
# The consolidator touches the index every cycle (every 5-15 min typically);
# beyond this window with a CONFIRMED OOM execution, the index is considered stale.
_DEFAULT_MAX_INDEX_AGE_MIN = 60.0


def check_consolidator_oom(
    *,
    asset_groups: Iterable[str],
    execution_oom_reader: ConsolidatorExecutionOomReader,
    index_age_reader: IndexAgeReader,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
    max_index_age_min: float = _DEFAULT_MAX_INDEX_AGE_MIN,
) -> dict[str, dict[str, object]]:
    """DP-WATCHER-005 — detect OOM-killed consolidator executions + index staleness.

    For each asset_group: reads the latest Cloud Run execution via the injected
    ``execution_oom_reader``, classifies OOM when ``failed_count > 0`` AND the
    conditions carry a known OOM signature, then cross-checks with the index blob
    age via the injected ``index_age_reader``. Emits
    ``CONSOLIDATOR_DOWN`` with ``details["oom"]=True`` when BOTH conditions are met
    (OOM execution + stale index). Gated on ``MissTracker`` so a transient blip
    only pages when sustained across ``min_consecutive`` sweeps.

    Returns a dict ``{ag: finding_details | {}}`` for logging/testing. A non-empty
    sub-dict means the finding fired for that asset_group.
    """
    oom_diagnostics = execution_oom_reader(asset_groups)

    findings: dict[str, dict[str, object]] = {}
    for ag in asset_groups:
        diag = oom_diagnostics.get(ag)
        miss_key = _oom_miss_key(ag)

        if diag is None:
            # No failed/OOM execution found — reset the miss counter.
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            findings[ag] = {}
            continue

        # An OOM execution exists. Cross-check: is the index ALSO stale?
        failed_count = int(diag.get("failed_count", 0))
        oom_reason = str(diag.get("oom_reason", ""))  # noqa: qg-empty-fallback — log-only diagnostic string, absent reason is a valid "unclassified" state
        completion_age_min = diag.get("completion_age_min")  # float | None

        index_age_min = index_age_reader(ag)

        # The index is stale when absent or older than the max budget.
        index_stale = index_age_min is None or index_age_min > max_index_age_min

        if not index_stale:
            # OOM execution but the index is still fresh — a subsequent run may
            # have already recovered. Reset the miss counter.
            logger.info(
                "consolidator_oom_watcher: OOM execution detected for '%s' (failed_count=%d, "
                "oom_reason=%r, completion_age=%.0fm) but index is fresh (age=%.0fm) — "
                "likely already recovered; suppressing",
                ag,
                failed_count,
                oom_reason,
                completion_age_min if completion_age_min is not None else float("nan"),
                index_age_min if index_age_min is not None else float("nan"),
            )
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            findings[ag] = {}
            continue

        # Both conditions met: OOM execution + stale index.
        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=True)
            if misses < min_consecutive:
                logger.info(
                    "consolidator_oom_watcher: consolidator OOM for '%s' below consecutive-miss "
                    "threshold (%d/%d) — not paging yet",
                    ag,
                    misses,
                    min_consecutive,
                )
                findings[ag] = {}
                continue

        summary = (
            f"manifest-consolidator-{ag} OOM-killed: {failed_count} failed task(s), "
            f"OOM signature={oom_reason!r}, index age={index_age_min:.0f}m "
            f"(budget={max_index_age_min:.0f}m) — auto-recovering with machine-tier escalation"
        )
        details: dict[str, object] = {
            "oom": True,
            "asset_group": ag,
            "failed_count": failed_count,
            "oom_reason": oom_reason,
            "completion_age_min": completion_age_min,
            "index_age_min": index_age_min,
            "max_index_age_min": max_index_age_min,
        }
        emit_finding(
            PipelineFinding(
                event="CONSOLIDATOR_DOWN",
                severity="CRITICAL",
                tier=EscalationTier.AUTO_RECOVER,
                summary=summary,
                details=details,
                registry_id="DP-WATCHER-005",
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
        findings[ag] = details

    return findings


def make_consolidator_execution_oom_reader(
    project_id: str,
    env_prefix: str = "",
    location: str = "asia-northeast1",
) -> ConsolidatorExecutionOomReader:
    """Return ``asset_groups -> {ag: oom_diagnostics | None}`` for DP-WATCHER-005.

    Reads the latest execution of each ``manifest-consolidator-{ag}`` Cloud Run Job
    via ``run_v2.ExecutionsClient``. Returns OOM diagnostics when the latest
    execution has ``failed_count > 0`` AND the conditions carry a known OOM signature
    (signal 9 / exit 137 / "out of memory" / "OOMKilled"). Returns ``None`` per-AG
    when the latest execution SUCCEEDED, no executions were found, or the API errors.

    Deferred-import of ``google.cloud.run_v2`` (credential-bound SDK boundary).
    An SDK import failure returns an empty dict for every call (fail toward silence
    on a transient API unavailability — the index-staleness watcher at
    ``check_cron_fired`` still catches a genuinely-dead consolidator).
    """
    import importlib  # noqa: imports-inside-functions
    from datetime import UTC, datetime  # noqa: imports-inside-functions

    try:
        run_mod = importlib.import_module("google.cloud.run_v2")  # noqa: imports-inside-functions
        executions_client = run_mod.ExecutionsClient()  # pyright: ignore[reportAny]
    except Exception as exc:
        logger.info("consolidator-oom reader unavailable (DP-WATCHER-005 off this sweep): %s", exc)
        return lambda _ags: {}

    def _read(asset_groups: Iterable[str]) -> dict[str, dict[str, object] | None]:
        if not project_id:
            return dict.fromkeys(asset_groups, None)
        result: dict[str, dict[str, object] | None] = {}
        for ag in asset_groups:
            job_name = (
                f"{env_prefix}manifest-consolidator-market-data-{ag}"
                if env_prefix
                else f"manifest-consolidator-market-data-{ag}"
            )
            parent = f"projects/{project_id}/locations/{location}/jobs/{job_name}"
            result[ag] = None
            try:
                latest_failed: object | None = None
                latest_completion: datetime | None = None
                for execution in executions_client.list_executions(parent=parent):  # pyright: ignore[reportAny]
                    failed_count = int(getattr(execution, "failed_count", 0) or 0)  # pyright: ignore[reportAny]
                    if failed_count <= 0:
                        continue
                    completion = getattr(execution, "completion_time", None)  # pyright: ignore[reportAny]
                    if completion is None:
                        continue
                    completed_dt = (
                        completion if isinstance(completion, datetime) else completion.ToDatetime(tzinfo=UTC)  # pyright: ignore[reportAny]
                    )
                    if latest_completion is None or completed_dt > latest_completion:
                        latest_completion = completed_dt
                        latest_failed = execution  # pyright: ignore[reportAny]
                if latest_failed is None:
                    continue
                conditions = list(getattr(latest_failed, "conditions", []) or [])  # pyright: ignore[reportAny]
                oom_reason = classify_oom_from_conditions(conditions)
                if not oom_reason:
                    continue
                completion_age = (
                    (datetime.now(UTC) - latest_completion).total_seconds() / 60.0
                    if latest_completion is not None
                    else None
                )
                result[ag] = {
                    "failed_count": int(getattr(latest_failed, "failed_count", 0) or 0),
                    "oom_reason": oom_reason,
                    "completion_age_min": completion_age,
                }
            except Exception as exc:
                logger.info("consolidator-oom lookup for %s -> unknown: %s", job_name, exc)
        return result

    return _read


def make_consolidator_index_age_reader(
    storage_client: object,
    market_data_bucket_for_ag: Callable[[str], str],
) -> IndexAgeReader:
    """Return ``asset_group -> index blob age in minutes`` for DP-WATCHER-005.

    Wraps ``_gcs.blob_age_minutes`` against each per-AG
    ``_index/availability_index.parquet``. ``None`` = blob absent or unreadable.
    A resolution failure (no bucket for the AG) also returns ``None``.

    ``storage_client`` is a ``unified_trading_library.StorageClient`` (typed as
    ``object`` to avoid pulling the full UTL import chain into this module).
    """
    from deployment_service.data_pipeline_monitors import _gcs

    def _read(asset_group: str) -> float | None:
        try:
            bucket = market_data_bucket_for_ag(asset_group)
        except Exception:
            return None
        return _gcs.blob_age_minutes(storage_client, bucket, "_index/availability_index.parquet")  # pyright: ignore[reportAny]

    return _read


__all__ = [
    "ConsolidatorExecutionOomReader",
    "IndexAgeReader",
    "check_consolidator_oom",
    "classify_oom_from_conditions",
    "make_consolidator_execution_oom_reader",
    "make_consolidator_index_age_reader",
]
