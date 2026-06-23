"""Out-of-band monitoring dead-man's-switch (the top-of-chain watcher).

Layer 2 of the "watch-the-watchers" closure (Layer 1 is the in-band
cron-watches-cron sentinels in ``meta_watchers.check_monitor_crons_fired``). The
in-band layer cannot detect the death of the LAST watcher (the meta sweep can't
probe its own sentinel while dead), nor the death of the alerting subscriber that
mirrors ``DP_*`` events to Slack. This poster is that final, *deliberately
independent* watcher.

Each tick it:

  (a) reads every fleet-monitor + meta-watcher ``vm-census/<mode>-last-run.json``
      sentinel freshness (the same sentinels Layer 1 writes/reads);
  (b) reads the ``lifecycle-events-sub`` pull subscription's
      ``oldest_unacked_message_age`` via the Cloud Monitoring API — a climbing
      backlog means the alerting subscriber is DOWN or the Slack relay is 404ing
      (events pile up unacked);
  (c) on ANY staleness/backlog, posts DIRECTLY to the Slack webhook in Secret
      Manager (``MONITORING_DEADMAN_SLACK_WEBHOOK``) with a ``{"text": ...}``
      payload naming exactly which watcher is stale.

CRITICAL — this path MUST NOT import / use the alerting-service, PubSub *publish*,
``log_event``/``DP_*`` events, or the ``#data-pipeline-alerts`` webhook. It is
deliberately independent so the SAME failure mode (Slack relay down, subscriber
down, monitor crons stopped) cannot swallow its own death-alert. It reads GCS +
Cloud Monitoring (read-only) and posts to a SEPARATE Slack channel/app
(``monitoring-deadman``). It never raises — a poster-side failure must not crash
the cron; it exits 0 always (the GCP-native alert policy on THIS job's own
execution-absence is the terminal bedrock above it).

The Monitoring query + webhook POST are injected (``monitoring_reader`` /
``webhook_poster``) so the poster is unit-testable credential-free + block-network.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from http.client import HTTPResponse
from typing import cast

from unified_trading_library import (  # noqa: qg-deep-import
    StorageClient,
    UnifiedCloudConfig,
    get_secret_client,
    get_storage_client,
)

from deployment_service.data_pipeline_monitors import meta_watchers

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Secret-Manager name of the SEPARATE deadman Slack webhook (App `monitoring-deadman`,
# a DIFFERENT channel from #data-pipeline-alerts — true defense-in-depth). Absent on a
# misconfigured job → the poster logs + exits gracefully (it cannot self-alert, so the
# GCP-native alert policy on its own execution-absence is the bedrock above it).
DEADMAN_SLACK_WEBHOOK_SECRET = "MONITORING_DEADMAN_SLACK_WEBHOOK"

# The alerting subscriber pulls from this subscription; a climbing oldest_unacked age
# means the subscriber is down OR the Slack relay 404s (events pile up unacked).
LIFECYCLE_EVENTS_SUBSCRIPTION = "lifecycle-events-sub"
# Backlog threshold (seconds): >30 min unacked ⇒ the subscriber/relay is down.
DEFAULT_OLDEST_UNACKED_THRESHOLD_SEC = 30 * 60.0


@dataclass(frozen=True)
class DeadmanStaleness:
    """One staleness finding (a stale monitor sentinel OR a pubsub backlog)."""

    name: str  # the stale watcher / subscription
    detail: str  # human one-line detail (age, backlog, missing)


def _project_id() -> str:
    """Resolve the GCP project id from UnifiedCloudConfig (never hardcoded)."""
    try:
        cfg = UnifiedCloudConfig()
    except (ValueError, RuntimeError, OSError):
        return ""
    return getattr(cfg, "gcp_project_id", "") or ""


def _log_bucket(project_id: str) -> str:
    """The durable VM log bucket: ``deployment-scripts-<project>`` (same as cli)."""
    return f"deployment-scripts-{project_id}" if project_id else ""


def check_monitor_sentinels(
    storage_client: StorageClient,
    log_bucket: str,
) -> list[DeadmanStaleness]:
    """Return a staleness finding for every stale/absent monitor-sweep sentinel.

    Reuses :func:`meta_watchers.monitor_cron_targets` (the SAME sentinels Layer 1
    writes) + :func:`meta_watchers.probe_freshness` so the budgets stay in one SSOT.
    A missing sentinel (the monitor never ran) counts as stale (fail-safe).
    """
    findings: list[DeadmanStaleness] = []
    for target in meta_watchers.monitor_cron_targets(log_bucket):
        result = meta_watchers.probe_freshness(storage_client, target)
        if result.stale:
            age = (
                f"{result.age_min:.0f}m (budget {target.max_age_min:.0f}m)"
                if result.age_min is not None
                else "missing (never ran)"
            )
            findings.append(DeadmanStaleness(name=target.label, detail=f"sentinel stale: {age}"))
    return findings


# Other critical monitors the deadman ALSO verifies OUT-OF-BAND (operator 2026-06-23):
# the in-band meta sweep already checks the vm-zombie-watchdog census + the per-AG
# manifest consolidator, but if the meta sweep ITSELF is down (it OOM'd 2026-06-23), those
# in-band checks don't run and a dead watchdog/consolidator goes unverified. So the deadman
# probes the watchdog's own census blob DIRECTLY — same read-only GCS pattern as the
# sentinel check, no in-band/PubSub dependency (the consolidator keeps its own
# consolidator-liveness-watchdog; tracked to fold its index-freshness in here too).
_WATCHDOG_CENSUS_BLOB = "vm-census/watchdog-census.json"
_WATCHDOG_MAX_AGE_MIN = 30.0  # the zombie watchdog ticks every ~5 min


def check_critical_infra(
    storage_client: StorageClient,
    log_bucket: str,
) -> list[DeadmanStaleness]:
    """Out-of-band freshness probe of the vm-zombie-watchdog's own census blob — the
    watchdog is a critical monitor whose death is otherwise only caught in-band (which
    is itself unreliable when the meta sweep is down). Same pattern as
    :func:`check_monitor_sentinels`; a missing/stale census = the watchdog is down."""
    target = meta_watchers.FreshnessTarget(
        bucket=log_bucket,
        blob_path=_WATCHDOG_CENSUS_BLOB,
        max_age_min=_WATCHDOG_MAX_AGE_MIN,
        label="vm-zombie-watchdog",
    )
    result = meta_watchers.probe_freshness(storage_client, target)
    if not result.stale:
        return []
    age = (
        f"{result.age_min:.0f}m (budget {target.max_age_min:.0f}m)"
        if result.age_min is not None
        else "missing (never ran)"
    )
    return [DeadmanStaleness(name=target.label, detail=f"census stale: {age}")]


def _default_monitoring_reader(project_id: str, subscription: str) -> float | None:
    """Read ``oldest_unacked_message_age`` (seconds) for a pull subscription, or ``None``.

    Queries the Cloud Monitoring API via an AuthorizedSession (the GCP SDK is
    confined to ``deployment_service.backends._gcp_sdk``, the approved boundary —
    we do NOT import ``google.cloud`` here). Read-only + failure-isolated: any
    error returns ``None`` (the caller treats ``None`` as "unknown", never as a
    backlog — the sentinel layer still covers the monitors).

    This is the DEFAULT injected reader; tests inject a stub (no network).
    """
    try:
        from deployment_service.backends import _gcp_sdk

        creds_pair = cast(
            "tuple[object, object]",
            _gcp_sdk.google_auth_default(  # pyright: ignore[reportUnknownMemberType]
                scopes=["https://www.googleapis.com/auth/monitoring.read"]
            ),
        )
        credentials = creds_pair[0]
        session = cast("object", _gcp_sdk.google_auth_requests.AuthorizedSession(credentials))
        # MQL: the freshest oldest_unacked_message_age point for this subscription.
        query = (
            "fetch pubsub_subscription"
            "| metric 'pubsub.googleapis.com/subscription/oldest_unacked_message_age'"
            f"| filter (resource.subscription_id == '{subscription}')"
            "| within 5m"
            "| group_by [], [value_max: max(value.oldest_unacked_message_age)]"
        )
        url = f"https://monitoring.googleapis.com/v3/projects/{project_id}/timeSeries:query"
        post = cast("Callable[..., object]", session.post)  # type: ignore[attr-defined]
        resp = post(url, json={"query": query}, timeout=20)
        status: int = int(cast("int", getattr(resp, "status_code", 0)))
        if status != 200:
            logger.warning("monitoring query non-200 (%s)", status)
            return None
        json_method = cast("Callable[[], object]", resp.json)  # type: ignore[attr-defined]
        body = json_method()
        if not isinstance(body, dict):
            return None
        return _extract_oldest_unacked_seconds(cast("dict[str, object]", body))
    except Exception as exc:
        logger.warning("monitoring reader failed (best-effort): %s", exc)
        return None


def _extract_oldest_unacked_seconds(body: dict[str, object]) -> float | None:
    """Pull the freshest oldest_unacked age (seconds) out of a timeSeries:query body.

    The MQL response shape is ``{"timeSeriesData": [{"pointData": [{"values":
    [{"doubleValue"|"int64Value": <n>}]}]}]}``. Returns ``None`` when no point is
    present (no data ⇒ unknown, never a backlog).
    """
    first = _first_dict(body.get("timeSeriesData"))
    if first is None:
        return None
    point0 = _first_dict(first.get("pointData"))
    if point0 is None:
        return None
    value0 = _first_dict(point0.get("values"))
    if value0 is None:
        return None
    for key in ("doubleValue", "int64Value"):
        raw = value0.get(key)
        if isinstance(raw, (int, float)):
            return float(raw)
        if isinstance(raw, str):
            try:
                return float(raw)
            except ValueError:
                return None
    return None


def _first_dict(node: object) -> dict[str, object] | None:
    """Return the first element of a non-empty list, cast to ``dict[str, object]``.

    The Cloud Monitoring JSON is ``Any``-typed; this narrows one nesting level
    (list → first dict) explicitly so the walk stays fully typed. ``None`` when the
    node isn't a non-empty list whose first element is a dict.
    """
    if not isinstance(node, list):
        return None
    items = cast("list[object]", node)
    if not items:
        return None
    first = items[0]
    if not isinstance(first, dict):
        return None
    return cast("dict[str, object]", first)


def check_subscriber_backlog(
    project_id: str,
    *,
    monitoring_reader: Callable[[str, str], float | None],
    threshold_sec: float = DEFAULT_OLDEST_UNACKED_THRESHOLD_SEC,
    subscription: str = LIFECYCLE_EVENTS_SUBSCRIPTION,
) -> DeadmanStaleness | None:
    """Return a finding when the alerting subscriber's unacked backlog is too old.

    A climbing ``oldest_unacked_message_age`` on ``lifecycle-events-sub`` means the
    alerting-service subscriber is DOWN or the Slack relay is 404ing → events pile
    up. ``None`` from the reader (unknown / no data) is NOT a finding (fail-safe —
    the sentinel layer still covers the monitors); only a value past ``threshold``
    fires.
    """
    age_sec = monitoring_reader(project_id, subscription)
    if age_sec is None:
        return None
    if age_sec > threshold_sec:
        return DeadmanStaleness(
            name=f"alerting-subscriber ({subscription})",
            detail=f"oldest_unacked_message_age {age_sec / 60.0:.0f}m > {threshold_sec / 60.0:.0f}m — subscriber/relay down",
        )
    return None


def _default_webhook_poster(webhook_url: str, text: str) -> bool:
    """POST ``{"text": text}`` to the deadman Slack webhook. Returns success.

    Plain ``urllib`` (no Slack SDK, no alerting-service). Best-effort — never
    raises; a non-2xx / network error logs + returns False.
    """
    payload = json.dumps({"text": text}).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json", "User-Agent": "monitoring-deadman"},
    )
    try:
        # nosec B310: webhook_url is a Slack hooks URL from Secret Manager, never a
        # user-controlled / file: scheme.
        resp = cast("HTTPResponse", urllib.request.urlopen(request, timeout=15))  # nosec B310
        try:
            status: int = resp.status
        finally:
            resp.close()
        return 200 <= status < 300
    except urllib.error.HTTPError as exc:
        logger.warning("deadman webhook HTTP %s: %s", exc.code, exc)
        return False
    except Exception as exc:
        logger.warning("deadman webhook post failed: %s", exc)
        return False


def _format_alert(findings: list[DeadmanStaleness]) -> str:
    """Build the Slack ``text`` naming exactly which watcher(s) are stale."""
    lines = [
        ":rotating_light: *MONITORING DEADMAN* — the data-pipeline monitoring may be DOWN.",
        f"{len(findings)} watcher(s) stale (out-of-band check, independent of #data-pipeline-alerts):",
    ]
    lines.extend(f"  • *{f.name}* — {f.detail}" for f in findings)
    lines.append(
        "Diagnose: `gcloud scheduler jobs list` / `gcloud run jobs executions list` for the dp-* monitors "
        "+ check the `lifecycle-events-sub` subscriber (alerting-paging job)."
    )
    return "\n".join(lines)


def run_deadman(
    *,
    storage_client: StorageClient,
    secret_reader: Callable[[str], str | None],
    monitoring_reader: Callable[[str, str], float | None],
    webhook_poster: Callable[[str, str], bool],
    project_id: str,
    threshold_sec: float = DEFAULT_OLDEST_UNACKED_THRESHOLD_SEC,
    dry_run: bool = False,
) -> list[DeadmanStaleness]:
    """One deadman tick: gather staleness, post directly to Slack on any finding.

    Returns the list of findings (empty ⇒ all watchers healthy). On a non-empty
    list (and not ``dry_run``) posts a SINGLE consolidated message naming every
    stale watcher to the deadman webhook. Never raises.
    """
    log_bucket = _log_bucket(project_id)
    findings: list[DeadmanStaleness] = []
    if log_bucket:
        findings.extend(check_monitor_sentinels(storage_client, log_bucket))
        findings.extend(check_critical_infra(storage_client, log_bucket))
    backlog = check_subscriber_backlog(project_id, monitoring_reader=monitoring_reader, threshold_sec=threshold_sec)
    if backlog is not None:
        findings.append(backlog)

    if not findings:
        logger.info("deadman: all monitor watchers + subscriber healthy")
        return findings

    text = _format_alert(findings)
    logger.warning("deadman: %d stale watcher(s) — %s", len(findings), ", ".join(f.name for f in findings))
    if dry_run:
        return findings

    webhook = secret_reader(DEADMAN_SLACK_WEBHOOK_SECRET)
    if not webhook:
        # Cannot self-alert (the bedrock GCP-native policy on this job's own
        # execution-absence covers a totally-dead deadman). Log loudly.
        logger.error(
            "deadman: %s unavailable — cannot post; GCP-native alert policy is the bedrock",
            DEADMAN_SLACK_WEBHOOK_SECRET,
        )
        return findings
    posted = webhook_poster(webhook, text)
    if not posted:
        logger.error("deadman: webhook post FAILED — alert not delivered")
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Out-of-band monitoring dead-man's-switch")
    parser.add_argument("--dry-run", action="store_true", help="probe + log; never post to Slack")
    parser.add_argument(
        "--threshold-sec",
        type=float,
        default=DEFAULT_OLDEST_UNACKED_THRESHOLD_SEC,
        help="oldest_unacked_message_age backlog threshold (seconds)",
    )
    args = parser.parse_args(argv)
    dry_run: bool = bool(cast("object", args.dry_run))
    threshold_sec: float = float(cast("str | float", args.threshold_sec))

    # The deadman is the out-of-band, top-of-chain watcher: it MUST NOT use
    # log_event / run_lifecycle (those route through the very PubSub + alerting
    # path whose health it monitors — see module docstring). Its own liveness is
    # covered by the GCP-native execution-absence alert policy, so it emits NO
    # lifecycle events. A poster-side failure must never crash the cron: it logs
    # loudly and exits 0 (the GCP-native execution-absence policy is the bedrock
    # above it). NOTE: an earlier `with run_lifecycle("monitoring-deadman")` here
    # crashed every run — run_lifecycle calls log_event without setup_events, and
    # using log_event at all violates this path's out-of-band contract.
    try:
        project_id = _project_id()
        storage_client = get_storage_client()

        def _secret_reader(name: str) -> str | None:
            try:
                return get_secret_client().get_secret(name)
            except Exception as exc:
                logger.warning("deadman: secret read %s failed: %s", name, exc)
                return None

        run_deadman(
            storage_client=storage_client,
            secret_reader=_secret_reader,
            monitoring_reader=_default_monitoring_reader,
            webhook_poster=_default_webhook_poster,
            project_id=project_id,
            threshold_sec=threshold_sec,
            dry_run=dry_run,
        )
    except Exception:
        logger.exception(
            "deadman: poster-side failure (non-fatal — exits 0; GCP-native "
            "execution-absence policy is the bedrock above this watcher)"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
