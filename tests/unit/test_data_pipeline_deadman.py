"""Unit tests for the watch-the-watchers closure (cron-watches-cron + deadman).

Covers the two new layers added 2026-06-23 (data_pipeline_hardening_self_monitoring):
  - Layer 1: `_gcs.write_monitor_last_run` / `read_monitor_last_run` sentinel
    round-trip + `meta_watchers.check_monitor_crons_fired` staleness →
    DP_CRON_DID_NOT_FIRE (DP-WATCHER-002).
  - Layer 2: `deadman_poster.run_deadman` — stale sentinel / pubsub backlog →
    DIRECT webhook post (mocked); healthy → no post; never imports alerting.

Credential-free + block-network safe: storage/secret/monitoring/webhook are all
injected fakes (no network, no cloud SDK).
"""

from __future__ import annotations

from deployment_service.data_pipeline_monitors import _gcs, deadman_poster, meta_watchers
from tests.unit.test_data_pipeline_monitors import LOG_BUCKET, FakeStorage


# ── Layer 1: sentinel write + read ───────────────────────────────────────────
def test_monitor_last_run_roundtrip():
    storage = FakeStorage({})
    assert _gcs.read_monitor_last_run(storage, LOG_BUCKET, "exit-code") is None
    _gcs.write_monitor_last_run(storage, LOG_BUCKET, "exit-code", ok=True, counts={"terminated": 3})
    loaded = _gcs.read_monitor_last_run(storage, LOG_BUCKET, "exit-code")
    assert loaded is not None
    assert loaded["mode"] == "exit-code"
    assert loaded["ok"] is True
    assert loaded["counts"] == {"terminated": 3}
    # sentinel landed at the canonical path
    assert (LOG_BUCKET, _gcs.MONITOR_LAST_RUN_BLOB.format(mode="exit-code")) in storage.uploaded


def test_monitor_last_run_corrupt_json_is_none():
    blob = _gcs.MONITOR_LAST_RUN_BLOB.format(mode="meta")
    storage = FakeStorage({(LOG_BUCKET, blob): (b"{not json", 0.0)})
    assert _gcs.read_monitor_last_run(storage, LOG_BUCKET, "meta") is None


def test_monitor_cron_targets_one_per_mode():
    targets = meta_watchers.monitor_cron_targets(LOG_BUCKET)
    labels = {t.label for t in targets}
    assert labels == {"dp-exit-code-monitor", "dp-heartbeat-monitor", "dp-meta-monitor"}
    # budget is 2x cadence
    by_label = {t.label: t for t in targets}
    assert by_label["dp-exit-code-monitor"].max_age_min == 10.0  # 2 * 5
    assert by_label["dp-meta-monitor"].max_age_min == 30.0  # 2 * 15


# ── Layer 1: check_monitor_crons_fired ───────────────────────────────────────
def test_check_monitor_crons_missing_sentinel_fires_dp_watcher_002(monkeypatch):
    storage = FakeStorage({})  # no sentinels at all → every monitor reads stale
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    results = meta_watchers.check_monitor_crons_fired(storage_client=storage, log_bucket=LOG_BUCKET)
    assert all(r.stale for r in results)
    # DP_CRON_DID_NOT_FIRE (DP-WATCHER-002) emitted once per stale monitor
    assert emitted.count("DP_CRON_DID_NOT_FIRE") == len(results)


def test_check_monitor_crons_fresh_sentinel_no_alert(monkeypatch):
    # All three sentinels fresh (age 1 min) → no alert.
    blobs = {
        (LOG_BUCKET, _gcs.MONITOR_LAST_RUN_BLOB.format(mode=m)): (b'{"ok": true}', 1.0)
        for m in ("exit-code", "heartbeat", "meta")
    }
    storage = FakeStorage(blobs)
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    results = meta_watchers.check_monitor_crons_fired(storage_client=storage, log_bucket=LOG_BUCKET)
    assert not any(r.stale for r in results)
    assert emitted == []


def test_check_monitor_crons_stale_one_fires(monkeypatch):
    # exit-code stale (20m > 10m budget), the other two fresh → exactly one alert.
    blobs = {
        (LOG_BUCKET, _gcs.MONITOR_LAST_RUN_BLOB.format(mode="exit-code")): (b"{}", 20.0),
        (LOG_BUCKET, _gcs.MONITOR_LAST_RUN_BLOB.format(mode="heartbeat")): (b"{}", 1.0),
        (LOG_BUCKET, _gcs.MONITOR_LAST_RUN_BLOB.format(mode="meta")): (b"{}", 1.0),
    }
    storage = FakeStorage(blobs)
    emitted: list[str] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    meta_watchers.check_monitor_crons_fired(storage_client=storage, log_bucket=LOG_BUCKET)
    assert emitted.count("DP_CRON_DID_NOT_FIRE") == 1


# ── Layer 2: deadman_poster ──────────────────────────────────────────────────
def _fresh_sentinels() -> dict:
    return {
        (LOG_BUCKET, _gcs.MONITOR_LAST_RUN_BLOB.format(mode=m)): (b"{}", 1.0)
        for m in ("exit-code", "heartbeat", "meta")
    }


def test_deadman_healthy_no_post(monkeypatch):
    # All sentinels fresh + no pubsub backlog → no webhook post.
    monkeypatch.setattr(deadman_poster, "_log_bucket", lambda _pid: LOG_BUCKET)
    storage = FakeStorage(_fresh_sentinels())
    posts: list[tuple[str, str]] = []
    findings = deadman_poster.run_deadman(
        storage_client=storage,
        secret_reader=lambda _name: "https://hooks.slack.test/deadman",
        monitoring_reader=lambda _pid, _sub: 5.0,  # 5s unacked — healthy
        webhook_poster=lambda url, text: posts.append((url, text)) or True,
        project_id="test-project",
    )
    assert findings == []
    assert posts == []


def test_deadman_stale_sentinel_posts(monkeypatch):
    monkeypatch.setattr(deadman_poster, "_log_bucket", lambda _pid: LOG_BUCKET)
    storage = FakeStorage({})  # no sentinels → all stale
    posts: list[tuple[str, str]] = []
    findings = deadman_poster.run_deadman(
        storage_client=storage,
        secret_reader=lambda _name: "https://hooks.slack.test/deadman",
        monitoring_reader=lambda _pid, _sub: None,  # unknown backlog (not a finding)
        webhook_poster=lambda url, text: posts.append((url, text)) or True,
        project_id="test-project",
    )
    assert len(findings) == 3  # one per stale monitor sentinel
    assert len(posts) == 1
    url, text = posts[0]
    assert url == "https://hooks.slack.test/deadman"
    assert "MONITORING DEADMAN" in text
    assert "dp-exit-code-monitor" in text


def test_deadman_pubsub_backlog_posts(monkeypatch):
    monkeypatch.setattr(deadman_poster, "_log_bucket", lambda _pid: LOG_BUCKET)
    storage = FakeStorage(_fresh_sentinels())  # sentinels fresh
    posts: list[tuple[str, str]] = []
    findings = deadman_poster.run_deadman(
        storage_client=storage,
        secret_reader=lambda _name: "https://hooks.slack.test/deadman",
        monitoring_reader=lambda _pid, _sub: 3600.0,  # 60m unacked > 30m threshold
        webhook_poster=lambda url, text: posts.append((url, text)) or True,
        project_id="test-project",
    )
    assert len(findings) == 1
    assert "alerting-subscriber" in findings[0].name
    assert len(posts) == 1
    assert "subscriber/relay down" in posts[0][1]


def test_deadman_dry_run_does_not_post(monkeypatch):
    monkeypatch.setattr(deadman_poster, "_log_bucket", lambda _pid: LOG_BUCKET)
    storage = FakeStorage({})  # stale
    posts: list = []
    findings = deadman_poster.run_deadman(
        storage_client=storage,
        secret_reader=lambda _name: "https://hooks.slack.test/deadman",
        monitoring_reader=lambda _pid, _sub: None,
        webhook_poster=lambda url, text: posts.append((url, text)) or True,
        project_id="test-project",
        dry_run=True,
    )
    assert findings  # detected
    assert posts == []  # but never posted in dry-run


def test_deadman_no_webhook_secret_does_not_crash(monkeypatch):
    monkeypatch.setattr(deadman_poster, "_log_bucket", lambda _pid: LOG_BUCKET)
    storage = FakeStorage({})  # stale
    posts: list = []
    # secret_reader returns None (SM access denied / missing) → cannot post, must not crash.
    findings = deadman_poster.run_deadman(
        storage_client=storage,
        secret_reader=lambda _name: None,
        monitoring_reader=lambda _pid, _sub: None,
        webhook_poster=lambda url, text: posts.append((url, text)) or True,
        project_id="test-project",
    )
    assert findings  # still detected the staleness
    assert posts == []  # but no post (no webhook) — and no exception


def test_deadman_does_not_import_alerting_or_pubsub_publish():
    """Defense-in-depth: the deadman module must not bind the alerting path.

    Checks the module NAMESPACE (actual imports/symbols), not the source text —
    the docstring legitimately *describes* what it avoids. The deadman is
    deliberately independent of PubSub publish / log_event / the alerting webhook
    so the same failure can't swallow its own death-alert.
    """
    names = set(vars(deadman_poster))
    # No DP_* event emission / PubSub publish bound in this path.
    assert "log_event" not in names
    assert "PubSubEventSink" not in names
    # It does NOT key off the #data-pipeline-alerts webhook — only its OWN separate one.
    assert "DATA_PIPELINE_ALERTS_SLACK_WEBHOOK" not in names
    assert deadman_poster.DEADMAN_SLACK_WEBHOOK_SECRET == "MONITORING_DEADMAN_SLACK_WEBHOOK"


# ── _extract_oldest_unacked_seconds parsing ──────────────────────────────────
def test_extract_oldest_unacked_double_value():
    body = {"timeSeriesData": [{"pointData": [{"values": [{"doubleValue": 1800.5}]}]}]}
    assert deadman_poster._extract_oldest_unacked_seconds(body) == 1800.5


def test_extract_oldest_unacked_int_string_value():
    body = {"timeSeriesData": [{"pointData": [{"values": [{"int64Value": "120"}]}]}]}
    assert deadman_poster._extract_oldest_unacked_seconds(body) == 120.0


def test_extract_oldest_unacked_empty_is_none():
    assert deadman_poster._extract_oldest_unacked_seconds({}) is None
    assert deadman_poster._extract_oldest_unacked_seconds({"timeSeriesData": []}) is None
