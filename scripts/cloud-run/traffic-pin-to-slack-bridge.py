#!/usr/bin/env python3
"""traffic-pin-to-slack-bridge — Cloud Run service (Pub/Sub push subscriber).

Receives Cloud Monitoring alert notifications from the
``cloud-monitoring-traffic-pin-alerts`` Pub/Sub topic, translates the
Cloud Monitoring incident format into Slack mrkdwn, and posts to
#ci-failures via the webhook URL stored in Secret Manager
(``cloud-monitoring-slack-ci-failures-webhook``).

Architecture::

    canary-deploy.sh rollback
      → gcloud logging write cloud-run-traffic-pin-alert (CRITICAL)
        → Cloud Monitoring log-based alert policy fires
          → Pub/Sub topic (cloud-monitoring-traffic-pin-alerts)
            → THIS SERVICE (push subscription)
              → Slack #ci-failures (incoming webhook)

Deploy as a Cloud Run service with --no-allow-unauthenticated and a
push subscription on the Pub/Sub topic.  The service itself needs:

- Secret Manager accessor role (for the webhook URL)
- No inbound auth (Pub/Sub push carries its own auth)

Epic: infrastructure_master
Lifecycle: permanent
Delete-when: NA
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.request import Request, urlopen

# ── Configuration (overridable via env vars for Cloud Run) ────────────────────

PROJECT = os.environ.get("GCP_PROJECT", "central-element-323112")
SECRET_NAME = os.environ.get("SLACK_WEBHOOK_SECRET_NAME", "cloud-monitoring-slack-ci-failures-webhook")
PORT = int(os.environ.get("PORT", "8080"))

# ── Helpers ───────────────────────────────────────────────────────────────────


def _load_slack_webhook_url() -> str:
    """Load the Slack webhook URL from Secret Manager.

    On Cloud Run the default service account typically already has the
    secretmanager.versions.access permission.  If the secret doesn't exist
    yet the service still starts (health checks pass) but /push requests
    will 500 until the secret is populated.
    """
    try:
        # Lazy-import google.cloud.secretmanager only when needed.
        from google.cloud import (
            secretmanager,  # type: ignore[import-untyped]
        )

        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT}/secrets/{SECRET_NAME}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8").strip()
    except Exception:
        # Fallback: allow direct env-var override for local testing so
        # the bridge is testable without deploying to Cloud Run.
        env_override = os.environ.get("SLACK_WEBHOOK_URL")
        if env_override:
            return env_override
        raise RuntimeError(
            f"Cannot load Slack webhook URL: secret {SECRET_NAME!r} not "
            f"accessible and SLACK_WEBHOOK_URL env var not set.  "
            f"Create the secret first:\n"
            f"  printf '<webhook-url>' | gcloud secrets versions add "
            f"{SECRET_NAME} --data-file=- --project={PROJECT}"
        ) from None


def _post_to_slack(webhook_url: str, text: str, blocks: list[dict[str, Any]]) -> int:
    """Post a message to Slack.  Returns the HTTP status code."""
    payload = json.dumps({"text": text, "blocks": blocks}).encode()
    req = Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=10) as resp:  # type: ignore[attr-defined]
        return resp.status


def _format_incident(incident: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    """Translate a Cloud Monitoring incident into Slack mrkdwn + blocks.

    The Cloud Monitoring webhook payload (when Pub/Sub delivers it) has
    shape::

        {"version": "1.0", "incident": {"policy_name": "...",
         "condition_name": "...", "summary": "...", "url": "...",
         "state": "open|closed", "started_at": <seconds>,
         "ended_at": <seconds|null>, "documentation": {"content": "...",
         "mimeType": "text/markdown"}, ...}}

    We render a compact Slack message with the key fields.
    """
    policy_name = incident.get("policy_name", "Unknown policy")
    condition_name = incident.get("condition_name", "Unknown condition")
    summary = incident.get("summary") or ""
    state = incident.get("state", "unknown")
    url = incident.get("url") or ""
    doc = incident.get("documentation", {})
    _raw_content = doc.get("content") if isinstance(doc, dict) else None
    doc_content = _raw_content if _raw_content else ""

    # ── Truthful severity + icon (per notify-slack.yml convention) ──────
    if state == "open":
        severity = "CRITICAL"
        emoji = ":rotating_light:"
        state_label = "FIRING"
    elif state == "closed":
        severity = "INFO"
        emoji = ":white_check_mark:"
        state_label = "RESOLVED"
    else:
        severity = "WARNING"
        emoji = ":warning:"
        state_label = state.upper()

    header = f"{emoji} *{severity}*  |  *{state_label}* — {policy_name}"

    # Build the body from available context fields.
    body_parts = [f"*Condition:* {condition_name}"]
    if summary:
        body_parts.append(f"*Summary:* {summary}")

    # The canary-deploy.sh log entry carries extra fields via labelExtractors
    # — those appear in the incident's `condition` section when available.
    scoping = incident.get("scoping", {}) or {}
    for label_key, display in [
        ("service", "Service"),
        ("region", "Region"),
        ("pinned_revision", "Pinned revision"),
    ]:
        labels = scoping.get("labels", {}) or {}
        val = labels.get(label_key, "")
        if val:
            body_parts.append(f"*{display}:* `{val}`")

    # Include the user-authored documentation (the canary-deploy.sh ALERT block
    # text is NOT in the Cloud Monitoring incident — the doc content is what we
    # set on the alert policy itself, which carries the recovery command).
    if doc_content:
        body_parts.append(f"\n{doc_content}")

    blocks: list[dict[str, Any]] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": header}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(body_parts)}},
    ]
    if url:
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"<{url}|View incident in Cloud Monitoring>"}],
            }
        )

    text = f"{emoji} {severity} — {state_label}: {policy_name}"
    return text, blocks


# ── HTTP handler for Pub/Sub push ─────────────────────────────────────────────

_SLACK_WEBHOOK_URL: str | None = None


def _get_webhook_url() -> str:
    """Lazy-load + cache the webhook URL so we only hit Secret Manager once."""
    global _SLACK_WEBHOOK_URL
    if _SLACK_WEBHOOK_URL is None:
        _SLACK_WEBHOOK_URL = _load_slack_webhook_url()
    return _SLACK_WEBHOOK_URL


class PushHandler(BaseHTTPRequestHandler):
    """Handles Pub/Sub push requests."""

    def do_POST(self) -> None:
        if self.path != "/push":
            self.send_error(404)
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length)
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            self.send_error(400, "Invalid JSON")
            return

        # Pub/Sub push payload: {"message": {"data": "<base64>", ...}}
        message = data.get("message", {})
        raw = message.get("data") or ""
        import base64

        try:
            decoded = base64.b64decode(raw).decode()
            incident_payload = json.loads(decoded)
        except Exception:
            self.send_error(400, "Malformed Pub/Sub message data")
            return

        incident = incident_payload.get("incident", {})
        state = incident.get("state", "unknown")
        print(
            f"[{datetime.now(UTC).isoformat()}] Alert: {incident.get('policy_name', '?')} → {state}",
            file=sys.stderr,
        )

        try:
            text, blocks = _format_incident(incident)
            status = _post_to_slack(_get_webhook_url(), text, blocks)
            print(f"Slack post: HTTP {status}", file=sys.stderr)
        except Exception:
            traceback.print_exc(file=sys.stderr)
            self.send_error(500, "Failed to post to Slack")
            return

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def do_GET(self) -> None:
        """Health-check endpoint — used by Cloud Run liveness probes."""
        if self.path == "/health":
            try:
                _get_webhook_url()
                secret_status = "ok"
            except Exception:
                secret_status = "unavailable"

            status = {
                "healthy": True,
                "secret": secret_status,
                "timestamp": datetime.now(UTC).isoformat(),
            }
            body = json.dumps(status).encode()
            self.send_response(200 if secret_status == "ok" else 503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"traffic-pin-to-slack-bridge")

    def log_message(self, fmt: str, *args: object) -> None:
        """Suppress the default access-log noise; we log our own."""
        pass


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    server = HTTPServer(("0.0.0.0", PORT), PushHandler)
    print(f"traffic-pin-to-slack-bridge listening on :{PORT}", file=sys.stderr)
    server.serve_forever()


if __name__ == "__main__":
    main()
