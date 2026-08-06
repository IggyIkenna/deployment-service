#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# setup-traffic-pin-alert.sh — Create the Cloud Monitoring log-based alert policy
# for the canary-deploy.sh rollback traffic-pin signal, plus its Pub/Sub notification
# channel.
#
# Background:
#   canary-deploy.sh writes a severity=CRITICAL Cloud Logging entry
#   (log name: cloud-run-traffic-pin-alert, jsonPayload.alert_type="cloud_run_traffic_pinned")
#   on rollback.  This script creates the Cloud Monitoring resources that convert
#   that log entry into an alert routed to Slack #ci-failures.
#
#   The full incident lineage + rationale is in:
#     /plans/active/issues/cloud_run_traffic_pin_silent_freeze_alert_wiring_2026_08_05.md
#     /codex/08-workflows/ci-cd-flow.md  § "Image deploy-hygiene" trap 5
#
# Resources created:
#   1. Pub/Sub topic (cloud-monitoring-traffic-pin-alerts) — notification channel target
#   2. Pub/Sub notification channel in Cloud Monitoring
#   3. Log-based alert policy matching the traffic-pin log entry, wired to the Pub/Sub channel
#
# Delivery path:
#   Cloud Logging entry → Cloud Monitoring alert policy → Pub/Sub topic
#     → traffic-pin-to-slack bridge → Slack #ci-failures
#
#   The bridge lives at scripts/cloud-run/traffic-pin-to-slack-bridge.py and is
#   deployed as a Cloud Run service (push-subscription on the Pub/Sub topic).
#
# Prerequisites:
#   - gcloud authenticated with monitoring.admin + pubsub.admin + logging.viewer roles
#   - Slack webhook URL for #ci-failures stored in Secret Manager as
#     cloud-monitoring-slack-ci-failures-webhook (for the bridge, not for gcloud directly)
#
# Usage:
#   bash scripts/cloud-run/setup-traffic-pin-alert.sh [OPTIONS]
#
# Options:
#   --project PROJECT_ID     GCP project (default: from gcloud config)
#   --dry-run                Print commands without executing
#
# Exit codes:
#   0 = resources created (or already exist — idempotent)
#   2 = gcloud error

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────

PROJECT=""
DRY_RUN=false
PUBSUB_TOPIC="cloud-monitoring-traffic-pin-alerts"
NOTIFICATION_CHANNEL_DISPLAY_NAME="Pub/Sub → traffic-pin alerts → Slack #ci-failures"
ALERT_POLICY_DISPLAY_NAME="Cloud Run Traffic Pin — canary-deploy rollback (CRITICAL)"

# ── Parse options ─────────────────────────────────────────────────────────────

while [ $# -gt 0 ]; do
  case "$1" in
    --project)  PROJECT="$2"; shift 2 ;;
    --dry-run)  DRY_RUN=true; shift ;;
    *) echo "ERROR: Unknown option: $1"; exit 2 ;;
  esac
done

if [ -z "$PROJECT" ]; then
  PROJECT=$(gcloud config get-value project 2>/dev/null || echo "")
  if [ -z "$PROJECT" ]; then
    echo "ERROR: No GCP project set. Use --project or configure gcloud."
    exit 2
  fi
fi

# ── Helper ────────────────────────────────────────────────────────────────────

run_cmd() {
  if [ "$DRY_RUN" = "true" ]; then
    echo "[DRY RUN] $*"
    return 0
  fi
  "$@"
}

log() { echo "[setup-traffic-pin-alert] $(date +%H:%M:%S) $*"; }

# ── Step 1: Create (or verify) the Pub/Sub topic ──────────────────────────────

log "Ensuring Pub/Sub topic exists: $PUBSUB_TOPIC ..."
EXISTING_TOPIC=$(gcloud pubsub topics describe "$PUBSUB_TOPIC" \
  --project="$PROJECT" --format="value(name)" 2>/dev/null || echo "")

if [ -n "$EXISTING_TOPIC" ]; then
  log "Pub/Sub topic already exists: $EXISTING_TOPIC"
else
  run_cmd gcloud pubsub topics create "$PUBSUB_TOPIC" \
    --project="$PROJECT" --quiet
  log "Pub/Sub topic created: $PUBSUB_TOPIC"
fi

FULL_TOPIC_NAME="projects/${PROJECT}/topics/${PUBSUB_TOPIC}"

# ── Step 2: Create (or reuse) the Pub/Sub notification channel ────────────────

EXISTING_CHANNEL=$(gcloud beta monitoring channels list \
  --project="$PROJECT" \
  --filter="displayName=\"$NOTIFICATION_CHANNEL_DISPLAY_NAME\"" \
  --format="value(name)" 2>/dev/null || echo "")

if [ -n "$EXISTING_CHANNEL" ]; then
  log "Notification channel already exists: $EXISTING_CHANNEL"
  CHANNEL_NAME="$EXISTING_CHANNEL"
else
  log "Creating Pub/Sub notification channel → $FULL_TOPIC_NAME ..."
  CHANNEL_NAME=$(run_cmd gcloud beta monitoring channels create \
    --project="$PROJECT" \
    --display-name="$NOTIFICATION_CHANNEL_DISPLAY_NAME" \
    --description="Routes Cloud Monitoring alerts to the Pub/Sub topic consumed by the traffic-pin-to-slack bridge" \
    --type=pubsub \
    --channel-labels="topic=$FULL_TOPIC_NAME" \
    --format="value(name)" 2>&1)

  if [ "${PIPESTATUS[0]}" -ne 0 ]; then
    log "ERROR: Failed to create notification channel"
    echo "$CHANNEL_NAME"
    exit 2
  fi
  log "Notification channel created: $CHANNEL_NAME"
fi

# ── Step 3: Create (or update) the log-based alert policy ─────────────────────

# The log filter matches the exact entry canary-deploy.sh writes on rollback:
#   gcloud logging write cloud-run-traffic-pin-alert \
#     '{"alert_type":"cloud_run_traffic_pinned",...}' \
#     --payload-type=json --severity=CRITICAL
#
# The log name written by `gcloud logging write <NAME>` is:
#   projects/<PROJECT>/logs/<NAME>
LOG_FILTER="logName=\"projects/${PROJECT}/logs/cloud-run-traffic-pin-alert\" AND jsonPayload.alert_type=\"cloud_run_traffic_pinned\""

# Build the alert policy as YAML via a small Python invocation.
# This avoids YAML quoting issues from bash variable interpolation
# (the LOG_FILTER contains double quotes that would break a plain
# heredoc).  Python's yaml module is available on this VM.
ALERT_POLICY_YAML=$(python3 -c "
import yaml
policy = {
    'displayName': '${ALERT_POLICY_DISPLAY_NAME}',
    'documentation': {
        'content': (
            'Cloud Run service traffic has been PINNED BY REVISION NAME after a canary\n'
            'rollback.  The service will NOT auto-track future deploys until an operator\n'
            'runs:\n'
            '\n'
            '    gcloud run services update-traffic <SERVICE> --region <REGION> --to-latest\n'
            '\n'
            'Full details in the log entry jsonPayload (service, region, pinned_revision,\n'
            'failed_revision, message).\n'
            '\n'
            'Incident doc: /plans/active/issues/cloud_run_traffic_pin_silent_freeze_alert_wiring_2026_08_05.md\n'
            'Codex: /codex/08-workflows/ci-cd-flow.md § \"Image deploy-hygiene\" trap 5'
        ),
        'mimeType': 'text/markdown',
    },
    'conditions': [
        {
            'conditionMatchedLog': {
                'filter': '${LOG_FILTER}',
                'labelExtractors': {
                    'service': 'EXTRACT(jsonPayload.service)',
                    'region': 'EXTRACT(jsonPayload.region)',
                    'pinned_revision': 'EXTRACT(jsonPayload.pinned_revision)',
                },
            },
            'displayName': 'Cloud Run traffic-pin log entry detected',
        },
    ],
    'enabled': True,
    'alertStrategy': {
        'notificationRateLimit': {'period': '300s'},
        'autoClose': '86400s',
    },
    'combiner': 'OR',
    'severity': 'CRITICAL',
}
print(yaml.dump(policy, default_flow_style=False, sort_keys=False, allow_unicode=True))
")

EXISTING_POLICY=$(gcloud beta monitoring policies list \
  --project="$PROJECT" \
  --filter="displayName=\"$ALERT_POLICY_DISPLAY_NAME\"" \
  --format="value(name)" 2>/dev/null || echo "")

POLICY_FILE="$(mktemp)"
printf '%s\n' "$ALERT_POLICY_YAML" > "$POLICY_FILE"

# Inject the notification channel name into the YAML.
# The YAML above uses a sentinel that we replace with the real channel name.
# We append it here because channel_name is only known after idempotency check.
cat >> "$POLICY_FILE" <<CHANEOF
notificationChannels:
  - "${CHANNEL_NAME}"
CHANEOF

if [ -n "$EXISTING_POLICY" ]; then
  log "Alert policy already exists — updating: $EXISTING_POLICY"
  run_cmd gcloud beta monitoring policies update "$EXISTING_POLICY" \
    --project="$PROJECT" \
    --policy-from-file="$POLICY_FILE" 2>&1 || {
    log "ERROR: Failed to update alert policy"
    rm -f "$POLICY_FILE"
    exit 2
  }
  POLICY_NAME="$EXISTING_POLICY"
else
  log "Creating log-based alert policy ..."
  POLICY_NAME=$(run_cmd gcloud beta monitoring policies create \
    --project="$PROJECT" \
    --policy-from-file="$POLICY_FILE" \
    --format="value(name)" 2>&1)

  if [ "${PIPESTATUS[0]}" -ne 0 ]; then
    log "ERROR: Failed to create alert policy"
    echo "$POLICY_NAME"
    rm -f "$POLICY_FILE"
    exit 2
  fi
  log "Alert policy created: $POLICY_NAME"
fi

rm -f "$POLICY_FILE"

# ── Done ──────────────────────────────────────────────────────────────────────

log ""
log "=== SETUP COMPLETE ==="
log "Project:              $PROJECT"
log "Pub/Sub topic:        $FULL_TOPIC_NAME"
log "Notification channel: $CHANNEL_NAME"
log "Alert policy:         $POLICY_NAME"
log "Log filter:           $LOG_FILTER"
log ""
log "Next steps:"
log "  1. Deploy the traffic-pin-to-slack bridge (scripts/cloud-run/traffic-pin-to-slack-bridge.py)"
log "     as a Cloud Run service with a push subscription on this Pub/Sub topic."
log "  2. Store the Slack #ci-failures webhook URL in Secret Manager:"
log "       gcloud secrets create cloud-monitoring-slack-ci-failures-webhook \\"
log "         --project=$PROJECT --replication-policy=automatic"
log "  3. Verify end-to-end: trigger a canary rollback against a disposable/UAT"
log "     Cloud Run service and confirm the Slack message arrives."
