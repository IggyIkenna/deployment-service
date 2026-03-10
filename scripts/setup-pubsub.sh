#!/usr/bin/env bash
#
# setup-pubsub.sh — Create all required GCP Pub/Sub topics and subscriptions
#
# Topics are the system-of-record message bus for the unified trading platform.
# Each service that publishes defines a topic; each downstream consumer gets
# a pull subscription. Idempotent — existing topics/subscriptions are preserved.
#
# Usage:
#   ./setup-pubsub.sh [project_id] [--dry-run] [--list-only]
#
# Options:
#   --dry-run     Print what would be created without creating anything
#   --list-only   List existing topics in the project
#
# AWS SNS/SQS equivalents: see setup-aws-messaging.sh (blocked — no AWS creds)
#

set -euo pipefail

# Parse flags first; collect non-flag args for positional project_id
DRY_RUN=false
LIST_ONLY=false
_POS_ARGS=()
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        --list-only) LIST_ONLY=true ;;
        *) _POS_ARGS+=("$arg") ;;
    esac
done

# Project ID: env var > first positional arg > gcloud default
PROJECT_ID="${GCP_PROJECT_ID:-${_POS_ARGS[0]:-$(gcloud config get-value project)}}"

echo "================================================="
echo "Unified Trading System — Pub/Sub Setup"
echo "================================================="
echo "Project:   $PROJECT_ID"
echo "Dry-run:   $DRY_RUN"
echo "================================================="
echo

gcloud config set project "$PROJECT_ID" --quiet

# Enable Pub/Sub API
if [[ "$DRY_RUN" == "false" ]]; then
    gcloud services enable pubsub.googleapis.com --project="$PROJECT_ID" --quiet || true
fi

# List-only mode
if [[ "$LIST_ONLY" == "true" ]]; then
    echo "--- Existing Pub/Sub topics ---"
    gcloud pubsub topics list --project="$PROJECT_ID" --format="value(name)"
    echo
    echo "--- Existing Pub/Sub subscriptions ---"
    gcloud pubsub subscriptions list --project="$PROJECT_ID" --format="value(name)"
    exit 0
fi

# ---------------------------------------------------------------------------
# Topic + subscription registry
# Format: TOPIC_NAME|MESSAGE_RETENTION_DAYS|SUBSCRIPTION_NAMES(comma-sep)|SUB_RETENTION_DAYS
# ---------------------------------------------------------------------------
# Core topics — deployment-service
TOPIC_REGISTRY=(
    # Deployment pipeline events
    "deployment-events|7|deployment-events-monitor|7"
    "deployment-status|7|deployment-status-monitor,deployment-status-ui|7"
    "deployment-alerts|7|deployment-alerts-monitor|7"

    # Execution service — fill events per CeFi venue (binance/bybit/okx/deribit)
    "fill-events-binance|3|fill-events-binance-pnl,fill-events-binance-risk,fill-events-binance-strategy|3"
    "fill-events-bybit|3|fill-events-bybit-pnl,fill-events-bybit-risk,fill-events-bybit-strategy|3"
    "fill-events-okx|3|fill-events-okx-pnl,fill-events-okx-risk,fill-events-okx-strategy|3"
    "fill-events-deribit|3|fill-events-deribit-pnl,fill-events-deribit-risk,fill-events-deribit-strategy|3"
    "fill-events-hyperliquid|3|fill-events-hyperliquid-pnl,fill-events-hyperliquid-risk|3"

    # Risk / circuit breaker
    "circuit-breaker-events|7|circuit-breaker-monitor,circuit-breaker-execution|7"
    "risk-breach-alerts|7|risk-breach-alerting,risk-breach-execution|7"
    "position-updates|3|position-updates-risk,position-updates-pnl,position-updates-monitor|3"

    # ML / predictions
    "cascade-predictions|7|cascade-predictions-strategy,cascade-predictions-monitor|7"
    "ml-predictions|7|ml-predictions-strategy,ml-predictions-risk|7"

    # Strategy / signals
    "strategy-sports-signals|7|strategy-sports-signals-execution,strategy-sports-signals-monitor|7"
    "strategy-signals|7|strategy-signals-execution,strategy-signals-risk|7"

    # Config / ops
    "config-updates|14|config-updates-execution,config-updates-strategy,config-updates-risk,config-updates-features|14"
    "system-health-events|7|system-health-alerting,system-health-monitor|7"
    "audit-log-events|30|audit-log-sink|30"
)

created_topics=0
skipped_topics=0
created_subs=0
skipped_subs=0
errors=0

# ---------------------------------------------------------------------------
create_topic_if_missing() {
    local topic="$1"
    local retention_days="$2"
    local retention_str="${retention_days}d"

    # Check existence
    if gcloud pubsub topics describe "$topic" --project="$PROJECT_ID" &>/dev/null; then
        echo "  [SKIP] topic: $topic (already exists)"
        ((skipped_topics++)) || true
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY]  topic: $topic (retention=${retention_str})"
        return 0
    fi

    echo "  [CREATE] topic: $topic (retention=${retention_str})"
    if gcloud pubsub topics create "$topic" \
        --project="$PROJECT_ID" \
        --message-retention-duration="${retention_str}" \
        --quiet 2>&1; then
        ((created_topics++)) || true
    else
        echo "  [ERROR] Failed to create topic: $topic"
        ((errors++)) || true
    fi
}

create_subscription_if_missing() {
    local topic="$1"
    local sub="$2"
    local retention_days="$3"
    local retention_str="${retention_days}d"

    if gcloud pubsub subscriptions describe "$sub" --project="$PROJECT_ID" &>/dev/null; then
        echo "    [SKIP] sub: $sub (already exists)"
        ((skipped_subs++)) || true
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "    [DRY]  sub: $sub → $topic (ack_deadline=60s, retention=${retention_str})"
        return 0
    fi

    echo "    [CREATE] sub: $sub → $topic"
    if gcloud pubsub subscriptions create "$sub" \
        --topic="$topic" \
        --project="$PROJECT_ID" \
        --ack-deadline=60 \
        --message-retention-duration="${retention_str}" \
        --quiet 2>&1; then
        ((created_subs++)) || true
    else
        echo "    [ERROR] Failed to create subscription: $sub"
        ((errors++)) || true
    fi
}

# ---------------------------------------------------------------------------
echo "--- Processing ${#TOPIC_REGISTRY[@]} topic entries ---"
echo

for entry in "${TOPIC_REGISTRY[@]}"; do
    IFS='|' read -r topic_name retention_days subscriptions sub_retention <<< "$entry"

    echo "Topic: $topic_name"
    create_topic_if_missing "$topic_name" "$retention_days"

    IFS=',' read -ra sub_list <<< "$subscriptions"
    for sub in "${sub_list[@]}"; do
        [[ -z "$sub" ]] && continue
        create_subscription_if_missing "$topic_name" "$sub" "$sub_retention"
    done
    echo
done

echo "================================================="
echo "Summary"
echo "================================================="
echo "Topics:        created=$created_topics  skipped=$skipped_topics"
echo "Subscriptions: created=$created_subs  skipped=$skipped_subs"
echo "Errors:        $errors"
echo

if [[ "$errors" -gt 0 ]]; then
    echo "WARNING: $errors errors encountered. Review output above."
    exit 1
fi

echo "Done. To verify:"
echo "  gcloud pubsub topics list --project=$PROJECT_ID"
echo "  python scripts/verify_infra.py --project-id=$PROJECT_ID --topics deployment-events"
