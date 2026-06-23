#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# setup-messaging.sh — Provision AWS SQS queues mirroring GCP Pub/Sub topics.
#
# Creates standard SQS queues for:
#   1. Per-service lifecycle event queues ({service}-events) used by QueueEventSink
#   2. Core trading pipeline queues (fill-events, strategy-signals, execution-results, etc.)
#
# Queue naming: matches GCP topic names directly (no prefix) for easy cross-cloud comparison.
# Existing queues are silently skipped (idempotent).
#
# Usage:
#   bash scripts/aws/setup-messaging.sh [--region REGION] [--dry-run]

set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --region) REGION="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

CREATED=0
SKIPPED=0

create_queue() {
    local name="$1"
    local fifo="${2:-false}"

    if [[ "$fifo" == "true" ]]; then
        name="${name}.fifo"
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [dry-run] would create: $name"
        return
    fi

    local url
    url=$(aws sqs get-queue-url --queue-name "$name" --region "$REGION" \
          --query QueueUrl --output text 2>/dev/null || true)

    if [[ -n "$url" ]]; then
        echo "  ✓ exists: $name"
        ((SKIPPED++)) || true
        return
    fi

    local extra_args=()
    if [[ "$fifo" == "true" ]]; then
        extra_args=(--attributes FifoQueue=true,ContentBasedDeduplication=true)
    else
        # Standard queue: 14-day retention, 30s visibility timeout
        extra_args=(--attributes MessageRetentionPeriod=1209600,VisibilityTimeout=30)
    fi

    aws sqs create-queue \
        --queue-name "$name" \
        --region "$REGION" \
        "${extra_args[@]}" \
        --query QueueUrl --output text

    echo "  ✅ created: $name"
    ((CREATED++)) || true
}

echo "=== setup-messaging.sh ==="
echo "Region: $REGION | Dry-run: $DRY_RUN"
echo ""

# ── Section 1: Per-service lifecycle event queues (used by QueueEventSink) ──
# Topic pattern: {service_name}-events (matches _sink_factory.py build_event_sink)
echo "[1/4] Per-service event queues..."
for svc in \
    alerting-service \
    deployment-api \
    execution-service \
    features-service \
    instruments-service \
    market-data-processing-service \
    strategy-service; do
    create_queue "${svc}-events"
done

# ── Section 2: Core trading pipeline queues ──
echo ""
echo "[2/4] Core trading pipeline queues..."
# Fill events — one per venue (matches GCP fill-events-{venue} pattern)
for venue in binance bybit deribit okx hyperliquid kalshi polymarket; do
    create_queue "fill-events-${venue}"
done
create_queue "fill-events"  # aggregate

# Strategy + execution
create_queue "strategy-signals"
create_queue "strategy-signal-events"
create_queue "execution-results"

# Features pipeline
create_queue "feature-updates"
create_queue "features-delta-one-ready"
create_queue "features-cross-instrument-ready"
create_queue "features-mtf-ready"
create_queue "instruments-data-ready"

# Market data
create_queue "market-ticks"
create_queue "order-book-updates"
create_queue "position-updates"

# ── Section 3: Alert + risk queues ──
echo ""
echo "[3/4] Alert and risk queues..."
create_queue "alert-notifications"
create_queue "risk-alerts"
create_queue "risk-breach-alerts"
create_queue "defi-risk-events"
create_queue "circuit-breaker-events"
create_queue "margin-warnings"
create_queue "health-alerts"
create_queue "system-health-events"

# ── Section 4: Infrastructure / deployment queues ──
echo ""
echo "[4/4] Infrastructure queues..."
create_queue "deployment-events"
create_queue "deployment-status"
create_queue "deployment-alerts"
create_queue "config-updates"
create_queue "lifecycle-events"
create_queue "service-lifecycle-events"
create_queue "secret-rotation-alerts"
create_queue "audit-log-events"

echo ""
echo "=== Done: created=$CREATED skipped=$SKIPPED ==="
