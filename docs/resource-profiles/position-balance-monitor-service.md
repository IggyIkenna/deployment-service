<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.md`, `data_status_multi_axis_shard_propagation_2026_05_06.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Resource Profile: position-balance-monitor-service

## Deployment Mode

- Mode: both (batch reconciliation + live monitoring)
- Cloud Run region: asia-northeast1
- Execution: Cloud Run Job (polling, scheduled every 5 minutes in live mode)

## Resource Allocation

### Batch / Live Polling Mode

| Resource    | Value         | Rationale                                                                                                                   |
| ----------- | ------------- | --------------------------------------------------------------------------------------------------------------------------- |
| CPU         | 1 vCPU        | Lightweight position query and comparison; network-bound, not CPU-bound                                                     |
| Memory      | 2 Gi          | Holds current positions + expected positions for drift comparison; larger than alerting-service due to position vector size |
| Timeout     | 240 s (4 min) | Shorter than the 5 min poll interval to prevent overlapping job executions                                                  |
| Max retries | 1             | Position snapshots are point-in-time; stale retries are misleading; fail fast                                               |

## Cost Estimate

GCP Cloud Run pricing (vCPU-second: $0.00002400, GB-second: $0.00000250).

| Scenario                   | Invocations/day | Avg duration | Est. monthly cost |
| -------------------------- | --------------- | ------------ | ----------------- |
| Live polling (every 5 min) | 288             | 60 s         | ~$0.20            |
| Daily reconciliation batch | 1               | 120 s        | ~$0.01            |

Assumptions: 1 vCPU + 2 Gi; monitor runs in tight poll loop during trading hours.

## Data Flow

- **Source:** Exchange position APIs (via UTEI adapters) + GCS internal position state
- **Sink:** GCS position snapshots; PubSub alerts topic (on position drift detected)
- **Live env backend:** Secret Manager (config), PubSub (event bus)

## Special Requirements

- Exchange read API keys required (read-only position query)
- Alerts when actual exchange positions diverge from internal expected positions by > threshold
- Live mode uses Cloud Scheduler to trigger every 5 minutes (same pattern as alerting-service)
- Timeout intentionally 4 min to prevent job overlap; if query takes > 4 min, investigate exchange API latency

## Source References

- `deployment-service/terraform/services/position-balance-monitor-service/gcp/variables.tf`
- `deployment-service/configs/services/position-balance-monitor-service/live.env`
