<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.plan.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.plan.md`, `data_status_multi_axis_shard_propagation_2026_05_06.plan.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Resource Profile: execution-service

## Deployment Mode

- Mode: both (batch backtest + live trading)
- Cloud Run region: asia-northeast1
- Execution: Cloud Run Job (batch/backtest) | Cloud Run Service (live trading)

## Resource Allocation

### Batch Mode (Backtesting)

| Resource    | Value          | Rationale                                                                                 |
| ----------- | -------------- | ----------------------------------------------------------------------------------------- |
| CPU         | 4 vCPU         | Parallel order simulation across instruments; tick-level replay is CPU-intensive          |
| Memory      | 16 Gi          | Full order book reconstruction + position tracking across many instruments simultaneously |
| Timeout     | 14400 s (4 hr) | Full backtest over multi-year history can take several hours                              |
| Max retries | 2              | Backtests are deterministic; retry on transient infra failure                             |

### Live Mode

| Resource      | Value  | Rationale                                                                        |
| ------------- | ------ | -------------------------------------------------------------------------------- |
| CPU           | 4 vCPU | Real-time order routing + position tracking requires headroom for latency spikes |
| Memory        | 16 Gi  | Must hold full live order book state for all active instruments                  |
| Min instances | 1      | Keep-warm: cold start unacceptable during live trading hours                     |

## Cost Estimate

GCP Cloud Run pricing (vCPU-second: $0.00002400, GB-second: $0.00000250).

| Scenario             | Invocations/day | Avg duration  | Est. monthly cost |
| -------------------- | --------------- | ------------- | ----------------- |
| Daily backtest run   | 1               | 7200 s (2 hr) | ~$8.50            |
| Full 4-hr backtest   | 1               | 14400 s       | ~$17.00           |
| Live service (24 hr) | always-on       | 86400 s       | ~$50/month        |

Assumptions: 4 vCPU + 16 Gi. Live mode billed continuously; batch billed per invocation.

## Data Flow

- **Source (batch):** GCS market data buckets (features + candles)
- **Source (live):** Market tick data via EventBus (PubSub)
- **Sink:** GCS execution results bucket; PubSub execution events topic

## Special Requirements

- Live mode requires exchange write API keys in Secret Manager (binance-write-api-key, deribit-write-api-key)
- Circuit breaker: 3-state machine owned by execution-service/engine/; propagated via alerting-service PubSub
- Live mode: min 1 instance to prevent cold-start during open market hours

## Source References

- `deployment-service/terraform/services/execution-services/gcp/variables.tf`
- `deployment-service/configs/services/execution-service/batch.env`
- `deployment-service/configs/services/execution-service/live.env`
