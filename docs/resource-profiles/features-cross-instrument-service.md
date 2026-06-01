<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.plan`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.plan`, `data_status_multi_axis_shard_propagation_2026_05_06.plan`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Resource Profile: features-cross-instrument-service

## Deployment Mode

- Mode: batch
- Cloud Run region: asia-northeast1
- Execution: Cloud Run Job

## Resource Allocation

### Batch Mode

| Resource    | Value           | Rationale                                                                                       |
| ----------- | --------------- | ----------------------------------------------------------------------------------------------- |
| CPU         | 4 vCPU          | Cross-instrument correlation matrices are compute-intensive; parallelised over instrument pairs |
| Memory      | 16 Gi           | Must hold OHLCV matrices for all instruments simultaneously to compute pair-wise correlations   |
| Timeout     | 86400 s (24 hr) | Maximum — computing rolling correlations across 2000+ instrument pairs is time-intensive        |
| Max retries | 3               | Idempotent; retry on transient failures                                                         |

## Cost Estimate

GCP Cloud Run pricing (vCPU-second: $0.00002400, GB-second: $0.00000250).

| Scenario                      | Invocations/day | Avg duration   | Est. monthly cost |
| ----------------------------- | --------------- | -------------- | ----------------- |
| Daily incremental (CEFI only) | 1               | 3600 s (1 hr)  | ~$4.25            |
| Full cross-asset run          | 1               | 10800 s (3 hr) | ~$12.75           |

Assumptions: 4 vCPU + 16 Gi; cross-instrument is the most resource-intensive feature service.

## Data Flow

- **Source:** GCS features bucket (upstream per-instrument features from delta-one + volatility)
- **Sink:** GCS features bucket (`features-cross-instrument-{project_id}`)

## Special Requirements

- Upstream dependency: features-delta-one-service and features-volatility-service must complete first
- Cross-instrument correlations span CEFI, TRADFI, DEFI — all category buckets must be available

## Source References

- `deployment-service/terraform/services/features-cross-instrument-service/gcp/variables.tf`
- `deployment-service/configs/services/features-cross-instrument-service/batch.env`
