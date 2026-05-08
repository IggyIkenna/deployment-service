<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.md`, `data_status_multi_axis_shard_propagation_2026_05_06.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Resource Profile: execution-results-api

## Deployment Mode

- Mode: batch (results retrieval job)
- Cloud Run region: asia-northeast1
- Execution: Cloud Run Job

## Resource Allocation

### Batch Mode

| Resource    | Value          | Rationale                                                                                |
| ----------- | -------------- | ---------------------------------------------------------------------------------------- |
| CPU         | 1 vCPU         | API query workload; network-bound rather than CPU-bound                                  |
| Memory      | 2 Gi           | Buffers execution result payloads; larger than alerting due to order history aggregation |
| Timeout     | 840 s (14 min) | Allows time for paginating large result sets from exchange APIs                          |
| Max retries | 1              | Results retrieval is idempotent; single retry on transient failure                       |

## Cost Estimate

GCP Cloud Run pricing (vCPU-second: $0.00002400, GB-second: $0.00000250).

| Scenario              | Invocations/day | Avg duration | Est. monthly cost |
| --------------------- | --------------- | ------------ | ----------------- |
| Daily reconciliation  | 4               | 300 s        | ~$0.15            |
| Full history backfill | 2               | 600 s        | ~$0.12            |

Assumptions: 1 vCPU + 2 Gi; primarily a reconciliation job run a few times per day.

## Data Flow

- **Source:** Exchange execution APIs (via UCI/UTEI adapters)
- **Sink:** GCS execution results bucket

## Special Requirements

- Exchange write API keys must be provisioned in Secret Manager (binance-write-api-key, deribit-write-api-key)
- Network egress to exchange endpoints required

## Source References

- `deployment-service/terraform/services/execution-results-api/gcp/variables.tf`
