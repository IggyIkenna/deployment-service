<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.plan.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.plan.md`, `data_status_multi_axis_shard_propagation_2026_05_06.plan.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Resource Profile: instruments-service

## Deployment Mode

- Mode: batch
- Cloud Run region: asia-northeast1
- Execution: Cloud Run Job

## Resource Allocation

### Batch Mode

| Resource    | Value           | Rationale                                                                                        |
| ----------- | --------------- | ------------------------------------------------------------------------------------------------ |
| CPU         | 2 vCPU          | Instrument universe enumeration and normalisation across exchanges; parallel venue lookups       |
| Memory      | 4 Gi            | Instrument metadata for full universe (CEFI + TRADFI + DEFI + SPORTS); ~100k instruments at peak |
| Timeout     | 86400 s (24 hr) | Maximum — some venues have slow metadata APIs; conservative ceiling                              |
| Max retries | 3               | Idempotent; retry on transient exchange API failures                                             |

## Cost Estimate

GCP Cloud Run pricing (vCPU-second: $0.00002400, GB-second: $0.00000250).

| Scenario              | Invocations/day | Avg duration   | Est. monthly cost |
| --------------------- | --------------- | -------------- | ----------------- |
| Daily refresh         | 1               | 900 s (15 min) | ~$0.55            |
| Full universe rebuild | 1               | 3600 s (1 hr)  | ~$2.20            |

Assumptions: 2 vCPU + 4 Gi; daily refresh updates metadata changes (delistings, new listings).

## Data Flow

- **Source:** Exchange metadata APIs (via UMI/UCI adapters for each venue)
- **Sink:** GCS instruments reference bucket + BigQuery instruments table

## Special Requirements

- Must run before all feature services and market-tick-data-service (provides canonical instrument IDs)
- First service in the daily pipeline DAG
- Requires exchange read API access (read-only keys, not write keys)

## Source References

- `deployment-service/terraform/services/instruments-service/gcp/variables.tf`
- `deployment-service/configs/services/instruments-service/batch.env`
