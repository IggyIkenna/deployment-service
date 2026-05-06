<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.plan.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.plan.md`, `data_status_multi_axis_shard_propagation_2026_05_06.plan.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Resource Profile: features-calendar-service

## Deployment Mode

- Mode: batch
- Cloud Run region: asia-northeast1
- Execution: Cloud Run Job

## Resource Allocation

### Batch Mode

| Resource    | Value          | Rationale                                                                              |
| ----------- | -------------- | -------------------------------------------------------------------------------------- |
| CPU         | 1 vCPU         | Calendar feature generation is I/O light; date arithmetic only                         |
| Memory      | 1 Gi           | Minimal in-memory state; produces small feature vectors (days-of-week, holidays, etc.) |
| Timeout     | 600 s (10 min) | Short job — calendar lookup across date ranges completes quickly                       |
| Max retries | 3              | Idempotent; retry aggressively on transient GCS write failures                         |

## Cost Estimate

GCP Cloud Run pricing (vCPU-second: $0.00002400, GB-second: $0.00000250).

| Scenario              | Invocations/day | Avg duration | Est. monthly cost |
| --------------------- | --------------- | ------------ | ----------------- |
| Daily calendar update | 1               | 120 s        | ~$0.02            |
| Historical backfill   | 1               | 300 s        | ~$0.04            |

Assumptions: 1 vCPU + 1 Gi; cheapest service in the features tier.

## Data Flow

- **Source:** Static calendar data (exchange holidays, market open/close times)
- **Sink:** GCS features bucket (`features-calendar-{project_id}`)

## Special Requirements

- None; no external API dependencies
- Upstream dependency: instruments-service must complete before calendar features are generated (exchange schedule by instrument)

## Source References

- `deployment-service/terraform/services/features-calendar-service/gcp/variables.tf`
- `deployment-service/configs/services/features-calendar-service/batch.env`
