<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.plan.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.plan.md`, `data_status_multi_axis_shard_propagation_2026_05_06.plan.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Resource Profile: features-onchain-service

## Deployment Mode

- Mode: batch
- Cloud Run region: asia-northeast1
- Execution: Cloud Run Job

## Resource Allocation

### Batch Mode

| Resource    | Value           | Rationale                                                                                  |
| ----------- | --------------- | ------------------------------------------------------------------------------------------ |
| CPU         | 2 vCPU          | Light API aggregation workload — fetches pre-computed protocol metrics from DeFi endpoints |
| Memory      | 4 Gi            | Small payloads; protocol metric responses are compact JSON                                 |
| Timeout     | 86400 s (24 hr) | Maximum set conservatively; typical runs complete in under 30 minutes                      |
| Max retries | 3               | Idempotent; retry on transient DeFi API failures                                           |

Note: The `variables.tf` defaults (2 vCPU / 4 Gi) are more conservative than the sharding YAML compute spec (1 vCPU / 4 Gi). The sharding YAML value (1 vCPU) reflects actual runtime; Terraform default is a safe upper bound.

### Sharding YAML Compute Recommendation

| Resource        | Value           | Rationale                                               |
| --------------- | --------------- | ------------------------------------------------------- |
| CPU             | 1 vCPU          | Mostly API aggregation — single-threaded is sufficient  |
| Memory          | 4 Gi            | Aggregates pre-computed protocol metrics                |
| Timeout         | 1800 s (30 min) | Fast — protocol metrics are already aggregated upstream |
| VM machine type | c2-standard-4   | 16 GB RAM; light workload                               |

## Cost Estimate

GCP Cloud Run pricing (vCPU-second: $0.00002400, GB-second: $0.00000250).

| Scenario                   | Invocations/day | Avg duration   | Est. monthly cost |
| -------------------------- | --------------- | -------------- | ----------------- |
| Daily update (CEFI + DEFI) | 2               | 600 s (10 min) | ~$0.30            |
| Historical backfill        | 1               | 3600 s (1 hr)  | ~$0.75            |

Assumptions: 1 vCPU + 4 Gi (sharding YAML values); estimated 10 min per category per day.

## Data Flow

- **Source:** DeFi protocol APIs (lending rates, LST yields, macro sentiment endpoints)
- **Sink:** GCS features bucket (shared, no category suffix: `features-onchain-{project_id}`)

## Feature Groups Produced

- `macro_sentiment` — on-chain sentiment indices
- `lending_rates` — DeFi lending protocol rates (Aave, Compound)
- `lst_yields` — Liquid Staking Token yields (stETH, rETH)
- `onchain_perps` — DEPRECATED; use features-delta-one-service `funding_oi` group instead

## Special Requirements

- TRADFI category not supported (no on-chain data)
- Output bucket is shared across categories (unlike other feature services which use per-category buckets)
- No exchange API keys required; uses public DeFi protocol APIs

## Source References

- `deployment-service/terraform/services/features-onchain-service/gcp/variables.tf`
- `deployment-service/configs/services/features-onchain-service/batch.env`
- `deployment-service/configs/sharding.features-onchain-service.yaml`
