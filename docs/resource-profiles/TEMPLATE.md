<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.plan.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.plan.md`, `data_status_multi_axis_shard_propagation_2026_05_06.plan.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Resource Profile: <service-name>

## Deployment Mode

- Mode: batch | live | both
- Cloud Run region: us-central1 (or asia-northeast1 — see per-service doc)
- Execution: Cloud Run Job (batch) | Cloud Run Service (live)

## Resource Allocation

### Batch Mode

| Resource      | Value      | Rationale                         |
| ------------- | ---------- | --------------------------------- |
| CPU           | X vCPU     | ...                               |
| Memory        | XGi        | ...                               |
| Timeout       | Xs (X min) | ...                               |
| Max retries   | X          | ...                               |
| Min instances | X          | N/A for batch jobs                |
| Max instances | X          | Controlled by sharding dispatcher |

### Live Mode (if applicable)

| Resource      | Value  | Rationale                             |
| ------------- | ------ | ------------------------------------- |
| CPU           | X vCPU | ...                                   |
| Memory        | XGi    | ...                                   |
| Timeout       | Xs     | ...                                   |
| Min instances | X      | Keep-warm to avoid cold-start latency |
| Max instances | X      | Scale ceiling                         |

## Cost Estimate

GCP Cloud Run pricing (vCPU-second: $0.00002400, GB-second: $0.00000250).

| Scenario     | Invocations/day | Avg duration | Est. monthly cost |
| ------------ | --------------- | ------------ | ----------------- |
| Normal batch | X               | X min        | $X–$Y             |
| Peak         | X               | X min        | $X–$Y             |

Assumptions: costs calculated on active execution time only (Cloud Run Jobs billed per invocation duration).

## VM Override (if applicable)

Some shards (e.g., COINBASE) exceed Cloud Run memory limits and must run on Compute Engine VMs.

| Resource     | Value          |
| ------------ | -------------- |
| Machine type | c2-standard-XX |
| RAM          | XXX GB         |
| Disk         | XXX GB         |
| Preemptible  | No             |

## Special Requirements

- Any special networking, GPU, or storage requirements
- Note any per-venue overrides from sharding YAML

## Source References

- `deployment-service/terraform/services/<service>/gcp/variables.tf`
- `deployment-service/configs/services/<service>/batch.env`
- `deployment-service/configs/sharding.<service>.yaml` (if applicable)
