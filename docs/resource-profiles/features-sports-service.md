<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.md`, `data_status_multi_axis_shard_propagation_2026_05_06.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Resource Profile: features-sports-service

## Deployment Mode

- Mode: batch
- Cloud Run region: asia-northeast1
- Execution: Cloud Run Job

## Resource Allocation

### Batch Mode

| Resource    | Value           | Rationale                                                                     |
| ----------- | --------------- | ----------------------------------------------------------------------------- |
| CPU         | 2 vCPU          | Feature computation over sports odds and market movement data                 |
| Memory      | 8 Gi            | Loads odds history across multiple sports, leagues, and bookmakers            |
| Timeout     | 86400 s (24 hr) | Maximum — sports universe is broad (multiple leagues, many markets per event) |
| Max retries | 3               | Idempotent; retry on transient API or GCS failures                            |

## Cost Estimate

GCP Cloud Run pricing (vCPU-second: $0.00002400, GB-second: $0.00000250).

| Scenario            | Invocations/day | Avg duration    | Est. monthly cost |
| ------------------- | --------------- | --------------- | ----------------- |
| Daily incremental   | 1               | 1800 s (30 min) | ~$1.10            |
| Historical backfill | 1               | 7200 s (2 hr)   | ~$4.30            |

Assumptions: 2 vCPU + 8 Gi; daily incremental is the typical pattern during sporting seasons.

## Data Flow

- **Source:** GCS market data buckets (SPORTS category tick data from market-tick-data-service)
- **Sink:** GCS features bucket (sports features)

## Special Requirements

- Upstream dependency: market-tick-data-service must have fetched SPORTS category data (via OpticOdds/OddsJam scrapers)
- Sports API keys required in Secret Manager: `odds-api-key`, `oddsjam-api-key`, `opticodds-api-key`
- Features are only relevant during active sporting seasons; batch frequency can be reduced in off-season

## Source References

- `deployment-service/terraform/services/features-sports-service/gcp/variables.tf`
