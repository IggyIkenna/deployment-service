<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.md`, `data_status_multi_axis_shard_propagation_2026_05_06.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Resource Profile: ml-training-service

## Deployment Mode

- Mode: batch
- Cloud Run region: asia-northeast1
- Execution: Cloud Run Job (standard) | Compute Engine VM (large training runs)

## Resource Allocation

### Batch Mode (Cloud Run)

| Resource    | Value          | Rationale                                                                               |
| ----------- | -------------- | --------------------------------------------------------------------------------------- |
| CPU         | 4 vCPU         | Parallel feature selection + model training across hyperparameter grid; CPU-intensive   |
| Memory      | 16 Gi          | Walk-forward training loads 6 years of feature history per instrument per period        |
| Timeout     | 14400 s (4 hr) | 4 hours per shard — each operation × instrument × timeframe × target combination        |
| Max retries | 2              | Training is deterministic; retry on transient infra failure; expensive so limit retries |

### Compute Engine VM Mode

| Resource     | Value         | Rationale                                                           |
| ------------ | ------------- | ------------------------------------------------------------------- |
| Machine type | c2-standard-8 | 8 vCPU, 32 GB RAM; suited for feature selection + LightGBM training |
| Disk         | 100 GB        | Feature data staging during training                                |
| GCSFuse      | Disabled      | Large feature buckets cause OOM; use GCS API instead                |

## Cost Estimate

GCP Cloud Run pricing (vCPU-second: $0.00002400, GB-second: $0.00000250).

| Scenario                            | Invocations/day | Avg duration  | Est. monthly cost |
| ----------------------------------- | --------------- | ------------- | ----------------- |
| Single instrument × timeframe       | 1               | 7200 s (2 hr) | ~$8.50            |
| Full training pipeline (all shards) | many            | 14400 s each  | ~$500–$2000/month |
| Monthly rolling update              | 1/month         | 14400 s       | ~$17              |

Assumptions: 4 vCPU + 16 Gi; full training across 4 instruments × 5 timeframes × 2 targets × 72 monthly periods is expensive. Typically run on a schedule (weekly or monthly), not daily.

## Data Flow

- **Source:** GCS features bucket (`uts-prod-features`) — reads pre-computed features
- **Sink:** GCS models bucket (`uts-prod-models/models/`) — writes trained model artefacts
- **Analytics:** BigQuery (`features_data` dataset) for feature queries and experiment tracking
- **Event bus:** Local (no PubSub during training; ml-inference-service polls model store)

## Sharding Dimensions

`operation × instrument × timeframe × target_type × training_period × config`

- `operation`: `train_phase1` | `train_phase2` | `train_phase3`
- `instrument`: BTC, ETH, SOL, SPY
- `timeframe`: 1m, 5m, 15m, 1h, 4h
- `target_type`: swing_high, swing_low
- `training_period`: monthly rolling, 72-month lookback (6 years)
- `config`: JSON grid configs from GCS (`ml-configs-store-{project_id}/training/grid_configs/`)

## Special Requirements

- Upstream dependency: all feature services must be complete and features uploaded to GCS
- GCSFuse disabled on VM: large feature buckets cause OOM; all I/O via GCS API
- Walk-forward methodology: each monthly period trains on expanding window of historical data
- Model naming convention: `{CATEGORY}_{ASSET}_{target-type}_{MODEL-TYPE}_{TIMEFRAME}_V{N}` (e.g., `CEFI_BTC_swing-high_LIGHTGBM_1h_V1`)

## Source References

- `deployment-service/terraform/services/ml-training-service/gcp/variables.tf`
- `deployment-service/configs/services/ml-training-service/batch.env`
- `deployment-service/configs/sharding.ml-training-service.yaml`
