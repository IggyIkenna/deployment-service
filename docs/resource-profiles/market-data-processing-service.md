<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.md`, `data_status_multi_axis_shard_propagation_2026_05_06.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Resource Profile: market-data-processing-service

## Deployment Mode

- Mode: batch
- Cloud Run region: asia-northeast1
- Execution: Cloud Run Job (standard shards) | Compute Engine VM (large venue overrides)

## Resource Allocation

### Batch Mode (Standard Cloud Run)

| Resource    | Value           | Rationale                                                                                 |
| ----------- | --------------- | ----------------------------------------------------------------------------------------- |
| CPU         | 2 vCPU          | Pandas/numpy OHLCV aggregation; parallelised by instrument                                |
| Memory      | 8 Gi            | Buffers all instruments per venue for a single day; typical venue has 200–500 instruments |
| Timeout     | 86400 s (24 hr) | Maximum — processing all timeframes × all venues can span many hours                      |
| Max retries | 3               | Idempotent; retry on transient GCS failures                                               |

### Compute Engine VM Mode (Large Venues)

| Resource     | Value         | Rationale                                       |
| ------------ | ------------- | ----------------------------------------------- |
| Machine type | c2-standard-8 | 8 vCPU, 32 GB RAM — base spec                   |
| Disk         | 80 GB         | At least 2x RAM for swap + temp parquet staging |
| Preemptible  | No            | Non-preemptible to avoid mid-run interruption   |

### Per-Venue Overrides (Cloud Run)

| Venue           | CPU    | Memory | Rationale                                   |
| --------------- | ------ | ------ | ------------------------------------------- |
| BINANCE-SPOT    | 8 vCPU | 32 Gi  | 2000+ instruments; default 8 Gi causes OOM  |
| BINANCE-FUTURES | 8 vCPU | 32 Gi  | Equivalent instrument depth to BINANCE-SPOT |

### Per-Venue Overrides (VM)

| Venue           | Machine Type   | Disk   | Rationale                                            |
| --------------- | -------------- | ------ | ---------------------------------------------------- |
| BINANCE-SPOT    | c2-standard-16 | 150 GB | 64 GB RAM for pandas operations on 2000+ instruments |
| BINANCE-FUTURES | c2-standard-16 | 150 GB | Same as BINANCE-SPOT                                 |

## Cost Estimate

GCP Cloud Run pricing (vCPU-second: $0.00002400, GB-second: $0.00000250).

| Scenario                       | Invocations/day | Avg duration    | Est. monthly cost |
| ------------------------------ | --------------- | --------------- | ----------------- |
| Daily (standard venues)        | 10              | 1800 s (30 min) | ~$25              |
| BINANCE shard (8 vCPU / 32 Gi) | 2               | 3600 s (1 hr)   | ~$21              |
| Full daily pipeline            | all shards      | various         | ~$50–$80          |

Assumptions: Sharded across category × venue × timeframe; many parallel Cloud Run Job tasks per day.

## Data Flow

- **Source:** GCS market data buckets (raw tick data: `uts-prod-market-data-cefi/tradfi/defi`)
- **Sink:** GCS market data buckets (processed candles, `candles/` prefix, parquet format)
- **Event bus:** Publishes `DATA_READY` event on PubSub after completion

## Candle Timeframes Produced

`15s, 1m, 5m, 15m, 1h, 4h, 24h`

## Special Requirements

- Upstream dependency: market-tick-data-service must have completed for the target date
- DEFI category not supported for all data types (limited to venues with structured tick data)
- GCSFuse disabled: large buckets cause OOM; all I/O via GCS API

## Source References

- `deployment-service/terraform/services/market-data-processing-service/gcp/variables.tf`
- `deployment-service/configs/services/market-data-processing-service/batch.env`
- `deployment-service/configs/sharding.market-data-processing-service.yaml`
