<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.md`, `data_status_multi_axis_shard_propagation_2026_05_06.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Resource Profile: market-tick-data-service

## Deployment Mode

- Mode: both (batch download + live streaming)
- Cloud Run region: asia-northeast1
- Execution: Cloud Run Job (batch) | Compute Engine VM (large shards) | Cloud Run Service (live)

## Resource Allocation

### Batch Mode (Standard Cloud Run)

| Resource    | Value           | Rationale                                                                                                    |
| ----------- | --------------- | ------------------------------------------------------------------------------------------------------------ |
| CPU         | 8 vCPU          | 8 concurrent download workers; each worker handles one instrument/data-type shard                            |
| Memory      | 64 Gi           | 8 workers × ~1.2 GB peak per large parquet file × 1.5 safety margin; sized from GCS file analysis (Jan 2026) |
| Timeout     | 86400 s (24 hr) | Maximum — CEFI trades with full book data can take many hours                                                |
| Max retries | 3               | Idempotent; retry on transient exchange API or GCS failures                                                  |

Note: Terraform `variables.tf` defaults (4 vCPU / 16 Gi) are conservative lower bounds. Sharding YAML (8 vCPU / 64 Gi) reflects actual sizing required for production runs.

### Live Mode

| Resource      | Value  | Rationale                                                  |
| ------------- | ------ | ---------------------------------------------------------- |
| CPU           | 4 vCPU | Parallel WebSocket streams per venue                       |
| Memory        | 16 Gi  | Live order book state + flush buffer for parquet writes    |
| Min instances | 1      | Always-on; streams must not drop during live trading hours |

## VM Override (Required for COINBASE Shard)

> **CRITICAL:** The COINBASE shard CANNOT run on Cloud Run. The largest BTC-USD order book snapshot files are 409 MB on disk (~1.2 GB in memory per file). With 8 concurrent workers, peak memory is ~10 GB per file × 8 = ~80 GB+ in worst case. Cloud Run max is 32 Gi. COINBASE must run on Compute Engine VM.

| Resource     | Value          | Rationale                                                          |
| ------------ | -------------- | ------------------------------------------------------------------ |
| Machine type | c2-standard-60 | 60 vCPU, 240 GB RAM — maximum C2 instance                          |
| Disk         | 500 GB         | Must be ≥ 2x RAM (240 GB) for swap + temp staging files            |
| Preemptible  | No             | Non-preemptible; PREEMPTIBLE_CPUS quota only 16 in asia-northeast1 |
| Timeout      | 86400 s        | 24 hours maximum                                                   |

### Standard VM (Non-COINBASE Shards)

| Resource     | Value          | Rationale             |
| ------------ | -------------- | --------------------- |
| Machine type | c2-standard-16 | 16 vCPU, 64 GB RAM    |
| Disk         | 150 GB         | ≥ 2x RAM for swap     |
| Preemptible  | No             | Same quota constraint |

## Cost Estimate

GCP Cloud Run pricing (vCPU-second: $0.00002400, GB-second: $0.00000250).

| Scenario                               | Invocations/day | Avg duration | Est. monthly cost |
| -------------------------------------- | --------------- | ------------ | ----------------- |
| CEFI trades (standard, 8 vCPU / 64 Gi) | 10 shards       | 3600 s       | ~$140             |
| COINBASE VM (c2-standard-60)           | 1               | 3600 s       | ~$25 (VM pricing) |
| SPORTS/DEFI                            | 5 shards        | 1200 s       | ~$20              |
| Full daily batch                       | all shards      | various      | ~$200–$300/month  |

Assumptions: Most expensive service in the pipeline due to data volume. CEFI trades with 8 concurrent workers is the dominant cost driver.

## Data Flow

- **Source:** Exchange APIs (CeFi: Binance, Deribit, Coinbase, etc. via UTEI adapters; TradFi: IBKR, Bloomberg; DeFi: Uniswap; Sports: OpticOdds, OddsJam)
- **Sink:** GCS market data buckets (raw tick data, parquet format):
  - `uts-prod-market-data-cefi`
  - `uts-prod-market-data-tradfi`
  - `uts-prod-market-data-defi`
- **Event bus:** Publishes `tick_data.received` event on PubSub after each batch flush

## Special Requirements

- GCSFuse disabled: memory overhead too high for large buckets; all I/O via GCS API
- COINBASE shard requires c2-standard-60 VM (exceeds all Cloud Run memory limits at 128 Gi max)
- Exchange API keys required in Secret Manager: betfair-app-key, polymarket-private-key, coinglass-api-key, hyblock-api-key, metabet-api-key
- Sports/DeFi-live/AltData adapters loaded via adapter_loader.py (NOT the UMI factory VENUE_REGISTRY)
- Max download workers: 8 (DOWNLOAD_MAX_WORKERS=8 env var)

## Sharding Dimensions

`category × venue × instrument_type × data_type × date`

This maximises parallelism — each combination runs as an independent Cloud Run Job or VM.

## Source References

- `deployment-service/terraform/services/market-tick-data-service/gcp/variables.tf`
- `deployment-service/configs/services/market-tick-data-service/live.env`
- `deployment-service/configs/sharding.market-tick-data-service.yaml`
