# Deployment Service — GCS Paths

<!-- POST_PLAN_SECTION_2026_05_06 -->

## Post-2026-05-06 additions

**Post-2026-05-06 additions** — `asset_group=` is canonical hive vocab for new writes; `category=` is legacy preserved on disk (do NOT rekey). Sports per-fixture data: manifest grain `(league_id, day)` per multi-axis plan; per-fixture detail at parquet row-level. Predictions canonical_question_group: manifest grain `(canonical_question_group, day)`; per-market detail at parquet row-level. Path templates per asset_group in [SHARDING_AND_DATA_ALIGNMENT.md](SHARDING_AND_DATA_ALIGNMENT.md).

**Workspace SSOTs**: [POST_PLAN_REALITY](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) (10 cross-cutting principles + active plans), [availability-manifest-and-data-status](../../unified-trading-pm/codex/02-data/availability-manifest-and-data-status.md), [deployment-clusters-live-vs-batch](../../unified-trading-pm/codex/05-infrastructure/deployment-clusters-live-vs-batch.md), [shard-level-failure-isolation](../../unified-trading-pm/codex/04-architecture/shard-level-failure-isolation.md), [error-handling](../../unified-trading-pm/codex/06-coding-standards/error-handling.md), [validation-patterns](../../unified-trading-pm/codex/06-coding-standards/validation-patterns.md).

## Bucket Pattern

State and build artifacts use GCS. Bucket from config or STATE_BUCKET env.

## Path Templates

- Deployment state: `{bucket}/deployment/state/`
- Build logs: via Cloud Build API

Variables: `{bucket}` from config.
