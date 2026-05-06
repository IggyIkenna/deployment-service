# Deployment Service — Schema Validation

<!-- POST_PLAN_SECTION_2026_05_06 -->

## Post-2026-05-06 additions

**Post-2026-05-06 additions** — schema validation now part of 4-pillar write-gate at `ManifestWriter.record_captured`: row count > 0 + NaN ratio < threshold + schema matches contract + cluster coverage ≥ expected for bundled types. `available_at` column required per row.

**Workspace SSOTs**: [POST_PLAN_REALITY](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) (10 cross-cutting principles + active plans), [availability-manifest-and-data-status](../../unified-trading-pm/codex/02-data/availability-manifest-and-data-status.md), [deployment-clusters-live-vs-batch](../../unified-trading-pm/codex/05-infrastructure/deployment-clusters-live-vs-batch.md), [shard-level-failure-isolation](../../unified-trading-pm/codex/04-architecture/shard-level-failure-isolation.md), [error-handling](../../unified-trading-pm/codex/06-coding-standards/error-handling.md), [validation-patterns](../../unified-trading-pm/codex/06-coding-standards/validation-patterns.md).

## Schema Location

- Config schemas in config_loader, config_validator
- runtime-topology.yaml validated against topology schema

## Validation Approach

- Config YAML validated at load
- Topology validated per RUNTIME_TOPOLOGY_DECISIONS.md
