# Deployment Service — Architecture

<!-- POST_PLAN_SECTION_2026_05_06 -->

## Post-2026-05-06 additions

**deployment-service post-2026-05-06**: per-VM shard isolation rule codified — every multi-worker batch cluster sets `VM_NAME=<unique>` + `MANIFEST_PER_VM_SHARDS=true`. UTL runtime guard `MultiWorkerWithoutShardIsolationError`. New base-service.sh QG STEP 5.66. Two deployment mechanisms (both eventually selectable in deployment-UI): tarball (current, fast iteration) + Cloud Build (immutable container, audit trail). All deployment scripts live in deployment-service NOT individual services. See [`unified-trading-pm/codex/05-infrastructure/deployment-clusters-live-vs-batch.md`](../../unified-trading-pm/codex/05-infrastructure/deployment-clusters-live-vs-batch.md) for live cluster vs batch cluster taxonomy.

**Workspace SSOTs**: [POST_PLAN_REALITY](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) (10 cross-cutting principles + active plans), [availability-manifest-and-data-status](../../unified-trading-pm/codex/02-data/availability-manifest-and-data-status.md), [deployment-clusters-live-vs-batch](../../unified-trading-pm/codex/05-infrastructure/deployment-clusters-live-vs-batch.md), [shard-level-failure-isolation](../../unified-trading-pm/codex/04-architecture/shard-level-failure-isolation.md), [error-handling](../../unified-trading-pm/codex/06-coding-standards/error-handling.md), [validation-patterns](../../unified-trading-pm/codex/06-coding-standards/validation-patterns.md).

## Overview

Deployment service is the orchestration engine for deploying and managing trading system services. It provides CLI (`deploy-shards`), config loading, shard calculation, and Cloud Build integration.

## Key Components

- **Config Loader**: Loads YAML configs from configs/ directory
- **Shard Calculator**: Computes deployment shards from config
- **Orchestrator**: Runs deployments via Cloud Build
- **Runtime Topology**: SSOT in configs/runtime-topology.yaml

## Data Flows

Configs → ConfigLoader → ShardCalculator → Orchestrator → Cloud Build API
