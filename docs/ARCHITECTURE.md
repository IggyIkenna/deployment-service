<!-- POST_PLAN_BANNER_2026_05_06 -->

> **POST-PLAN REALITY (2026-05-06)** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) BEFORE making code or doc changes informed by this doc. This doc is partially stale: missing per-VM shard isolation rule (`MANIFEST_PER_VM_SHARDS=true` + unique `VM_NAME`) for concurrent backfills. The post-plan-reality doc lists the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision, cluster validation mandatory, per-row write-time `available_at`, prediction lifecycle timing, temporary state must have named successor, per-VM shard isolation, etc.) plus the active plans where the canonical post-plan reality is being implemented (`writegate_honest_coverage_endtoend_2026_05_06.plan.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.plan.md`). If this doc and the active plans disagree, the plans win. If you find a contradiction the plans don't address, flag to user — don't decide unilaterally.

# Deployment Service — Architecture

## Overview

Deployment service is the orchestration engine for deploying and managing trading system services. It provides CLI (`deploy-shards`), config loading, shard calculation, and Cloud Build integration.

## Key Components

- **Config Loader**: Loads YAML configs from configs/ directory
- **Shard Calculator**: Computes deployment shards from config
- **Orchestrator**: Runs deployments via Cloud Build
- **Runtime Topology**: SSOT in configs/runtime-topology.yaml

## Data Flows

Configs → ConfigLoader → ShardCalculator → Orchestrator → Cloud Build API
