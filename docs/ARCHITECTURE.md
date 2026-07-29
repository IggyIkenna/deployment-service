# Deployment Service — Architecture

> **Canonical SSOT:** [deployment-clusters-live-vs-batch](../../unified-trading-pm/codex/05-infrastructure/deployment-clusters-live-vs-batch.md).
> This file carries only deployment-service-specific details. The cross-cutting live-cluster-vs-batch-cluster taxonomy,
> shard-isolation rule, and deployment-mechanism contracts live in the codex SSOT above — **do not duplicate them here**;
> if this file disagrees with codex, codex wins.

## deployment-service-specific architecture

Deployment service is the orchestration engine for deploying and managing trading-system services. It provides the
`deploy-shards` CLI, config loading, shard calculation, and Cloud Build integration. It is **not** a long-lived service —
`deployment-api` orchestrates it as a library.

### Key components (repo-local)

- **Config Loader** — loads YAML configs from `configs/`.
- **Shard Calculator** — computes deployment shards from config.
- **Orchestrator** — runs deployments via Cloud Build.
- **Runtime Topology** — SSOT in `configs/runtime-topology.yaml`.

### Data flow (repo-local)

`configs/ → ConfigLoader → ShardCalculator → Orchestrator → Cloud Build API`

Every multi-worker batch cluster sets `VM_NAME=<unique>` + `MANIFEST_PER_VM_SHARDS=true` (UTL runtime guard
`MultiWorkerWithoutShardIsolationError`). All deployment scripts live in deployment-service, not in individual services.
