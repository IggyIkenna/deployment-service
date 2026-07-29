<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.md`, `data_status_multi_axis_shard_propagation_2026_05_06.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Documentation Index

**Last Updated:** February 28, 2026

**Start here.** This index maps common tasks to the right documents.

---

## Optimization

| Task                                            | Document                                                                     |
| ----------------------------------------------- | ---------------------------------------------------------------------------- |
| GCS lifecycle policy (save 76% storage)         | [GCS_LIFECYCLE_AGGRESSIVE_STRATEGY.md](GCS_LIFECYCLE_AGGRESSIVE_STRATEGY.md) |
| Cost analysis (lifecycle, backfill, monitoring) | [COST.md](COST.md)                                                           |

---

## Quick Links

### Setup & Infrastructure

| Task                                        | Document                                                 |
| ------------------------------------------- | -------------------------------------------------------- |
| First-time setup, validation                | [SETUP.md](SETUP.md)                                     |
| GCP/AWS access, secrets, builds, quotas     | [INFRASTRUCTURE.md](INFRASTRUCTURE.md)                   |
| Deployment UI (dashboard, Cloud Build logs) | (see deployment-ui repo)                                 |
| GCS FUSE on VMs (faster I/O)                | [GCS_FUSE_VM_SETUP.md](GCS_FUSE_VM_SETUP.md)             |
| Multi-cloud migration (GCP → AWS)           | [MIGRATION.md](MIGRATION.md)                             |
| AWS migration execution details             | [AWS_MIGRATION_EXECUTION.md](AWS_MIGRATION_EXECUTION.md) |

### Deployment & Operations

| Task                                                         | Document                                                                                                                |
| ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| **Runtime tiers (T0-T6) & cluster orchestration**            | [codex: runtime-tiers-and-deployment.md](../../unified-trading-codex/05-infrastructure/runtime-tiers-and-deployment.md) |
| **Cluster configs** (cefi, tradfi, defi, sports, full)       | [configs/clusters/](../configs/clusters/)                                                                               |
| **Cluster CLI** (bootstrap, teardown, batch, live, schedule) | See `deploy-shards cluster --help`                                                                                      |
| **Deployment API** (for Deployment UI)                       | `POST /deployment/clusters/{name}/bootstrap`, `GET /deployment/live/status`, etc.                                       |
| Deploy services, sharding, data catalog                      | [CLI.md](CLI.md)                                                                                                        |
| **Live mode:** data-status --mode live, live job monitoring  | [LIVE_MODE.md](LIVE_MODE.md)                                                                                            |
| Backfill runbook, troubleshooting                            | [RUNBOOKS.md](RUNBOOKS.md)                                                                                              |
| Cache strategy, deployment state, VM self-deletion           | [CACHE_AND_STATE.md](CACHE_AND_STATE.md)                                                                                |
| Dashboard UI spec                                            | [UI_SPEC.md](UI_SPEC.md)                                                                                                |

### Quality & Hardening

| Task                                   | Document                                                                             |
| -------------------------------------- | ------------------------------------------------------------------------------------ |
| Hardening checklist, 4-stage readiness | [HARDENING.md](HARDENING.md)                                                         |
| Service audit framework                | [COMPREHENSIVE_SERVICE_AUDIT_FRAMEWORK.md](COMPREHENSIVE_SERVICE_AUDIT_FRAMEWORK.md) |
| E2E specs (MDPS, features-calendar)    | [E2E_SPECS.md](E2E_SPECS.md)                                                         |
| Testing walkthrough                    | [TESTING.md](TESTING.md)                                                             |

### ML & Data

| Task                                | Document                                                       |
| ----------------------------------- | -------------------------------------------------------------- |
| BigQuery external tables setup      | [BIGQUERY_INTEGRATION_GUIDE.md](BIGQUERY_INTEGRATION_GUIDE.md) |
| GCS paths, schema, key=value format | [GCS_AND_SCHEMA.md](GCS_AND_SCHEMA.md)                         |

### Cost & Optimization

| Task                                       | Document                                                                     |
| ------------------------------------------ | ---------------------------------------------------------------------------- |
| Cost analysis (storage, compute, BigQuery) | [COST.md](COST.md)                                                           |
| GCS lifecycle policy (aggressive strategy) | [GCS_LIFECYCLE_AGGRESSIVE_STRATEGY.md](GCS_LIFECYCLE_AGGRESSIVE_STRATEGY.md) |

---

## By Role

**New joiner:** SETUP → TESTING → CLI → INDEX (this file)
**Deployment ops:** CLI → RUNBOOKS → CACHE_AND_STATE
**Hardening / checklist:** HARDENING → COMPREHENSIVE_SERVICE_AUDIT_FRAMEWORK
**ML / features:** GCS_AND_SCHEMA → BIGQUERY_INTEGRATION_GUIDE
**Multi-cloud / AWS:** MIGRATION → AWS_MIGRATION_EXECUTION → INFRASTRUCTURE
**Cost optimization:** COST → GCS_LIFECYCLE_AGGRESSIVE_STRATEGY

---

## Sports Betting Features

| Task                                | Document                                                                                                                                      |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Sports feature pipeline integration | [../unified-trading-codex/04-architecture/sports-integration-plan.md](../../unified-trading-codex/04-architecture/sports-integration-plan.md) |
| Sports feature horizons             | [../unified-trading-codex/04-architecture/sports-feature-horizons.md](../../unified-trading-codex/04-architecture/sports-feature-horizons.md) |

---

## Config Layout

See [configs/README.md](../configs/README.md) for config directory structure and checklist usage.
