<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.md`, `data_status_multi_axis_shard_propagation_2026_05_06.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# External and Internal Specs (Condensed)

**Last consolidated:** 2026-02-09

This document condenses external contractor specs, infrastructure specs, and operational notes.

---

## Shahriyar Deployment Infrastructure Spec

**Focus:** Integration with existing sharding system. Use ShardCalculator from `src/shard_calculator.py`, configs in `configs/sharding.*.yaml`, `configs/venues.yaml`. CloudClient in `src/cloud_client.py` is cloud-agnostic. Deploy via `python deploy.py` or `python -m deployment_service.cli deploy`. See [docs/CLI.md](CLI.md) for full CLI reference.

---

## Paradise Getting Started

**Sprint onboarding:** (1) Open deployment dashboard; (2) Read repeatable workflow (11 steps); (3) Set up market-tick-data-handler locally. Pipeline order: instruments → market-tick → market-data-processing → features-_ → ml-_ → strategy → execution. Data Status tab shows per-service completion. See [docs/SETUP.md](SETUP.md) and [docs/TESTING.md](TESTING.md).

---

## Contractor Trial Spec (Completed)

**Scope:** Deployment UI bug fixes and optimization. Trial completed Feb 2026. Project: test-project. Region: asia-northeast1. Failover: europe-west1, us-central1. Service pipeline documented in [docs/HARDENING.md](HARDENING.md).

---

## Dynamic Dimension Services

**Services with dynamic dimensions** (completion % not calculable): execution-service, ml-training-service, strategy-service. Configs come from GCS (`grid_configs/*.json`, etc.). Data-status shows file counts and timestamps instead of percentages. See [docs/CLI.md](CLI.md) Data Catalog section.

---

## Unified Trading Deployment Tweaks

**Known issues:** (1) Status flickering — concurrent writers (auto-sync, manual refresh, frontend poll) update state.json; (2) Zombie VMs — VMs that don't self-delete; (3) Incorrect terminal status — completed vs completed_with_errors semantics. **Status types:** ShardStatus (pending, running, succeeded, failed, cancelled), DeploymentStatus (pending, running, completed, completed_with_errors, completed_with_warnings, failed, cancelled). Key files: `deployment/state.py`, `api/routes/deployments.py`, `api/main.py`, `backends/vm.py`. See [docs/CACHE_AND_STATE.md](CACHE_AND_STATE.md) for state management details.
