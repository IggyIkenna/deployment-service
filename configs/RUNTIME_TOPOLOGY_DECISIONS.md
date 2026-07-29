# Deployment Service — Runtime Topology Decisions

> **Canonical SSOT:** [runtime-deployment-topology](../../unified-trading-pm/codex/04-architecture/runtime-deployment-topology.md)
> (per-service behavior, pipeline layers, cluster shapes, diagrams). The machine-readable wiring SSOT is
> `unified-trading-pm/configs/runtime-topology.yaml`. This file carries only deployment-service-specific details — **do
> not duplicate the topology decisions here**; if this file disagrees with codex, codex wins.

## deployment-service-specific notes

- **Machine-readable SSOT:** `unified-trading-pm/configs/runtime-topology.yaml` (owned by PM).
- **Companion diagram (repo-local):** `configs/RUNTIME_DEPLOYMENT_TOPOLOGY_DAG.svg` — the rendered DAG lives in this
  repo's `configs/` and is regenerated from the topology YAML.
- Legacy node names (`live-health-monitor-ui`, `logs-dashboard-ui`, `batch-audit-ui`, `onboarding-ui`, `batch-audit-api`,
  `odum-research-website`) are archived / superseded by `unified-trading-system-ui`, `deployment-ui`,
  `unified-trading-api`, and `auth-api`.

The naming conventions, UI→API→Service chain rules, cluster-shape taxonomy, and every topology decision rationale are
owned by the codex runtime-deployment-topology SSOT above.
