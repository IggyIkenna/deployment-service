# Deployment Service — UI Specification

> **Canonical SSOT:** [deployment-ui-architecture](../../unified-trading-pm/codex/05-infrastructure/deployment-ui-architecture.md)
> (6 tabs, 4 lifecycle classes, 4 orthogonal axes). This file carries only deployment-service-specific details. The
> deployment-UI architecture, tab layout, and lifecycle-class model live in the codex SSOT above — **do not duplicate
> them here**; if this file disagrees with codex, codex wins.

## deployment-service-specific UI contract

The deployment/monitoring UI (`deployment-ui`) is backed by `deployment-api`, which wraps this service's CLI. The single
load-bearing repo-local principle:

- **CLI-first.** Every UI action MUST map to a `deploy-shards` / deployment CLI command. If a UI feature needs an arg the
  CLI lacks, add it to the CLI first. CLI and UI must each work independently; the CLI is the safety net.
- **Cloud-agnostic.** UI operations route through `CLOUD_PROVIDER` mode (GCP/AWS) — users never touch the cloud console
  for routine deploy/monitor actions.
- CLI surface lives in the deployment-service `cli.py`; the full UI screen/flow spec is owned by the codex SSOT above.
