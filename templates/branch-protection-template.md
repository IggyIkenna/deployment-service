# Deployment Service — Branch Protection Template

> **Canonical SSOT:** [ci-cd-flow](../../unified-trading-pm/codex/08-workflows/ci-cd-flow.md). This file carries only
> deployment-service-specific details. The canonical required-check set, branch-protection model (ruleset + classic),
> and promotion-gate contract live in the codex SSOT above — **do not duplicate them here**; if this file disagrees with
> codex, codex wins.

## deployment-service-specific notes

- Required check on protected branches: `quality-gates-v2` (the single required check across all repos — the codex SSOT
  above is authoritative; older `quality-gates` naming here is superseded).
- Branch protection is enforced as **both** a ruleset and a classic rule.
- Run local quality gates before opening a PR: `bash scripts/quality-gates.sh` (mirrors CI).

Everything else that used to live in this template (approval counts, status-check enumeration, timeout tables, emergency
hotfix procedure) is owned by the codex ci-cd-flow SSOT above.
