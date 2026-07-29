# Deployment Service — GCS Paths

> **Canonical SSOT:** [path-registry](../../unified-trading-pm/codex/05-infrastructure/path-registry.md). This file
> carries only deployment-service-specific details. The canonical bucket-naming and hive-partition path templates
> (`asset_group=` for new writes, `category=` legacy-preserved) live in the codex SSOT above — **do not duplicate them
> here**; if this file disagrees with codex, codex wins.

## deployment-service-specific paths

Deployment state and build artifacts live in GCS. The bucket resolves from config or the `STATE_BUCKET` env var.

- **Deployment state:** `{bucket}/deployment/state/` (bucket = `deployment-orchestration-{project}`, separate from the
  Terraform-state backend).
- **Build logs:** via the Cloud Build API (no GCS object of its own).

Per-asset-group data path templates are owned by the codex path-registry SSOT above, not this file.
