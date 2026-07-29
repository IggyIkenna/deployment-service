# Deployment Service — Schema Validation

> **Canonical SSOT:** [availability-manifest-and-data-status](../../unified-trading-pm/codex/02-data/availability-manifest-and-data-status.md).
> This file carries only deployment-service-specific details. The write-gate / `record_captured` schema-validation
> contract (row count > 0, NaN ratio, schema match, cluster coverage, `available_at` per row) lives in the codex SSOT
> above — **do not duplicate it here**; if this file disagrees with codex, codex wins.

## deployment-service-specific validation

Deployment service validates its own **config** inputs (not data manifests):

- Config schemas enforced in `config_loader` / `config_validator` at load time.
- `configs/runtime-topology.yaml` validated against the topology schema (see `configs/RUNTIME_TOPOLOGY_DECISIONS.md`).

Data-manifest / parquet schema validation is a UTL write-gate concern documented in the codex SSOT above.
