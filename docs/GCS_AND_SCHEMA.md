# Deployment Service — GCS and Schema Reference

> **Canonical SSOT:** [availability-manifest-and-data-status](../../unified-trading-pm/codex/02-data/availability-manifest-and-data-status.md)
> (manifest + schema contract) and [path-registry](../../unified-trading-pm/codex/05-infrastructure/path-registry.md)
> (bucket naming + hive path templates). This file carries only deployment-service-specific details. The canonical
> `key=value` hive-partition format, per-service path templates, bucket-naming patterns, and BigQuery-external-table
> guidance live in the codex SSOTs above — **do not duplicate them here**; if this file disagrees with codex, codex wins.

## deployment-service-specific notes

- `asset_group=` is the canonical hive vocab for new writes; `category=` is legacy preserved on disk (do **not** rekey).
- Bundled data_types requiring cluster validation at write (`record_captured`): `options_chain`, `futures_chain`,
  `prediction_canonical_question_group`, and sports per-fixture odds bundles.
- BigQuery external tables over deployment-managed buckets are created via
  `deployment-service/scripts/create_bigquery_external_tables.sh` once data exists (external-table storage is $0; queries
  bill per TB scanned).

Everything else that used to live here (the full `key=value` format tables, per-service path matrix, bucket-naming
matrix) is now owned by the codex SSOTs above.
