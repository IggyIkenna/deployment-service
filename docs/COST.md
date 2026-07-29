# Deployment Service — Cost

> **Canonical SSOT:** [billing-cost-observability](../../unified-trading-pm/codex/05-infrastructure/billing-cost-observability.md)
> (billing export + cost observability) and
> [gcs-lifecycle-policies](../../unified-trading-pm/codex/05-infrastructure/gcs-lifecycle-policies.md) (storage-tier cost
> controls). This file carries only deployment-service-specific pointers — **do not duplicate the cost model here**; if
> this file disagrees with codex, codex wins.

## deployment-service-specific cost notes

- Primary region `asia-northeast1`; GCS external tables are $0 storage (queries bill per TB scanned).
- Storage cost is driven mostly by the aggressive lifecycle tiering (STANDARD → NEARLINE → COLDLINE → ARCHIVE) applied
  via `scripts/setup-gcs-lifecycle-policies.sh` — the tiering thresholds and savings model are owned by the
  gcs-lifecycle-policies codex SSOT above.
- Compute cost: backfill/idempotent VMs default to SPOT (~60–91% cheaper); live/forward/paper VMs stay on-demand.

Per-service size estimates and dollar projections that previously lived here are stale point-in-time figures — treat the
codex billing-cost-observability SSOT (live export) as authoritative, not any hardcoded estimate.
