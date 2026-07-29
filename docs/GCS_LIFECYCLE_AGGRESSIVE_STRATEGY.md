# Deployment Service — GCS Lifecycle (Aggressive Strategy)

> **Canonical SSOT:** [gcs-lifecycle-policies](../../unified-trading-pm/codex/05-infrastructure/gcs-lifecycle-policies.md).
> This file carries only deployment-service-specific details. The canonical lifecycle-tier thresholds, cost + list-latency
> rationale, and rollout model live in the codex SSOT above — **do not duplicate them here**; if this file disagrees with
> codex, codex wins.

## deployment-service-specific lifecycle application

Apply / update lifecycle policies on deployment-managed buckets via the repo script:

```bash
cd deployment-service
./scripts/setup-gcs-lifecycle-policies.sh
```

The aggressive-tiering rationale (most data is rarely accessed after generation, so it should move to cold storage fast)
and the exact age thresholds (STANDARD → NEARLINE → COLDLINE → ARCHIVE), minimum-duration charges, and rollback policy
are owned by the codex SSOT above. The dollar-savings tables that previously lived in this file are stale point-in-time
estimates — do not treat them as authoritative.
