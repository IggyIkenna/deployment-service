# Deployment Service — Infrastructure, Access, and Quotas

> **Canonical SSOT:** [auth-setup](../../unified-trading-pm/codex/05-infrastructure/auth-setup.md) (auth / IAM / secrets)
> and [cicd-setup](../../unified-trading-pm/codex/05-infrastructure/cicd-setup.md) (Cloud Build triggers). This file
> carries only deployment-service-specific details — **do not duplicate the cross-cutting auth/IAM contract here**; if
> this file disagrees with codex, codex wins.

## deployment-service-specific infrastructure

- **Region:** `asia-northeast1` (Tokyo); failover `europe-west1`, `us-central1`.
- **Deployment state bucket:** `deployment-orchestration-{project}` — separate from `terraform-state-{project}`.

### Required GCP APIs

Cloud Run · Compute Engine · Cloud Storage · Cloud Workflows · Cloud Scheduler · Secret Manager · Artifact Registry.

### Cloud Build triggers (repo-local CLI)

All triggers live in `asia-northeast1` (not global). Manage from the CLI:

```bash
bash scripts/setup-cloud-build-triggers.sh setup        # add repos, create missing triggers
bash scripts/setup-cloud-build-triggers.sh list
bash scripts/setup-cloud-build-triggers.sh run-ordered  # run builds in dependency order
```

Build order: libraries first (config → events → UCS → domain → market/order/algo), then services (instruments →
market-tick → market-data-processing → execution → others).

### Quota target (repo-local)

~20,000 concurrent shards for a 6-year backfill; heaviest is market-tick-data-handler (26 venues × 2,190 days). Cap
`--max-concurrent 2000` (hard limit 2500).

Secret inventory and IAM role bindings are owned by the codex auth-setup SSOT above.
