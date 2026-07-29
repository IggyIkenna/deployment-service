# Deployment Service — Deployment Guide

> **Canonical SSOT:** [vm-tarball-deployment](../../unified-trading-pm/codex/05-infrastructure/vm-tarball-deployment.md).
> This file carries only deployment-service-specific details. The tarball-VM architecture, invariants, and code-tarball
> contract live in the codex SSOT above — **do not duplicate them here**; if this file disagrees with codex, codex wins.

## deployment-service-specific guide

Deployment service runs as a CLI or is invoked by `deployment-api` (which depends on it as a library). It does not run
as a long-lived service.

```bash
deploy-shards --config-dir configs/ ...
```

### Two deployment surfaces

| Surface                                                   | When                      | How                                                                                                       |
| --------------------------------------------------------- | ------------------------- | --------------------------------------------------------------------------------------------------------- |
| **Long-lived services** (Cloud Run)                       | Production batch / live   | Docker image via Artifact Registry, deployed by deployment-api                                            |
| **Backfill / migration / smoke / forward-poll VMs** (GCE) | Ad-hoc data-pipeline runs | Tarball-from-GCS via `scripts/vm/setup-data-pipeline-vm.sh` + `gs://deployment-scripts-.../code/*.tar.gz` |

Every `launch-*.sh` in `scripts/vm/` uses the tarball VM pattern — operational howto in
[`scripts/vm/README.md`](../scripts/vm/README.md); architecture + invariants in the codex SSOT above.

### Refresh tarballs after every code change

`bash scripts/vm/create-code-tarballs.sh <flag>`:

- `--all` — safest for any multi-repo feature
- `--asset-group SPORTS|CEFI|TRADFI|DEFI|PREDICTION` — scopes to a category's pipeline
- `--include <repo>` — one-off addition
- bare invocation only re-tars CORE (UAC/UTL/MTDS/deployment-service)

Forgetting the flag silently runs stale code on VMs with no error signal.

### Singleton-locked launchers

Adapters with shared API keys / per-IP rate limits use a singleton-lock in the launcher (refuse to launch if a
same-prefix VM is RUNNING in the zone; pass `--force` as the first arg to bypass for legitimate parallel work):

- `launch-sfi-forward-poll.sh` — SFI/RapidAPI per-key rate limit
- `launch-mtds-prediction-backfill-vm.sh` — Polymarket gamma per-IP rate limit (shared NAT)
