# Deployment Service — Deployment Guide

## Overview

Deployment service runs as a CLI or is invoked by deployment-api. It does not run as a long-lived service; deployment-api orchestrates it.

## Prerequisites

- GCP project, Cloud Build API, Artifact Registry
- GCP_PROJECT_ID set
- Service account with Cloud Build and Storage roles

## Deployment

The deployment service is packaged and invoked by deployment-api. To run CLI locally:

```bash
deploy-shards --config-dir configs/ ...
```

## Via deployment-api

deployment-api depends on deployment-service. Deploy deployment-api; it uses deployment-service as a library.

## Two deployment surfaces

| Surface                                                   | When                         | How                                                                                                           |
| --------------------------------------------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------- |
| **Long-lived services** (Cloud Run)                       | Production batch / live mode | Docker image via Artifact Registry, deployed by deployment-api                                                |
| **Backfill / migration / smoke / forward-poll VMs** (GCE) | Ad-hoc data pipeline runs    | **Tarball-from-GCS** via `scripts/vm/setup-data-pipeline-vm.sh` + `gs://deployment-scripts-.../code/*.tar.gz` |

For the **tarball VM pattern** (every `launch-*.sh` in `scripts/vm/` uses this), see:

- Operational howto: [`scripts/vm/README.md`](../scripts/vm/README.md)
- Architecture + invariants: [`unified-trading-pm/codex/05-infrastructure/vm-tarball-deployment.md`](../../unified-trading-pm/codex/05-infrastructure/vm-tarball-deployment.md)

**Refresh tarballs after every code change** with `bash scripts/vm/create-code-tarballs.sh <flag>`:

- `--all` — safest for any multi-repo feature
- `--category SPORTS|CEFI|TRADFI|DEFI|PREDICTION` — scopes to a category's pipeline
- `--include <repo>` — one-off addition
- bare invocation only re-tars CORE (UAC/UTL/MTDS/deployment-service)

Forgetting the flag silently runs stale code on VMs with no error signal.

## Singleton-locked launchers

Adapters with shared API keys / per-IP rate limits use a singleton-lock pattern in the launcher:

- `launch-sfi-forward-poll.sh` — SFI/RapidAPI per-key rate limit
- `launch-mtds-prediction-backfill-vm.sh` — Polymarket gamma per-IP rate limit (shared NAT)

Both refuse to launch if a same-prefix VM is RUNNING in the zone. Pass `--force` as the first arg to bypass for legitimate parallel investigations. Reference incident: 2026-04-19 SFI thundering herd (10 concurrent VMs / 6 hours / ~4 useful writes).
