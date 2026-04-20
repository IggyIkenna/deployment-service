# deployment-engine

Deployment orchestration engine for the unified trading system. Extracted from `deployment-service`.

## What it does

- Orchestrates T+1 batch jobs with dependency graph and cascade failure propagation
- Tracks data completion across services via the data catalog (supports manifest-based Parquet queries
  via `--source manifest` for fast lookups, or direct GCS blob scans via `--source gcs` -- see
  [docs/cli.md](docs/cli.md#data-source-toggle))
- Loads YAML sharding configuration (venues, services, cloud providers)
- Provides cloud-agnostic storage operations
- Verifies infrastructure connectivity (Layer 2 gate) before deployments

## Structure

```
deployment_engine/
    orchestrator.py     — T+1 job orchestration
    catalog.py          — Data completion tracking
    config_loader.py    — YAML config loading
    cloud_client.py     — Cloud-agnostic storage
    monitor.py          — Deployment progress monitoring
    shard_builder.py    — Shard construction
    shard_calculator.py — Shard calculation
    backends/           — Cloud execution backends (GCP, AWS)
scripts/
    verify_infra.py     — Layer 2 infra connectivity verification
```

## Usage

```bash
# Install
uv pip install -e ".[dev]"

# Verify infrastructure
python scripts/verify_infra.py --project-id my-project

# Run quality gates
bash scripts/quality-gates.sh
```

## Dependencies

- `unified-trading-library` — cloud primitives, event logging
- Arch tier: devops | Merge level: 6

## Related repos

- `deployment-api` — thin FastAPI that imports this package; exposes `/infra/health`
- `deployment-ui` — React UI for deployment management
- `system-integration-tests` — Layer 3a/3b smoke tests triggered post-deploy

## Signal Broadcast wiring (Plan B Phase 4)

Signal leasing (`strategy-service` → external counterparty webhook) is
deployed by extending the existing strategy-service Cloud Run Job — no
67th service. deployment-service owns three things:

- **Per-counterparty HMAC secrets** — provision with:
  `bash scripts/provision-signal-broadcast-secrets.sh <project-id>`.
  Secret-name convention: `signal-broadcast-counterparty-{cp_id}-hmac`.
- **Webhook allowlist + transport defaults** —
  `configs/signal-broadcast/counterparties.yaml`. Source of truth for
  runtime entitlements is UAC `Counterparty` records; this file is the
  deploy-time mirror (Secret Manager coverage + egress allowlist + ops
  catalogue).
- **Cloud Run env injection** —
  `terraform/services/strategy-service/gcp/` mounts the HMAC secrets via
  `secret_environment_variables` and injects the `SIGNAL_BROADCAST_*`
  env vars consumed by `SignalBroadcastConfig`. Per-counterparty rate
  limits are UAC-side (`Counterparty.rate_limit_per_strategy_per_sec`);
  service-wide transport knobs (timeout / retries / backoff / JWT / refresh
  cadence / pull buffer) are terraform variables.

VM tarball refresh: strategy-service is already in every category
tarball in `scripts/vm/create-code-tarballs.sh`. After merging
signal_broadcast changes, operators run
`bash scripts/vm/create-code-tarballs.sh --all` so VMs pick up the new
signal_broadcast sub-package. Bare invocation only re-tars CORE — do
not forget the flag.

Local-emulator smoke: `bash scripts/smoke-signal-broadcast.sh` (uses
`responses`; no live HTTP). Live-staging smoke is an operator follow-up.

SSOT: `unified-trading-pm/plans/active/signal_leasing_broadcast_architecture_2026_04_20.plan.md`
Phase 4. D-decisions (D1 sub-package, D3 HMAC webhook auth, D7 per-cp-
per-strategy rate limit, D10 shard-level failure isolation) locked
2026-04-20.
