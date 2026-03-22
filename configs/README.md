# Deployment Configuration

## Operational Config SSOT: `unified-trading-pm/configs/`

All operational YAML configs (sharding, data-catalogue, venues, dependencies, expected start dates, data providers)
are owned by `unified-trading-pm/configs/` as the single source of truth (as of 2026-03-11).

**This directory contains symlinks** pointing to `../../unified-trading-pm/configs/` for all operational files.
deployment-service code (ConfigLoader, tests) resolves configs from `deployment-service/configs/` via these symlinks.
Never duplicate or copy these files locally -- always edit the PM original.

### Local-only files (NOT symlinked)

| File                             | Purpose                                                                           |
| -------------------------------- | --------------------------------------------------------------------------------- |
| `cloud-providers.yaml`           | Deployment-service-specific cloud config (GCP/AWS project, region, registry URLs) |
| `README.md`                      | This file                                                                         |
| `*.md`, `*.dot`, `*.svg`, `*.py` | Documentation, topology diagrams, scripts                                         |

### Symlinked from PM (operational SSOT)

| Pattern                         | Purpose                                                                         |
| ------------------------------- | ------------------------------------------------------------------------------- |
| `sharding.{service}.yaml`       | Shard dimensions (category, venue, date, etc.)                                  |
| `sharding_config.yaml`          | Global sharding configuration                                                   |
| `data-catalogue.{service}.yaml` | GCS paths, output structure                                                     |
| `dependencies.yaml`             | Service dependency-readiness checks (derived readiness view, not primary SSOT). |
| `venues.yaml`                   | Canonical venue-category mappings                                               |
| `venue_data_types.yaml`         | Per-venue data type expectations                                                |
| `expected_start_dates.yaml`     | Per-service/category/venue expected start dates                                 |
| `data-providers.yaml`           | Data provider configuration                                                     |

## Sharding Config

Shard dimensions are defined in `sharding.{service}.yaml` (SSOT: `unified-trading-pm/configs/`).
The deployment CLI uses these to compute combinatorics. See [docs/CLI.md](../docs/CLI.md).

## Deployment Flexibility (dependencies.yaml)

Upstream entries use `required: true` (must have) or `required: false` (optional, can_use). This file is a derived readiness/check model from the primary SSOTs and enables different deployment topologies without code changes:

- **features-delta-one** can use: market-data-processing, features-calendar, market-tick-data-service
- **features-volatility** can use: market-data-processing, features-calendar (in addition to market-tick-data-service)
- **features-onchain** can use: features-calendar, market-tick-data-service (in addition to market-data-processing)
- **ml-training** can use: market-tick-data-service, features-calendar, market-data-processing (in addition to features-\*)
- **ml-inference** can use: features-calendar, features-volatility, features-onchain (in addition to ml-training, features-delta-one)
- **strategy-service** can use: market-data-processing, features-volatility, features-onchain, market-tick-data-service, features-calendar, risk-and-exposure, position-balance-monitor
- **execution-service** can use: features-delta-one, features-calendar (in addition to strategy, market-tick-data-service, instruments)
- **risk-and-exposure** can use: pnl-attribution. Does NOT need Order/Algo (talks to strategy for risk management).

## Runtime Topology SSOT (runtime-topology.yaml)

Two primary machine-readable SSOTs:

- `unified-trading-pm/workspace-manifest.json` defines _what code components exist and their DAG/dependencies_.
- `runtime-topology.yaml` defines _how runtime interaction happens_ (service flows, API interactions, storage patterns, transport by mode/profile).

`dependencies.yaml` is a derived readiness subset used for dependency checks.

`runtime-topology.yaml` defines runtime behavior by mode and deployment profile:

- `mode=batch|live`
- `deployment_profile=distributed|co_located_vm`
- `transport=storage|pubsub|in_memory`
- `dependency_check=gcs|none`

Example: `market-data-processing-service <- market-tick-data-service` can run:

- `batch`: `storage` (`gcs` dependency check enabled)
- `live` on `co_located_vm`: `in_memory` (`dependency_check: none`)

This keeps hybrid live coupling in deployment config, not service import wiring.
