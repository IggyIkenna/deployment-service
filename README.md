# deployment-service

Deployment orchestration service for the Unified Trading System. Coordinates deployment workflows, manages service catalog, and loads deployment configuration.

## Structure

- `deployment_service/orchestrator/` — deployment workflow coordination
- `deployment_service/catalog/` — service definitions and inventory
- `deployment_service/config_loader/` — configuration loading and validation
- `deployment_service/deployment/` — deployment backend implementations

## Setup

```bash
bash scripts/setup.sh
```

## Quality Gates

```bash
bash scripts/quality-gates.sh
```

## Quickmerge

```bash
bash scripts/quickmerge.sh "feat: your message"
```

## Dependencies

- unified-trading-library
- unified-config-interface
