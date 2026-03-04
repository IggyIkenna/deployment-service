# deployment-service Architecture

## Overview

Deployment-service is an infrastructure component that orchestrates deployment workflows across the Unified Trading System. It provides:

- **Orchestrator**: Coordinates deployment workflows and sequencing
- **Catalog**: Service definitions and deployment inventory
- **Config Loader**: Loads and validates deployment configuration from YAML/config store
- **Deployment**: Backend implementations for deployment execution

## Package Layout

```
deployment_service/
├── orchestrator/   # Workflow coordination
├── catalog/       # Service catalog
├── config_loader/ # Config loading
└── deployment/    # Deployment backends
```

## Dependencies

- **unified-trading-library**: Cloud primitives, storage, utilities
- **unified-config-interface**: Configuration schema and loading

## Integration

Deployment-service is type=infrastructure in the workspace manifest. It integrates with deployment-api and unified-trading-deployment-v3 for full deployment pipelines.
