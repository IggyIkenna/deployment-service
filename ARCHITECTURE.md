# ARCHITECTURE.md
## deployment-service-v3 Architecture & Purpose

**Date:** February 25, 2026  
**Repository:** deployment-service-v3  
**Type:** **Deployment Orchestration Tool** (NOT a shared library)

---

## What UTDv3 Actually Is

**deployment-service-v3 is a standalone deployment orchestration platform** that manages the deployment infrastructure for the entire Unified Trading System. It is **NOT** a shared library that services import or depend on.

### Primary Purpose
- **Deployment Orchestration:** Calculate and execute deployment shards for parallel processing
- **Infrastructure Management:** Terraform modules for GCP/AWS infrastructure
- **CI/CD Pipelines:** Automated Docker image builds and deployments  
- **Deployment UI:** Self-service web interface for deployment management
- **Monitoring Dashboard:** Track deployment status, not analyze results

### Key Characteristics
- **Standalone Tool:** Operates independently of services
- **CLI Tool:** `deploy-shards` command for deployment operations
- **Web Application:** UI at ports 8000 (API) and 5173 (frontend)
- **DevOps Focus:** Deployment monitoring, not business logic
- **Cloud Agnostic:** Supports GCP now, AWS migration planned

---

## Architecture Components

### 1. Core Deployment Engine
```
deployment_service/
├── shard_calculator.py      # Calculate parallel deployment shards
├── orchestrator.py           # Orchestrate multi-service deployments
├── monitor.py                # Monitor deployment status
├── dependencies.py           # Service dependency graph
└── catalog.py               # Data catalog tracking
```

### 2. Cloud Abstraction Layer
```
deployment_service/
├── cloud_client.py          # Cloud-agnostic storage (wraps GCS/S3)
├── config_loader.py         # Configuration management
└── config/
    ├── base_config.py       # Base configuration classes
    ├── config_validator.py  # Configuration validation
    └── env_substitutor.py   # Environment variable substitution
```

### 3. Backend Executors
```
backends/
├── vm.py                    # VM-based deployment backend
├── cloud_run.py             # Cloud Run deployment backend
└── services/
    ├── vm_config.py         # VM configuration
    └── quota_manager.py     # Quota management
```

### 4. API & Web UI
```
api/
├── main.py                  # FastAPI application
├── routes/
│   ├── deployments.py       # Deployment management endpoints
│   ├── data_status.py       # Data status monitoring
│   └── health.py            # Health checks
└── services/
    ├── deployment_manager.py # Deployment orchestration
    └── deployment_state.py   # State management
```

### 5. Infrastructure as Code
```
terraform/
├── modules/                 # Reusable Terraform modules
├── services/                # Service-specific infrastructure
│   ├── instruments-service/
│   ├── market-tick-data-handler/
│   └── ...
└── shared/                  # Shared infrastructure
    ├── gcp/                 # GCP-specific resources
    └── aws/                 # AWS-specific resources (future)
```

### 6. Service Configurations
```
configs/
├── checklist.*.yaml         # Service capability checklists
├── data-catalogue.*.yaml    # Data catalog definitions
├── sharding.*.yaml          # Sharding configurations
└── bucket_config.yaml       # Storage bucket configurations
```

---

## How Services Interact with UTDv3

### Services DO NOT:
- ❌ Import UTDv3 as a Python dependency
- ❌ Use UTDv3 classes/functions in their code
- ❌ Depend on UTDv3 for runtime operations
- ❌ Call UTDv3 APIs programmatically (typically)

### Services ARE Managed By UTDv3:
- ✅ Deployed through UTDv3's orchestration
- ✅ Monitored via UTDv3's dashboard
- ✅ Configured in UTDv3's YAML files
- ✅ Sharded by UTDv3 for parallel execution

### Deployment Flow
```
Developer/Operator
        ↓
   UTDv3 CLI/UI
        ↓
  Shard Calculator
        ↓
  Deployment API
        ↓
  Backend Executor (VM/Cloud Run)
        ↓
  Service Container (Docker)
```

---

## Design Principles

### 1. Cloud Agnostic
- `CloudClient` wraps cloud-specific storage (GCS now, S3 later)
- Terraform modules abstract cloud providers
- Configuration is cloud-independent

### 2. Service Independence
- Services don't know about UTDv3
- UTDv3 orchestrates services without code coupling
- Clean separation of concerns

### 3. Parallel Execution
- Shard calculation enables massive parallelization
- Zone failover for high availability
- Quota management prevents resource exhaustion

### 4. Self-Service
- Web UI for non-technical users
- CLI for automation
- API for programmatic access (if needed)

---

## Key Technologies

### Backend
- **Python 3.13:** Core orchestration engine
- **FastAPI:** REST API framework
- **Click:** CLI framework
- **Terraform:** Infrastructure as Code
- **Docker:** Container orchestration

### Frontend
- **React/Vue:** Web UI (port 5173)
- **WebSockets:** Real-time deployment updates

### Cloud Services
- **Google Cloud Storage:** State and artifact storage
- **Cloud Run:** Serverless compute backend
- **Compute Engine:** VM-based compute backend
- **Cloud Build:** CI/CD pipelines

### Observability
- **Structured Logging:** Using UEI for event logging
- **Metrics:** Deployment success rates, durations
- **Health Checks:** Service availability monitoring

---

## Deployment Modes

### 1. Batch Mode
- Date-based sharding for historical processing
- Ephemeral Cloud Run jobs
- Parallel execution across dates/venues/symbols

### 2. Live Mode (v3 addition)
- Scheduled Cloud Run jobs (e.g., every 15 minutes)
- Persistent state management
- Real-time monitoring

---

## Security Considerations

### Current Issues to Fix:
- ❌ Hardcoded project IDs → Move to configuration
- ❌ Direct environment variables → Use ConfigLoader
- ❌ Broad exception handling → Specific error handling

### Best Practices:
- ✅ Service account isolation
- ✅ Least privilege access
- ✅ Secrets in Secret Manager
- ✅ Audit logging

---

## Clarifications

### UTDv3 is NOT:
- ❌ A shared library for services to import
- ❌ A dependency services need in their code
- ❌ Part of service runtime operations
- ❌ An analysis platform (that's separate)

### UTDv3 IS:
- ✅ A deployment orchestration platform
- ✅ A DevOps monitoring dashboard
- ✅ An infrastructure management tool
- ✅ A standalone deployment service

---

## Future Roadmap

### Near Term
- Fix technical debt (lazy imports, type errors)
- Improve test coverage to 35%+
- Add UEI structured logging
- Remove hardcoded values

### Medium Term
- AWS support (cloud-agnostic goal)
- Enhanced monitoring capabilities
- Automated rollback mechanisms
- Cost optimization features

### Long Term
- Multi-region deployments
- Advanced orchestration patterns
- Integration with analysis platform
- ML-based deployment optimization

---

## Conclusion

UTDv3 is a **deployment orchestration tool**, not a shared library. Its purpose is to manage the deployment lifecycle of all services in the Unified Trading System without requiring services to have any knowledge of or dependency on UTDv3 itself. This clean separation ensures services remain focused on business logic while UTDv3 handles the complexity of deployment orchestration.