# VM Setup Scripts for Data Pipeline

## Current approach (manual VMs)

`setup-data-pipeline-vm.sh` — standalone script for setting up bare Ubuntu VMs
for data pipeline work (MTDS backfill, migrations, etc.). Can be used as a
startup-script or run via SSH.

## Production approach (Docker VMs)

The deployment-service already has a proper VM backend at:

- `deployment_service/backends/vm.py` — VMBackend (Container-Optimized OS + Docker)
- `deployment_service/backends/services/vm_lifecycle.py` — create/delete/failover
- `deployment_service/backends/services/vm_config.py` — VM config management
- `deployment_service/backends/services/vm_monitoring.py` — health checks

This backend uses Docker images from Artifact Registry, which is the correct
production approach. The manual setup script captures lessons learned:

- Python 3.13 required (UAC dependency)
- build-essential + python3.13-dev needed for C extensions
- Must use python3.13 -m venv (not python3 -m venv)
- Tarballs to /tmp/ then extract to workspace
- nohup uses full venv path

## TODO

- Build a data-pipeline Docker image with Python 3.13 + all deps
- Wire it into the VMBackend for deployment-UI "VM Instance" launches
- Use shard_dimensions.py for automatic work distribution
