# Deployment Service — Cloud-Agnostic Migration

> **Canonical SSOT:** [cloud-agnostic-script-pattern](../../unified-trading-pm/codex/05-infrastructure/cloud-agnostic-script-pattern.md).
> This file carries only deployment-service-specific details. The cross-cutting cloud-agnostic script/storage pattern
> lives in the codex SSOT above — **do not duplicate it here**; if this file disagrees with codex, codex wins.

## deployment-service-specific migration state

The data layer is cloud-agnostic via `api/utils/storage_facade.py` (GCS/S3). It uses GCS FUSE when
`DEPLOYMENT_ENV=production` and mounts exist, falling back to the GCS API otherwise.

### Compute backends (repo-local)

| Backend                                       | Cloud | Module                         |
| --------------------------------------------- | ----- | ------------------------------ |
| Compute Engine VMs                            | GCP   | `backends/vm.py`               |
| Cloud Run Jobs                                | GCP   | `backends/cloud_run.py`        |
| EC2 instances                                 | AWS   | `backends/aws_ec2.py`          |
| AWS Batch (Fargate)                           | AWS   | `backends/aws_batch.py`        |
| Provider factory (`CLOUD_PROVIDER` detection) | —     | `backends/provider_factory.py` |

### Usage (repo-local)

Use `storage_facade`, never a direct GCS client (bypasses the facade + GCS FUSE optimization):

```python
from api.utils.storage_facade import list_objects, list_prefixes, object_exists, read_object_text, write_object_text
```

Lock operations remain on GCS directly (they require `if_generation_match`).
