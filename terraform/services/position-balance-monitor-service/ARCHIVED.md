# ARCHIVED — Terraform resources for position-balance-monitor-service should be destroyed

This service has been consolidated into strategy-service as `strategy_service.position`.
Run after operator confirms `gh repo archive IggyIkenna/position-balance-monitor-service`:

```bash
cd terraform/services/position-balance-monitor-service/gcp && terraform destroy -var="project_id=<PROJECT_ID>"
cd terraform/services/position-balance-monitor-service/aws && terraform destroy
```

Resources managed: Cloud Run service, IAM bindings, GCS bucket ACLs.

Replacement: strategy-service with `--operation position-recon`.
SSOT: `plans/active/strategy_repo_consolidation_2026_05_19.md`.
