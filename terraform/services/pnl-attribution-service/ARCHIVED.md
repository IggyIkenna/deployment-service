# ARCHIVED — Terraform resources for pnl-attribution-service should be destroyed

This service has been consolidated into strategy-service as `strategy_service.pnl`.
Run after operator confirms `gh repo archive IggyIkenna/pnl-attribution-service`:

```bash
cd terraform/services/pnl-attribution-service/gcp && terraform destroy -var="project_id=<PROJECT_ID>"
cd terraform/services/pnl-attribution-service/aws && terraform destroy
```

Resources managed: Cloud Run service, IAM bindings, GCS bucket ACLs.

Replacement: strategy-service with `--operation pnl-attribution`.
SSOT: `plans/active/strategy_repo_consolidation_2026_05_19.md`.
