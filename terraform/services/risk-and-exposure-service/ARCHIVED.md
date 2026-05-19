# ARCHIVED — Terraform resources for risk-and-exposure-service should be destroyed

This service has been consolidated into strategy-service as `strategy_service.risk`.
Run after operator confirms `gh repo archive IggyIkenna/risk-and-exposure-service`:

```bash
cd terraform/services/risk-and-exposure-service/gcp && terraform destroy -var="project_id=<PROJECT_ID>"
cd terraform/services/risk-and-exposure-service/aws && terraform destroy
```

Resources managed: Cloud Run service, IAM bindings, GCS bucket ACLs.

Replacement: strategy-service with `--operation risk-monitor`.
SSOT: `plans/active/strategy_repo_consolidation_2026_05_19.md`.
