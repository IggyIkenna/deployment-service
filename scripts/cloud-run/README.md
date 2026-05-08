# Cloud Run deploy scripts (canonical home)

Codified 2026-05-08 per CLAUDE.md "VM launcher script SSOT" + operator direction
"VM + Cloud Run stuff in one place". Every script that runs `gcloud run deploy`
or builds Cloud Run images via `gcloud builds submit` lives here.

## Scripts

| Script | What it deploys | Original location (now wrapper) |
|--------|-----------------|----------------------------------|
| `deploy-shared.sh` | `uts-shared-deployment-api` (FastAPI + bundled deployment-ui SPA, asia-northeast1) | `unified-trading-pm/scripts/dev/deploy-shared-cloudrun.sh` |
| `canary-deploy.sh` | Generic Cloud Run canary framework (any service, any region) | `unified-trading-pm/scripts/deployment/canary-deploy.sh` |
| `deploy-ui.sh` | `odum-portal` / `odum-portal-staging` (UI, europe-west4) | `unified-trading-system-ui/scripts/deploy-cloud-run.sh` |

## Adding a new Cloud Run deploy script

1. Place the script here.
2. If a backwards-compatible launch path exists (operator workflow `bash <repo>/scripts/...`), add a 1-line wrapper
   in the original repo that `exec bash $(workspace)/deployment-service/scripts/cloud-run/<canonical>.sh "$@"`.
3. Document the script in this README.
4. (Optional) Wire to `deployment-api`'s deploy-missing UI registry.

## Cross-references

- CLAUDE.md § "VM launcher script SSOT (codified 2026-05-07)" — VM + Cloud Run unification.
- `plans/active/issues/vm_launcher_consolidation_audit_2026_05_08.md` — Phase 4 migration record.
