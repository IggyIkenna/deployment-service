# AGENTS.md

## Setup

```bash
uv sync --extra dev
source .venv/bin/activate
```

## Quality Gates

```bash
bash scripts/quality-gates.sh
```

## Type Checking

```bash
timeout 120 basedpyright deployment_service/
```

## Key Entry Points

- `deployment_service/` — source package
- `deploy.py` — deployment orchestration script

## Notes

- Initialize events with `from unified_events_interface import setup_events`
- Required env vars: `GCP_PROJECT_ID` — see `ARCHITECTURE.md`
- Requires GCP credentials: `gcloud auth application-default login`
- Part of the 4-repo deployment cluster: `deployment-service` + `deployment-api` + `deployment-ui` + `system-integration-tests`
- Manages service deployments to GCP Cloud Run / Cloud Build
