# AGENTS.md — deployment-service

## Quick Reference for AI Agents

### Key Commands

- **Quality gates**: `cd deployment-service && bash scripts/quality-gates.sh`
- **Source dir**: `deployment-service/deployment_service/` (underscored)
- **Typecheck**: `run_timeout 120 basedpyright deployment_service/`

### Mandatory Rules

Before any action, read:
`unified-trading-pm/cursor-configs/SUB_AGENT_MANDATORY_RULES.md`

### Rules Summary

- `uv pip install` not `pip install`
- Flat deps only — no `[project.optional-dependencies]`
- `basedpyright` not `pyright`
- `UnifiedCloudConfig` not `os.getenv()`
- No `# type: ignore` to hide architectural violations
- No `try/except ImportError` fallbacks

### Workspace

WORKSPACE_ROOT: `/Users/ikennaigboaka/Code/unified-trading-system-repos`
