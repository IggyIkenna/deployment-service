#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
#
# Safe OpenTofu wrapper for deployment-service/terraform/gcp.
#
# Removes two footguns baked into this directory:
#
#   1. STATE-PREFIX TRAP — the backend block in main.tf hardcodes the state
#      prefix default to `terraform/state/dev`, a near-empty (~11-entry) state.
#      The LIVE prod resources (~198: SAs, every Cloud Scheduler/Run job, all
#      buckets) live under `terraform/state/prod`. A bare `tofu init && apply`
#      with no explicit `-backend-config=prefix=...` silently targets the DEV
#      state, so you either think prod resources are missing or you apply
#      against the wrong environment.
#
#   2. WRONG-BINARY TRAP — this dir is OpenTofu, not Terraform. The committed
#      .terraform.lock.hcl pins providers from `registry.opentofu.org`. Running
#      the `terraform` binary here rewrites those provider sources to
#      `registry.terraform.io` (HashiCorp) — a subtle, committable regression.
#
# This wrapper makes the CORRECT invocation the only easy path:
#   - REQUIRES an explicit ENV (dev|staging|prod); refuses to run without it.
#   - maps ENV -> `-backend-config=prefix=terraform/state/<env>` on `init`.
#   - invokes `tofu` (never `terraform`); hard-fails if `tofu` is absent
#     (it does NOT silently fall back to the terraform binary).
#   - for non-init commands, verifies the currently-initialised backend prefix
#     matches the requested ENV and refuses on mismatch (a cross-invocation
#     guard against "inited dev, applied thinking it was prod").
#   - injects the required tofu vars (project_id / region / environment /
#     bucket_prefix) via TF_VAR_* so plan/apply never prompt.
#
# Usage:
#   ENV=prod ./tofu.sh init
#   ENV=prod ./tofu.sh plan
#   ENV=prod ./tofu.sh apply
#   ./tofu.sh prod plan          # ENV as leading positional also works
#
# Overridable env: PROJECT_ID (default central-element-323112),
#                  REGION (default asia-northeast1),
#                  BUCKET_PREFIX (default uts).
#
# SSOT: unified-trading-pm/codex/05-infrastructure/deployment-service-gcp-tofu-state.md

set -euo pipefail

TF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ID="${PROJECT_ID:-central-element-323112}"
REGION="${REGION:-asia-northeast1}"
BUCKET_PREFIX="${BUCKET_PREFIX:-uts}"
STATE_BUCKET="${BUCKET_PREFIX}-terraform-state-${PROJECT_ID}"

# --- ENV resolution: env var, or a leading positional if it is an env token --
if [[ -z "${ENV:-}" && "${1:-}" =~ ^(dev|staging|prod)$ ]]; then
  ENV="$1"
  shift
fi

if [[ -z "${ENV:-}" ]]; then
  cat >&2 <<'EOF'
ERROR: ENV is required (dev | staging | prod).

  ENV=prod ./tofu.sh <init|plan|apply|...>
  ./tofu.sh prod <init|plan|apply|...>

Refusing to run: a bare init/apply would silently target the DEV state
(terraform/state/dev), NOT the live PROD state (terraform/state/prod).
EOF
  exit 2
fi

if [[ ! "$ENV" =~ ^(dev|staging|prod)$ ]]; then
  echo "ERROR: ENV must be one of dev|staging|prod (got: '$ENV')." >&2
  exit 2
fi

PREFIX="terraform/state/${ENV}"

# --- binary guard: OpenTofu only ---------------------------------------------
if ! command -v tofu >/dev/null 2>&1; then
  cat >&2 <<'EOF'
ERROR: `tofu` (OpenTofu) is not on PATH.

This directory is OpenTofu — the committed .terraform.lock.hcl pins providers
from registry.opentofu.org. Do NOT substitute the `terraform` binary here: it
would rewrite provider sources to registry.terraform.io (HashiCorp), a
committable regression. Install OpenTofu: https://opentofu.org/docs/intro/install/
EOF
  exit 3
fi

SUBCMD="${1:-}"
if [[ -z "$SUBCMD" ]]; then
  echo "ERROR: no tofu subcommand given (e.g. init, plan, apply)." >&2
  exit 2
fi

# --- backend-prefix cross-invocation guard -----------------------------------
# The initialised backend prefix is cached in .terraform/terraform.tfstate.
# If it disagrees with the requested ENV, the dir was inited for a different
# env and a plan/apply would hit the wrong state — force a re-init.
BACKEND_CACHE="${TF_DIR}/.terraform/terraform.tfstate"
if [[ "$SUBCMD" != "init" && -f "$BACKEND_CACHE" ]]; then
  CACHED_PREFIX="$(grep -o '"prefix"[[:space:]]*:[[:space:]]*"[^"]*"' "$BACKEND_CACHE" | head -1 | sed 's/.*"\([^"]*\)"$/\1/')"
  if [[ -n "$CACHED_PREFIX" && "$CACHED_PREFIX" != "$PREFIX" ]]; then
    echo "ERROR: initialised backend prefix is '$CACHED_PREFIX' but ENV=$ENV wants '$PREFIX'." >&2
    echo "       Re-initialise for this env first:  ENV=$ENV $0 init" >&2
    exit 4
  fi
fi

# Required tofu vars (project_id / environment / bucket_prefix have no default)
# — exported so ANY var-consuming subcommand picks them up and the rest ignore.
export TF_VAR_project_id="$PROJECT_ID"
export TF_VAR_region="$REGION"
export TF_VAR_environment="$ENV"
export TF_VAR_bucket_prefix="$BUCKET_PREFIX"

# --- dispatch ----------------------------------------------------------------
if [[ "$SUBCMD" == "init" ]]; then
  shift
  echo "==> tofu init [$ENV] — backend gs://${STATE_BUCKET}/${PREFIX}" >&2
  exec tofu -chdir="$TF_DIR" init \
    -backend-config="bucket=${STATE_BUCKET}" \
    -backend-config="prefix=${PREFIX}" \
    -reconfigure "$@"
fi

echo "==> tofu $SUBCMD [$ENV] (backend prefix: $PREFIX)" >&2
exec tofu -chdir="$TF_DIR" "$@"
