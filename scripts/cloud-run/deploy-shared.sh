#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# deploy-shared-cloudrun.sh — Tier-3 shared Cloud Run deployment.
#
# Builds deployment-api (FastAPI + bundled deployment-ui SPA) via Cloud Build
# in asia-northeast1 and rolls a new revision of the shared Cloud Run service
# `uts-shared-deployment-api`. Co-located with the project's GCS buckets so
# data-status reads avoid cross-region latency.
#
# Tradeoffs vs `dev-tiers.sh --tier 2 --real`:
#   * Build+deploy ~5-10 min vs hot-reload, but data-status under 5s in-region
#     (vs minutes from a laptop hitting cross-region GCS).
#   * Shared instance — every dev pushing to `live-defi-rollout` lands on the
#     same URL. No per-user state.
#
# Usage:
#   bash unified-trading-pm/scripts/dev/deploy-shared-cloudrun.sh
#   bash unified-trading-pm/scripts/dev/deploy-shared-cloudrun.sh --build-only
#   bash unified-trading-pm/scripts/dev/deploy-shared-cloudrun.sh --no-build
#
# Env overrides:
#   GCP_PROJECT_ID    (default: central-element-323112)
#   REGION            (default: asia-northeast1)
#   SERVICE_NAME      (default: uts-shared-deployment-api)
#   BRANCH            (default: live-defi-rollout)
#   ROLLUP_JOB_NAME   (default: uts-prod-data-status-rollup)

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
REGION="${REGION:-asia-northeast1}"
SERVICE_NAME="${SERVICE_NAME:-uts-shared-deployment-api}"
IMAGE="asia-northeast1-docker.pkg.dev/${PROJECT_ID}/unified-trading-system/deployment-api:latest"
BRANCH="${BRANCH:-live-defi-rollout}"
# Per-tier prod SA (bucket_iam_write_protection_per_tier_2026_06_09.md, hybrid
# ruling BLK-0c84ceac "C") — replaces the god-SA unified-trading-sa per
# issues/bucket_iam_p2_tier_sa_scope_gap_and_default_compute_sa_overprivilege_2026_07_30.md
# P2. Live-verified (2026-07-31) to hold the 7 non-storage roles
# unified-trading-sa's declared grants cover (secretmanager.secretAccessor,
# pubsub.editor, bigquery.dataEditor, run.invoker, iam.serviceAccountUser,
# compute.instanceAdmin.v1, artifactregistry.reader) plus bigquery.jobUser
# (needed for deployment-api's BQ query-job path, a gap the original P1 grant
# missed) and bucket-level storage.objectAdmin on the 2 non-tier buckets
# deployment-api's runtime actually writes to (unified-deployment-state-*,
# deployment-scripts-*), in addition to its existing Group A/B -prd- write
# scope + project-wide storage.objectViewer read.
SA="uts-prd-sa@${PROJECT_ID}.iam.gserviceaccount.com"
ROLLUP_JOB_NAME="${ROLLUP_JOB_NAME:-uts-prod-data-status-rollup}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="${UNIFIED_TRADING_WORKSPACE_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
DEPLOYMENT_API_DIR="${WORKSPACE_ROOT}/deployment-api"

DO_BUILD=true
DO_DEPLOY=true
case "${1:-}" in
  --build-only)  DO_DEPLOY=false ;;
  --deploy-only|--no-build) DO_BUILD=false ;;
  --rebuild|"")  ;;
  -h|--help)
    sed -n '1,/^set -euo/p' "$0" | sed 's/^# *//; /^!/d; /^$/d; q' >&2
    grep -E '^# (Usage|  bash|Env|  GCP_|  REGION|  SERVICE|  BRANCH)' "$0" | sed 's/^# *//' >&2
    exit 0
    ;;
  *) echo "Unknown arg: $1"; exit 1 ;;
esac

if [ ! -d "$DEPLOYMENT_API_DIR" ]; then
  echo "ERROR: deployment-api dir not found at $DEPLOYMENT_API_DIR" >&2
  echo "Set UNIFIED_TRADING_WORKSPACE_ROOT or run from workspace root." >&2
  exit 1
fi

echo "═══════════════════════════════════════════════════════════"
echo "  Tier-3 shared Cloud Run deploy"
echo "═══════════════════════════════════════════════════════════"
echo "  Project:  $PROJECT_ID"
echo "  Region:   $REGION (co-located with GCS buckets)"
echo "  Service:  $SERVICE_NAME"
echo "  Image:    $IMAGE"
echo "  Branch:   $BRANCH"
echo "  SA:       $SA"
echo "  Rollup:   $ROLLUP_JOB_NAME"
echo

if $DO_BUILD; then
  echo "==> [1/2] Cloud Build — full QG + UI bundle + image push (~5-10 min)"
  cd "$DEPLOYMENT_API_DIR"

  # The cloudbuild.yaml fetch-ui step tries to git-clone deployment-ui from
  # GitHub — that fails inside Cloud Build because the cloud-builders/git
  # image has no GitHub auth (private repo → 404). The same step has an
  # explicit fast-path: "if ./ui/package.json exists, skip the clone".
  # We pre-bundle a tarball of deployment-ui (HEAD on the same branch) so
  # the fetch-ui step short-circuits.
  UI_SRC="${WORKSPACE_ROOT}/deployment-ui"
  if [ ! -d "$UI_SRC" ]; then
    echo "ERROR: deployment-ui sibling repo not found at $UI_SRC" >&2
    exit 1
  fi

  echo "==> Pre-bundling deployment-ui into ./ui/ (skips Cloud Build git-clone)"
  TEMP_UI="${DEPLOYMENT_API_DIR}/ui"
  if [ -e "$TEMP_UI" ] || [ -L "$TEMP_UI" ]; then rm -rf "$TEMP_UI"; fi
  # rsync exclude the heavy/local-only paths — node_modules (will be reinstalled
  # by the Dockerfile's npm ci), dist, .git, test caches.
  rsync -a \
    --exclude='node_modules' \
    --exclude='dist' \
    --exclude='.git' \
    --exclude='coverage' \
    --exclude='.turbo' \
    --exclude='.cache' \
    --exclude='playwright-report' \
    --exclude='test-results' \
    "$UI_SRC/" "$TEMP_UI/"
  echo "    bundled ui/ size: $(du -sh "$TEMP_UI" | cut -f1)"

  # Bundle the deployment-service sibling repo. deployment-api imports from
  # ``deployment_service.*`` at runtime (vm_deployments route + others) but
  # the UTL base image doesn't ship it (it's an application-level lib, not a
  # T0/T1 library). pyproject.toml's ``[tool.uv.sources]`` points at
  # ``../deployment-service`` for local dev, which doesn't exist in Cloud
  # Build context. We rsync it into ``./_deployment-service/`` for the
  # Dockerfile to pip-install.
  DS_SRC="${WORKSPACE_ROOT}/deployment-service"
  if [ ! -d "$DS_SRC" ]; then
    echo "ERROR: deployment-service sibling repo not found at $DS_SRC" >&2
    exit 1
  fi
  TEMP_DS="${DEPLOYMENT_API_DIR}/_deployment-service"
  if [ -e "$TEMP_DS" ] || [ -L "$TEMP_DS" ]; then rm -rf "$TEMP_DS"; fi
  rsync -a \
    --exclude='.venv' --exclude='.git' --exclude='__pycache__' \
    --exclude='*.egg-info' --exclude='build' --exclude='dist' \
    --exclude='node_modules' --exclude='.pytest_cache' \
    --exclude='.coverage' --exclude='htmlcov' \
    --exclude='terraform' --exclude='terraform.tfstate*' \
    --exclude='.terraform' --exclude='*.tfstate*' \
    --exclude='tests' --exclude='docs' --exclude='data' \
    --exclude='coverage.json' --exclude='coverage.xml' \
    "$DS_SRC/" "$TEMP_DS/"
  echo "    deployment-service: $(du -sh "$TEMP_DS" | cut -f1)"

  # Bundle unified-api-contracts — the UTL base image ships a cached UAC that
  # may predate modules added since the last UTL image build (e.g.
  # live_cluster_registry, scheduler_registry). The Dockerfile re-installs
  # UAC from this directory with --no-deps (deps already in the base image).
  UAC_SRC="${WORKSPACE_ROOT}/unified-api-contracts"
  if [ ! -d "$UAC_SRC" ]; then
    echo "ERROR: unified-api-contracts sibling repo not found at $UAC_SRC" >&2
    exit 1
  fi
  TEMP_UAC="${DEPLOYMENT_API_DIR}/_unified-api-contracts"
  if [ -e "$TEMP_UAC" ] || [ -L "$TEMP_UAC" ]; then rm -rf "$TEMP_UAC"; fi
  rsync -a \
    --exclude='.venv' --exclude='.git' --exclude='__pycache__' \
    --exclude='*.egg-info' --exclude='build' --exclude='dist' \
    --exclude='.pytest_cache' --exclude='tests' \
    "$UAC_SRC/" "$TEMP_UAC/"
  echo "    unified-api-contracts: $(du -sh "$TEMP_UAC" | cut -f1)"

  # Bundle strategy-service — deployment-api's treasury_routes.py imports
  # strategy_service.position; the Dockerfile installs it from this directory with
  # --no-deps (sqlalchemy is installed separately first). buildspec.aws.yaml clones
  # it; the tier-3 path must rsync it the same way (added 2026-06-14 — the Dockerfile
  # gained this COPY in 7be50f0 but this script was never updated → COPY _strategy-service
  # failed).
  SS_SRC="${WORKSPACE_ROOT}/strategy-service"
  if [ ! -d "$SS_SRC" ]; then
    echo "ERROR: strategy-service sibling repo not found at $SS_SRC" >&2
    exit 1
  fi
  TEMP_SS="${DEPLOYMENT_API_DIR}/_strategy-service"
  if [ -e "$TEMP_SS" ] || [ -L "$TEMP_SS" ]; then rm -rf "$TEMP_SS"; fi
  rsync -a \
    --exclude='.venv' --exclude='.git' --exclude='__pycache__' \
    --exclude='*.egg-info' --exclude='build' --exclude='dist' \
    --exclude='node_modules' --exclude='.pytest_cache' \
    --exclude='.coverage' --exclude='htmlcov' \
    --exclude='terraform' --exclude='terraform.tfstate*' \
    --exclude='.terraform' --exclude='*.tfstate*' \
    --exclude='tests' --exclude='docs' --exclude='data' \
    --exclude='coverage.json' --exclude='coverage.xml' \
    "$SS_SRC/" "$TEMP_SS/"
  echo "    strategy-service: $(du -sh "$TEMP_SS" | cut -f1)"

  # Materialise the codex-data / pm-plans / pm-configs symlinks. Local checkouts
  # have these as symlinks into ../unified-trading-pm/{codex/10-audit/repos,plans,configs};
  # Cloud Build's tarball prep doesn't follow symlinks, so the COPY steps
  # in the Dockerfile would fail with "file not found in build context".
  PM_ROOT="${WORKSPACE_ROOT}/unified-trading-pm"
  echo "==> Materialising codex-data/ pm-plans/ pm-configs/ from ${PM_ROOT}"
  for dst in codex-data pm-plans pm-configs; do
    target="${DEPLOYMENT_API_DIR}/${dst}"
    if [ -L "$target" ]; then rm -f "$target"; fi
    if [ -d "$target" ] && [ ! -L "$target" ]; then continue; fi  # real dir, leave it
  done
  rsync -a "${PM_ROOT}/codex/10-audit/repos/" "${DEPLOYMENT_API_DIR}/codex-data/"
  rsync -a --exclude='archive' "${PM_ROOT}/plans/" "${DEPLOYMENT_API_DIR}/pm-plans/"
  rsync -a "${PM_ROOT}/configs/" "${DEPLOYMENT_API_DIR}/pm-configs/"
  echo "    codex-data:  $(du -sh "${DEPLOYMENT_API_DIR}/codex-data" | cut -f1)"
  echo "    pm-plans:    $(du -sh "${DEPLOYMENT_API_DIR}/pm-plans" | cut -f1)"
  echo "    pm-configs:  $(du -sh "${DEPLOYMENT_API_DIR}/pm-configs" | cut -f1)"
  trap 'rm -rf "${DEPLOYMENT_API_DIR}/ui" "${DEPLOYMENT_API_DIR}/codex-data" "${DEPLOYMENT_API_DIR}/pm-plans" "${DEPLOYMENT_API_DIR}/pm-configs" "${DEPLOYMENT_API_DIR}/_deployment-service" "${DEPLOYMENT_API_DIR}/_unified-api-contracts" "${DEPLOYMENT_API_DIR}/_strategy-service" 2>/dev/null; ln -sfn ../unified-trading-pm/codex/10-audit/repos "${DEPLOYMENT_API_DIR}/codex-data"; ln -sfn ../unified-trading-pm/plans "${DEPLOYMENT_API_DIR}/pm-plans"; ln -sfn ../unified-trading-pm/configs "${DEPLOYMENT_API_DIR}/pm-configs"' EXIT

  # Manual `gcloud builds submit` does not auto-populate SHORT_SHA / COMMIT_SHA (those only come from
  # Cloud Build triggers). The repo's cloudbuild.yaml references SHORT_SHA in the ``images:`` push list,
  # so we synthesise it from local git HEAD and override via --substitutions. The image SHA tag is
  # informational; :latest is what Cloud Run actually consumes.
  # NOTE: REPO_NAME is a RESERVED automatic substitution — gcloud rejects setting it on a manual submit
  # ("not matched in the template"), even with ALLOW_LOOSE. The repo-CI Image column instead attributes
  # this DEPLOYED tier-3 build to deployment-api via the _SERVICE_NAME fallback in
  # deployment-api/_cloud_builds_history.py (_recent_builds_by_repo_name).
  SHORT_SHA="$(git -C "$DEPLOYMENT_API_DIR" rev-parse --short HEAD)"
  COMMIT_SHA="$(git -C "$DEPLOYMENT_API_DIR" rev-parse HEAD)"

  gcloud builds submit . \
    --project="$PROJECT_ID" \
    --config=cloudbuild-tier3.yaml \
    --substitutions="SHORT_SHA=${SHORT_SHA},_SERVICE_NAME=deployment-api" \
    --region="$REGION"
  echo "==> Build complete. :latest now points at the new digest."
fi

if $DO_DEPLOY; then
  echo
  echo "==> [2/2] Cloud Run deploy — rolling new revision (~30s)"
  gcloud run deploy "$SERVICE_NAME" \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --image="$IMAGE" \
    --port=8080 \
    --memory=4Gi \
    --cpu=2 \
    --min-instances=1 \
    --max-instances=20 \
    --concurrency=80 \
    --timeout=900 \
    --service-account="$SA" \
    --allow-unauthenticated \
    --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},CLOUD_PROVIDER=gcp,CLOUD_MOCK_MODE=false,DEPLOYMENT_ENV=prod,DISABLE_AUTH=true" \
    --quiet

  URL=$(gcloud run services describe "$SERVICE_NAME" \
    --project="$PROJECT_ID" --region="$REGION" \
    --format='value(status.url)')

  echo
  echo "═══════════════════════════════════════════════════════════"
  echo "  Tier-3 deployed"
  echo "═══════════════════════════════════════════════════════════"
  echo "  URL:    $URL"
  echo "  UI:     $URL/                       (Vite SPA bundled)"
  echo "  API:    $URL/api/health             (health check)"
  echo "  Stats:  $URL/api/data-status/turbo/stats"
  echo
  echo "  Data-status latency (in-region GCS reads): expect ~2-5s warm."
  echo "  Subsequent re-deploys: same URL, zero-downtime revision swap."
  echo

  # ── Sync data-status rollup Job to the new image ─────────────────────────
  # The rollup job (uts-prod-data-status-rollup, cron */5) runs independently
  # from the Cloud Run SERVICE and is pinned to its own image tag.  Without
  # this step the job keeps executing old code after every service deploy,
  # so live data-status (served_from=rollup) silently reflects stale logic.
  # We update the job to the SAME $IMAGE the service just rolled to so the
  # two can never drift, then fire one async execution so the rollup refreshes
  # immediately without waiting for the next cron tick.
  echo "==> [3/3] Sync data-status rollup Job to the new image"
  if gcloud run jobs describe "$ROLLUP_JOB_NAME" \
       --project="$PROJECT_ID" --region="$REGION" \
       --format='value(name)' >/dev/null 2>&1; then
    gcloud run jobs update "$ROLLUP_JOB_NAME" \
      --image="$IMAGE" \
      --region="$REGION" \
      --project="$PROJECT_ID"
    gcloud run jobs execute "$ROLLUP_JOB_NAME" \
      --region="$REGION" \
      --project="$PROJECT_ID" \
      --async
    echo "    Rollup job updated + execution triggered (async)."
  else
    echo "    WARNING: rollup job '$ROLLUP_JOB_NAME' not found in project" \
         "'$PROJECT_ID' / region '$REGION' — skipping rollup sync." \
         "(Set ROLLUP_JOB_NAME= to suppress this warning.)" >&2
  fi
  # ── end rollup sync ───────────────────────────────────────────────────────
fi
