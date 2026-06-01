---
title: Deployment-UI — Staging + Prod Cloud Run Provisioning
owner: ikenna
cadence: one-time per new environment tier (or on service-account rotation)
verifier: operator (GCP console + healthz probe after each step)
last_executed: never
source_plan: plans/active/deployment_ui_lifecycle_tabs_2026_05_08.md (Phase H.4)
---

# Deployment-UI — Staging + Prod Cloud Run Provisioning

Provisions two Cloud Run instances of **deployment-api** (which serves the bundled deployment-UI SPA
under `/`) for **staging** and **prod** environment tiers.

Architecture:

- deployment-ui SPA is bundled INTO the deployment-api Cloud Run image (`./ui/` rsync pattern).
- No separate Firebase Hosting — the Vite build output is served by the FastAPI static-files mount.
- Each env tier gets its OWN Cloud Run service + its own Firebase project for auth data.
- Pattern mirrors the trading-system-UI `firebase-split-topology.md` (compute on prod GCP,
  Firebase data on tier-specific Firebase project).

Reference script: `deployment-service/scripts/cloud-run/deploy-shared.sh` (Tier-3 shared deploy,
which this runbook extends to cover per-tier instances).

---

## 0. Prerequisites

- ADC authenticated: `gcloud auth application-default login` (or CI SA key)
- GCP project: `central-element-323112` (prod) for ALL Cloud Run compute
- Firebase projects: `odum-staging` (staging) + `odum-research` (prod) — pre-existing
- Artifact Registry repo: `asia-northeast1-docker.pkg.dev/central-element-323112/unified-trading-system`
- Service account in place: `unified-trading-sa@central-element-323112.iam.gserviceaccount.com`
- Domains registered + delegated to Cloud DNS:
  - staging: `staging.odum-research.com` → A record pointing to Cloud Run ingress
  - prod: `deployment.odum-research.com` (or path `/deployment` on root domain — see Step 4)

---

## 1. Build and push the deployment-api image

Uses the same Cloud Build pipeline as the Tier-3 shared deploy. Set `ENV_TIER` for the environment
you're provisioning (`staging` or `prod`).

```bash
WORKSPACE_ROOT="$(pwd)"  # run from workspace root
ENV_TIER="staging"       # or "prod"

cd deployment-api

# Pre-bundle deployment-ui + deployment-service (same as deploy-shared.sh)
rsync -a --exclude='node_modules' --exclude='dist' --exclude='.git' \
  ../deployment-ui/ ./ui/
rsync -a --exclude='.venv' --exclude='.git' --exclude='__pycache__' \
  ../deployment-service/ ./_deployment-service/
rsync -a ../unified-trading-pm/codex/10-audit/repos/ ./codex-data/
rsync -a --exclude='archive' ../unified-trading-pm/plans/ ./pm-plans/
rsync -a ../unified-trading-pm/configs/ ./pm-configs/

SHORT_SHA="$(git rev-parse --short HEAD)"

gcloud builds submit . \
  --project="central-element-323112" \
  --config=cloudbuild-tier3.yaml \
  --substitutions="SHORT_SHA=${SHORT_SHA}" \
  --region="asia-northeast1"

# Cleanup temp dirs
rm -rf ./ui ./_deployment-service ./codex-data ./pm-plans ./pm-configs
```

Verify image landed:

```bash
gcloud artifacts docker images list \
  asia-northeast1-docker.pkg.dev/central-element-323112/unified-trading-system/deployment-api \
  --project=central-element-323112 --limit=3
```

---

## 2. Create (or update) the Cloud Run service

Two independent services — one per tier. NEVER share a service between staging and prod.

### 2a. Staging service

```bash
SERVICE_NAME="deployment-api-staging"
IMAGE="asia-northeast1-docker.pkg.dev/central-element-323112/unified-trading-system/deployment-api:latest"
SA="unified-trading-sa@central-element-323112.iam.gserviceaccount.com"

gcloud run deploy "${SERVICE_NAME}" \
  --project="central-element-323112" \
  --region="asia-northeast1" \
  --image="${IMAGE}" \
  --port=8080 \
  --memory=2Gi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=5 \
  --concurrency=80 \
  --timeout=900 \
  --service-account="${SA}" \
  --allow-unauthenticated \
  --set-env-vars="GCP_PROJECT_ID=central-element-323112,CLOUD_PROVIDER=gcp,CLOUD_MOCK_MODE=false,CLOUD_DEPLOYMENT_ENV=staging,DISABLE_AUTH=false" \
  --quiet
```

### 2b. Prod service

```bash
SERVICE_NAME="deployment-api-prod"

gcloud run deploy "${SERVICE_NAME}" \
  --project="central-element-323112" \
  --region="asia-northeast1" \
  --image="${IMAGE}" \
  --port=8080 \
  --memory=4Gi \
  --cpu=2 \
  --min-instances=1 \
  --max-instances=20 \
  --concurrency=80 \
  --timeout=900 \
  --service-account="${SA}" \
  --allow-unauthenticated \
  --set-env-vars="GCP_PROJECT_ID=central-element-323112,CLOUD_PROVIDER=gcp,CLOUD_MOCK_MODE=false,CLOUD_DEPLOYMENT_ENV=prod,DISABLE_AUTH=false" \
  --quiet
```

Key env-var: `CLOUD_DEPLOYMENT_ENV` must be `staging` or `prod` — deployment-api reads this at boot
to scope all Cloud Scheduler, GCS bucket, and live-cluster lookups to the correct tier
(per `codex/05-infrastructure/runtime-tiers-and-deployment.md`).

---

## 3. IAM bindings — Firebase cross-project access

Following the `firebase-split-topology.md` pattern: compute runs on prod project, Firebase data is on
the per-tier Firebase project. The prod compute SA needs Firebase roles on each Firebase project.

### 3a. Staging Firebase project (`odum-staging`)

```bash
SA_EMAIL="unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
STAGING_FB_PROJECT="odum-staging"

for role in roles/datastore.user roles/storage.admin roles/firebaseauth.admin; do
  gcloud projects add-iam-policy-binding "${STAGING_FB_PROJECT}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${role}" \
    --quiet
done
```

### 3b. Prod Firebase project (`odum-research` / `central-element-323112`)

Prod Firebase runs on the same GCP project as compute — no cross-project bindings needed. The SA
already has required roles via the project-level IAM.

### 3c. Verify Secret Manager access (both tiers)

deployment-api reads API keys + Firebase service account JSON from Secret Manager:

```bash
# Verify SA can access secrets (smoke-test)
gcloud secrets versions access latest \
  --secret="deployment-api-config" \
  --project="central-element-323112" \
  --impersonate-service-account="${SA_EMAIL}" 2>/dev/null && echo "OK" || echo "MISSING — provision secret first"
```

If the secret is missing, add it:

```bash
echo '{"firebase_project_id": "odum-staging", "gcp_project_id": "central-element-323112"}' | \
  gcloud secrets create deployment-api-config --data-file=- --project=central-element-323112
```

---

## 4. DNS + TLS — custom domain mapping

Cloud Run manages TLS automatically via Google-managed certificates when a domain mapping is created.

### 4a. Staging domain

```bash
gcloud beta run domain-mappings create \
  --service="deployment-api-staging" \
  --domain="staging.odum-research.com" \
  --region="asia-northeast1" \
  --project="central-element-323112"
```

Copy the DNS records printed by the above command and add them to Cloud DNS:

```bash
# Example — actual values come from domain-mappings describe output
gcloud beta run domain-mappings describe \
  --domain="staging.odum-research.com" \
  --region="asia-northeast1" \
  --project="central-element-323112"
# Add the CNAME or A record to Cloud DNS zone odum-research-com
```

TLS cert is provisioned automatically (allow 15–60 min for propagation).

### 4b. Prod domain

```bash
gcloud beta run domain-mappings create \
  --service="deployment-api-prod" \
  --domain="deployment.odum-research.com" \
  --region="asia-northeast1" \
  --project="central-element-323112"
```

Add DNS records from the output to the `odum-research-com` Cloud DNS zone.

---

## 5. Verify provisioning

Run after DNS propagates (verify with `dig staging.odum-research.com`):

```bash
# Staging
curl -sf https://staging.odum-research.com/api/health && echo "STAGING OK"
curl -sf https://staging.odum-research.com/api/health/detailed | python3 -m json.tool

# Prod
curl -sf https://deployment.odum-research.com/api/health && echo "PROD OK"
```

Expected response: `{"status": "ok", "env": "staging" | "prod", ...}`

Verify env badge in UI: open `https://staging.odum-research.com` in a browser — header badge should
show amber **STAGING** badge. Prod should show red **PROD** badge.

---

## 6. CI promotion wiring

Once staging and prod services exist, promote via the existing semver-agent + workflow machinery:

1. `live-defi-rollout` pushes trigger `cloudbuild-tier3.yaml` → image `:latest` updated.
2. Deploy to staging: `bash deployment-service/runbooks/deployment-ui-staging-deploy.md` (see G.2
   runbook at `deployment-service/runbooks/deployment-ui-staging-deploy.md`).
3. Promote to prod: operator runs the G.2 runbook with `--env=prod` after staging smoke-test passes.

No automated prod promotion — operator gate required (per G.3 operator sign-off gate, B6 gate).

---

## Temporary states + successor plans

- `deployment-api-staging` service: exists until env-tier is torn down (no planned teardown).
- Per-tier bucket provisioning (`CLOUD_DEPLOYMENT_ENV`-scoped bucket names) is tracked in
  `plans/active/bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0c.
