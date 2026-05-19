---
title: Deployment-UI — Staging Deploy
owner: ikenna
cadence: on-demand (per feature branch / pre-prod promotion)
verifier: operator (runs verification checklist below after deploy)
last_executed: never
source_plan: plans/active/deployment_ui_lifecycle_tabs_2026_05_08.md (Phase G.2)
prerequisite_runbook: deployment-service/runbooks/deployment-ui-staging-prod-provisioning.md (Phase H.4)
---

# Deployment-UI — Staging Deploy

Deploys the latest `live-defi-rollout` build of **deployment-api** (bundled with deployment-ui SPA) to
the **staging** Cloud Run service (`deployment-api-staging`), then runs the per-axis verification
checklist before promoting to prod.

Full-execution criterion: staging URL returns HTTP 200 on healthz + all 6 checklist axes pass.

---

## Pre-flight

```bash
# 1. Confirm staging service exists
gcloud run services describe deployment-api-staging \
  --project=central-element-323112 --region=asia-northeast1 \
  --format='value(status.url)' && echo "EXISTS" || echo "MISSING — run H.4 provisioning runbook first"

# 2. Confirm you are on the correct branch
git -C deployment-api log --oneline -3
# Expected: most recent commit on live-defi-rollout

# 3. Check for in-progress VM migrations (do not deploy during active migrations)
gcloud compute instances list \
  --project=central-element-323112 \
  --filter="name~gcs-migration-bundle AND status=RUNNING" \
  --format="value(name)" | wc -l
# Must be 0 before deploying to staging (data-status reads would reflect mid-migration state)
```

---

## 1. Build + push image

```bash
WORKSPACE_ROOT="$(pwd)"  # run from workspace root
cd deployment-api

# Pre-bundle deployment-ui, deployment-service, PM artefacts
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

# Cleanup
rm -rf ./ui ./_deployment-service ./codex-data ./pm-plans ./pm-configs

echo "Build complete — image: asia-northeast1-docker.pkg.dev/central-element-323112/unified-trading-system/deployment-api:latest"
```

Expected build time: ~5–10 min. Watch Cloud Build logs for FAILURE before proceeding.

---

## 2. Deploy to staging Cloud Run

```bash
IMAGE="asia-northeast1-docker.pkg.dev/central-element-323112/unified-trading-system/deployment-api:latest"
SA="unified-trading-sa@central-element-323112.iam.gserviceaccount.com"

gcloud run deploy "deployment-api-staging" \
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

STAGING_URL=$(gcloud run services describe deployment-api-staging \
  --project=central-element-323112 --region=asia-northeast1 \
  --format='value(status.url)')
echo "Staging URL: ${STAGING_URL}"
```

Expected: zero-downtime revision swap, new revision live within ~30s.

---

## 3. Per-axis verification checklist

Run ALL 6 axes. Staging deploy is complete only when all pass.

### Axis 1 — Health + env badge

```bash
# API health
curl -sf "${STAGING_URL}/api/health" | python3 -m json.tool
# Must contain: "status": "ok", "env": "staging"

# Detailed health (all components green)
curl -sf "${STAGING_URL}/api/health/detailed" | python3 -c "
import sys, json
d = json.load(sys.stdin)
failed = [k for k,v in d.items() if v.get('status') != 'ok']
print('FAIL:', failed) if failed else print('ALL COMPONENTS OK')
"
```

Open `${STAGING_URL}` in a browser:

- [ ] Header env badge shows amber **STAGING** label
- [ ] No JS console errors on page load

### Axis 2 — Cloud-toggle latency

In the browser UI:

- [ ] Switch cloud-toggle GCP → AWS: skeleton loaders appear, data refreshes in < 5s
- [ ] Switch back AWS → GCP: instant from cache (< 200ms)

### Axis 3 — Monitor sub-tab instant-feel

Open Monitor → Backfill, then switch to Experiments, Live, Scheduled:

- [ ] Each sub-tab switch: < 50ms perceptible delay (data pre-fetched by LifecyclePrefetchContext)
- [ ] No network requests fire on sub-tab toggle when cache is warm (check Network tab in DevTools)

### Axis 4 — Deploy-missing-schedulers idempotence

```bash
# POST to deploy-missing — should return 200 with an empty or already-scheduled list
curl -sf -X POST "${STAGING_URL}/api/monitor/scheduled/deploy-missing?cloud=gcp" \
  -H "Content-Type: application/json" | python3 -m json.tool
# Must not return 500. Idempotent: re-run twice, results identical.
```

### Axis 5 — Live-cluster lifecycle actions

```bash
# List live clusters (staging scope only)
curl -sf "${STAGING_URL}/api/monitor/live?cloud=gcp" | python3 -c "
import sys, json
clusters = json.load(sys.stdin)
print(f'Live clusters (staging): {len(clusters)}')
for c in clusters[:3]:
    print(f'  {c.get(\"name\")}: {c.get(\"lifecycle_class\")} / {c.get(\"status\")}')
"
```

- [ ] Response is a list (possibly empty if staging has no live clusters)
- [ ] No 500 / unhandled exception

### Axis 6 — Streaming logs render

```bash
# SSE log stream — confirm endpoint is reachable (HEAD check)
curl -sf -I "${STAGING_URL}/api/logs/stream/test-target-does-not-exist" 2>&1 | head -5
# Accept 404 (no such target) or 200 + event-stream content-type; reject 500
```

In browser, navigate Monitor → Backfill → select any backfill row → "Stream logs":

- [ ] Log panel opens (may show "no logs available" for staging — that is expected)
- [ ] No uncaught React errors

---

## 4. After all axes pass — record result

Update this runbook's frontmatter `last_executed` field and commit:

```bash
# Edit: deployment-service/runbooks/deployment-ui-staging-deploy.md
# Set: last_executed: <YYYY-MM-DD> (<short-sha>)
git -C deployment-api rev-parse --short HEAD  # use this SHA

cd unified-trading-pm
git add -p  # stage only the runbook line
git commit -m "docs(plans): deployment-ui staging deploy verified — <date>"
git push origin HEAD:live-defi-rollout
```

---

## 5. Promote to prod (operator-gated — G.3 B6 gate)

Production promotion requires **explicit operator sign-off** (G.3 gate). Do NOT auto-promote.

After sign-off:

```bash
gcloud run deploy "deployment-api-prod" \
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

PROD_URL=$(gcloud run services describe deployment-api-prod \
  --project=central-element-323112 --region=asia-northeast1 \
  --format='value(status.url)')
echo "Prod URL: ${PROD_URL}"

# Smoke-test prod
curl -sf "${PROD_URL}/api/health" | python3 -m json.tool
# Must contain "env": "prod"
```

Re-run Axis 1 health check against prod URL. Axis 2-6 smoke on the prod domain.
