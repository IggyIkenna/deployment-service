---
title: VM Log Archival — Daily archival of VM logs to durable storage
owner: ikenna
cadence: "0 2 * * * UTC (Cloud Scheduler — vm-log-archival-prd, ENABLED)"
verifier: operator
last_executed: "2026-06-02 (manual gcloud run jobs execute — execution t8j2d succeeded=1; image fixed to deployment-service:latest + jinja2/flask/functions-framework deps)"
source_plan: plans/active/issues/deployment_scripts_bucket_softdelete_log_churn_2026_06_01.md
---

# VM Log Archival — Daily Maintenance

Daily Cloud Run Job that copies live VM logs from the 14-day-TTL prefix
(`gs://deployment-scripts-{project}/vm-logs/`) to a durable archive
(`gs://deployment-scripts-{project}/log-archive/rolling/{date}/`). This is the
**snapshot-before-delete** half of the deployment-scripts lifecycle: the
`vm-logs/`>14d deletion rule is only safe because this job archives first.

**Terraform SSOT**: `terraform/gcp/vm_log_archival_scheduler.tf`
**Script**: `scripts/vm/vm_log_archival_cron.py`
**Image**: `unified-trading-system/deployment-service:latest` (maintenance-jobs stage)
**Job**: `vm-log-archival-prd` · **Cron**: `vm-log-archival-prd`

---

## What the job does

1. Lists VM log objects under `gs://deployment-scripts-{project}/vm-logs/`.
2. Copies each to `log-archive/rolling/{date}/` (durable, no 14-day TTL).
3. Exits 0 on success; failure-isolated logging.

The job imports the full `deployment_service` backends chain, so its image is the
`maintenance-jobs` Dockerfile stage (api stage + `scripts/` + `jinja2`/`flask`/
`functions-framework` — deps the `--no-deps` UTL base lacks). Built+pushed by
`cloud-build/deployment-service-jobs-image.cloudbuild.yaml`.

---

## Manual execution

```bash
# Dry-run (preview, no copies)
gcloud run jobs execute vm-log-archival-prd \
  --project=central-element-323112 \
  --region=asia-northeast1 \
  --args="scripts/vm/vm_log_archival_cron.py,--project-id,central-element-323112,--dry-run" \
  --wait

# Live run
gcloud run jobs execute vm-log-archival-prd \
  --project=central-element-323112 \
  --region=asia-northeast1 \
  --wait
```

Inspect logs:

```bash
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="vm-log-archival-prd"' \
  --project=central-element-323112 --limit=100 --format='value(textPayload)'
```

---

## Verification

After a run, confirm the execution succeeded and the archive prefix grew:

```bash
gcloud run jobs executions list --job=vm-log-archival-prd \
  --project=central-element-323112 --region=asia-northeast1 \
  --limit=1 --format='value(name, status.succeededCount, status.failedCount)'
# expect: <name>  1  (succeeded=1, failed empty)
```

---

## Image refresh

The job runs whatever `deployment-service:latest` resolved to at job-deploy time
(Cloud Run pins the digest). To pick up new code, rebuild + re-resolve:

```bash
gcloud builds submit \
  --config=cloud-build/deployment-service-jobs-image.cloudbuild.yaml \
  --region=asia-northeast1 --project=central-element-323112 \
  --substitutions=_GIT_SHA="$(git rev-parse --short HEAD)" .
gcloud run jobs update vm-log-archival-prd \
  --image="asia-northeast1-docker.pkg.dev/central-element-323112/unified-trading-system/deployment-service:latest" \
  --project=central-element-323112 --region=asia-northeast1
```

A Cloud Build trigger (`deployment-service-jobs-image-build`, push to `main`,
`includedFiles` scoped to `Dockerfile` / `cloud-build/deployment-service-jobs-image.cloudbuild.yaml`
/ `scripts/vm/**` / `pyproject.toml` / `uv.lock`) automates the rebuild; the
`gcloud run jobs update` re-resolve is still required after a fresh image push
(or rely on the next scheduled run if the job is re-deployed).
