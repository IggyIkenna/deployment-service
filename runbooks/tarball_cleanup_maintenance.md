---
title: Tarball Cleanup — Daily maintenance of SHA-versioned code tarballs
owner: ikenna
cadence: "0 2 * * * UTC (Cloud Scheduler)"
verifier: operator
last_executed: never
source_plan: plans/active/issues/issue_docs_remediation_sweep_2026_06_02.md
---

# Tarball Cleanup — Daily Maintenance

Daily Cloud Run Job that prunes old SHA-versioned code tarballs from the
`deployment-scripts-{project_id}` GCS bucket. Retains the 5 most-recent
tarballs per service; deletes the rest.

**Terraform SSOT**: `terraform/gcp/tarball_cleanup_scheduler.tf`
**Script**: `scripts/vm/cleanup_old_tarballs.py`
**Bucket**: `gs://deployment-scripts-{project_id}/code/`

---

## What the job does

1. Lists all `.tar.gz` objects under `gs://deployment-scripts-{project_id}/code/`.
2. Groups them by service name (parsed from `<service>@<sha>.tar.gz` or
   `<service>-code-<sha>.tar.gz` filename patterns).
3. For each service, keeps the 5 most-recent tarballs (ordered by GCS object mtime)
   and deletes the rest using `gcs_delete_object` (REST API, not gsutil subprocess).
4. Single-version tarballs (`<service>-code.tar.gz` — no sha suffix) are untouched.

---

## Manual execution

Run a dry-run first to confirm what would be deleted:

```bash
# Dry-run (no deletions)
gcloud run jobs execute uts-prod-tarball-cleanup \
  --project=central-element-323112 \
  --region=asia-northeast1 \
  --args="deployment-service/scripts/vm/cleanup_old_tarballs.py,--project,central-element-323112,--keep,5,--dry-run" \
  --wait
```

Inspect the Cloud Run Job logs:

```bash
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="uts-prod-tarball-cleanup"' \
  --project=central-element-323112 \
  --limit=100 \
  --format='value(textPayload)'
```

Live run (actually deletes old tarballs):

```bash
gcloud run jobs execute uts-prod-tarball-cleanup \
  --project=central-element-323112 \
  --region=asia-northeast1 \
  --wait
```

Or invoke the script directly (requires ADC + UTL installed):

```bash
cd deployment-service
python scripts/vm/cleanup_old_tarballs.py \
  --project central-element-323112 \
  --keep 5 \
  --dry-run
```

---

## Verification

After a successful run, verify the bucket object count dropped:

```bash
# Count SHA-versioned tarballs per service
gsutil ls -l gs://deployment-scripts-central-element-323112/code/*.tar.gz 2>/dev/null \
  | grep -v '^TOTAL' \
  | awk '{print $NF}' \
  | sed 's|.*/||' \
  | sed 's/@[a-f0-9]*\.tar\.gz$//' \
  | sort | uniq -c | sort -rn | head -20

# Expected: each service has ≤5 entries
```

Confirm the Cloud Scheduler cron last-execution status:

```bash
gcloud scheduler jobs describe uts-prod-tarball-cleanup-cron \
  --project=central-element-323112 \
  --location=asia-northeast1 \
  --format='value(lastAttemptTime,status.code)'
# Expected: recent timestamp + SUCCESS (or empty if never fired)
```

---

## Triggering ad-hoc via Cloud Scheduler

```bash
gcloud scheduler jobs run uts-prod-tarball-cleanup-cron \
  --project=central-element-323112 \
  --location=asia-northeast1
```

---

## After verification — update last_executed

```bash
# Edit this runbook frontmatter: last_executed: <YYYY-MM-DD> (<sha>)
cd unified-trading-pm
git add deployment-service/runbooks/tarball_cleanup_maintenance.md
git commit -m "docs(plans): tarball-cleanup runbook verified — <date>"
git push origin HEAD:live-defi-rollout
```

---

## Escalation

If the job fails repeatedly:

1. Check ADC credentials on the Cloud Run Job SA (`unified-trading-sa@...`).
2. Confirm `storage.objectAdmin` is granted on `deployment-scripts-central-element-323112`.
3. Check for GCS API quota errors in logs.
4. File a `plans/active/issues/` doc if the root cause is systemic.
