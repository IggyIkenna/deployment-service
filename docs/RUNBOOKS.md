<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.md`, `data_status_multi_axis_shard_propagation_2026_05_06.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Operations Runbooks

**Last consolidated:** 2026-08-05

This document consolidates operational runbooks for the unified trading deployment system.

---

## Part 1: 2025 Backfill Runbook (Optimized Mode)

For large backfills (year-scale) where shard counts can reach tens of thousands and the UI must remain responsive.

### Why Optimized Mode Exists

- Large deployments often require **rolling concurrency** (you cannot run 10k–50k jobs at once).
- Cloud Run has a **hard cap of 1000 running executions per region**, and the Job Run API has relatively low write quotas.
- The UI is fast only if it **doesn't download every shard** and **doesn't analyze logs** on every poll.

Optimized mode enables:

- **Quota-safe concurrency** (rolling scheduling + throttled launch rate)
- **Shard count reduction** (date chunking heuristics if you didn't explicitly set it)
- **Fast UI polling** (summary mode + paginated shard inspection)

### Recommended Workflow (UI)

1. **Select service** (e.g. `market-tick-data-handler`).
2. **Compute**
   - Prefer **Cloud Run** for broad, steady throughput.
   - Prefer **VM** when shards are heavy (e.g. venue overrides like COINBASE) or you need large RAM.
3. **Set date range**: `2025-01-01` → `2025-12-31`.
4. **Select categories**: `CEFI`, `TRADFI`, `DEFI`.
5. Turn on **Optimized mode**.
   - Keep the recommended **date granularity** (weekly/monthly) unless you have a strong reason.
   - Keep **max concurrent** at or below the suggested value (Cloud Run: ≤ 900).
6. **Dry run first**.
   - Review the "Advisor" warnings/notes in the dry-run result panel.
   - Confirm shard count is reasonable (monthly/weekly should reduce job count dramatically).
7. Click **Deploy Live**.

### Monitoring a Large Backfill (UI)

- The deployment details view polls with `skip_logs=true&summary=true` for speed.
- The **Shards** tab does not auto-load all shards. Use:
  - **Load page** (paginated) to inspect running/failed shards without downloading everything.
  - **Load all** only if you truly need the full shard list (can be slow for huge deployments).
- The **Logs** tab loads logs lazily when opened.

### Common Failure Modes & Fixes

#### 1) Cloud Run quota / 429s / "too many running executions"

- Lower **max concurrent** (stay ≤ 900).
- Expect rolling behavior for large shard counts.
- If a single region is saturated, Cloud Run backend may failover regions for launches; refresh uses region-aware batching.

#### 2) Vendor/API rate limits → shards "hang" in RUNNING

- Reduce **container max_workers**.
- Ensure key rotation is enabled via `SHARD_INDEX`/`TOTAL_SHARDS` (Optimized mode injects these for Cloud Run/VM).
- If failures are concentrated in one vendor/venue, reduce concurrency for that slice.

#### 3) Cloud Run memory/CPU constraints

- Cloud Run max memory is **32Gi**. If service configs suggest 64Gi/128Gi, use **VM** or resize the Job template.
- Avoid `skip_venue_sharding` on Cloud Run unless you are sure the Job template can handle the heavier shard payload.

#### 4) Long-running or stuck VM shards

- VM shards use `timeout_seconds` (from compute config). Auto-sync applies conservative "stuck" detection when a shard exceeds `timeout_seconds + grace`.
- Use **Refresh** and **Report** to see failure categories and retry patterns.

---

## Part 2: Status Tab Blank - Troubleshooting Guide

**Issue:** Status tab works for some users but shows blank/empty for others.
**Root Cause:** GCP permissions differences.

### Why Status Tab Might Be Blank

The Status tab needs access to **4 different GCP resources:**

1. **GCS Buckets** (Data timestamps)
2. **Deployment State Bucket** (Last deployment)
3. **Cloud Build API** (Last build)
4. **GitHub Token via Secret Manager** (Last code push)

If **any** of these fail, the tab might show blank or partial data.

### Required Permissions

#### 1. GCS Bucket Access (For Data Timestamps)

**Buckets needed:**

- `instruments-store-cefi-test-project`
- `instruments-store-tradfi-test-project`
- `instruments-store-defi-test-project`
- `market-data-tick-cefi-test-project`
- `market-data-tick-tradfi-test-project`
- `market-data-tick-defi-test-project`

```bash
# Check if you have access:
gsutil ls gs://instruments-store-cefi-test-project/ | head -5

# If fails, grant roles/storage.objectViewer
```

#### 2. Deployment State Bucket (For Last Deployment)

**Bucket:** `deployment-orchestration-test-project`

```bash
gsutil ls gs://deployment-orchestration-test-project/deployments/ | head -5
```

#### 3. Cloud Build API (For Last Build)

```bash
gcloud builds list --region=asia-northeast1 --limit=1
# Requires roles/cloudbuild.builds.viewer
```

#### 4. GitHub Token Access (For Last Code Push)

Requires **TWO** permissions:

**A. Secret Manager:**

```bash
gcloud projects add-iam-policy-binding test-project \
  --member="user:YOUR_EMAIL" \
  --role="roles/secretmanager.secretAccessor"
```

**B. Service Account impersonation:**

```bash
gcloud iam service-accounts add-iam-policy-binding \
  github-token-sa@test-project.iam.gserviceaccount.com \
  --project=test-project \
  --member="user:YOUR_EMAIL" \
  --role="roles/iam.serviceAccountTokenCreator"
```

### Diagnostic Steps

1. **Check API response:** `curl http://localhost:8000/api/service-status/instruments-service/status | python3 -m json.tool`
2. Look for `null` or `"error"` in `last_data_update`, `last_deployment`, `last_build`, `last_code_push`
3. Check backend logs for error messages
4. Check browser console (F12) for 403/404 on `/api/service-status/`

### Most Likely Issue

If Status tab works for one user but not another: the failing user likely lacks **GitHub token impersonation** (`roles/iam.serviceAccountTokenCreator` on `github-token-sa`).

### Permissions Checklist

- [ ] `roles/storage.objectViewer` on project or buckets
- [ ] `roles/cloudbuild.builds.viewer` on project
- [ ] `roles/secretmanager.secretAccessor` on project (or github-token secret)
- [ ] `roles/iam.serviceAccountTokenCreator` on github-token-sa

**The last one (SA impersonation) is often missing.**

---

## Part 3: `DP_RUN_MOSTLY_EMPTY` — `check_high_attempted_failed` Alerts (DP-FETCH-009)

### What This Monitor Does

`check_high_attempted_failed` (`meta_watchers.py`, DP-FETCH-009) pages when a
`(asset_group, data_type)` cell in the consolidated availability manifest has a HIGH
`attempted_failed` count or ratio. It reuses the `DP_RUN_MOSTLY_EMPTY` event (CRITICAL,
PAGE_OPERATOR, routed to `#data-pipeline-alerts`).

A cell is HIGH when either:

- `attempted_failed` count ≥ `ATTEMPTED_FAILED_ABS_THRESHOLD` (default 500), **OR**
- `attempted_failed` count ≥ `MIN_ATTEMPTED_FAILED_FOR_RATIO` (default 100) **AND**
  `attempted_failed / (captured + attempted_failed)` ≥ `ATTEMPTED_FAILED_RATIO_THRESHOLD`
  (default 0.10 / 10%)

The monitor reads the same consolidated `_index/availability_index.parquet` blob the
manifest consolidator writes — it does NOT walk GCS objects. It gates on
`min_consecutive` consecutive sweeps before paging (transient-blip suppression), and
annotates stale cells with `STATIC BACKLOG` labels (≥1 day since newest
`attempted_failed` row) via `attempted_failed_staleness.py`.

### Known Alert: sports/TRADES ~87.2% Ratio Spike (K1/K2 Denominator-Shrink Artifact)

**Symptom:** `DP_RUN_MOSTLY_EMPTY` fires for `(sports, TRADES)` with an
`attempted_failed` ratio in the 80-90% range (observed ~87.2%).

**Root cause — K1/K2 casing-migration denominator shrink.** The K1/K2 casing migration
(`market-tick-data-service@2536b91c` / `@ad4f1872`, 2026-07-20) physically copied
~260,298 GCS objects + manifest rows from lower-case to UPPER-case paths for sports
`instrument_type=ODDS, data_type=TRADES`. This doubled the denominator
(`captured + attempted_failed`) in the consolidated manifest — but the UPPER-case twin
cells are predominantly `attempted_failed` (historical residue from before the casing
flip, never genuinely captured at the uppercase path), while the lower-case originals
continue to capture normally. The net effect: a single `(sports, TRADES)` cell
aggregates both populations, producing an ~87.2% ratio from the uppercase twin's
`attempted_failed` tail dwarfing the genuine lower-case `captured` count.

**Status — already-dead residue, NOT a live outage.** The K1/K2 migration is slated for
REVERT (Track C, `sports_consolidated_closeout_2026_07_19.md`). The uppercase twin
objects are frozen historical residue — no new attempts are being made against those
paths. The lower-case originals continue to capture normally. The `DP_RUN_MOSTLY_EMPTY`
alert is a denominator artifact, not evidence of a new capture regression.

**What to do when this alert fires:**

1. **Check for fresh activity.** Verify whether the cell's `max_attempted_at` timestamp
   is recent (within the last day) or stale. If the newest `attempted_failed` row is
   days/weeks old, the alert is the known K1/K2 backlog — acknowledge and suppress.
2. **Check the STATIC BACKLOG annotation.** The alert body includes staleness
   annotations from `attempted_failed_staleness.py`. A "STATIC BACKLOG" label with
   `no new attempted_failed activity in Nd` confirms this is the known artifact.
3. **Verify via the manifest.** Query `instruments-store-sports-prd-{project_id}`
   `_index/availability_index.parquet` for `asset_group=sports, data_type=TRADES`
   — check whether `captured` is still climbing (normal lower-case capture continues)
   while `attempted_failed` is static.
4. **Do NOT re-diagnose from scratch.** This is a tracked, pending-revert artifact.
   The resolution is the K1/K2 casing REVERT (Track C, operator-scheduled) — not a new
   capture bug, not a writer regression, not a credential gap.

**Related:**

- Plan: `/plans/active/sports_consolidated_closeout_2026_07_19.md` Track S2,
  Track C (K1/K2 revert)
- Issue: `/plans/active/issues/cefi_high_attempted_failed_batch_cluster_2026_07_23.md`
  (alerting-hygiene question, staleness labeling)
- Code: `deployment_service/data_pipeline_monitors/attempted_failed_staleness.py`
  (staleness labeling), `known_dead_cells_registry.py` (suppression registry —
  sports/TRADES is NOT registered here because it is pending-revert, not
  deliberately-narrowed)

### Other Known-False or Static-Backlog Cells

The `known_dead_cells_registry.py` (`KNOWN_DEAD_CELLS` dict) registers cells whose
`attempted_failed` population is deliberately-deferred — their UAC
`expected_coverage`/`VENUE_DATA_TYPE_CAPABILITIES` entry was narrowed to stop new
attempts, freezing the historical count. See that module's docstring for the full
registry and its safety gate (any new activity AFTER `narrowed_at` re-enables paging).

**Currently registered:** `(tradfi, ohlcv_15m)` — CBOE ohlcv_15m narrowed 2026-07-15
(`unified-api-contracts@78b9e899`).

**Adding a new entry:** confirm the cell has zero new `attempted_failed` activity since
the narrowing date, then add a `KnownDeadCell` entry to `KNOWN_DEAD_CELLS`. The entry
suppresses only while `max_attempted_at ≤ narrowed_at` — new activity automatically
re-enables paging without a code change.
