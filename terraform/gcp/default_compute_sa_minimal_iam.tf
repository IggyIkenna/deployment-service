# =============================================================================
# Default Compute SA — Minimal IAM Profile
# =============================================================================
#
# AUDIT DATE: 2026-08-04
# SOURCE PLAN: bucket_iam_p2_tier_sa_scope_gap_and_default_compute_sa_overprivilege_2026_07_30.md P3.1
#
# BACKGROUND: The GCP default compute SA
#   {project_number}-compute@developer.gserviceaccount.com
# held 28 unconditional project-wide IAM roles as of 2026-07-30 (live-verified via
# `gcloud projects get-iam-policy`). Only a subset is genuinely needed by the VM launchers
# and their runtime applications.
#
# LAUNCHER MIGRATION STATUS (2026-08-04):
#   116/169 launchers now use explicit --service-account= (mostly lc_tier_service_account())
#   53/169 launchers still fall back to the default compute SA
#   (full list: bucket_iam_p2_tier_sa_scope_gap_and_default_compute_sa_overprivilege_2026_07_30.md)
#
# =============================================================================
# AUDIT: What the remaining 53 launchers + their VMs actually use
# =============================================================================
#
# Launcher (gcloud CLI on the orchestrator/Cloud Run Job host):
#   - gcloud compute instances create/delete/ssh  → roles/compute.instanceAdmin.v1
#   - gcloud secrets versions access               → roles/secretmanager.secretAccessor
#   - gcloud run jobs execute                       → roles/run.invoker
#   - gcloud scheduler jobs describe                → roles/cloudscheduler.viewer
#   - gcloud artifacts repositories                 → roles/artifactregistry.reader
#   - gcloud auth print-access-token                → (no IAM role needed — OAuth2 metadata)
#
# VM runtime (setup-data-pipeline-vm.sh + Python service code):
#   - GCS read (code tarballs, wheel cache)        → roles/storage.objectViewer
#   - GCS write (vm-logs, heartbeat, exit status)  → roles/storage.objectCreator
#     ^ objectUser = objectViewer + objectCreator; using objectUser for simplicity
#   - gcloud compute instances delete (preemption) → roles/compute.instanceAdmin.v1
#   - Secret Manager (API keys for Tardis/Databento/etc.) → roles/secretmanager.secretAccessor
#   - Pub/Sub publish (live data streaming)        → roles/pubsub.publisher
#   - Pub/Sub subscribe (alerting relay)           → roles/pubsub.subscriber
#   - Firestore/Datastore (deployment registry)    → roles/datastore.user
#   - BigQuery (query jobs — some services)        → roles/bigquery.jobUser
#   - GCP Logging (auto-granted by GCP)            → roles/logging.logWriter
#   - GCP Monitoring (auto-granted by GCP)         → roles/monitoring.metricWriter
#
# =============================================================================
# ROLES THAT CAN BE SAFELY REMOVED (not used by any launcher or VM runtime)
# =============================================================================
#
# HIGH-RISK (should remove first):
#   roles/storage.admin                    — bucket IAM policy writes (no launcher does this)
#   roles/iam.serviceAccountTokenCreator   — SA impersonation (no launcher impersonates other SAs)
#   roles/bigquery.admin                   — full BQ admin (excessive; only jobUser needed)
#   roles/firebaseauth.admin               — Firebase auth (not used)
#
# MEDIUM-RISK:
#   roles/bigquery.dataEditor              — covered by jobUser for query-only services
#   roles/bigquery.user                    — covered by jobUser for query-only services
#   roles/cloudscheduler.admin             — only viewer needed for launcher describe calls
#   roles/run.developer                    — only invoker needed for job execution
#
# LOW-RISK (duplicates or over-scoped):
#   roles/storage.objectAdmin              — objectUser covers read+write needs
#   roles/storage.objectCreator            — covered by objectUser
#   roles/storage.objectViewer             — covered by objectUser
#   roles/compute.instanceAdmin            — instanceAdmin.v1 is the modern equivalent
#   (any remaining ~14 roles not listed above — audit each against the "actually uses" list)
#
# =============================================================================
# MINIMAL PROJECT-LEVEL IAM PROFILE
# =============================================================================
#
# These are the project-level roles the default compute SA genuinely needs.
# The operator should:
#   1. Apply this terraform (idempotent — adds only what's listed here)
#   2. Manually remove any project-level IAM binding for the default compute SA
#      that is NOT listed below (use `gcloud projects remove-iam-policy-binding`)
#   3. Verify via `gcloud projects get-iam-policy` that only these roles remain
#
# Per-bucket and per-topic grants (scoped, not project-wide) are in their respective
# terraform files: alerting_relay_pubsub.tf, features_service_events_pubsub.tf,
# catalogue_regen_scheduler.tf, honest_coverage_scheduler.tf. These are already
# correctly scoped and are NOT part of the 28 unconditional roles problem.

# ---------------------------------------------------------------------------
# Compute Engine — VM lifecycle management
# ---------------------------------------------------------------------------
# Launch scripts create VMs; VMs self-delete on preemption.
# Also covers gcloud compute ssh (debug) and firewall-rules describe.
resource "google_project_iam_member" "default_compute_sa_instance_admin" {
  project = var.project_id
  role    = "roles/compute.instanceAdmin.v1"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# ---------------------------------------------------------------------------
# Storage — GCS object read/write
# ---------------------------------------------------------------------------
# VMs download code tarballs/wheels from GCS and upload logs/heartbeats/exit status.
# Using objectUser (objectViewer + objectCreator) — objectAdmin is NOT needed
# (no launcher sets object ACLs or manages bucket metadata).
resource "google_project_iam_member" "default_compute_sa_storage_object_user" {
  project = var.project_id
  role    = "roles/storage.objectUser"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# ---------------------------------------------------------------------------
# Secret Manager — API key access
# ---------------------------------------------------------------------------
# VMs read Tardis, Databento, and other API keys from Secret Manager.
resource "google_project_iam_member" "default_compute_sa_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# ---------------------------------------------------------------------------
# Pub/Sub — live data streaming
# ---------------------------------------------------------------------------
# Some VM services publish to Pub/Sub topics (event log, market data).
# Per-topic grants are in features_service_events_pubsub.tf; this project-level
# grant covers the general case for services that create topics dynamically.
resource "google_project_iam_member" "default_compute_sa_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# ---------------------------------------------------------------------------
# Artifact Registry — Docker image pull
# ---------------------------------------------------------------------------
# Dashboard VM and other container-image VMs pull from Artifact Registry.
resource "google_project_iam_member" "default_compute_sa_artifact_registry_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# ---------------------------------------------------------------------------
# Cloud Run — job invocation
# ---------------------------------------------------------------------------
# Some launcher scripts invoke Cloud Run jobs (gcloud run jobs execute).
resource "google_project_iam_member" "default_compute_sa_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# ---------------------------------------------------------------------------
# BigQuery — query jobs only (NOT admin/dataEditor/user)
# ---------------------------------------------------------------------------
# Some VM services run BigQuery queries. jobUser allows creating query jobs
# without the broader dataEditor/user/admin grants.
resource "google_project_iam_member" "default_compute_sa_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# =============================================================================
# ALREADY MANAGED ELSEWHERE (not duplicated here)
# =============================================================================
# - roles/datastore.user              → main.tf (google_project_iam_member.default_compute_sa_datastore_user)
# - roles/pubsub.subscriber (alerting)→ alerting_relay_pubsub.tf (topic-level grant)
# - roles/pubsub.publisher (features) → features_service_events_pubsub.tf (topic-level grant)
# - roles/storage.objectViewer (cat.) → catalogue_regen_scheduler.tf (bucket-level grant)
# - roles/iam.serviceAccountUser      → honest_coverage_scheduler.tf (actAs binding ON the SA)
#
# =============================================================================
# GCP-MANAGED DEFAULTS (auto-granted, harmless, no terraform needed)
# =============================================================================
# - roles/logging.logWriter           → automatically granted to all compute SAs
# - roles/monitoring.metricWriter     → automatically granted to all compute SAs
# These are NOT part of the over-privilege problem and are excluded from this audit.

# =============================================================================
# REMOVAL INSTRUCTIONS (operator action — NOT terraform-automated)
# =============================================================================
#
# After `tofu apply` of this file, run the following for EACH role NOT listed above
# that the default compute SA currently holds (use `gcloud projects get-iam-policy`
# to see the live list; compare against the 7 project-level roles declared above
# + the 5 scoped grants in other .tf files):
#
#   gcloud projects remove-iam-policy-binding ${PROJECT_ID} \
#     --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
#     --role="<ROLE_TO_REMOVE>"
#
# Priority order:
#   1. roles/storage.admin (bucket IAM writes — highest blast radius)
#   2. roles/iam.serviceAccountTokenCreator (SA impersonation)
#   3. roles/bigquery.admin (full BQ admin)
#   4. roles/firebaseauth.admin (Firebase — not used by any VM)
#   5. roles/cloudscheduler.admin (full scheduler admin)
#   6. roles/run.developer (full Cloud Run developer)
#   7. roles/bigquery.dataEditor, roles/bigquery.user (excessive for query-only)
#   8. roles/storage.objectAdmin (excessive — objectUser is sufficient)
#   9. All remaining non-default, non-listed-above roles
#
# Each removal is reversible via `gcloud projects add-iam-policy-binding` if a
# previously-unseen path surfaces. This file is the SSOT for which roles belong.
