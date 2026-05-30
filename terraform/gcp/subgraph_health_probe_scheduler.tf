# Subgraph Health Probe — Cloud Run Job + Cloud Scheduler cron (6h cadence)
#
# Plan-of-record:
#   CLAUDE.md HARD RULE "Data Pipeline Correctness Is The Heartbeat"
#   (codified 2026-05-20).
#   plans/active/solana_defi_legacy_migration_2026_05_27.md § Hardening.
#
# What the Job does:
#   1. Clone market-tick-data-service @ live-defi-rollout using a GH_PAT
#      from Secret Manager.
#   2. Install probe deps (UAC + UTL + MTDS + pyarrow / requests /
#      google-cloud-* libs).
#   3. Run `python scripts/subgraph_health_probe.py` — probes every
#      (protocol, chain) in SUBGRAPH_IDS for four signals (HEAD_LAG /
#      EMPTY_YESTERDAY / DEPLOYMENT_CHANGED / SCHEMA_INVALID) and
#      publishes alerts to `defi_data_quality_alerts` Pub/Sub topic
#      (subscribed by alerting-service).
#
# Image
#   `gcr.io/google.com/cloudsdktool/google-cloud-cli:slim` — has bash +
#   git + python + gcloud preinstalled. No private image build needed.
#
# Service account
#   Reuses `t1_batch_sa` (already has Secret Manager + Pub/Sub publish
#   roles via existing bindings). GH_PAT + thegraph-api-key secret
#   accessor bindings reuse the orphan-ping-audit grant pattern.
#
# Cadence — `0 */6 * * *` UTC (every 6h)
#   * 1/4 day catches drift within the HEAD_LAG threshold.
#   * Matches the operational gap that hit AAVE_V3-OPTIMISM between
#     2026-05-08 and 2026-05-29 (silent Aave republish to empty v0.0.5).

# -------------------------------------------------------
# Pub/Sub topic for probe alerts (subscribed by alerting-service).
# -------------------------------------------------------
resource "google_pubsub_topic" "defi_data_quality_alerts" {
  name    = "defi_data_quality_alerts"
  project = var.project_id

  labels = {
    "managed-by" = "terraform"
    "service"    = "subgraph-health-probe"
    "purpose"    = "defi-data-quality"
  }
}

# Bind thegraph-api-key secret accessor to t1_batch SA.
resource "google_secret_manager_secret_iam_member" "t1_batch_thegraph_api_key_accessor" {
  secret_id = "thegraph-api-key"
  project   = var.project_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.t1_batch.email}"
}

# Allow t1_batch SA to publish to the alerts topic.
resource "google_pubsub_topic_iam_member" "t1_batch_defi_alerts_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.defi_data_quality_alerts.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.t1_batch.email}"
}

# -------------------------------------------------------
# Cloud Run Job — subgraph health probe
# -------------------------------------------------------
resource "google_cloud_run_v2_job" "subgraph_health_probe" {
  name                = "${local.env_prefix}-subgraph-health-probe"
  location            = var.region
  project             = var.project_id
  deletion_protection = false

  template {
    parallelism = 1
    task_count  = 1

    template {
      timeout               = "900s" # 15min — probes are sequential, ~70 cells × ~1s each
      service_account       = google_service_account.t1_batch.email
      max_retries           = 1
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"

      containers {
        image = "gcr.io/google.com/cloudsdktool/google-cloud-cli:slim"

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }

        command = ["bash"]
        args = [
          "-c",
          "git clone --depth=10 --branch=live-defi-rollout https://x-access-token:$${GH_PAT}@github.com/IggyIkenna/market-tick-data-service.git /tmp/mtds && bash /tmp/mtds/scripts/cron_subgraph_health_probe_entrypoint.sh",
        ]

        env {
          name  = "MTDS_BRANCH"
          value = "live-defi-rollout"
        }
        env {
          name  = "MTDS_REPO_URL"
          value = "https://github.com/IggyIkenna/market-tick-data-service.git"
        }
        env {
          name  = "CLOUD_PROVIDER"
          value = "gcp"
        }
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "DEPLOYMENT_ENV_SHORT"
          value = local.env_prefix
        }
        env {
          name = "GH_PAT"
          value_source {
            secret_key_ref {
              secret  = "GH_PAT"
              version = "latest"
            }
          }
        }
      }
    }
  }

  labels = {
    "managed-by" = "terraform"
    "service"    = "subgraph-health-probe"
    "purpose"    = "defi-data-quality"
    "owner"      = "ikenna"
  }

  depends_on = [
    google_secret_manager_secret_iam_member.t1_batch_thegraph_api_key_accessor,
    google_pubsub_topic_iam_member.t1_batch_defi_alerts_publisher,
  ]
}

# -------------------------------------------------------
# Cloud Scheduler — 6h cadence
# -------------------------------------------------------
resource "google_cloud_scheduler_job" "subgraph_health_probe_cron" {
  name        = "${local.env_prefix}-subgraph-health-probe-cron"
  description = "Run subgraph_health_probe.py every 6h. Catches silent data-loss / deployment-hash changes / schema regressions. Plan: solana_defi_legacy_migration_2026_05_27.md."
  schedule    = "0 */6 * * *"
  time_zone   = "UTC"
  region      = var.region
  project     = var.project_id

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.subgraph_health_probe.name}:run"

    oauth_token {
      service_account_email = google_service_account.t1_batch.email
    }
  }

  retry_config {
    retry_count          = 0
    min_backoff_duration = "5s"
    max_backoff_duration = "60s"
    max_doublings        = 1
  }
}

# -------------------------------------------------------
# Outputs
# -------------------------------------------------------
output "subgraph_health_probe_job_name" {
  description = "Cloud Run Job name for ad-hoc manual fire."
  value       = google_cloud_run_v2_job.subgraph_health_probe.name
}

output "subgraph_health_probe_cron_name" {
  description = "Cloud Scheduler cron name."
  value       = google_cloud_scheduler_job.subgraph_health_probe_cron.name
}

output "defi_data_quality_alerts_topic" {
  description = "Pub/Sub topic alerting-service subscribes to for probe alerts."
  value       = google_pubsub_topic.defi_data_quality_alerts.name
}
