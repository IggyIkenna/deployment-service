# Variables for the live_event_log terraform module.
# Plan: live_persist_03_infra_pubsub_sinks_2026_06_26.md

variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "central-element-323112"
}

variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
  default     = "prod"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod"
  }
}

variable "warm_gcs_bucket" {
  description = "GCS bucket for warm sink (~7-day retention, 5-minute parquet batches)"
  type        = string
}

variable "cold_gcs_bucket" {
  description = "GCS bucket for cold compacted storage (daily parquet files)"
  type        = string
}

variable "compactor_image" {
  description = "Container image for the daily cold compaction Cloud Run Job. Reuses the shared deployment-service maintenance-jobs image (same one used by uts-prod-tarball-cleanup / vm-log-archival-prd) rather than a dedicated never-built image — no new build pipeline needed; the CMD is overridden per-job (see compaction_job.tf's container command/args)."
  type        = string
  default     = "asia-northeast1-docker.pkg.dev/central-element-323112/unified-trading-system/deployment-service:latest"
}

variable "compactor_sa_email" {
  description = "Service account email for the compaction job (needs GCS read/write + Cloud Run invoker)"
  type        = string
}

variable "create_bq_external_tables" {
  description = "Create BigQuery external tables over warm GCS sink. Enable once warm-sink data exists (autodetect requires at least one file)."
  type        = bool
  default     = false
}

variable "project_number" {
  description = "GCP project number — used to derive default compute SA email (PROJECT_NUMBER-compute@developer.gserviceaccount.com) for publisher IAM on persist-* topics."
  type        = string
  default     = "1060025368044"
}
