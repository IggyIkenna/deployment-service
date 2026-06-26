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
  description = "Container image for the daily cold compaction Cloud Run Job"
  type        = string
  default     = "gcr.io/central-element-323112/live-event-log-compactor:latest"
}

variable "compactor_sa_email" {
  description = "Service account email for the compaction job (needs GCS read/write + Cloud Run invoker)"
  type        = string
}
