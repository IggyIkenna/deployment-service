# Variables for GCP Shared Infrastructure Module

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for Artifact Registry"
  type        = string
  default     = "asia-northeast1"
}

variable "gcs_location" {
  description = "GCS bucket location"
  type        = string
  default     = "asia-northeast1"
}

variable "services" {
  description = "List of services to create Artifact Registry repositories for"
  type        = list(string)
  default = [
    "market-data-processing-service",
    "features-delta-one-service",
    "features-volatility-service",
    "features-onchain-service",
    "features-calendar-service",
    "ml-training-service",
    "ml-inference-service",
    "strategy-service",
    "execution-services",
  ]
}

variable "create_artifact_registry" {
  description = "Whether to create Artifact Registry repositories"
  type        = bool
  default     = true
}

variable "create_gcs_buckets" {
  description = "Whether to create GCS buckets"
  type        = bool
  default     = true
}

variable "create_service_accounts" {
  description = "Whether to create service accounts"
  type        = bool
  default     = true
}

variable "labels" {
  description = "Labels to apply to all resources"
  type        = map(string)
  default     = {}
}
