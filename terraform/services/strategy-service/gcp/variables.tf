# Variables for strategy-service Terraform configuration

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "asia-northeast1"
}

variable "gcs_location" {
  description = "GCS location"
  type        = string
  default     = "asia-northeast1"
}

variable "job_name" {
  description = "Name of the Cloud Run Job"
  type        = string
  default     = "strategy-service-job"
}

variable "docker_image" {
  description = "Docker image URL"
  type        = string
}

variable "service_account_email" {
  description = "Service account email for the job"
  type        = string
}

variable "cpu" {
  description = "CPU allocation"
  type        = string
  default     = "2"
}

variable "memory" {
  description = "Memory allocation"
  type        = string
  default     = "8Gi"
}

variable "timeout_seconds" {
  description = "Job timeout in seconds"
  type        = number
  default     = 86400
}

variable "max_retries" {
  description = "Maximum retry attempts"
  type        = number
  default     = 3
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "prod"
}

# Domain-specific buckets
variable "strategy_bucket_cefi" {
  description = "GCS bucket for CEFI strategies"
  type        = string
  default     = "strategy-store-cefi-{project_id}"
}

variable "strategy_bucket_tradfi" {
  description = "GCS bucket for TRADFI strategies"
  type        = string
  default     = "strategy-store-tradfi-{project_id}"
}

variable "strategy_bucket_defi" {
  description = "GCS bucket for DEFI strategies"
  type        = string
  default     = "strategy-store-defi-{project_id}"
}

variable "workflow_name" {
  description = "Name of the daily Cloud Workflow"
  type        = string
  default     = "strategy-service-daily"
}

variable "backfill_workflow_name" {
  description = "Name of the backtest Cloud Workflow"
  type        = string
  default     = "strategy-service-backtest"
}

variable "schedule" {
  description = "Cron schedule (default: 15:00 PM UTC - after inference)"
  type        = string
  default     = "0 15 * * *"
}

variable "time_zone" {
  description = "Time zone for the schedule"
  type        = string
  default     = "UTC"
}

variable "scheduler_service_account_email" {
  description = "Service account for Cloud Scheduler"
  type        = string
}

# ── Signal-broadcast (Plan B Phase 4) ──
# Per-counterparty HMAC secrets are provisioned by
# `scripts/provision-signal-broadcast-secrets.sh`. Each map entry below
# maps the container-side env-var name to the Secret Manager resource
# name. Cloud Run mounts each secret at container start and the
# strategy-service signal_broadcast sub-package reads them via
# `CounterpartyCredentialsManager` (`ApiKeyReloader`).

variable "signal_broadcast_counterparty_secrets" {
  description = "Per-counterparty HMAC secrets (env-var name → Secret Manager secret name)"
  type = map(object({
    secret_name = string
    version     = string
  }))
  default = {
    # Staging fixtures; prod entries added in the same PR as the UAC
    # `Counterparty` record + the provisioning-script counterparty id.
    SIGNAL_BROADCAST_CP1_HMAC_SECRET = {
      secret_name = "signal-broadcast-counterparty-signal-lease-cp1-staging-hmac"
      version     = "latest"
    }
    SIGNAL_BROADCAST_CP2_HMAC_SECRET = {
      secret_name = "signal-broadcast-counterparty-signal-lease-cp2-staging-hmac"
      version     = "latest"
    }
  }
}

variable "signal_broadcast_webhook_timeout_seconds" {
  description = "Per-request HTTP timeout for counterparty webhook POST (SIGNAL_BROADCAST_WEBHOOK_TIMEOUT_SECONDS)"
  type        = number
  default     = 10
}

variable "signal_broadcast_webhook_max_retries" {
  description = "Retry budget for 5xx / connection errors (SIGNAL_BROADCAST_WEBHOOK_MAX_RETRIES)"
  type        = number
  default     = 3
}

variable "signal_broadcast_webhook_backoff_base_seconds" {
  description = "Exponential backoff base seconds (SIGNAL_BROADCAST_WEBHOOK_BACKOFF_BASE_SECONDS)"
  type        = number
  default     = 0.5
}

variable "signal_broadcast_jwt_issuer" {
  description = "JWT iss claim on the Authorization bearer token (SIGNAL_BROADCAST_JWT_ISSUER)"
  type        = string
  default     = "odum-research"
}

variable "signal_broadcast_jwt_audience" {
  description = "JWT aud claim on the Authorization bearer token (SIGNAL_BROADCAST_JWT_AUDIENCE)"
  type        = string
  default     = "signal-leasing-counterparty"
}

variable "signal_broadcast_jwt_ttl_seconds" {
  description = "JWT TTL on the bearer token (SIGNAL_BROADCAST_JWT_TTL_SECONDS)"
  type        = number
  default     = 300
}

variable "signal_broadcast_credential_refresh_interval_seconds" {
  description = "ApiKeyReloader refresh cadence for HMAC secrets (SIGNAL_BROADCAST_CREDENTIAL_REFRESH_INTERVAL_SECONDS)"
  type        = number
  default     = 300
}

variable "signal_broadcast_pull_buffer_size" {
  description = "Max emissions retained per-counterparty for REST-pull reconciliation (SIGNAL_BROADCAST_PULL_BUFFER_SIZE)"
  type        = number
  default     = 256
}
