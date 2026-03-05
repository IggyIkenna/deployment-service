# Variables for Deployment Dashboard Terraform

variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "central-element-323112"
}

variable "project_number" {
  description = "GCP project number (for default compute service account)"
  type        = string
  # Find with: gcloud projects describe central-element-323112 --format='value(projectNumber)'
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "asia-northeast1"
}

variable "repository_connection" {
  description = "Cloud Build repository connection (format: projects/PROJECT/locations/REGION/connections/CONNECTION/repositories/REPO)"
  type        = string
  # Example: projects/central-element-323112/locations/asia-northeast1/connections/iggyikenna-github/repositories/deployment-service
}

variable "cloud_build_sa" {
  description = "Cloud Build service account email"
  type        = string
  default     = "cloud-build@central-element-323112.iam.gserviceaccount.com"
}
