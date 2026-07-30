# Variables for AWS CodeBuild configuration

variable "region" {
  description = "AWS region"
  type        = string
  default     = "ap-northeast-1"
}

variable "environment" {
  description = "Environment name (feeds the provider default_tags Environment tag)"
  type        = string
  default     = "prod"
}

variable "github_owner" {
  description = "GitHub repository owner"
  type        = string
  default     = "IggyIkenna"
}

# NOTE: `branch_pattern` and `compute_type` were removed 2026-07-30.
#   * branch_pattern only fed the aws_codebuild_webhook HEAD_REF filter; the webhooks were deleted
#     live on 2026-07-03 and removed from this module (dispatch is the GitHub Actions router).
#   * compute_type was a single fleet-wide knob, but compute size is genuinely per-service
#     (the two heavy base builds run LARGE), so it now lives in locals.services in main.tf.
