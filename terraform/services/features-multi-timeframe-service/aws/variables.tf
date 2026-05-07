# Variables for features-multi-timeframe-service on AWS

variable "region" {
  description = "AWS region"
  type        = string
  default     = "ap-northeast-1"
}

# Job Configuration
variable "job_name" {
  description = "Name of the Batch Job"
  type        = string
  default     = "features-multi-timeframe-service"
}

variable "docker_image" {
  description = "Docker image URL in ECR"
  type        = string
}

variable "vcpus" {
  description = "vCPUs for the job"
  type        = string
  default     = "4"
}

variable "memory_mb" {
  description = "Memory in MB"
  type        = string
  default     = "8192"
}

variable "timeout_seconds" {
  description = "Job timeout in seconds"
  type        = number
  default     = 7200
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

# IAM Roles
variable "execution_role_arn" {
  description = "ECS task execution role ARN"
  type        = string
}

variable "job_role_arn" {
  description = "Job role ARN"
  type        = string
}

# Networking
variable "subnet_ids" {
  description = "Subnet IDs for Fargate tasks"
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security group IDs"
  type        = list(string)
}

variable "job_queue_arn" {
  description = "Batch job queue ARN"
  type        = string
}

# S3 Buckets - Input (delta-one features — upstream dependency)
variable "s3_bucket_delta_one_cefi" {
  description = "S3 bucket for CEFI delta-one features (input)"
  type        = string
}

variable "s3_bucket_delta_one_tradfi" {
  description = "S3 bucket for TRADFI delta-one features (input)"
  type        = string
}

variable "s3_bucket_delta_one_defi" {
  description = "S3 bucket for DEFI delta-one features (input)"
  type        = string
}

# S3 Buckets - Output (multi-timeframe features)
variable "features_bucket_cefi" {
  description = "S3 bucket for CEFI multi-timeframe features output"
  type        = string
}

variable "features_bucket_tradfi" {
  description = "S3 bucket for TRADFI multi-timeframe features output"
  type        = string
}

variable "features_bucket_defi" {
  description = "S3 bucket for DEFI multi-timeframe features output"
  type        = string
}

# Workflow Configuration
variable "workflow_name" {
  description = "Name of the Step Functions workflow"
  type        = string
  default     = "features-multi-timeframe-service-daily"
}

# Scheduler Configuration
variable "schedule" {
  description = "Cron schedule (default: 12:00 PM UTC - after features-delta-one at 10:30 AM)"
  type        = string
  default     = "0 12 * * *"
}

variable "time_zone" {
  description = "Time zone for the schedule"
  type        = string
  default     = "UTC"
}
