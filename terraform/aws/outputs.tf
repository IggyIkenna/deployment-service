# Outputs for UCI Cloud Abstraction — AWS bootstrap

output "bucket_names" {
  description = "Map of bucket logical name to S3 bucket name"
  value = {
    for k, b in aws_s3_bucket.unified_trading :
    k => b.id
  }
}

output "bucket_arns" {
  description = "Map of bucket logical name to S3 bucket ARN"
  value = {
    for k, b in aws_s3_bucket.unified_trading :
    k => b.arn
  }
}

output "athena_workgroup_arn" {
  description = "Athena workgroup ARN"
  value       = aws_athena_workgroup.unified_trading.arn
}

output "glue_database_arns" {
  description = "Map of Glue catalog database name to ARN"
  value = {
    for k, db in aws_glue_catalog_database.unified_trading :
    k => "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:database/${db.name}"
  }
}

output "ecs_cluster_arn" {
  description = "ECS cluster ARN"
  value       = aws_ecs_cluster.unified_trading.arn
}

output "iam_role_arn" {
  description = "IAM role ARN for unified-trading services"
  value       = aws_iam_role.unified_trading.arn
}

output "secret_arns" {
  description = "Map of secret name to Secrets Manager ARN"
  value = {
    for k, s in aws_secretsmanager_secret.unified_trading :
    k => s.arn
  }
}
