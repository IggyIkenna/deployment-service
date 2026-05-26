# Outputs for AWS EventBridge Rule + Batch Target Module

output "id" {
  description = "The ID of the EventBridge Rule"
  value       = aws_cloudwatch_event_rule.schedule.id
}

output "name" {
  description = "The name of the EventBridge Rule"
  value       = aws_cloudwatch_event_rule.schedule.name
}

output "arn" {
  description = "The ARN of the EventBridge Rule"
  value       = aws_cloudwatch_event_rule.schedule.arn
}

output "schedule" {
  description = "The schedule expression"
  value       = var.schedule
}

output "state" {
  description = "The state of the rule (ENABLED or DISABLED)"
  value       = var.enabled ? "ENABLED" : "DISABLED"
}

output "scheduler_role_arn" {
  description = "The IAM role ARN used by the scheduler"
  value       = var.create_scheduler_role ? aws_iam_role.scheduler_role[0].arn : var.scheduler_role_arn
}

output "schedule_group_name" {
  description = "Not applicable for EventBridge Rules (compatibility shim)"
  value       = null
}
