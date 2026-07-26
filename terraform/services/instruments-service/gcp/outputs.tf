# Outputs for instruments-service Terraform configuration

output "job_id" {
  description = "The ID of the Cloud Run Job"
  value       = module.daily_job.id
}

output "job_name" {
  description = "The name of the Cloud Run Job"
  value       = module.daily_job.name
}

output "job_uid" {
  description = "The UID of the Cloud Run Job"
  value       = module.daily_job.uid
}

# NOTE (2026-07-26): daily_workflow and backfill_workflow module blocks were disabled in
# main.tf on 2026-06-26 (dead CLI flags, superseded by the t1-recon Cloud Run jobs) but their
# outputs here were left dangling, which made `terraform plan/apply` fail outright in this
# directory ever since — so the intended destroy of the dead google_workflows_workflow +
# google_cloud_scheduler_job resources never actually applied. Removing the dangling outputs
# so apply can run and retire those live resources.
