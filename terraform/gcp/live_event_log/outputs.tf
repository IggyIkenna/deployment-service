# Key outputs for the live_event_log terraform module.
# Plan: live_persist_03_infra_pubsub_sinks_2026_06_26.md

output "warm_bucket_prefix" {
  description = "GCS path prefix for the warm event-log sink"
  value       = "live-events/warm/"
}

output "cold_bucket_prefix" {
  description = "GCS path prefix for the cold compacted event-log"
  value       = "live-events/cold/"
}

output "bq_dataset" {
  description = "BigQuery dataset ID for live event-log external tables"
  value       = google_bigquery_dataset.live_events.dataset_id
}

output "compactor_job_name" {
  description = "Cloud Run Job name for the daily cold compaction job"
  value       = google_cloud_run_v2_job.live_event_log_compactor.name
}

output "compactor_scheduler_name" {
  description = "Cloud Scheduler job name for the daily compaction trigger"
  value       = google_cloud_scheduler_job.live_event_log_compactor_daily.name
}
