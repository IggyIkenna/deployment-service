# Epic: batch_live_symmetry_master | Lifecycle: permanent | Delete-when: central-event-log-spine decommissioned
"""Cloud Run job entry-points for the central event-log spine.

Modules:
  ``live_event_log_compactor`` — daily cold compaction of warm GCS parquet files.
"""
