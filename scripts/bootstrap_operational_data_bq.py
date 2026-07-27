#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""
Bootstrap the durable-operational-data BigQuery dataset + tables.

deployment_durable_operational_data_bigquery_2026_07_21.md — persists four
operational signals (VM resource stats, run history, idle/orphan spend,
process-category breakdown) that are otherwise ephemeral, via the existing UTL
event spine -> dedicated Pub/Sub topics -> native BigQuery subscriptions.

This script creates ONLY the dataset + tables (the durable storage side).
Run scripts/setup-pubsub.sh AFTER this — its BQ_SUBSCRIPTION_REGISTRY entries
reference these exact dataset.table names and will fail if the tables don't
exist yet.

Uses the UTL cloud interface exclusively (get_analytics_client) — never raw
google.cloud.bigquery, per this workspace's QG-enforced coding standard.

Usage:
    python scripts/bootstrap_operational_data_bq.py --project-id central-element-323112
    python scripts/bootstrap_operational_data_bq.py --project-id central-element-323112 --dry-run
"""

from __future__ import annotations

import argparse
import logging

from unified_trading_library import get_analytics_client

logger = logging.getLogger("bootstrap_operational_data_bq")

DATASET = "deployment_operational_data"
LOCATION = "asia-northeast1"

# Table name -> (schema, time_partitioning_field, clustering_fields)
# Schema entries are (column_name, bq_type, mode) tuples per the UTL
# GCPAnalyticsClient.create_table wrapper's signature.
TABLES: dict[str, tuple[list[tuple[str, str, str]], str, list[str]]] = {
    "resource_samples": (
        [
            ("deployment_id", "STRING", "NULLABLE"),
            ("vm_name", "STRING", "NULLABLE"),
            ("service", "STRING", "NULLABLE"),
            ("asset_group", "STRING", "NULLABLE"),
            ("mode", "STRING", "NULLABLE"),
            ("ts", "TIMESTAMP", "REQUIRED"),
            ("cpu_pct", "FLOAT", "NULLABLE"),
            ("mem_pct", "FLOAT", "NULLABLE"),
            ("mem_slope", "FLOAT", "NULLABLE"),
            ("disk_pct", "FLOAT", "NULLABLE"),
            ("io_write_rate_bytes_sec", "FLOAT", "NULLABLE"),
            ("net_recv_rate_bytes_sec", "FLOAT", "NULLABLE"),
            ("workload_alive", "BOOLEAN", "NULLABLE"),
        ],
        "ts",
        ["vm_name", "service"],
    ),
    "run_ledger": (
        [
            ("deployment_id", "STRING", "REQUIRED"),
            ("vm_name", "STRING", "NULLABLE"),
            ("asset_group", "STRING", "NULLABLE"),
            ("task", "STRING", "NULLABLE"),
            ("mode", "STRING", "NULLABLE"),
            ("status", "STRING", "NULLABLE"),
            ("exit_code", "INTEGER", "NULLABLE"),
            ("started_at", "TIMESTAMP", "NULLABLE"),
            ("completed_at", "TIMESTAMP", "REQUIRED"),
            ("rows_in", "INTEGER", "NULLABLE"),
            ("rows_out", "INTEGER", "NULLABLE"),
            ("rows_error", "INTEGER", "NULLABLE"),
            ("events_emitted", "INTEGER", "NULLABLE"),
            ("log_uri", "STRING", "NULLABLE"),
            ("image_digest", "STRING", "NULLABLE"),
            ("git_commit", "STRING", "NULLABLE"),
            ("cpu_pct", "FLOAT", "NULLABLE"),
            ("mem_pct", "FLOAT", "NULLABLE"),
            ("disk_pct", "FLOAT", "NULLABLE"),
            ("workload_alive", "BOOLEAN", "NULLABLE"),
        ],
        "completed_at",
        ["vm_name", "asset_group"],
    ),
    # Central-scheduled snapshot: one ROLLUP row per run (resource_name IS NULL)
    # plus one row per idle/reapable resource (resource_name set) — same table,
    # distinguished by resource_name being NULL or not, rather than two tables,
    # since both share the same ts/lifecycle grain and are always queried together.
    "idle_spend": (
        [
            ("ts", "TIMESTAMP", "REQUIRED"),
            ("resource_name", "STRING", "NULLABLE"),
            ("lifecycle_class", "STRING", "NULLABLE"),
            ("age_hours", "FLOAT", "NULLABLE"),
            ("stopped_total", "INTEGER", "NULLABLE"),
            ("reapable_total", "INTEGER", "NULLABLE"),
            ("monthly_idle_usd", "FLOAT", "NULLABLE"),
            ("monthly_reapable_usd", "FLOAT", "NULLABLE"),
        ],
        "ts",
        ["resource_name"],
    ),
    "reap_events": (
        [
            ("ts", "TIMESTAMP", "REQUIRED"),
            ("vm_name", "STRING", "REQUIRED"),
            ("age_hours", "FLOAT", "NULLABLE"),
            ("reclaimed_usd_per_month", "FLOAT", "NULLABLE"),
            ("actor", "STRING", "NULLABLE"),
            ("dry_run", "BOOLEAN", "NULLABLE"),
        ],
        "ts",
        ["vm_name"],
    ),
    # 4th signal (process-category breakdown) — multi-tenant hosts only, see
    # the plan's "4th signal detail" section. Schema mirrors the bridge
    # resource-monitor.sh cron's field list (the reference schema for this table).
    "process_samples": (
        [
            ("vm_name", "STRING", "REQUIRED"),
            ("ts", "TIMESTAMP", "REQUIRED"),
            ("category", "STRING", "NULLABLE"),  # worker_agent|orchestrator|ci|ao_plan_work|other
            ("pid", "INTEGER", "NULLABLE"),
            ("comm", "STRING", "NULLABLE"),
            ("cpu_pct", "FLOAT", "NULLABLE"),
            ("mem_pct", "FLOAT", "NULLABLE"),
            ("mem_rss_kb", "INTEGER", "NULLABLE"),
            ("elapsed_sec", "INTEGER", "NULLABLE"),
        ],
        "ts",
        ["vm_name", "category"],
    ),
}


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if args.dry_run:
        logger.info("[DRY RUN] would create dataset=%s (location=%s)", DATASET, LOCATION)
        for table, (schema, partition_field, clustering) in TABLES.items():
            logger.info(
                "[DRY RUN] would create table=%s.%s (partition=%s, cluster=%s, %d columns)",
                DATASET,
                table,
                partition_field,
                clustering,
                len(schema),
            )
        return 0

    client = get_analytics_client(provider="gcp", project_id=args.project_id)
    logger.info("Creating dataset %s (location=%s, idempotent)...", DATASET, LOCATION)
    client.create_dataset(DATASET, location=LOCATION)

    for table, (schema, partition_field, clustering) in TABLES.items():
        logger.info("Creating table %s.%s (idempotent)...", DATASET, table)
        client.create_table(
            DATASET,
            table,
            schema,
            time_partitioning_field=partition_field,
            clustering_fields=clustering,
            exists_ok=True,
        )
        logger.info(
            "  OK: %s.%s (partitioned by DATE(%s), clustered by %s)", DATASET, table, partition_field, clustering
        )

    logger.info("Done. Next: run scripts/setup-pubsub.sh to create the dedicated topics + native BQ subscriptions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
