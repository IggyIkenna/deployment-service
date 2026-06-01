#!/usr/bin/env bash
# Convenience wrapper: sync prod → dev (development) buckets with a 1-year truncated window.
# Thin delegate to sync-buckets-prod-to-env.sh (the implementation + full docs).
#
# Usage:
#   bash deployment-service/scripts/sync-buckets-prod-to-dev.sh                    # GCP, all env-tiered kinds, last 1 year
#   bash deployment-service/scripts/sync-buckets-prod-to-dev.sh --years 2          # custom window
#   bash deployment-service/scripts/sync-buckets-prod-to-dev.sh --kind features-delta-one --dry-run
#   bash deployment-service/scripts/sync-buckets-prod-to-dev.sh --cloud aws
#
# Plan: bucket_name_ssot_canonicalisation_2026_05_10.md Phase 0h /
#       code_freeze_migrate_backfill_sequencing_2026_05_10.md GAP-2.4.E.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/sync-buckets-prod-to-env.sh" --target-env dev "$@"
