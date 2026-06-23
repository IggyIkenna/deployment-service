#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Convenience wrapper: sync prod → staging buckets with a 2-year truncated window.
# Thin delegate to sync-buckets-prod-to-env.sh (the implementation + full docs).
#
# Usage:
#   bash deployment-service/scripts/sync-buckets-prod-to-staging.sh                    # GCP, all env-tiered kinds, last 2 years
#   bash deployment-service/scripts/sync-buckets-prod-to-staging.sh --years 3          # custom window
#   bash deployment-service/scripts/sync-buckets-prod-to-staging.sh --kind market-data --dry-run
#   bash deployment-service/scripts/sync-buckets-prod-to-staging.sh --cloud aws
#
# Plan: bucket_name_ssot_canonicalisation_2026_05_10.md Phase 0h /
#       code_freeze_migrate_backfill_sequencing_2026_05_10.md GAP-2.4.E.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/sync-buckets-prod-to-env.sh" --target-env staging "$@"
