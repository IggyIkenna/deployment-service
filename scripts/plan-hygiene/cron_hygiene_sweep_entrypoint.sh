#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Entrypoint for Cloud Run Job — hygiene sweep cron.
# Cloned from PM @ live-defi-rollout; called by bootstrap.
set -euo pipefail
cd /tmp/pm
bash scripts/plan-hygiene/run_hygiene_sweep.sh --ci
