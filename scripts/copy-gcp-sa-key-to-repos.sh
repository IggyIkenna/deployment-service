#!/usr/bin/env bash
# Copy GCP_SA_KEY secret to all repos that need it.
#
# Prerequisites:
#   - gh CLI installed and authenticated (gh auth login)
#   - GCP service account JSON key file
#
# Usage:
#   ./scripts/copy-gcp-sa-key-to-repos.sh /path/to/your-service-account-key.json
#
# The service account must have roles/artifactregistry.reader for Artifact Registry.

set -euo pipefail

KEY_FILE="${1:-}"
if [[ -z "$KEY_FILE" || ! -f "$KEY_FILE" ]]; then
  echo "Usage: $0 /path/to/service-account-key.json"
  echo ""
  echo "Create a GCP service account key:"
  echo "  1. GCP Console → IAM → Service Accounts"
  echo "  2. Create or select SA → Keys → Add Key → JSON"
  echo "  3. Grant roles/artifactregistry.reader (or equivalent)"
  echo ""
  exit 1
fi

REPOS=(
  "IggyIkenna/unified-trading-library"
  "IggyIkenna/unified-config-interface"
  "IggyIkenna/unified-events-interface"
  "IggyIkenna/unified-market-interface"
  "IggyIkenna/features-calendar-service"
  "IggyIkenna/execution-services"
  "IggyIkenna/position-balance-monitor-service"
  "IggyIkenna/risk-and-exposure-service"
  "IggyIkenna/strategy-service"
  "IggyIkenna/ml-training-service"
  "IggyIkenna/ml-inference-service"
  "IggyIkenna/features-delta-one-service"
  "IggyIkenna/features-volatility-service"
  "IggyIkenna/features-onchain-service"
  "IggyIkenna/deployment-service"
  "IggyIkenna/market-tick-data-handler"
  "IggyIkenna/matching-engine-library"
  "IggyIkenna/instruments-service"
  "IggyIkenna/settlement-ui"
  "IggyIkenna/market-data-processing-service"
)

echo "Setting GCP_SA_KEY in ${#REPOS[@]} repos..."
for repo in "${REPOS[@]}"; do
  if gh secret set GCP_SA_KEY --repo "$repo" < "$KEY_FILE" 2>/dev/null; then
    echo "  ✅ $repo"
  else
    echo "  ⚠️  $repo (may not exist or no access)"
  fi
done
echo "Done."
