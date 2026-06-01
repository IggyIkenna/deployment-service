#!/usr/bin/env bash
# Configure GCP Artifact Registry cleanup policies for all unified-trading repos.
#
# Policy (per deployment_and_qg_strategy_implementation_2026_05_13.md Phase 5 P0):
#
#   keep-release-tags      KEEP_FOREVER    matches tag-regex ^v[0-9]+\.[0-9]+\.[0-9]+$
#   delete-commit-sha      DELETE after 14d   matches tag-regex ^[0-9a-f]{7,40}$
#   delete-branch-feature  DELETE after 3d    matches tag-regex ^feat[-_/]
#   delete-pr-images       DELETE after 7d    matches tag-regex ^pr-[0-9]+
#
# Usage:
#   bash scripts/audit/artifact-registry-retention.sh                    # dry-run (default)
#   bash scripts/audit/artifact-registry-retention.sh --apply            # write policies
#   bash scripts/audit/artifact-registry-retention.sh --location <loc>   # default asia-northeast1
#   bash scripts/audit/artifact-registry-retention.sh --repo <name>      # single repo
#
# Cron wiring: registered weekly via cloud-scheduler (Mondays 02:00 UTC) — see
# deployment-service/scripts/cloud-scheduler/artifact-registry-retention-cron.tf
# (REMOTE-ONLY: see codex/05-infrastructure/act-preflight-coverage.md).
#
# Owner: deployment-service slot. Cadence: weekly cron + manual on policy change.
# Verifier: gcloud artifacts repositories describe <repo> --location=<loc> --format='value(cleanupPolicies)'
# Last executed: 2026-05-17 (dry-run from slot-8)

set -euo pipefail

LOCATION="asia-northeast1"
PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
APPLY=false
SINGLE_REPO=""

usage() {
    cat <<EOF
Usage: $0 [--apply] [--location <loc>] [--repo <name>]

  --apply             Write policies (default: dry-run, print gcloud commands only)
  --location <loc>    Artifact Registry location (default: asia-northeast1)
  --repo <name>       Configure single repo (default: all)

Exit codes:
  0 — success (or dry-run printed)
  1 — at least one repo policy-set failed
  2 — pre-flight error (gcloud missing, project unset, etc.)
EOF
    exit 2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply) APPLY=true; shift ;;
        --location) LOCATION="$2"; shift 2 ;;
        --repo) SINGLE_REPO="$2"; shift 2 ;;
        --help|-h) usage ;;
        *) echo "Unknown arg: $1" >&2; usage ;;
    esac
done

if ! command -v gcloud >/dev/null 2>&1; then
    echo "ERROR: gcloud not installed" >&2
    exit 2
fi

if [[ -z "$PROJECT_ID" ]]; then
    echo "ERROR: GCP_PROJECT_ID env var not set" >&2
    exit 2
fi

echo "=== Artifact Registry retention policy ==="
echo "Project:  $PROJECT_ID"
echo "Location: $LOCATION"
echo "Mode:     $([[ "$APPLY" == "true" ]] && echo APPLY || echo DRY-RUN)"
echo ""

# Resolve target repos
if [[ -n "$SINGLE_REPO" ]]; then
    REPOS=("$SINGLE_REPO")
else
    mapfile -t REPOS < <(gcloud artifacts repositories list \
        --location="$LOCATION" \
        --project="$PROJECT_ID" \
        --format="value(name)" 2>/dev/null | xargs -n1 basename)
fi

if [[ ${#REPOS[@]} -eq 0 ]]; then
    echo "WARN: no Artifact Registry repos found at $LOCATION"
    exit 0
fi

echo "Targets: ${#REPOS[@]} repo(s)"
for r in "${REPOS[@]}"; do echo "  - $r"; done
echo ""

# Write cleanup policy JSON to tmp file (gcloud --set-cleanup-policies takes a file)
POLICY_JSON=$(mktemp -t artreg-policy-XXXXXX.json)
trap 'rm -f "$POLICY_JSON"' EXIT

cat > "$POLICY_JSON" <<'EOF'
[
  {
    "name": "keep-release-tags",
    "action": {"type": "KEEP"},
    "condition": {
      "tagState": "TAGGED",
      "tagPrefixes": ["v"],
      "versionNamePrefixes": []
    },
    "mostRecentVersions": null
  },
  {
    "name": "delete-commit-sha-after-14d",
    "action": {"type": "DELETE"},
    "condition": {
      "tagState": "TAGGED",
      "olderThan": "1209600s"
    }
  },
  {
    "name": "delete-feature-branch-after-3d",
    "action": {"type": "DELETE"},
    "condition": {
      "tagState": "TAGGED",
      "tagPrefixes": ["feat-", "feat_", "feat/"],
      "olderThan": "259200s"
    }
  },
  {
    "name": "delete-pr-images-after-7d",
    "action": {"type": "DELETE"},
    "condition": {
      "tagState": "TAGGED",
      "tagPrefixes": ["pr-"],
      "olderThan": "604800s"
    }
  },
  {
    "name": "delete-untagged-after-1d",
    "action": {"type": "DELETE"},
    "condition": {
      "tagState": "UNTAGGED",
      "olderThan": "86400s"
    }
  }
]
EOF

echo "Policy JSON written to: $POLICY_JSON"
echo ""

FAILED=()
for repo in "${REPOS[@]}"; do
    cmd=(gcloud artifacts repositories set-cleanup-policies "$repo"
        --location="$LOCATION"
        --project="$PROJECT_ID"
        --policy="$POLICY_JSON")

    if [[ "$APPLY" == "true" ]]; then
        echo "--- $repo: applying ---"
        if ! "${cmd[@]}"; then
            echo "  ❌ FAILED"
            FAILED+=("$repo")
        else
            echo "  ✅ OK"
        fi
    else
        echo "--- $repo: (dry-run) would run ---"
        printf '  %q ' "${cmd[@]}"; echo
    fi
done

if [[ ${#FAILED[@]} -gt 0 ]]; then
    echo ""
    echo "=== ${#FAILED[@]} failure(s): ${FAILED[*]} ==="
    exit 1
fi

echo ""
echo "=== Done ==="
