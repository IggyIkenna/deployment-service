#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# canary-deploy.sh — Cloud Run canary deployment with traffic splitting and auto-rollback.
#
# Implements safe canary rollout:
#   1. Deploy new revision with 0% traffic (--no-traffic)
#   2. Route CANARY_PERCENT traffic to the new revision
#   3. Monitor /health endpoint for MONITOR_DURATION seconds
#   4. If healthy: promote to 100% traffic
#      If unhealthy: rollback to previous revision (100% traffic)
#
# For non-critical services: uses Artifact Registry image re-tag for fast rollback
# instead of a full Cloud Run revision rollback.
#
# Usage:
#   bash scripts/deployment/canary-deploy.sh <SERVICE> <REGION> <IMAGE_TAG> [OPTIONS]
#
# Arguments:
#   SERVICE          Cloud Run service name
#   REGION           GCP region (e.g. asia-northeast1)
#   IMAGE_TAG        Full image tag to deploy (e.g. asia-northeast1-docker.pkg.dev/proj/repo/svc:v1.2.3)
#
# Options:
#   --canary-percent N     Canary traffic percentage (default: 5)
#   --monitor-duration N   Monitoring window in seconds (default: 300 = 5 min)
#   --health-path PATH     Health check path (default: /health)
#   --health-interval N    Seconds between health checks (default: 30)
#   --non-critical         Use AR image re-tag for fast rollback instead of revision rollback
#   --project PROJECT_ID   GCP project ID (default: from gcloud config)
#   --dry-run              Print commands without executing
#
# Exit codes:
#   0 = canary promoted to 100%
#   1 = canary rolled back (health check failed)
#   2 = deployment error (could not deploy new revision)
#
# Integration:
#   cloud-build-router.yml will call this script after building the image.
#   This script does NOT modify cloud-build-router.yml — it is standalone.

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────

SERVICE="${1:?Usage: canary-deploy.sh <SERVICE> <REGION> <IMAGE_TAG> [OPTIONS]}"
REGION="${2:?Usage: canary-deploy.sh <SERVICE> <REGION> <IMAGE_TAG> [OPTIONS]}"
IMAGE_TAG="${3:?Usage: canary-deploy.sh <SERVICE> <REGION> <IMAGE_TAG> [OPTIONS]}"
shift 3

CANARY_PERCENT=5
MONITOR_DURATION=300
HEALTH_PATH="/health"
HEALTH_INTERVAL=30
NON_CRITICAL=false
PROJECT=""
DRY_RUN=false

# ── Parse options ─────────────────────────────────────────────────────────────

while [ $# -gt 0 ]; do
  case "$1" in
    --canary-percent)  CANARY_PERCENT="$2"; shift 2 ;;
    --monitor-duration) MONITOR_DURATION="$2"; shift 2 ;;
    --health-path)     HEALTH_PATH="$2"; shift 2 ;;
    --health-interval) HEALTH_INTERVAL="$2"; shift 2 ;;
    --non-critical)    NON_CRITICAL=true; shift ;;
    --project)         PROJECT="$2"; shift 2 ;;
    --dry-run)         DRY_RUN=true; shift ;;
    *) echo "ERROR: Unknown option: $1"; exit 2 ;;
  esac
done

STABLE_PERCENT=$((100 - CANARY_PERCENT))

if [ -z "$PROJECT" ]; then
  PROJECT=$(gcloud config get-value project 2>/dev/null || echo "")
  if [ -z "$PROJECT" ]; then
    echo "ERROR: No GCP project set. Use --project or configure gcloud."
    exit 2
  fi
fi

# ── Helper functions ──────────────────────────────────────────────────────────

run_cmd() {
  if [ "$DRY_RUN" = "true" ]; then
    echo "[DRY RUN] $*"
    return 0
  fi
  "$@"
}

log() {
  echo "[canary-deploy] $(date +%H:%M:%S) $*"
}

get_service_url() {
  gcloud run services describe "$SERVICE" \
    --region "$REGION" \
    --project "$PROJECT" \
    --format "value(status.url)" 2>/dev/null || echo ""
}

get_current_revision() {
  gcloud run services describe "$SERVICE" \
    --region "$REGION" \
    --project "$PROJECT" \
    --format "value(status.traffic[0].revisionName)" 2>/dev/null || echo ""
}

get_latest_revision() {
  gcloud run services describe "$SERVICE" \
    --region "$REGION" \
    --project "$PROJECT" \
    --format "value(status.latestCreatedRevisionName)" 2>/dev/null || echo ""
}

# ── Step 0: Record previous stable revision ──────────────────────────────────

log "=== Canary Deploy ==="
log "Service:    $SERVICE"
log "Region:     $REGION"
log "Image:      $IMAGE_TAG"
log "Canary:     ${CANARY_PERCENT}% new / ${STABLE_PERCENT}% old"
log "Monitor:    ${MONITOR_DURATION}s (check every ${HEALTH_INTERVAL}s)"
log "Non-crit:   $NON_CRITICAL"
log "Project:    $PROJECT"

PREV_REVISION=$(get_current_revision)
log "Previous stable revision: ${PREV_REVISION:-<none>}"

# ── Step 1: Deploy new revision with no traffic ──────────────────────────────

log "Deploying new revision (no traffic)..."
if ! run_cmd gcloud run deploy "$SERVICE" \
  --image "$IMAGE_TAG" \
  --region "$REGION" \
  --project "$PROJECT" \
  --no-traffic \
  --quiet 2>&1; then
  log "ERROR: Failed to deploy new revision"
  exit 2
fi

NEW_REVISION=$(get_latest_revision)
log "New revision deployed: ${NEW_REVISION}"

if [ -z "$PREV_REVISION" ]; then
  log "No previous revision — this is the first deployment. Sending 100% traffic."
  run_cmd gcloud run services update-traffic "$SERVICE" \
    --region "$REGION" \
    --project "$PROJECT" \
    --to-latest \
    --quiet
  log "First deployment complete. 100% traffic to $NEW_REVISION."
  exit 0
fi

# ── Step 2: Split traffic (canary) ───────────────────────────────────────────

log "Splitting traffic: ${CANARY_PERCENT}% → $NEW_REVISION, ${STABLE_PERCENT}% → $PREV_REVISION"
run_cmd gcloud run services update-traffic "$SERVICE" \
  --region "$REGION" \
  --project "$PROJECT" \
  --to-revisions "${NEW_REVISION}=${CANARY_PERCENT},${PREV_REVISION}=${STABLE_PERCENT}" \
  --quiet

# ── Step 3: Monitor health ───────────────────────────────────────────────────

SERVICE_URL=$(get_service_url)
if [ -z "$SERVICE_URL" ]; then
  log "WARNING: Could not determine service URL — skipping health monitoring"
  log "Promoting to 100% without health verification"
else
  HEALTH_URL="${SERVICE_URL}${HEALTH_PATH}"
  log "Monitoring health at: $HEALTH_URL"

  ELAPSED=0
  FAILURES=0
  MAX_CONSECUTIVE_FAILURES=3
  HEALTHY=true

  while [ "$ELAPSED" -lt "$MONITOR_DURATION" ]; do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "$HEALTH_URL" 2>/dev/null || echo "000")

    if [ "$HTTP_CODE" = "200" ]; then
      FAILURES=0
      log "  Health check OK (HTTP $HTTP_CODE) — ${ELAPSED}s / ${MONITOR_DURATION}s"
    else
      FAILURES=$((FAILURES + 1))
      log "  Health check FAIL (HTTP $HTTP_CODE) — failure $FAILURES/$MAX_CONSECUTIVE_FAILURES"

      if [ "$FAILURES" -ge "$MAX_CONSECUTIVE_FAILURES" ]; then
        log "UNHEALTHY: $MAX_CONSECUTIVE_FAILURES consecutive health check failures"
        HEALTHY=false
        break
      fi
    fi

    sleep "$HEALTH_INTERVAL"
    ELAPSED=$((ELAPSED + HEALTH_INTERVAL))
  done

  # ── Step 4: Rollback or promote ──────────────────────────────────────────

  if [ "$HEALTHY" = "false" ]; then
    log ""
    log "=== ROLLBACK ==="

    if [ "$NON_CRITICAL" = "true" ]; then
      # Fast rollback for non-critical services: re-tag the old image in AR
      # so cloud-build-router sees the old image as "latest"
      log "Non-critical service — using AR image re-tag for fast rollback"

      # Extract the image base (without tag) from the deployed image
      IMAGE_BASE="${IMAGE_TAG%:*}"
      PREV_IMAGE=$(gcloud run revisions describe "$PREV_REVISION" \
        --region "$REGION" \
        --project "$PROJECT" \
        --format "value(spec.containers[0].image)" 2>/dev/null || echo "")

      if [ -n "$PREV_IMAGE" ]; then
        PREV_TAG="${PREV_IMAGE##*:}"
        log "Re-tagging $IMAGE_BASE:$PREV_TAG as $IMAGE_BASE:latest"
        run_cmd gcloud artifacts docker tags add \
          "$IMAGE_BASE:$PREV_TAG" \
          "$IMAGE_BASE:latest" \
          --quiet 2>/dev/null || log "WARNING: AR re-tag failed — falling back to revision rollback"
      fi
    fi

    # Always roll back traffic regardless of re-tag success
    log "Rolling back traffic: 100% → $PREV_REVISION"
    run_cmd gcloud run services update-traffic "$SERVICE" \
      --region "$REGION" \
      --project "$PROJECT" \
      --to-revisions "${PREV_REVISION}=100" \
      --quiet

    # Delete the failed canary revision to avoid confusion
    log "Cleaning up failed revision: $NEW_REVISION"
    run_cmd gcloud run revisions delete "$NEW_REVISION" \
      --region "$REGION" \
      --project "$PROJECT" \
      --quiet 2>/dev/null || log "WARNING: Could not delete failed revision (may still be serving)"

    log ""
    log "ROLLBACK COMPLETE: $SERVICE reverted to $PREV_REVISION"
    exit 1
  fi
fi

# ── Step 5: Promote canary to 100% ──────────────────────────────────────────

log ""
log "=== PROMOTE ==="
log "Canary healthy for ${MONITOR_DURATION}s — promoting to 100%"

run_cmd gcloud run services update-traffic "$SERVICE" \
  --region "$REGION" \
  --project "$PROJECT" \
  --to-latest \
  --quiet

log "PROMOTION COMPLETE: $SERVICE now serving 100% traffic on $NEW_REVISION"
exit 0
