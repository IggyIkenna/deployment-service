#!/usr/bin/env bash
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch a 48h GCE VM that runs alerting-service in "quietness baseline" mode.
#
# Purpose: Phase 7 of `alerting_service_live_rules_2026_05_07` — run alerting-
# service against staging-noise channel only (no PagerDuty paging) for 48h
# continuous so the operator can measure per-AlertCode false-positive rate +
# tune `ALERT_THRESHOLDS` before the live cutover on 2026-05-23.
#
# Acceptance criterion (per plan Phase 7):
#   * 48h continuous run with 0 PagerDuty-severity false positives
#   * <=2 Telegram-severity false positives across the 48h window
#   * Per-AlertCode FP rate < 5% per 24h
#
# What this VM runs (assembled by setup-data-pipeline-vm.sh from metadata):
#   python -m alerting_service \
#     --operation run --mode live \
#     --quietness-baseline \
#     --duration-hours 48 \
#     --pagerduty-disabled \
#     --telegram-channel-override "uts-staging-noise"
#
# Outputs:
#   * Per-AlertCode fired-count summary in events stream
#     (gs://${PROJECT}-events/events/alerting-service/{YYYY-MM-DD}/{vm-name}/)
#   * Routing decisions persisted to AlertStorageStore (existing path)
#   * Daily checkpoint event QUIETNESS_BASELINE_CHECKPOINT every 24h with
#     {fired_count_per_code, total_alerts, fp_estimate_per_code}
#
# Singleton lock: refuses to launch a second baseline VM in the same zone —
# two concurrent baselines would double-count alerts in the FP-rate analysis.
# Pass --force for legitimate re-runs after stop.
#
# Usage:
#   bash launch-alerting-quietness-baseline.sh                                # default 48h, prod
#   bash launch-alerting-quietness-baseline.sh --hours 72                     # custom duration
#   bash launch-alerting-quietness-baseline.sh --env staging                  # staging env tier
#   bash launch-alerting-quietness-baseline.sh --force                        # bypass singleton
#
# DO NOT FIRE without operator green-light: Phases 4-6 must be green first
# (paging targets wired, DART panel up, runbooks landed). The Phase 7 todo in
# `unified-trading-pm/plans/active/alerting_service_live_rules_2026_05_07.md`
# tracks the prerequisite gate.
#
# Reference incident the singleton lock prevents: 2026-04-19 SFI thundering herd
# (10 VMs / 6h / 4 useful writes) — same shape applies to baseline VMs that
# would race on the alert-event stream.
set -euo pipefail

FORCE=false
DURATION_HOURS=48
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      FORCE=true
      shift
      ;;
    --hours)
      DURATION_HOURS="$2"
      shift 2
      ;;
    --env)
      DEPLOYMENT_ENV="$2"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1" >&2
      echo "Usage: $0 [--force] [--hours <N>] [--env <prod|staging|dev>]" >&2
      exit 2
      ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
PROJECT_NUMBER="${PROJECT_NUMBER:-1060025368044}"
CODE_BUCKET="deployment-scripts-${PROJECT}"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="alerting-quietness-${RUN_TS}"
# SA: default to the Compute Engine default SA (matches running watchdog +
# every other actively-deployed VM in this project — see `gcloud compute
# instances list --format='value(serviceAccounts.email)'`). The historical
# data-pipeline SA referenced in some launchers does not exist in this
# project (verified 2026-05-12 — every `gcloud iam service-accounts list`
# enumeration; absence is the cause of the `serviceAccount of type was not
# found` failure mode). Override via
# `SERVICE_ACCOUNT=foo@... bash launch-alerting-quietness-baseline.sh` if needed.
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-${PROJECT_NUMBER}-compute@developer.gserviceaccount.com}"

# ── Singleton lock ──────────────────────────────────────────────────────────
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^alerting-quietness-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: alerting-service quietness-baseline VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — two concurrent baselines double-count alerts
in the FP-rate analysis, invalidating Phase 7's acceptance criteria.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gcloud storage cat gs://${PROJECT}-events/events/alerting-service/$(date -u +%Y-%m-%d)/${EXISTING}/hour=*/events.jsonl | tail -50
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force --hours ${DURATION_HOURS}
EOF
    exit 1
  fi
fi

# ── Verify Telegram secret exists in SM (Phase 4 prerequisite) ──────────────
if ! gcloud secrets describe alerting-telegram-bot-token --project="$PROJECT" >/dev/null 2>&1; then
  cat >&2 <<EOF
ERROR: GCP Secret Manager missing 'alerting-telegram-bot-token' (Phase 4
prerequisite). Quietness baseline cannot fire without a paging credential —
even staging-noise alerts route via Telegram.

Run the Phase 4 SM-push step first; see plan
'unified-trading-pm/plans/active/alerting_service_live_rules_2026_05_07.md'
Phase 4 (operator action #1).
EOF
  exit 1
fi

echo "Launching alerting-service quietness-baseline VM:"
echo "  name:      $VM_NAME"
echo "  zone:      $ZONE"
echo "  duration:  ${DURATION_HOURS}h"
echo "  project:   $PROJECT"
echo "  env:       $DEPLOYMENT_ENV"
echo "  channel:   uts-staging-noise (Telegram only — PagerDuty DISABLED)"
echo "  events:    gs://${PROJECT}-events/events/alerting-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"

# Launch via the standard data-pipeline-vm bootstrap; service-specific args
# pass through the metadata channel that setup-data-pipeline-vm.sh consumes.
gcloud compute instances create "$VM_NAME" \
  --zone="$ZONE" \
  --project="$PROJECT" \
  --machine-type=e2-standard-2 \
  --image-family=debian-12 \
  --image-project=debian-cloud \
  --boot-disk-size=20GB \
  --service-account="${SERVICE_ACCOUNT}" \
  --scopes=cloud-platform \
  --metadata="\
SERVICE_REPO=alerting-service,\
SERVICE_MODULE=alerting_service,\
VM_NAME=${VM_NAME},\
VM_DURATION_HOURS=${DURATION_HOURS},\
VM_SHUTDOWN_ON_COMPLETION=true,\
RUN_OPERATION=run,\
RUN_MODE=live,\
QUIETNESS_BASELINE=true,\
PAGERDUTY_DISABLED=true,\
TELEGRAM_CHANNEL_OVERRIDE=uts-staging-noise,\
TELEGRAM_BOT_TOKEN_SECRET=alerting-telegram-bot-token,\
TELEGRAM_CHAT_ID_SECRET=alerting-telegram-chat-id,\
DEPLOYMENT_ENV=${DEPLOYMENT_ENV},\
CODE_BUCKET=${CODE_BUCKET},\
PROJECT_ID=${PROJECT}" \
  --labels=purpose=alerting-quietness-baseline,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}" \
  --metadata-from-file="startup-script=$(dirname "$0")/setup-data-pipeline-vm.sh"

echo ""
echo "VM launched. Verify event stream within 90s:"
echo "  gcloud storage ls gs://${PROJECT}-events/events/alerting-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo ""
echo "Per CLAUDE.md 'No fire-and-forget VM launches' rule, schedule a follow-up"
echo "in 90s + every 12h for QUIETNESS_BASELINE_CHECKPOINT events. Auto-shutdown"
echo "fires at +${DURATION_HOURS}h via VM_SHUTDOWN_ON_COMPLETION=true."
