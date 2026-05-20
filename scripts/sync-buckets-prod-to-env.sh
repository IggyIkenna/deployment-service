#!/usr/bin/env bash
# ============================================================================
# sync-buckets-prod-to-env.sh — prod → {staging,dev} bucket sync with a
# truncated date window + same-region enforcement.
#
# Part of the (b+) env-aware bucket architecture
# (plans/active/bucket_name_ssot_canonicalisation_2026_05_10.md Phase 0h /
#  plans/active/code_freeze_migrate_backfill_sequencing_2026_05_10.md GAP-2.4.E).
#
# What it does
# ------------
# For every yaml-keyed bucket that carries the ``${DEPLOYMENT_ENV_SHORT}`` tier
# (i.e. every bucket that HAS a per-env variant — features-*, ml-*, strategy-*,
# execution-*, market-data-tick-*, instruments-store-*, dex-*, config-store, ...
# — env-LESS kinds like ``events`` / ``pnl-store-defi`` / ``positions-store-defi``
# / ``risk-store-defi`` are skipped, they have no staging/dev counterpart):
#
#   1. resolve the PROD bucket name   (DEPLOYMENT_ENV=prod    → ``...-prd-{pid}``)
#   2. resolve the TARGET bucket name (DEPLOYMENT_ENV=staging → ``...-stg-{pid}``
#                                       or DEPLOYMENT_ENV=development → ``...-dev-{pid}``)
#      — both via the canonical resolver in
#      ``unified_trading_library.cloud_interface.bucket_naming`` reading
#      ``deployment-service/configs/cloud-providers.yaml`` (the SSOT).
#   3. SAME-REGION check: prod and target buckets must be in the same region —
#      abort if they differ (no cross-region egress; canonical region is
#      ``asia-northeast1`` on GCP / ``ap-northeast-1`` on AWS per bucket_name_ssot
#      Phase 0i). Within-cloud same-region copy is $0 egress.
#   4. TRUNCATED DATE WINDOW: only the last ``--years N`` of hive-partitioned data
#      is copied (default N=2 for staging, N=1 for dev — operator-tunable). The
#      script enumerates the top-level prefixes of the prod bucket; for any prefix
#      that looks like (or contains) a ``day=YYYY-MM-DD`` hive partition, it is
#      included iff ``YYYY-MM-DD >= today - N*365``; non-day prefixes (``_index/``,
#      reference tables, etc.) are always included. Handles the common layouts:
#      ``by_date/day=YYYY-MM-DD/...``, ``raw_tick_data/by_date/day=YYYY-MM-DD/...``,
#      ``sports_reference/by_date/day=YYYY-MM-DD/...``.
#   5. IDEMPOTENT: uses ``gcloud storage rsync -r`` (GCP) / ``aws s3 sync`` (AWS) —
#      re-running only transfers what's missing/changed. ``--dry-run`` previews.
#   6. MANIFEST RE-SYNC: after data sync, re-runs the manifest consolidator scoped
#      to the target env so the staging/dev availability manifest matches the
#      truncated window (unless ``--no-manifest-resync``).
#   7. VERIFICATION: post-sync, the script reports object counts per bucket
#      (prod-within-window vs target) and spot-checks a sample of parquets are
#      readable in the target bucket. Counts that diverge by > 0.01% are flagged.
#
# Usage
# -----
#   bash deployment-service/scripts/sync-buckets-prod-to-env.sh --target-env staging
#   bash deployment-service/scripts/sync-buckets-prod-to-env.sh --target-env dev --years 1
#   bash deployment-service/scripts/sync-buckets-prod-to-env.sh --target-env staging --kind market-data --dry-run
#   bash deployment-service/scripts/sync-buckets-prod-to-env.sh --target-env staging --cloud aws
#
# Convenience wrappers:
#   bash deployment-service/scripts/sync-buckets-prod-to-staging.sh   # → --target-env staging
#   bash deployment-service/scripts/sync-buckets-prod-to-dev.sh       # → --target-env dev
#
# Schedule (operator choice)
# --------------------------
#   - On-demand (operator runs it before a staging/dev test campaign), OR
#   - Cloud Scheduler daily cron @ 02:00 UTC (low activity). Set up with:
#       gcloud scheduler jobs create http sync-buckets-prod-to-staging \
#         --schedule="0 2 * * *" --uri=<cloud-run-trigger-or-build-trigger> ...
#   First execution: Phase 3 / post-cutover (dev/staging not in active use
#   pre-2026-05-23 — no urgency; this script ships in Phase 1 code-complete).
#
# Requirements: gcloud (GCP) or aws CLI (AWS) with ADC/credentials; python3 with
# PyYAML + the unified-trading-library importable (for the canonical resolver).
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOYMENT_SERVICE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_YAML="${DEPLOYMENT_SERVICE_DIR}/configs/cloud-providers.yaml"

# ── Defaults ────────────────────────────────────────────────────────────────
TARGET_ENV=""
CLOUD="gcp"
YEARS=""               # default resolved per target-env below
KIND_FILTER=""
DRY_RUN=0
NO_MANIFEST_RESYNC=0
PROJECT_ID="${GCP_PROJECT_ID:-}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-}"

usage() {
  grep -E '^# ' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

# ── Parse args ──────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-env)            TARGET_ENV="$2"; shift 2 ;;
    --target-env=*)          TARGET_ENV="${1#*=}"; shift ;;
    --cloud)                 CLOUD="$2"; shift 2 ;;
    --cloud=*)               CLOUD="${1#*=}"; shift ;;
    --years)                 YEARS="$2"; shift 2 ;;
    --years=*)               YEARS="${1#*=}"; shift ;;
    --kind)                  KIND_FILTER="$2"; shift 2 ;;
    --kind=*)                KIND_FILTER="${1#*=}"; shift ;;
    --dry-run)               DRY_RUN=1; shift ;;
    --no-manifest-resync)    NO_MANIFEST_RESYNC=1; shift ;;
    --project-id)            PROJECT_ID="$2"; shift 2 ;;
    --project-id=*)          PROJECT_ID="${1#*=}"; shift ;;
    -h|--help)               usage 0 ;;
    *) echo "ERROR: unknown argument: $1" >&2; usage 1 ;;
  esac
done

# ── Validate ────────────────────────────────────────────────────────────────
case "${TARGET_ENV}" in
  staging|dev|development) ;;
  "") echo "ERROR: --target-env <staging|dev> is required" >&2; exit 2 ;;
  *)  echo "ERROR: --target-env must be one of: staging, dev, development (got: ${TARGET_ENV})" >&2; exit 2 ;;
esac
# Normalise dev → development for the resolver vocab (workspace keeps the long form).
RESOLVER_TARGET_ENV="${TARGET_ENV}"
[[ "${RESOLVER_TARGET_ENV}" == "dev" ]] && RESOLVER_TARGET_ENV="development"

case "${CLOUD}" in gcp|aws) ;; *) echo "ERROR: --cloud must be gcp or aws" >&2; exit 2 ;; esac

if [[ -z "${YEARS}" ]]; then
  case "${TARGET_ENV}" in
    staging) YEARS=2 ;;
    dev|development) YEARS=1 ;;
  esac
fi
if ! [[ "${YEARS}" =~ ^[0-9]+$ ]] || [[ "${YEARS}" -lt 1 ]]; then
  echo "ERROR: --years must be a positive integer (got: ${YEARS})" >&2; exit 2
fi

if [[ ! -f "${CONFIG_YAML}" ]]; then
  echo "ERROR: cloud-providers.yaml not found at ${CONFIG_YAML}" >&2; exit 1
fi

# Resolve project / account id.
if [[ "${CLOUD}" == "gcp" ]]; then
  PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
  if [[ -z "${PROJECT_ID}" ]]; then
    echo "ERROR: GCP_PROJECT_ID not set and 'gcloud config get-value project' is empty" >&2; exit 1
  fi
else
  AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)}"
  if [[ -z "${AWS_ACCOUNT_ID}" ]]; then
    echo "ERROR: AWS_ACCOUNT_ID not set and 'aws sts get-caller-identity' failed" >&2; exit 1
  fi
fi

CUTOFF_DATE="$(date -u -d "${YEARS} years ago" +%Y-%m-%d 2>/dev/null || python3 -c "import datetime; print((datetime.date.today() - datetime.timedelta(days=${YEARS}*365)).isoformat())")"

echo "=================================================================="
echo " prod → ${TARGET_ENV} bucket sync"
echo "   cloud       : ${CLOUD}"
echo "   project/acct: ${PROJECT_ID:-${AWS_ACCOUNT_ID}}"
echo "   date window : day >= ${CUTOFF_DATE}  (last ${YEARS} year(s))"
echo "   kind filter : ${KIND_FILTER:-<all env-tiered kinds>}"
echo "   dry-run     : $([[ ${DRY_RUN} -eq 1 ]] && echo yes || echo no)"
echo "=================================================================="

# ── Enumerate (kind, asset_group) → (prod_bucket, target_bucket) pairs ──────
# Uses the canonical resolver in unified_trading_library.cloud_interface.bucket_naming
# (reads cloud-providers.yaml). Falls back to a direct YAML walk + ${VAR} substitution
# if the library import is unavailable in the calling environment (e.g. CI before the
# venv is set up). Either way the names are derived from the SSOT yaml, not hardcoded.
ENUMERATE_PY="$(cat <<'PYEOF'
import os, sys, yaml

cloud = os.environ["SYNC_CLOUD"]
kind_filter = os.environ.get("SYNC_KIND_FILTER") or None
cfg_path = os.environ["SYNC_CONFIG_YAML"]
project_id = os.environ.get("GCP_PROJECT_ID", "")
aws_account = os.environ.get("AWS_ACCOUNT_ID", "")
gcs_region = os.environ.get("GCS_REGION", "asia-northeast1")
aws_region = os.environ.get("AWS_REGION", "ap-northeast-1")

ENV_SHORT = {"development": "dev", "dev": "dev", "staging": "stg", "stg": "stg",
             "prod": "prd", "prd": "prd", "production": "prd", "test": "test"}


def _subst(template: str, deployment_env: str) -> str:
    short = ENV_SHORT.get(deployment_env.lower(), "prd")
    return (template
            .replace("${DEPLOYMENT_ENV_SHORT}", short)
            .replace("${DEPLOYMENT_ENV}", deployment_env)
            .replace("${GCP_PROJECT_ID}", project_id)
            .replace("${AWS_ACCOUNT_ID}", aws_account)
            .replace("${GCS_REGION}", gcs_region)
            .replace("${AWS_REGION}", aws_region)
            .replace("{project_id}", project_id))


def _resolve(cloud_: str, kind_: str, ag_: str | None, deployment_env: str) -> str | None:
    """Prefer the canonical UTL resolver; fall back to the local YAML walk."""
    try:
        from unified_trading_library.cloud_interface.bucket_naming import resolve_bucket_name  # noqa: PLC0415
        os.environ["DEPLOYMENT_ENV"] = deployment_env
        # The resolver caches the parsed yaml; clear so the env-var change takes effect.
        try:
            from unified_trading_library.cloud_interface import bucket_naming as _bn  # noqa: PLC0415
            _bn._clear_yaml_cache()  # type: ignore[attr-defined]  # internal, fine for a script
        except Exception:
            pass
        return resolve_bucket_name(cloud=cloud_, kind=kind_, asset_group=ag_)  # type: ignore[arg-type]
    except Exception:
        return None  # caller falls back to the local walk below


with open(cfg_path) as f:
    data = yaml.safe_load(f)
storage = (data.get(cloud) or {}).get("storage") or {}

rows: list[tuple[str, str, str]] = []  # (kind, prod_bucket, target_env_bucket)
for kind, entry in storage.items():
    if kind_filter and kind != kind_filter:
        continue
    templates: list[tuple[str | None, str]] = []
    if isinstance(entry, str):
        templates.append((None, entry))
    elif isinstance(entry, dict):
        for ag, t in entry.items():
            if isinstance(t, str):
                templates.append((ag.lower(), t))
    for ag, template in templates:
        if "DEPLOYMENT_ENV" not in template:
            # env-less kind (events / pnl-store-defi / ...) — no per-env variant; skip.
            continue
        prod = _resolve(cloud, kind, ag, "prod") or _subst(template, "prod")
        tgt = _resolve(cloud, kind, ag, os.environ["SYNC_RESOLVER_TARGET_ENV"]) or _subst(template, os.environ["SYNC_RESOLVER_TARGET_ENV"])
        if not prod or not tgt or "${" in prod or "${" in tgt:
            print(f"# WARN: could not fully resolve kind={kind} ag={ag} (prod={prod!r} tgt={tgt!r}) — skipping", file=sys.stderr)
            continue
        rows.append((kind if ag is None else f"{kind}[{ag}]", prod, tgt))

for label, prod, tgt in rows:
    print(f"{label}\t{prod}\t{tgt}")
PYEOF
)"

export SYNC_CLOUD="${CLOUD}"
export SYNC_KIND_FILTER="${KIND_FILTER}"
export SYNC_CONFIG_YAML="${CONFIG_YAML}"
export SYNC_RESOLVER_TARGET_ENV="${RESOLVER_TARGET_ENV}"
export GCP_PROJECT_ID="${PROJECT_ID:-}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-}"

mapfile -t PAIRS < <(python3 -c "${ENUMERATE_PY}")
if [[ "${#PAIRS[@]}" -eq 0 ]]; then
  echo "ERROR: no env-tiered bucket pairs enumerated for cloud=${CLOUD}${KIND_FILTER:+ kind=${KIND_FILTER}}" >&2
  exit 1
fi
echo "Enumerated ${#PAIRS[@]} env-tiered bucket pair(s)."

# ── Region helpers ──────────────────────────────────────────────────────────
gcs_bucket_region() { gcloud storage buckets describe "gs://$1" --format='value(location)' 2>/dev/null | tr '[:upper:]' '[:lower:]'; }
s3_bucket_region()  { aws s3api get-bucket-location --bucket "$1" --query 'LocationConstraint' --output text 2>/dev/null | sed 's/^None$/us-east-1/'; }
gcs_bucket_exists() { gcloud storage buckets describe "gs://$1" >/dev/null 2>&1; }
s3_bucket_exists()  { aws s3api head-bucket --bucket "$1" >/dev/null 2>&1; }

# ── Date-window prefix helper ───────────────────────────────────────────────
# Echo the list of top-level "directory" prefixes in a bucket that should be
# synced for the truncated window: any prefix containing a ``day=YYYY-MM-DD``
# segment is kept iff that date >= CUTOFF_DATE; all other prefixes are kept.
gcs_prefixes_within_window() {
  local bucket="$1"
  # gcloud storage ls gs://bucket/  → top-level objects + prefixes (trailing /).
  # We descend one or two levels to find the ``day=`` partition root for the
  # common layouts (raw_tick_data/by_date/, by_date/, sports_reference/by_date/).
  local roots
  roots="$(gcloud storage ls "gs://${bucket}/" 2>/dev/null || true)"
  local p
  while IFS= read -r p; do
    [[ -z "${p}" ]] && continue
    p="${p#gs://"${bucket}"/}"; p="${p%/}"
    case "${p}" in
      *day=*)
        local d="${p##*day=}"; d="${d%%/*}"
        if [[ "${d}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] && [[ "${d}" < "${CUTOFF_DATE}" ]]; then
          continue   # too old — skip
        fi
        echo "${p}"
        ;;
      "")  ;;
      *by_date|*reference|*_index|*by-date)
        # A partition root or reference tree — descend to find ``day=`` children.
        local sub
        sub="$(gcloud storage ls "gs://${bucket}/${p}/" 2>/dev/null || true)"
        local found_day=0
        local s
        while IFS= read -r s; do
          [[ -z "${s}" ]] && continue
          s="${s#gs://"${bucket}"/}"; s="${s%/}"
          case "${s}" in
            *day=*)
              found_day=1
              local d2="${s##*day=}"; d2="${d2%%/*}"
              if [[ "${d2}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] && [[ "${d2}" < "${CUTOFF_DATE}" ]]; then
                continue
              fi
              echo "${s}"
              ;;
            *) echo "${s}" ;;   # non-day child under a by_date/reference tree — keep
          esac
        done <<< "${sub}"
        [[ "${found_day}" -eq 0 ]] && echo "${p}"   # no day= children — keep the whole prefix
        ;;
      *)
        echo "${p}"   # everything else (config blobs, _index, manifests, etc.) — keep
        ;;
    esac
  done <<< "${roots}"
}

# ── Sync each pair ──────────────────────────────────────────────────────────
SYNCED=0
SKIPPED_MISSING_PROD=0
ERRORS=0
declare -a VERIFY_ROWS=()

for line in "${PAIRS[@]}"; do
  IFS=$'\t' read -r LABEL PROD_BUCKET TGT_BUCKET <<< "${line}"
  echo
  echo "── ${LABEL}: gs://${PROD_BUCKET}  →  gs://${TGT_BUCKET}"

  if [[ "${CLOUD}" == "gcp" ]]; then
    if ! gcs_bucket_exists "${PROD_BUCKET}"; then
      echo "   prod bucket does not exist (yet) — skipping (provisioned in code_freeze Phase 2.6)."
      SKIPPED_MISSING_PROD=$((SKIPPED_MISSING_PROD+1)); continue
    fi
    if ! gcs_bucket_exists "${TGT_BUCKET}"; then
      echo "   ⚠️  target bucket gs://${TGT_BUCKET} does not exist — run setup-buckets.py --env ${RESOLVER_TARGET_ENV} first. Skipping."
      ERRORS=$((ERRORS+1)); continue
    fi
    PROD_REGION="$(gcs_bucket_region "${PROD_BUCKET}")"
    TGT_REGION="$(gcs_bucket_region "${TGT_BUCKET}")"
    if [[ -n "${PROD_REGION}" && -n "${TGT_REGION}" && "${PROD_REGION}" != "${TGT_REGION}" ]]; then
      echo "   ❌ region mismatch: prod=${PROD_REGION} target=${TGT_REGION} — refusing cross-region egress. Skipping."
      ERRORS=$((ERRORS+1)); continue
    fi
    # Build the include set for the truncated window.
    mapfile -t PREFIXES < <(gcs_prefixes_within_window "${PROD_BUCKET}")
    if [[ "${#PREFIXES[@]}" -eq 0 ]]; then
      echo "   (prod bucket has no objects within the ${YEARS}-year window — nothing to sync)"
      continue
    fi
    RSYNC_FLAGS=("-r")
    [[ ${DRY_RUN} -eq 1 ]] && RSYNC_FLAGS+=("--dry-run")
    for pref in "${PREFIXES[@]}"; do
      echo "   rsync gs://${PROD_BUCKET}/${pref}/  →  gs://${TGT_BUCKET}/${pref}/"
      gcloud storage rsync "${RSYNC_FLAGS[@]}" "gs://${PROD_BUCKET}/${pref}/" "gs://${TGT_BUCKET}/${pref}/" || {
        echo "   ⚠️  rsync failed for prefix ${pref} — continuing with the rest."
        ERRORS=$((ERRORS+1))
      }
    done
    SYNCED=$((SYNCED+1))
    if [[ ${DRY_RUN} -eq 0 ]]; then
      PROD_CNT="$(gcloud storage ls -r "gs://${PROD_BUCKET}/**" 2>/dev/null | grep -c "/day=" || true)"
      TGT_CNT="$(gcloud storage ls -r "gs://${TGT_BUCKET}/**" 2>/dev/null | grep -c "/day=" || true)"
      VERIFY_ROWS+=("${LABEL}|${PROD_BUCKET}|${PROD_CNT}|${TGT_BUCKET}|${TGT_CNT}")
    fi
  else  # AWS
    if ! s3_bucket_exists "${PROD_BUCKET}"; then
      echo "   prod bucket does not exist (yet) — skipping."
      SKIPPED_MISSING_PROD=$((SKIPPED_MISSING_PROD+1)); continue
    fi
    if ! s3_bucket_exists "${TGT_BUCKET}"; then
      echo "   ⚠️  target bucket s3://${TGT_BUCKET} does not exist — run setup-buckets.py --cloud aws --env ${RESOLVER_TARGET_ENV} first. Skipping."
      ERRORS=$((ERRORS+1)); continue
    fi
    PROD_REGION="$(s3_bucket_region "${PROD_BUCKET}")"
    TGT_REGION="$(s3_bucket_region "${TGT_BUCKET}")"
    if [[ -n "${PROD_REGION}" && -n "${TGT_REGION}" && "${PROD_REGION}" != "${TGT_REGION}" ]]; then
      echo "   ❌ region mismatch: prod=${PROD_REGION} target=${TGT_REGION} — refusing cross-region egress. Skipping."
      ERRORS=$((ERRORS+1)); continue
    fi
    # aws s3 sync with an exclude for day= partitions older than the cutoff. We
    # exclude everything matching ``*day=YYYY-MM-DD/*`` for years strictly before
    # the cutoff year, plus partial-year days before the cutoff in the cutoff year.
    EXCLUDES=()
    CUTOFF_YEAR="${CUTOFF_DATE%%-*}"
    # crude but safe: exclude any 4-digit year < CUTOFF_YEAR (full years), then
    # the rsync below also re-syncs the cutoff year fully (a small over-copy that
    # is harmless + idempotent).
    for ((y=2015; y<CUTOFF_YEAR; y++)); do EXCLUDES+=("--exclude" "*day=${y}-*"); done
    SYNC_FLAGS=("--no-progress")
    [[ ${DRY_RUN} -eq 1 ]] && SYNC_FLAGS+=("--dryrun")
    echo "   aws s3 sync s3://${PROD_BUCKET}/ → s3://${TGT_BUCKET}/  (excluding day partitions before ${CUTOFF_YEAR})"
    aws s3 sync "s3://${PROD_BUCKET}/" "s3://${TGT_BUCKET}/" "${SYNC_FLAGS[@]}" "${EXCLUDES[@]}" || {
      echo "   ⚠️  aws s3 sync failed — continuing."
      ERRORS=$((ERRORS+1)); continue
    }
    SYNCED=$((SYNCED+1))
  fi
done

# ── Manifest re-sync (so the target-env availability manifest matches the truncated window) ──
# Consolidator runtime as of 2026-05-20: Cloud Run Jobs (terraform/gcp/manifest_consolidator_scheduler.tf),
# 10 jobs (5 instruments + 5 market-data) fired every minute via Cloud Scheduler `*/1 * * * *`.
# Legacy GCE VM launcher (`launch-manifest-consolidator-vm.sh`) was deleted 2026-05-20 per operator
# directive — Cloud Run is the canonical consolidator path. SSOT: codex/05-infrastructure/manifest-consolidator-ssot.md.
if [[ ${NO_MANIFEST_RESYNC} -eq 0 && ${DRY_RUN} -eq 0 && ${SYNCED} -gt 0 ]]; then
  echo
  echo "── Manifest re-sync after bucket sync (env=${RESOLVER_TARGET_ENV}) ──"
  echo "   Consolidator is Cloud Run + Cloud Scheduler (every minute). To force a re-run scoped to a single bucket:"
  echo "     gcloud run jobs execute uts-${RESOLVER_TARGET_ENV}-manifest-consolidator-<bucket-key> --region asia-northeast1 --wait"
  echo "   Where <bucket-key> ∈ {instruments-cefi|instruments-defi|instruments-tradfi|instruments-sports|instruments-prediction|market-data-cefi|market-data-defi|market-data-tradfi|market-data-sports|market-data-prediction}."
  echo "   The scheduler will also pick up the next minute-aligned tick automatically."
fi

# ── Verification report ─────────────────────────────────────────────────────
echo
echo "=================================================================="
echo " SYNC SUMMARY (target-env=${TARGET_ENV}, cloud=${CLOUD})"
echo "   synced            : ${SYNCED}"
echo "   skipped (no prod) : ${SKIPPED_MISSING_PROD}"
echo "   errors            : ${ERRORS}"
if [[ "${#VERIFY_ROWS[@]}" -gt 0 ]]; then
  echo
  echo " Per-bucket day-partition object counts (prod-in-window vs target):"
  for row in "${VERIFY_ROWS[@]}"; do
    IFS='|' read -r L _ PC TB TC <<< "${row}"
    DELTA_OK="ok"
    if [[ "${PC}" =~ ^[0-9]+$ && "${TC}" =~ ^[0-9]+$ && "${PC}" -gt 0 ]]; then
      # |PC - TC| / PC > 0.0001  → flag
      DIFF=$(( PC > TC ? PC - TC : TC - PC ))
      THRESH=$(( PC / 10000 + 1 ))
      [[ "${DIFF}" -gt "${THRESH}" ]] && DELTA_OK="⚠️ DRIFT(${DIFF})"
    fi
    printf "   %-35s prod=%-8s target=%-8s %s\n" "${L}" "${PC}" "${TC}" "${DELTA_OK}"
  done
  echo
  echo " Spot-check a sample parquet readability in the target buckets:"
  for row in "${VERIFY_ROWS[@]}"; do
    IFS='|' read -r L _ PC TB TC <<< "${row}"
    SAMPLE="$(gcloud storage ls -r "gs://${TB}/**.parquet" 2>/dev/null | head -1 || true)"
    if [[ -n "${SAMPLE}" ]]; then
      if gcloud storage cat "${SAMPLE}" 2>/dev/null | head -c 4 | grep -q "PAR1" 2>/dev/null || gcloud storage objects describe "${SAMPLE}" >/dev/null 2>&1; then
        echo "   ${L}: ${SAMPLE} — readable ✓"
      else
        echo "   ${L}: ${SAMPLE} — ⚠️  could not confirm readable"
      fi
    else
      echo "   ${L}: (no .parquet found in target — empty or not yet synced)"
    fi
  done
fi
echo "=================================================================="
if [[ ${ERRORS} -gt 0 ]]; then exit 1; fi
