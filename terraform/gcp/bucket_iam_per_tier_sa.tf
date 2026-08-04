# Per-tier service accounts — bucket-isolation-model §8 write-protection (P1.1).
#
# plans/active/bucket_iam_write_protection_per_tier_2026_06_09.md: replaces the
# project-wide unified-trading-sa (roles/storage.objectAdmin over every bucket,
# defined above in main.tf) with per-tier identities so a dev/staging credential
# can be IAM-denied a prod-bucket write, not just code-level-guarded.
#
# uts-migration-sa is the sanctioned cross-tier write exception (operator
# 2026-06-09: "exceptions for migration scripts") — used only by
# *_service/scripts/migration_*.py once P1.2/P2 wire it in.
#
# HISTORICAL / PERMANENTLY UNBOUND (2026-07-27, operator-approved per BLK-4b104acc):
# uts-dev-sa and uts-stg-sa below were created under P1.1's original 4-tier design,
# which predates a 2026-07-13 operator ruling that permanently retired the dev/stg
# BUCKET tiers workspace-wide (canonical_buckets.tf:44-46 — "prd + test are the only
# provisioned tiers"; 20 empty dev/stg canonical buckets deleted). There is no
# -dev-/-stg- bucket suffix for these SAs to ever bind to. GCP service-account
# account_ids are IMMUTABLE, so they cannot be renamed to reflect this — kept as
# NAMED, DOCUMENTED, PERMANENTLY-UNBOUND historical resources (zero IAM role
# bindings, zero access, deliberately not destroyed — a destroy is unnecessary
# risk for an already-inert resource). See
# issues/bucket_iam_per_tier_dev_stg_retired_ssot_contradiction_2026_07_27.md.
# uts-test-sa (new, below) is the correctly-named tier-writer for the ACTUAL
# non-prod tier that exists: -test- (ephemeral, CI/E2E).

resource "google_service_account" "uts_dev" {
  account_id   = "uts-dev-sa"
  display_name = "UTS dev-tier service account (HISTORICAL — permanently unbound)"
  description  = "RETIRED 2026-07-27: -dev- bucket tier permanently retired 2026-07-13. No IAM bindings — historical, not destroyed. See bucket_iam_per_tier_dev_stg_retired_ssot_contradiction_2026_07_27.md."
  project      = var.project_id
}

resource "google_service_account" "uts_stg" {
  account_id   = "uts-stg-sa"
  display_name = "UTS staging-tier service account (HISTORICAL — permanently unbound)"
  description  = "RETIRED 2026-07-27: -stg- bucket tier permanently retired 2026-07-13. No IAM bindings — historical, not destroyed. See bucket_iam_per_tier_dev_stg_retired_ssot_contradiction_2026_07_27.md."
  project      = var.project_id
}

resource "google_service_account" "uts_prd" {
  account_id   = "uts-prd-sa"
  display_name = "UTS prod-tier service account"
  description  = "Write access to *-prd-* buckets only, read-only elsewhere (bucket-isolation-model.md §8)."
  project      = var.project_id
}

resource "google_service_account" "uts_test" {
  account_id   = "uts-test-sa"
  display_name = "UTS test-tier service account"
  description  = "Write access to *-test-* buckets only (ephemeral CI/E2E), read-only elsewhere (bucket-isolation-model.md §8). Replaces the retired dev/stg-tier concept."
  project      = var.project_id
}

resource "google_service_account" "uts_migration" {
  account_id   = "uts-migration-sa"
  display_name = "UTS cross-tier migration service account"
  description  = "Sanctioned cross-tier write exception for migration scripts only (bucket-isolation-model.md §8)."
  project      = var.project_id
}

# ---------------------------------------------------------------------------
# P1.2 — per-suffix objectAdmin + broad objectViewer.
#
# RESOLVED 2026-07-27 (operator approval, BLK-4b104acc): P1.2's literal text
# specified uts-dev-sa -> *-dev-* and uts-stg-sa -> *-stg-* bindings. Per the
# operator-approved resolution, those are NOT shipped — the -dev-/-stg- bucket
# tiers were permanently retired 2026-07-13 (canonical_buckets.tf:44-46), so
# there is nothing for those bindings to ever match. uts-test-sa (new) is the
# correctly-named replacement, bound to the tier that actually exists. See
# issues/bucket_iam_per_tier_dev_stg_retired_ssot_contradiction_2026_07_27.md.

locals {
  # Group A canonical bucket-name family prefixes (raw data). Matches
  # canonical_buckets.tf's market-data-tick / instruments-store /
  # features-calendar kinds — the exact 22-bucket set the operator
  # personally enumerated 2026-07-25 (bucket_iam_write_protection_per_tier_
  # 2026_06_09.md Phase-1 scoping note): {prd,test}-tiered only, no
  # -dev-/-stg- suffix exists for this family.
  #
  # FIXED 2026-08-01 (bucket_iam_group_a_market_data_tick_prefix_missing_
  # asset_group_2026_08_01.md): market-data-tick- AND instruments-store- are
  # BOTH AG-scoped (`{family}-{ag}-{prd,test}-{project}` — confirmed live +
  # cross-checked against docs/GCS_PATHS.md, cloud-providers.yaml's `tick-data`
  # kind, and this file's own canonical_buckets.tf derivation). A flat family
  # prefix can never match a real bucket in either family, because the AG
  # segment sits BETWEEN the family prefix and the tier — live-confirmed as a
  # 100%-write-failure regression for uts-prd-sa/uts-test-sa against every
  # market-data-tick-{ag} bucket (4425 PERMISSION_DENIED 403s, both tiers,
  # every asset group; fixed live+source first) and every instruments-store-
  # {ag} bucket (same shape, confirmed live via a real instruments-service
  # -test-run write; source added same commit as this reconciliation). Only
  # features-calendar- is genuinely flat/non-AG-scoped (no per-AG variant
  # exists). Both AG-scoped families get their own per-AG list, mirroring
  # Group B's per-AG treatment below.
  group_a_flat_bucket_prefixes = [
    "features-calendar-",
  ]
  group_a_market_data_tick_ag_prefixes = [
    "market-data-tick-cefi-",
    "market-data-tick-defi-",
    "market-data-tick-tradfi-",
    "market-data-tick-sports-",
    "market-data-tick-pred-",
  ]
  group_a_instruments_store_ag_prefixes = [
    "instruments-store-cefi-",
    "instruments-store-defi-",
    "instruments-store-tradfi-",
    "instruments-store-sports-",
    "instruments-store-pred-",
  ]
  group_a_bucket_prefixes = concat(
    local.group_a_flat_bucket_prefixes,
    local.group_a_market_data_tick_ag_prefixes,
    local.group_a_instruments_store_ag_prefixes,
  )

  # Group B canonical bucket-name family prefixes (derived data) — the ONE
  # per-AG folded kind that is env-tiered today: `features-{ag}` (Wave-3 Fold A,
  # plans/active/bucket_fold_features_2026_07_17.md, code-complete + redeployed
  # 2026-07-26). Per bucket-isolation-model.md §2, `features-{ag}` resolves to
  # `features-{cefi,tradfi,defi,pred,sports}-{prd,test}-{project_id}` —
  # `cloud-providers.yaml`'s `features:` per-AG dict (CEFI/TRADFI/DEFI/
  # PREDICTION) + the flat `features-sports` key. Excludes `features-calendar`
  # (Group A, listed above) and `features-commodity` (flat/non-env-split — no
  # `-prd-`/`-test-` suffix to match, and NOT part of this fold). The other
  # Wave-3-folded Group-B kinds (ml-store, execution-store, strategy-store,
  # portfolio-state) belong to their own sibling fold plans
  # (bucket_fold_{ml,execution_strategy,portfolio_state}_2026_07_17.md) and are
  # NOT joined here — each joins independently once ITS fold is code-complete.
  group_b_bucket_prefixes = [
    "features-cefi-",
    "features-tradfi-",
    "features-defi-",
    "features-pred-",
    "features-sports-",
  ]
}

resource "google_project_iam_member" "uts_prd_objectadmin_group_a" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.uts_prd.email}"

  condition {
    title       = "group-a-prd-tier-only"
    description = "uts-prd-sa writes ONLY Group A -prd- buckets (bucket_iam_write_protection_per_tier_2026_06_09.md P1.2). GCP IAM Condition CEL supports only startsWith/endsWith on resource.name (contains/matches are BOTH undeclared references, confirmed live 2026-07-29) -- prd/test immediately follows the group prefix in every real bucket name (e.g. features-cefi-prd-<project>), so a single startsWith per prefix+tier is exact, not an approximation."
    expression = join(" || ", [
      for prefix in local.group_a_bucket_prefixes :
      "resource.name.startsWith(\"projects/_/buckets/${prefix}prd-\")"
    ])
  }
}

resource "google_project_iam_member" "uts_test_objectadmin_group_a" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.uts_test.email}"

  condition {
    title       = "group-a-test-tier-only"
    description = "uts-test-sa writes ONLY Group A -test- buckets (ephemeral CI/E2E; bucket_iam_write_protection_per_tier_2026_06_09.md P1.2, test-tier slice). See group-a-prd-tier-only's comment for why this is startsWith-only."
    expression = join(" || ", [
      for prefix in local.group_a_bucket_prefixes :
      "resource.name.startsWith(\"projects/_/buckets/${prefix}test-\")"
    ])
  }
}

resource "google_project_iam_member" "uts_prd_objectadmin_group_b" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.uts_prd.email}"

  condition {
    title       = "group-b-prd-tier-only"
    description = "uts-prd-sa writes ONLY Group B -prd- buckets (bucket_fold_features_2026_07_17.md IAM+lifecycle todo — Group B joins Phase-2 once its fold provisions the -{env}- form). See group-a-prd-tier-only's comment for why this is startsWith-only."
    expression = join(" || ", [
      for prefix in local.group_b_bucket_prefixes :
      "resource.name.startsWith(\"projects/_/buckets/${prefix}prd-\")"
    ])
  }
}

resource "google_project_iam_member" "uts_test_objectadmin_group_b" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.uts_test.email}"

  condition {
    title       = "group-b-test-tier-only"
    description = "uts-test-sa writes ONLY Group B -test- buckets (ephemeral CI/E2E; bucket_fold_features_2026_07_17.md IAM+lifecycle todo, test-tier slice). See group-a-prd-tier-only's comment for why this is startsWith-only."
    expression = join(" || ", [
      for prefix in local.group_b_bucket_prefixes :
      "resource.name.startsWith(\"projects/_/buckets/${prefix}test-\")"
    ])
  }
}

# Broad read access for all 5 per-tier SAs (incl. the historical/unbound
# dev+stg pair) — operator 2026-06-09: "read from anything but only WRITE to
# the bucket with our SA credentials." Unconditional (project-wide)
# objectViewer is safe: it grants read only, never write.
resource "google_project_iam_member" "uts_dev_objectviewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.uts_dev.email}"
}

resource "google_project_iam_member" "uts_stg_objectviewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.uts_stg.email}"
}

resource "google_project_iam_member" "uts_prd_objectviewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.uts_prd.email}"
}

resource "google_project_iam_member" "uts_test_objectviewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.uts_test.email}"
}

resource "google_project_iam_member" "uts_migration_objectviewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.uts_migration.email}"
}

# ---------------------------------------------------------------------------
# P3 (issues/bucket_iam_per_tier_dev_stg_retired_ssot_contradiction_2026_07_27.md)
# — CI/CD SA (github-actions-deploy) read-only on Group A's -test-/-prd- bucket
# families (bucket-isolation-model.md §8 "CI/CD SA read-only on prod").
#
# github-actions-deploy is NOT terraform-managed (it predates this repo's IaC
# adoption — confirmed 2026-07-28 via exhaustive grep of every
# google_service_account in this directory: zero matches). Its binding is a
# literal-email reference, never a google_service_account resource (creating
# one would either error on import-collision or create a stray duplicate of a
# real, already-provisioned identity).
#
# Scoped to Group A only (Group B's features-{ag} fold is still in its own
# plan's IAM phase — bucket_fold_features_2026_07_17.md). Both -prd- and -test-
# tiers: CI reads both (tofu plan against prod state, test-run validation
# against ephemeral test buckets). GCP IAM Condition CEL supports only
# startsWith/endsWith on resource.name (see group-a-prd-tier-only's comment
# above), so this uses the same single-startsWith-per-prefix+tier pattern.
resource "google_project_iam_member" "github_actions_deploy_objectviewer_group_a" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:github-actions-deploy@${var.project_id}.iam.gserviceaccount.com"

  condition {
    title       = "ci-read-group-a-prd-test"
    description = "github-actions-deploy reads Group A -prd- and -test- buckets (bucket_iam_per_tier_dev_stg_retired_ssot_contradiction_2026_07_27.md P3). See group-a-prd-tier-only's comment for why this is startsWith-only."
    expression = join(" || ", concat(
      [for prefix in local.group_a_bucket_prefixes :
        "resource.name.startsWith(\"projects/_/buckets/${prefix}prd-\")"
      ],
      [for prefix in local.group_a_bucket_prefixes :
        "resource.name.startsWith(\"projects/_/buckets/${prefix}test-\")"
      ],
    ))
  }
}

# P2.2f/g (bucket_iam_write_protection_per_tier_2026_06_09.md) — uts-migration-sa
# held ONLY objectViewer despite being "the sanctioned cross-tier writer" per this
# plan's own design intent. Live-audit 2026-08-03 of the 3 launchers this grant was
# meant to unblock found: launch-legacy-bucket-migration-sharded.sh and
# launch-gcs-migration-bundle-vm.sh are BOTH dead code (their target scripts were
# deleted 2026-07-25 and 2026-05-20 respectively, each for a completed, already-
# verified migration — not a live blocker; see the plan's P2.2f addendum for the
# exact commits). Only launch-bucket-rsync-vm.sh (Lifecycle: permanent) is live.
#
# Unconditioned (not scoped like uts_prd_objectadmin_group_a/b above) because this
# launcher is a GENERIC cross-family migration tool — its --dest-bucket varies
# per invocation (market-data-tick-{ag}, strategy-store-{ag}, manual-audit, ... —
# an open set that grows with each future cutover wave, unlike uts-prd-sa/
# uts-test-sa's fixed, enumerable Group A/B prefix lists). GCP IAM Conditions on
# resource.name support only startsWith/endsWith (see group-a-prd-tier-only's
# comment above) — startsWith needs a fixed family prefix, and endsWith can't
# reach the bucket segment for an Object-level condition (resource.name is
# projects/_/buckets/{bucket}/objects/{object}; the unbounded object key, not the
# bucket name, sits at the end of the string). Neither works when the family
# prefix is a variable, so a real least-privilege CEL condition is not achievable
# here — an unconditioned grant IS the correct design for this SA's declared
# purpose ("sanctioned cross-tier write exception... used only by migration
# scripts"), not a shortcut around scoping it.
#
# INERT today: launch-bucket-rsync-vm.sh does not pass --service-account at all
# (confirmed by reading the launcher source 2026-08-03) — its VM currently runs
# under the project default compute SA, not uts-migration-sa. Wiring the launcher
# to actually authenticate as uts-migration-sa is tracked separately (P2.2h) since
# it is a code change, not an IAM change; this grant is forward-provisioned so
# that todo is unblocked the moment it lands, mirroring this file's own
# established "grant now, INERT until a runtime is wired to it" pattern (see the
# P2.2b section below).
resource "google_project_iam_member" "uts_migration_objectadmin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.uts_migration.email}"
}

# ---------------------------------------------------------------------------
# P2.2b — non-storage roles for uts-prd-sa / uts-test-sa / uts-migration-sa
# (plans/active/bucket_iam_write_protection_per_tier_2026_06_09.md).
#
# Operator ruling 2026-07-31 (BLK-0c84ceac, "C: hybrid"): per-tier SAs stay
# the write-owner for the Group A/B raw-data buckets this plan already
# covers; per-service SAs (deployment-service/configs/gcp_service_accounts.yaml)
# own already-migrated domain services — see
# issues/bucket_iam_p2_tier_sa_scope_gap_and_default_compute_sa_overprivilege_2026_07_30.md
# "Hybrid (C) boundary proposal" for the full bucket->scheme table.
#
# That issue doc's Finding 1 (live-verified 2026-07-30): uts-prd-sa/
# uts-test-sa/uts-migration-sa held ONLY storage.objectAdmin/objectViewer —
# wiring any real runtime to them (P2.2c Cloud Run / P2.2d VM launchers,
# both still open + gated on this todo) would immediately break that
# runtime's Secret Manager / Pub/Sub / BigQuery / Cloud Run access. This
# grants the 7 non-storage roles unified-trading-sa currently holds
# (main.tf's unified_trading_* project members) to the 3 live per-tier SAs —
# same roles, same project-wide scope (none of these 7 carry a bucket-tier
# IAM Condition on unified-trading-sa either, so there is nothing
# tier-specific to condition here). uts-dev-sa/uts-stg-sa are intentionally
# excluded (permanently unbound/historical, see the file header comment).
#
# INERT until P2.2c/P2.2d actually wire a runtime to authenticate as one of
# these 3 SAs — granting these roles today does not change any live
# runtime's identity or behavior.
#
# LIVE-APPLIED 2026-07-31 (slot-14): `tofu apply` against terraform/state/prod
# already ran these 21 grants into both GCP IAM and the remote state before
# this session restarted and lost the uncommitted .tf source — re-adding the
# declaration here to bring config back in sync with state + live reality
# (confirmed via `tofu state list` + a live `gcloud projects get-iam-policy`
# read: all 21 bindings present). A clean `tofu plan` after this commit shows
# 0 changes.
locals {
  uts_tier_sa_non_storage_grantees = {
    prd       = google_service_account.uts_prd.email
    test      = google_service_account.uts_test.email
    migration = google_service_account.uts_migration.email
  }
}

resource "google_project_iam_member" "uts_tier_sa_bq_editor" {
  for_each = local.uts_tier_sa_non_storage_grantees
  project  = var.project_id
  role     = "roles/bigquery.dataEditor"
  member   = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "uts_tier_sa_secret_accessor" {
  for_each = local.uts_tier_sa_non_storage_grantees
  project  = var.project_id
  role     = "roles/secretmanager.secretAccessor"
  member   = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "uts_tier_sa_run_invoker" {
  for_each = local.uts_tier_sa_non_storage_grantees
  project  = var.project_id
  role     = "roles/run.invoker"
  member   = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "uts_tier_sa_pubsub_editor" {
  for_each = local.uts_tier_sa_non_storage_grantees
  project  = var.project_id
  role     = "roles/pubsub.editor"
  member   = "serviceAccount:${each.value}"
}

# compute.instanceAdmin.v1 + iam.serviceAccountUser mirror unified-trading-sa's
# self-impersonating VM-launch path (main.tf's unified_trading_compute_instance_admin
# / unified_trading_service_account_user comment) — required for P2.2d VM launchers
# to eventually run `--service-account=uts-{prd,test}-sa@...`.
resource "google_project_iam_member" "uts_tier_sa_compute_instance_admin" {
  for_each = local.uts_tier_sa_non_storage_grantees
  project  = var.project_id
  role     = "roles/compute.instanceAdmin.v1"
  member   = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "uts_tier_sa_service_account_user" {
  for_each = local.uts_tier_sa_non_storage_grantees
  project  = var.project_id
  role     = "roles/iam.serviceAccountUser"
  member   = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "uts_tier_sa_artifactregistry_reader" {
  for_each = local.uts_tier_sa_non_storage_grantees
  project  = var.project_id
  role     = "roles/artifactregistry.reader"
  member   = "serviceAccount:${each.value}"
}

# ---------------------------------------------------------------------------
# P2 (issues/bucket_iam_p2_tier_sa_scope_gap_and_default_compute_sa_overprivilege_2026_07_30.md)
# — wiring deploy-shared.sh (deployment-api's Cloud Run identity) to uts-prd-sa
# surfaced 2 additional live gaps this todo's own P1 grant list (mirrored from
# main.tf's *declared* unified_trading_bq_editor member) didn't cover, because
# unified-trading-sa's LIVE role set is broader than its terraform-declared
# grants (a separate, out-of-band `roles/bigquery.admin` grant exists live —
# not reproduced here, since bigquery.admin is itself over-privileged; only the
# specific capability deployment-api's code actually exercises is granted):
#
# 1. deployment-api's UTL GCP provider (execute_query() ->
#    unified_trading_library/cloud_interface/providers/gcp.py:695, used by
#    deployment_api/services/operational_data_queries.py's run_query() and the
#    cost_observability routes) calls `bq.query(...)`, which creates a BigQuery
#    QUERY JOB — this requires `bigquery.jobUser` (job-creation), which
#    `bigquery.dataEditor` alone does NOT grant. Streaming-insert paths
#    (operational_data_writer.py's insert_rows) only need dataEditor and
#    already work under the P1 grant.
resource "google_project_iam_member" "uts_tier_sa_bq_job_user" {
  for_each = local.uts_tier_sa_non_storage_grantees
  project  = var.project_id
  role     = "roles/bigquery.jobUser"
  member   = "serviceAccount:${each.value}"
}

# 2. deployment-api's runtime writes to 2 GCS buckets that are NOT part of the
#    Group A/B -prd- prefix set uts-prd-sa's conditional storage.objectAdmin
#    covers: `unified-deployment-state-{project}` (deployment lock/state —
#    deployment_api/workers/auto_sync.py + deployment_processor.py +
#    services/sync_service.py) and `deployment-scripts-{project}` (VM
#    heartbeat/signal writes — deployment_api/routes/vm_admin.py, via UTL's
#    DEFAULT_BUCKET). Both are single, non-tiered buckets shared across envs
#    (no -prd-/-test- split), so they're granted here at the BUCKET level
#    (not a new project-wide condition) — scoped to exactly what deployment-api
#    needs, tighter than unified-trading-sa's current project-wide
#    storage.admin. `deployment-scripts` already has a live precedent for this
#    exact pattern (uts-prod-batch-sa holds the same 3 roles on it). Bucket
#    referenced by literal name (not google_storage_bucket.deployment_scripts[0].name)
#    deliberately, to keep this IAM-only grant's -target dependency graph clean
#    of that resource's own unrelated pending soft-delete-policy drift
#    (604800 -> 0, pre-existing, out of scope for this IAM-only change).
resource "google_storage_bucket_iam_member" "uts_prd_deployment_scripts_object_admin" {
  bucket = "deployment-scripts-${var.project_id}"
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.uts_prd.email}"
}

# uts-test-sa mirror of the uts-prd-sa grant above — LIVE-APPLIED 2026-08-01 (ad-hoc
# `gcloud storage buckets add-iam-policy-binding`, per
# issues/pipeline_e2e_check_missing_env_flag_test_bucket_403_2026_08_01.md) but never
# terraform-declared until now (P2.2d2c, bucket_iam_write_protection_per_tier_2026_06_09.md):
# a -test--tier VM (uts-test-sa, e.g. any launcher run with --env staging, or a
# pipeline_e2e_check.py --test-run leg) needs the SAME deployment-scripts write access as
# uts-prd-sa for its heartbeat/run.log/LAUNCH_PARAMS.json/EXIT_STATUS observability writes —
# the bucket lacks Uniform Bucket-Level Access, so this is unconditional like its prd sibling
# (an IAM Condition scoped to vm-logs/vm-heartbeat/deployments prefixes was attempted live
# and rejected with a 412 for exactly that reason, per the cited issue doc). Adding the
# declaration here brings config back in sync with already-live state; a `tofu plan` after
# this commit should show 0 changes for this resource (import if the live binding predates
# this declaration and tofu reports it as a new create).
resource "google_storage_bucket_iam_member" "uts_test_deployment_scripts_object_admin" {
  bucket = "deployment-scripts-${var.project_id}"
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.uts_test.email}"
}

# unified-deployment-state-{project} predates Terraform and was never adopted
# (no google_storage_bucket resource manages it — confirmed via `gcloud storage
# buckets list`; tracked as its own IaC-adoption gap, out of scope here). This
# grants IAM on it by literal name without taking ownership of the bucket
# resource itself.
resource "google_storage_bucket_iam_member" "uts_prd_deployment_state_object_admin" {
  bucket = "unified-deployment-state-${var.project_id}"
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.uts_prd.email}"
}
