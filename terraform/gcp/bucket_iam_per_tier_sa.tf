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
  group_a_bucket_prefixes = [
    "market-data-tick-",
    "instruments-store-",
    "features-calendar-",
  ]

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
