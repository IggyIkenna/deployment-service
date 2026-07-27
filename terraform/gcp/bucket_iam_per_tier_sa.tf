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
}

resource "google_project_iam_member" "uts_prd_objectadmin_group_a" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.uts_prd.email}"

  condition {
    title       = "group-a-prd-tier-only"
    description = "uts-prd-sa writes ONLY Group A -prd- buckets (bucket_iam_write_protection_per_tier_2026_06_09.md P1.2)."
    expression = join(" || ", [
      for prefix in local.group_a_bucket_prefixes :
      "(resource.name.startsWith(\"projects/_/buckets/${prefix}\") && resource.name.contains(\"-prd-\"))"
    ])
  }
}

resource "google_project_iam_member" "uts_test_objectadmin_group_a" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.uts_test.email}"

  condition {
    title       = "group-a-test-tier-only"
    description = "uts-test-sa writes ONLY Group A -test- buckets (ephemeral CI/E2E; bucket_iam_write_protection_per_tier_2026_06_09.md P1.2, test-tier slice)."
    expression = join(" || ", [
      for prefix in local.group_a_bucket_prefixes :
      "(resource.name.startsWith(\"projects/_/buckets/${prefix}\") && resource.name.contains(\"-test-\"))"
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
