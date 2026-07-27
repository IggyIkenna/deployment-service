# Per-tier service accounts — bucket-isolation-model §8 write-protection (P1.1).
#
# plans/active/bucket_iam_write_protection_per_tier_2026_06_09.md: replaces the
# project-wide unified-trading-sa (roles/storage.objectAdmin over every bucket,
# defined above in main.tf) with per-tier identities so a dev/staging credential
# can be IAM-denied a prod-bucket write, not just code-level-guarded. This file
# defines the 4 SAs only (P1.1) — no IAM role bindings yet. Per-suffix bindings
# (dev SA -> objectAdmin on *-dev-*, etc.) are P1.2, deliberately NOT done here:
# the plan's own 2026-07-25 finding is that Group A's real buckets are two-tier
# (-test-/-prd- only, no -dev-/-stg- suffix), so which SAs bind to which Group A
# buckets needs a re-derivation pass before P1.2 is authored. Defining the SAs
# now is safe + reversible either way (an unbound SA grants zero access) and
# unblocks that follow-up without pre-empting it.
#
# uts-migration-sa is the sanctioned cross-tier write exception (operator
# 2026-06-09: "exceptions for migration scripts") — used only by
# *_service/scripts/migration_*.py once P1.2/P2 wire it in.

resource "google_service_account" "uts_dev" {
  account_id   = "uts-dev-sa"
  display_name = "UTS dev-tier service account"
  description  = "Write access to *-dev-* buckets only, read-only elsewhere (bucket-isolation-model.md §8)."
  project      = var.project_id
}

resource "google_service_account" "uts_stg" {
  account_id   = "uts-stg-sa"
  display_name = "UTS staging-tier service account"
  description  = "Write access to *-stg-* buckets only, read-only elsewhere (bucket-isolation-model.md §8)."
  project      = var.project_id
}

resource "google_service_account" "uts_prd" {
  account_id   = "uts-prd-sa"
  display_name = "UTS prod-tier service account"
  description  = "Write access to *-prd-* buckets only, read-only elsewhere (bucket-isolation-model.md §8)."
  project      = var.project_id
}

resource "google_service_account" "uts_migration" {
  account_id   = "uts-migration-sa"
  display_name = "UTS cross-tier migration service account"
  description  = "Sanctioned cross-tier write exception for migration scripts only (bucket-isolation-model.md §8)."
  project      = var.project_id
}
