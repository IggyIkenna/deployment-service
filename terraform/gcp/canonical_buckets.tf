# Canonical GCS buckets — derived directly from cloud-providers.yaml (SSOT)
#
# Operator ruling 2026-07-13 (terraform_bucket_estate_drift_resurrection_2026_07_13.md +
# bucket_estate_consolidation_to_sub100_2026_07_13.md Wave 0): terraform is
# DERIVED-FROM-YAML — one `for_each` bucket block generated from cloud-providers.yaml's
# canonical (prd+test) names, existing canonical buckets imported into it, hand-written
# blocks kept only for genuine infra buckets (deployment-scripts, terraform-state, events-
# adjacent one-offs, the audit-records retention-locked bucket, …). `terraform plan`
# becomes the drift detector going forward — a nonzero diff against this file means either
# the yaml SSOT or the live estate moved without the other.
#
# Lifecycle ruling 2026-07-13: STANDARD → COLDLINE at age 60 (operator verbatim
# "nearlcoldline nmove after 60d", read as straight COLDLINE@60d) — replaces the untracked,
# out-of-band STANDARD→COLDLINE@14d rule that existed on ~78 live buckets with no in-repo
# applier. This is now the ONE tracked place for canonical-bucket lifecycle.
#
# Derivation matches Wave-0-verified counts exactly (scratchpad/compute_canonical_set.py):
# 81 canonical names total — 11 state-mv (old hand resource -> this for_each, see the
# per-resource "REMOVED ... double-declare" comments in main.tf), 68 import (live bucket,
# previously untracked by any TF resource), 2 create (recon-prd / recon-test — the new
# `recon` kind added to cloud-providers.yaml 2026-07-13, not yet provisioned). The full
# state-surgery (rm / mv / import) is scratchpad/tf_state_surgery.sh — NOT auto-run.

locals {
  # yamldecode gives native HCL values (maps/strings) — no interpolation happens on the
  # `${VAR}` placeholder tokens inside the yaml's bucket-name templates, they stay literal
  # strings until the `replace()` substitution below.
  cloud_providers = yamldecode(file("${path.module}/../../configs/cloud-providers.yaml"))

  # Kinds intentionally EXCLUDED from this for_each (own hand-written resource or owned by
  # another in-flight plan):
  #   dex-pools, lst-rates, perp-funding — their `-prd` buckets are LIVE but pending
  #     deletion under defi_dedicated_bucket_shared_migration_2026_07_13.md; folding them
  #     into `canonical` now would fight that plan's upcoming `terraform state rm` +
  #     bucket delete.
  #   audit-records — keeps its dedicated hand-written `google_storage_bucket.audit_records`
  #     (Object Versioning + 7-year Retention Lock; a generic for_each block cannot express
  #     the locked retention_policy safely).
  #   manual-audit — same compliance class (discovered at first live plan 2026-07-13:
  #     manual-audit-prd carries a LOCKED retentionPeriod=220752000; the generic block would
  #     force a prevent_destroy-blocked replacement). TF-unmanaged for now; a dedicated
  #     hand-written block mirroring audit_records is the follow-up if TF management is wanted.
  canonical_excluded_kinds = toset(["dex-pools", "lst-rates", "perp-funding", "audit-records", "manual-audit"])

  # prd + test are the only provisioned tiers (dev/stg retired per the 2026-07-13 operator
  # ruling — bucket_estate_consolidation_to_sub100_2026_07_13.md Wave 1).
  canonical_tiers = ["prd", "test"]

  canonical_storage_kinds = {
    for kind, entry in local.cloud_providers["gcp"]["storage"] :
    kind => entry
    if !contains(local.canonical_excluded_kinds, kind)
  }

  # Flatten every kind's template(s) into one list(string): a flat-string kind (e.g.
  # `events`) contributes ONE template; a per-asset_group kind (e.g. `features-delta-one`)
  # contributes one template per asset_group. asset_group identity doesn't matter past this
  # point — only the substituted bucket name does.
  canonical_templates = flatten([
    for kind, entry in local.canonical_storage_kinds :
    can(tostring(entry)) ? [tostring(entry)] : values(entry)
  ])

  # Substitute both placeholder tokens for every (template × tier) pair, then `toset()`
  # dedupes: an env-LESS template (no `${DEPLOYMENT_ENV_SHORT}` token — e.g. the Group-B
  # features/ml/strategy/execution kinds, per the 2026-05 env-split-rollback note in the
  # yaml) substitutes to the IDENTICAL string for both tiers and collapses to one bucket;
  # an env-tiered template produces two distinct names (…-prd-…, …-test-…).
  canonical_bucket_names = toset(flatten([
    for template in local.canonical_templates : [
      for tier in local.canonical_tiers :
      replace(
        replace(template, "$${GCP_PROJECT_ID}", var.project_id),
        "$${DEPLOYMENT_ENV_SHORT}", tier
      )
    ]
  ]))
}

resource "google_storage_bucket" "canonical" {
  for_each = local.canonical_bucket_names

  name     = each.value
  project  = var.project_id
  location = var.region

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    condition {
      age = 60
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  labels = { "managed-by" = "terraform-canonical" }

  # This is the canonical, yaml-derived estate — a stray `terraform destroy`/`taint`
  # must never take one of these buckets out from under a live resolver path.
  lifecycle {
    prevent_destroy = true
  }
}
