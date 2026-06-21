# Epic: citadel_paper_batch_live_reconciliation_2026_06_19
# Lifecycle: permanent
# ──────────────────────────────────────────────────────────────────────────────
# Cloud Run custom domain mappings (stable URLs for operator-facing UIs).
#
# SSOT: plans/active/citadel_paper_batch_live_reconciliation_2026_06_19.md (P11.7)
# ──────────────────────────────────────────────────────────────────────────────
#
# DNS RECORD REQUIRED at odum-research.com registrar (set by operator):
#
#   Name:    portal
#   Type:    CNAME
#   Value:   ghs.googlehosted.com.
#
# Certificate provisioning begins once the CNAME resolves.
# To verify: gcloud beta run domain-mappings describe --domain=portal.odum-research.com
#            --project=central-element-323112 --region=asia-northeast1
# ──────────────────────────────────────────────────────────────────────────────

resource "google_cloud_run_domain_mapping" "odum_portal" {
  # Domain mapping: portal.odum-research.com → odum-portal Cloud Run service.
  # Created imperatively 2026-06-21 (P11.7); this block captures it in IaC.
  name     = "portal.odum-research.com"
  location = var.region
  project  = var.project_id

  metadata {
    namespace = var.project_id
    labels = merge(local.common_labels, {
      "purpose" = "paper-ui-stable-url"
    })
  }

  spec {
    route_name       = "odum-portal"
    certificate_mode = "AUTOMATIC"
    force_override   = false
  }

  lifecycle {
    # The domain mapping is created externally (gcloud CLI) and imported;
    # prevent tofu from deleting and re-creating on annotation drift.
    ignore_changes = [metadata[0].annotations]
  }
}

# ──────────────────────────────────────────────────────────────────────────────
# Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "odum_portal_domain" {
  description = "Custom domain mapped to odum-portal Cloud Run service. Requires CNAME portal → ghs.googlehosted.com. at registrar."
  value       = google_cloud_run_domain_mapping.odum_portal.name
}
