# Deployment-registry DynamoDB backend — AWS analogue of the GCP Firestore
# `deployments` collection (unified_trading_library/cloud_interface
# `DeploymentRegistryStore` Protocol).
#
# PROVISIONING ONLY — NOT ACTIVE. Cloud selection stays GCP -> Firestore by
# default (mirrors resolve_bucket_name's GCS/S3 selection); this table exists
# so the AWS cutover is a backend swap, not a rewrite. Activation = flip the
# active cloud so the store factory selects the DynamoDB impl — see
# "AWS backend activation" in
# unified-trading-pm/codex/05-infrastructure/deployment-observability.md.
#
# Shape: partition key deployment_id (one item per deployment_id, mirrors the
# Firestore doc-per-deployment model); GSI on status for query_by_status
# (mirrors the Firestore `.where("status", "==", ...)` query).
#
# SSOT: unified-trading-pm/plans/active/deployment_registry_firestore_p4_dynamodb_2026_07_14.md

resource "aws_dynamodb_table" "deployments" {
  name         = "unified-trading-${var.environment}-deployments"
  billing_mode = var.deployment_registry_dynamodb_billing_mode
  hash_key     = "deployment_id"

  read_capacity  = var.deployment_registry_dynamodb_billing_mode == "PROVISIONED" ? 25 : null
  write_capacity = var.deployment_registry_dynamodb_billing_mode == "PROVISIONED" ? 25 : null

  attribute {
    name = "deployment_id"
    type = "S"
  }

  attribute {
    name = "status"
    type = "S"
  }

  global_secondary_index {
    name            = "status-index"
    hash_key        = "status"
    projection_type = "ALL"

    read_capacity  = var.deployment_registry_dynamodb_billing_mode == "PROVISIONED" ? 25 : null
    write_capacity = var.deployment_registry_dynamodb_billing_mode == "PROVISIONED" ? 25 : null
  }

  server_side_encryption {
    enabled = true
  }

  tags = {
    Name    = "unified-trading-${var.environment}-deployments"
    Purpose = "deployment-registry"
  }
}
