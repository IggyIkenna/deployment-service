# Outputs for AWS CodeBuild configuration

output "codebuild_role_arn" {
  description = "CodeBuild service role ARN"
  value       = aws_iam_role.codebuild_role.arn
}

output "codebuild_projects" {
  description = "Map of service names to CodeBuild project ARNs"
  value = {
    for name, project in aws_codebuild_project.services :
    name => project.arn
  }
}

output "codebuild_project_names" {
  description = "List of CodeBuild project names"
  value       = [for name, project in aws_codebuild_project.services : project.name]
}

output "codeartifact_repository_arn" {
  description = "CodeArtifact repository the unified-api-contracts wheel publishes to"
  value       = aws_codeartifact_repository.libraries.arn
}

output "dispatch_instructions" {
  description = "How builds in this module are triggered"
  value       = <<-EOT
    ============================================================
    AWS CodeBuild — dispatch model
    ============================================================

    Builds are started by the GitHub Actions router, NOT by PUSH webhooks:

      unified-trading-pm/.github/workflows/cloud-build-router-aws.yml
        -> aws codebuild start-build --project-name <repo>-<env>

    The router is gated on the PM Actions variable AWS_BUILDS_ENABLED
    (unset / anything but 'true' == disabled). AWS image builds were
    switched OFF on 2026-07-03 (GCP Cloud Build is the production path);
    all 18 CodeBuild webhooks were deleted the same day. Do NOT re-add
    PUSH webhooks -- see /codex/05-infrastructure/dual-cloud-image-builds.md.

    Manual dispatch:
      aws codebuild start-build --project-name instruments-service

    Secrets the fleet reads (the only three the IAM policy grants):
      GH_PAT, github-pat, unified-trading/github-actions-sa-key

    ============================================================
  EOT
}
