# Deployment Service — GitHub Token for Cloud Build

> **Canonical SSOT:** [cicd-setup](../../unified-trading-pm/codex/05-infrastructure/cicd-setup.md). This file carries
> only deployment-service-specific details. The cross-cutting CI/CD setup and secret-handling contract lives in the
> codex SSOT above — **do not duplicate it here**; if this file disagrees with codex, codex wins.

## deployment-service-specific token notes

**Preferred: test-in-image (no token needed).** Services build from `unified-trading-library:latest` (Artifact
Registry), run quality gates inside the built image, and push only if tests pass — no git clone, no token. All current
MVP services use this pattern.

**Legacy git-clone path (token required).** Where a build still clones private repos:

- GCP Secret Manager secret `github-token` (`projects/{project}/secrets/github-token`), a fine-grained read-only PAT
  scoped to `unified-trading-library` + `deployment-service` (Contents: Read, Metadata: Read).
- Cloud Build SA needs `roles/secretmanager.secretAccessor` on the secret.
- Fetch the token at build runtime (never a `--build-arg`); use Docker BuildKit `--mount=type=secret` so it never lands
  in an image layer; keep `options.logging: CLOUD_LOGGING_ONLY`.
- Pin clones to a verified git tag / commit SHA (not a moving branch) so the service builds the ref that passed QG.

Rotate: add a new secret version (`echo -n <TOKEN> | gcloud secrets versions add github-token --data-file=-`), revoke
the old PAT, re-run a build to confirm.
