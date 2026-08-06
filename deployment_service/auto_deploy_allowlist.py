# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Explicit allowlist for the `service-deployed` repository_dispatch auto-deploy listener.

Every service's `cloudbuild.yaml` / `buildspec.aws.yaml` fires a `service-deployed`
`repository_dispatch` to this repo once its image is built + pushed — but until
`.github/workflows/service-deployed-listener.yml` was added (2026-08-06), NOTHING
listened, so a green build never became a live Cloud Run redeploy (a
`repository_dispatch` POST returns 204 whether or not anyone subscribes — "delivered"
and "nobody's listening" are indistinguishable at runtime; caught statically via
`unified-trading-pm/scripts/quality_gates/check_dispatch_listeners.py`).

**This is a DEFAULT-DENY allowlist, not "deploy anything that dispatches" (operator
ruling 2026-08-06)** — we are pre-live-cutover, "focusing on data and quality, not
live trading yet." A service NOT listed here is a silent, logged no-op when its
`service-deployed` dispatch fires — never an error, never a page. Only services
verified (live, 2026-08-06, `gcloud run services/jobs list/describe --project
<prod GCP project> --region asia-northeast1`) to be:

  1. a genuine long-lived Cloud Run **SERVICE** (not a Job — Jobs resolve their
     `:latest`-tagged image at EXECUTION time, so they self-refresh on their next
     scheduled run with no redeploy step needed at all; confirmed for every
     `uts-prod-manifest-consolidator-*` / `uts-prod-dp-*-watcher*` / `uts-prod-
     monitoring-deadman` job — all on `:latest`, all excluded on this basis alone);
  2. pinned to a specific image tag (not tracking `:latest`), so it genuinely goes
     stale without an explicit redeploy call; AND
  3. NOT already covered by its own dedicated `-main-deploy` Cloud Build trigger
     with an inline `gcloud run deploy` step (deployment-api's `cloudbuild.yaml`
     `_DEPLOY=true` gate already redeploys `uts-shared-deployment-api` +
     `uts-prod-data-status-rollup-svc` on every main build — adding it here would
     race a second, redundant deploy path against the first); AND
  4. NOT trading/execution-adjacent — `execution-service`, `strategy-service`,
     `trading-agent-service`, `batch-live-reconciliation-service`,
     `fund-administration-service`, `client-reporting-api` must NEVER auto-deploy
     under this mechanism regardless of build success, per the operator's explicit
     "not live trading yet" boundary — even if a future revision of this file adds
     other candidates, these six are a standing exclusion, not merely "currently
     unlisted."

Every future addition belongs on this file, not scattered `if service_name ==
"..."` checks. Adding a future service is a one-line addition to
`AUTO_DEPLOY_ALLOWLIST` below (plus, per the reasoning above, actually
re-verifying that service is a live pinned-tag long-lived Cloud Run Service
before adding it — don't pattern-match the name).

Full investigation + per-candidate verdicts: see the Progress Log in
`unified-trading-pm/plans/active/issues/post_cutover_silent_assumption_sweep_2026_07_23.md`
(the `service-deployed` slice of the F3 orphan-dispatch todo, line 583).
"""

from __future__ import annotations

from typing import Final

# Dispatch `service_name` (the repo/image name from the `service-deployed`
# client_payload) -> the live Cloud Run SERVICE name(s) to redeploy. One repo can
# map to multiple Cloud Run services (not currently the case here, but the shape
# supports it — e.g. a hypothetical future service running under both a primary
# and a dedicated secondary Cloud Run instance, mirroring deployment-api's own
# uts-shared-deployment-api + uts-prod-data-status-rollup-svc split, which is
# EXCLUDED here only because it already has its own working deploy mechanism).
AUTO_DEPLOY_ALLOWLIST: Final[dict[str, tuple[str, ...]]] = {
    # alerting-service's image builds as `alerting-service:<tag>`, but the LIVE
    # Cloud Run service is `dp-alerting-subscriber` — the SPOF for every DP_* and
    # DEPLOYMENT_* alert (terraform/gcp/critical_service_uptime.tf). Confirmed
    # live 2026-08-06: pinned to a commit-sha tag (not `:latest`), min-scale=1
    # long-lived Service, no existing dedicated deploy trigger of its own — this
    # is the exact case that was stale for 9+ days (2026-07-28 revision vs a
    # 2026-08-04 successful build) before this listener existed.
    "alerting-service": ("dp-alerting-subscriber",),
}


def resolve_deploy_targets(service_name: str) -> tuple[str, ...]:
    """Return the Cloud Run service name(s) to redeploy for a dispatched repo.

    Returns an empty tuple for anything not explicitly allowlisted — the caller
    (`scripts/cicd/handle_service_deployed_dispatch.py`) treats that as a clean,
    logged no-op, never an error.
    """
    return AUTO_DEPLOY_ALLOWLIST.get(service_name, ())
