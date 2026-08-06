#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Handler for the `service-deployed` `repository_dispatch` — the listener side of
what was, until 2026-08-06, an orphaned dispatch (every service's `cloudbuild.yaml`
/ `buildspec.aws.yaml` `notify-deployment` step POSTs
`repos/IggyIkenna/deployment-service/dispatches` with
`{"event_type":"service-deployed","client_payload":{"service_name","version",
"image"}}` once a build succeeds — nobody in this repo listened, so a green build
never became a live Cloud Run redeploy; see
`unified-trading-pm/scripts/quality_gates/check_dispatch_listeners.py`).

Invoked by `.github/workflows/service-deployed-listener.yml` on every
`service-deployed` dispatch. Safety model (default-deny, two independent gates):

  1. ALLOWLIST (the real safety boundary) — `deployment_service.auto_deploy_allowlist`
     is a small, explicit, operator-approved map of `service_name -> Cloud Run
     service name(s)`. A `service_name` not on it is a clean, logged no-op — never
     an error, never a page. See that module's docstring for the full "why an
     allowlist, not deploy-everything" reasoning (operator ruling 2026-08-06).
  2. Version-shape sanity check (defense-in-depth, not the primary gate) — the
     dispatching trigger is main-only (`${repo}-build`, branch-pattern=^main$;
     `setup-cloud-build-triggers.sh`), so `version` should always be a clean
     `X.Y.Z` semver with no branch suffix. Reject anything else rather than
     assume the trigger topology never changes.

Reuses `deployment-api`'s existing `POST /api/deployments/{service}/deploy`
(`deployment_api/routes/builds.py::deploy_build`) — the same manual-deploy
endpoint the deployment-ui Deploy console calls — via its
`cloud_run_service_name` override (added alongside this listener) rather than
reimplementing `gcloud run deploy` here.

Usage::

    handle_service_deployed_dispatch.py --service-name X --version Y --image Z \\
        --api-base-url URL [--api-key KEY] [--dry-run]

Exit codes:
  0 — handled cleanly (a real deploy call that succeeded, OR a legitimate no-op:
      not allowlisted / non-prod-shaped version)
  1 — an allowlisted deploy call failed
  2 — argument error

SSOT: unified-trading-pm/plans/active/issues/post_cutover_silent_assumption_sweep_2026_07_23.md
(F3 orphan-dispatch todo, `service-deployed` slice).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from typing import Final

from deployment_service.auto_deploy_allowlist import resolve_deploy_targets

# Pure semver, no branch suffix — see module docstring gate 2.
_PROD_VERSION_RE: Final[re.Pattern[str]] = re.compile(r"^\d+\.\d+\.\d+$")

_DEPLOY_TIMEOUT_SECONDS: Final[float] = 90.0


def _deploy_one(
    *,
    api_base_url: str,
    api_key: str,
    image_service: str,
    cloud_run_service: str,
    version: str,
) -> tuple[bool, str]:
    """POST to deployment-api's existing manual-deploy endpoint. Never raises.

    `api_key` may be empty — omit the `X-API-Key` header rather than send a
    blank one. deployment-api's `uts-shared-deployment-api` currently runs
    with `DISABLE_AUTH=true` in prod (a separate flagged gap — see
    `deployment_service/auto_deploy_allowlist.py` module docstring), so an
    unauthenticated call succeeds today; this becomes a real credential the
    moment `DEPLOYMENT_API_KEY` is populated and that gap is closed, with no
    code change here.
    """
    url = f"{api_base_url.rstrip('/')}/api/deployments/{image_service}/deploy"
    payload = json.dumps(
        {
            "image_tag": version,
            "environment": "prod",
            "cloud_run_service_name": cloud_run_service,
        }
    ).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key
    req = urllib.request.Request(url, data=payload, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=_DEPLOY_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return True, f"HTTP {resp.status}: {body[:500]}"
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode('utf-8', errors='replace')[:500]}"
    except (urllib.error.URLError, TimeoutError) as e:
        return False, f"request error: {e!r}"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--service-name", required=True, help="`service_name` from the dispatch client_payload")
    ap.add_argument("--version", required=True, help="`version` from the dispatch client_payload")
    ap.add_argument(
        "--image",
        default="",
        help="`image` from the dispatch client_payload (logged only, not sent — deployment-api resolves the AR image itself from service+version)",
    )
    ap.add_argument("--api-base-url", required=True, help="deployment-api base URL")
    ap.add_argument(
        "--api-key",
        default="",
        help="deployment-api X-API-Key (optional — omitted header if empty; see _deploy_one docstring)",
    )
    ap.add_argument("--dry-run", action="store_true", help="resolve + log, but never call deploy")
    args = ap.parse_args(argv)

    service_name: str = args.service_name
    version: str = args.version

    targets = resolve_deploy_targets(service_name)
    if not targets:
        print(
            f"NO-OP: '{service_name}' is not in AUTO_DEPLOY_ALLOWLIST — logging and exiting cleanly "
            "(default-deny; see deployment_service/auto_deploy_allowlist.py for the full list + reasoning)."
        )
        return 0

    if not _PROD_VERSION_RE.match(version):
        print(
            f"NO-OP: version '{version}' does not look like a clean prod semver tag (X.Y.Z, no branch suffix) — "
            "defense-in-depth gate 2, refusing to guess rather than deploy a non-prod build."
        )
        return 0

    print(
        f"service-deployed: service={service_name} version={version} image={args.image or 'n/a'} -> targets={targets}"
    )

    if args.dry_run:
        print(f"DRY-RUN: would call deploy for {service_name}:{version} -> {targets}")
        return 0

    failures: list[str] = []
    for cloud_run_service in targets:
        ok, detail = _deploy_one(
            api_base_url=args.api_base_url,
            api_key=args.api_key,
            image_service=service_name,
            cloud_run_service=cloud_run_service,
            version=version,
        )
        status = "OK" if ok else "FAILED"
        print(f"  {status} deploy {service_name}:{version} -> {cloud_run_service}: {detail}")
        if not ok:
            failures.append(cloud_run_service)

    if failures:
        print(f"ERROR: deploy failed for {len(failures)} target(s): {failures}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
