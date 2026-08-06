#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Cloud Run traffic-pin drift check — detects when a Cloud Run service's live
traffic is pinned to a stale named revision while a newer Ready revision exists.

Compares ``status.traffic`` against ``status.latestReadyRevisionName`` for each
target service. A service is DRIFTED when ALL of these hold:
  1. Traffic carries an explicit revision-name pin (not ``latestRevision: true``).
  2. The pin target is NOT the latest-ready revision.
  3. The latest-ready revision is older than ``--grace-minutes`` (default 30) —
     suppresses false alerts on a just-landed deploy whose traffic hasn't
     auto-promoted yet (the normal ``gcloud run deploy`` default is instant).

Uses ``gcloud run services describe`` via subprocess (no Cloud Run Admin SDK dep)
so it runs from any GHA runner with ``google-github-actions/auth@v3``
authentication. Mirrors the ``slot_drift_check.py`` cadence + structure.

Exit 0 = all services OK (or skipped); exit 1 = ≥1 service DRIFTED.

Usage:
    cloud_run_traffic_drift_check.py --project <id> --region <region>     [--services svc1,svc2,...] [--grace-minutes 30]

SSOT: plans/active/issues/cloud_run_traffic_pin_silent_freeze_alert_wiring_2026_08_05.md
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

# Default service set: auto-deployed Cloud Run services with ``-main-deploy``
# triggers. MUST stay in sync with the actual Cloud Run service names deployed
# by each repo's ``cloudbuild.yaml`` / ``*-main-deploy`` trigger.
DEFAULT_SERVICES: tuple[str, ...] = (
    "uts-shared-deployment-api",
    "deployment-ui",
    "unified-trading-system-ui-uat",
)


@dataclass(frozen=True)
class ServiceTraffic:
    """Parsed traffic + revision state for one Cloud Run service."""

    service: str
    latest_ready_revision: str | None
    """``status.latestReadyRevisionName`` — the most recent Ready revision."""

    traffic_revision: str | None
    """The revision-name that holds 100% traffic, when the pin is explicit."""

    tracks_latest: bool
    """True when traffic is ``latestRevision: true`` (auto-promote, no pin)."""

    error: str | None
    """gcloud / parse error message (``None`` = healthy read)."""

    @property
    def traffic_pinned(self) -> bool:
        """Traffic carries an explicit by-name pin (not tracking latestRevision)."""
        return not self.tracks_latest and self.traffic_revision is not None

    @property
    def drifted(self) -> bool:
        """Traffic pinned to a revision that is NOT the latest-ready revision."""
        return (
            self.traffic_pinned
            and self.latest_ready_revision is not None
            and self.traffic_revision is not None
            and self.traffic_revision != self.latest_ready_revision
        )


def _gcloud(*args: str) -> tuple[int, str]:
    """Run ``gcloud``, return (returncode, stdout)."""
    p = subprocess.run(["gcloud", *args], capture_output=True, text=True)
    return p.returncode, p.stdout.strip()


def _describe_service(project: str, region: str, service: str) -> ServiceTraffic:
    """Query one Cloud Run service via ``gcloud run services describe``.

    Returns a ``ServiceTraffic`` with the parsed traffic + revision state.
    A gcloud error is captured in ``error`` (never raised).
    """
    rc, stdout = _gcloud(
        "run",
        "services",
        "describe",
        service,
        "--region",
        region,
        "--project",
        project,
        "--format",
        "json",
    )
    if rc != 0:
        return ServiceTraffic(
            service=service,
            latest_ready_revision=None,
            traffic_revision=None,
            tracks_latest=False,
            error=f"gcloud exit {rc}: {stdout[:300]}",
        )

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        return ServiceTraffic(
            service=service,
            latest_ready_revision=None,
            traffic_revision=None,
            tracks_latest=False,
            error=f"JSON parse error: {exc}",
        )

    status = data.get("status", {}) if isinstance(data, dict) else {}
    latest_ready: str | None = status.get("latestReadyRevisionName")

    # Traffic is a list of {percent, revisionName?, latestRevision?} dicts.
    traffic_list: list[dict[str, object]] = data.get("spec", {}).get("traffic", []) if isinstance(data, dict) else []
    traffic_list = [t for t in traffic_list if isinstance(t, dict)]

    # Find the 100% traffic target (or the highest-percent entry).
    pinned_revision: str | None = None
    tracks_latest = False
    if traffic_list:
        max_pct = max(
            (int(cast("int | float", t.get("percent", 0))) for t in traffic_list),
            default=0,
        )
        for t in traffic_list:
            pct = int(cast("int | float", t.get("percent", 0)))
            if pct == max_pct:
                if t.get("latestRevision") is True:
                    tracks_latest = True
                else:
                    rev = t.get("revisionName")
                    if isinstance(rev, str) and rev:
                        pinned_revision = rev

    return ServiceTraffic(
        service=service,
        latest_ready_revision=latest_ready,
        traffic_revision=pinned_revision,
        tracks_latest=tracks_latest,
        error=None,
    )


def _revision_created(project: str, region: str, revision: str) -> datetime | None:
    """Get the creation timestamp of a Cloud Run revision via ``gcloud run revisions describe``.

    Returns a UTC datetime or ``None`` on any error (gcloud failure, parse error).
    """
    rc, stdout = _gcloud(
        "run",
        "revisions",
        "describe",
        revision,
        "--region",
        region,
        "--project",
        project,
        "--format",
        "json",
    )
    if rc != 0:
        return None
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    ts_str = data.get("metadata", {}).get("creationTimestamp") if isinstance(data, dict) else None
    if not isinstance(ts_str, str) or not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Cloud Run traffic-pin drift check")
    ap.add_argument("--project", required=True, help="GCP project ID")
    ap.add_argument("--region", required=True, help="GCP region (e.g. asia-northeast1)")
    ap.add_argument(
        "--services",
        default=",".join(DEFAULT_SERVICES),
        help="Comma-separated Cloud Run service names to check (default: %(default)s)",
    )
    ap.add_argument(
        "--grace-minutes",
        type=float,
        default=30.0,
        help="Skip drift alert if the latest-ready revision is newer than this many minutes (default: %(default)s)",
    )
    ap.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Emit a JSON array of per-service results to stdout (useful for programmatic consumers)",
    )
    args = ap.parse_args()

    project: str = cast(str, args.project)
    region: str = cast(str, args.region)
    service_names: list[str] = [s.strip() for s in cast(str, args.services).split(",") if s.strip()]
    grace: float = cast(float, args.grace_minutes)

    if not service_names:
        print("No services to check (empty --services)")
        return 0

    results: list[ServiceTraffic] = []
    drifted: list[str] = []
    errors: list[str] = []

    for svc in service_names:
        st = _describe_service(project, region, svc)
        results.append(st)

        if st.error:
            errors.append(f"  {svc}: ERROR — {st.error}")
            continue

        if not st.traffic_pinned:
            # Tracking latestRevision — healthy, nothing to check.
            continue

        if not st.drifted:
            # Pinned to the latest revision — intentional (e.g. canary rollback).
            continue

        # Traffic pinned to a stale revision.  Apply the grace window:
        # if the latest-ready revision is brand-new, a deploy may still be
        # settling — suppress to avoid false-alerting on the normal auto-promote
        # window (gcloud run deploy default is effectively instant, but a
        # canary/deploy-script could have a small delay).
        if st.latest_ready_revision:
            created = _revision_created(project, region, st.latest_ready_revision)
            if created is not None:
                age = datetime.now(UTC) - created
                if age < timedelta(minutes=grace):
                    continue  # within grace window — skip

        drifted.append(f"  {svc}: PINNED to {st.traffic_revision}, latest-ready is {st.latest_ready_revision}")

    # ── Output ──────────────────────────────────────────────────────────
    if args.json_output:
        json_results = [
            {
                "service": r.service,
                "latest_ready_revision": r.latest_ready_revision,
                "traffic_revision": r.traffic_revision,
                "tracks_latest": r.tracks_latest,
                "traffic_pinned": r.traffic_pinned,
                "drifted": r.drifted,
                "error": r.error,
            }
            for r in results
        ]
        print(json.dumps(json_results, indent=2))

    if errors:
        print("⚠️  Errors (skipped — no alert):")
        for e in errors:
            print(e)

    if drifted:
        print(f"❌ {len(drifted)} Cloud Run service(s) with traffic pinned to a stale revision:")
        for d in drifted:
            print(d)
        print(
            f"   Restore: gcloud run services update-traffic <service> "
            f"--to-latest --region={region} --project={project}"
        )
        return 1

    print("✅ all Cloud Run service traffic is current (latestRevision or pinned-to-latest)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
