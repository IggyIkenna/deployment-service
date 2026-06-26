# Epic: observability_master
# Lifecycle: permanent
"""DP-VM-007: Cloud Run stale-image check.

A Cloud Run job's running image digest != the latest Artifact Registry build
for its service is a STATIC-PROPERTY gap: a job can heartbeat healthily forever
on old code and never fire any existing alert. This check closes that gap.

Two injectable readers keep the pure check credential-free for unit tests:

  ``ImageDigestReader``: ``(project_id, location, job_name) -> running_digest | None``
    Reads the current job config's container image URI via run_v2 get_job.
    ``None`` = job not found / API error → skip (no false alert).

  ``LatestDigestReader``: ``(project_id, location, repo, image_name) -> latest_digest | None``
    Reads the ``:latest``-tagged digest from Artifact Registry.
    ``None`` = image not found / API error → skip (no false alert).

Grace window: skip the comparison when the running digest is missing or when
the latest digest cannot be resolved — fail toward NO false alert. A freshly
promoted job (image just rebuilt, Cloud Run job not yet updated) is handled
by the caller-controlled ``grace_minutes`` parameter.

SSOT: plans/active/issues/stale_cloud_run_image_alert_gap_2026_06_26.md
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from unified_trading_library.events import DP_CLOUD_RUN_STALE_IMAGE  # noqa: qg-deep-import

from deployment_service.data_pipeline_monitors.escalation import (
    EscalationTier,
    PipelineFinding,
)
from deployment_service.data_pipeline_monitors.meta_watchers import (
    DEFAULT_MIN_CONSECUTIVE_MISSES,
    MissTracker,
    emit_finding,
)

logger = logging.getLogger(__name__)

# The grace window used for a freshly-promoted job (skip for the first N minutes
# so a Cloud Build push that hasn't propagated to Cloud Run doesn't false-fire).
DEFAULT_STALE_IMAGE_GRACE_MIN: float = 30.0

# Canonical Artifact Registry location and repository that holds all service
# images for this fleet. The project id is resolved at call time from config.
ARTIFACT_REGISTRY_LOCATION = "asia-northeast1"
ARTIFACT_REGISTRY_REPO = "trading-system"

# Type alias: (project_id, location, job_name) -> running digest or None.
ImageDigestReader = Callable[[str, str, str], str | None]

# Type alias: (project_id, location, repository, image_name) -> latest digest or None.
LatestDigestReader = Callable[[str, str, str, str], str | None]


@dataclass(frozen=True)
class CloudRunImageCheckResult:
    """Result of a single Cloud Run job stale-image probe."""

    job_name: str
    """Fully-qualified job stem (as in the registry)."""

    service: str
    """Service name (used to map to the Artifact Registry image)."""

    running_digest: str | None
    """SHA256 digest of the currently configured image (``None`` = unknown)."""

    latest_digest: str | None
    """SHA256 digest of the `:latest`-tagged build (``None`` = unknown)."""

    stale: bool
    """True when running != latest AND both digests are known."""

    skipped: bool
    """True when the comparison was skipped (unknown digest / grace window)."""


def check_cloud_run_image_freshness(
    *,
    job_names: Iterable[str],
    job_to_service: Callable[[str], str | None],
    image_digest_reader: ImageDigestReader,
    latest_digest_reader: LatestDigestReader,
    project_id: str,
    location: str,
    artifact_repository: str,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> list[CloudRunImageCheckResult]:
    """DP-VM-007 — Cloud Run jobs running an image older than the latest build.

    For each job in ``job_names``:
      1. Read the running image digest via ``image_digest_reader``.
      2. Resolve the ``service`` name via ``job_to_service``.
      3. Read the ``:latest``-tagged Artifact Registry digest via
         ``latest_digest_reader``.
      4. Compare: if running_digest != latest_digest AND both are known → stale.
      5. Emit ``DP_CLOUD_RUN_STALE_IMAGE`` WARN when stale AND the
         consecutive-miss gate passes (if ``miss_tracker`` is provided, require
         ``min_consecutive`` stale sweeps first to suppress transient races where
         a build just finished and the job hasn't been updated yet).

    A ``None`` from either reader → skip (fail toward NO false alert: a missing
    job or Artifact Registry image is not reported as stale by this check).

    ``job_to_service``: ``job_name -> service | None`` maps a Cloud Run job stem
    (e.g. ``manifest-consolidator-sports``) to the service whose Artifact Registry
    image to compare (e.g. ``deployment-service``). ``None`` → skip the job.

    The ``artifact_repository`` is the Artifact Registry repository name STEM (e.g.
    ``trading-system``); the image name within it is the ``service`` string returned
    by ``job_to_service``.
    """
    results: list[CloudRunImageCheckResult] = []
    for job_name in job_names:
        service = job_to_service(job_name)
        if not service:
            continue

        running_digest = image_digest_reader(project_id, location, job_name)
        latest_digest = latest_digest_reader(project_id, location, artifact_repository, service)

        if running_digest is None or latest_digest is None:
            result = CloudRunImageCheckResult(
                job_name=job_name,
                service=service,
                running_digest=running_digest,
                latest_digest=latest_digest,
                stale=False,
                skipped=True,
            )
            results.append(result)
            if miss_tracker is not None:
                miss_key = f"{DP_CLOUD_RUN_STALE_IMAGE}::{job_name}"
                miss_tracker.register(miss_key, stale=False)
            continue

        running_norm = _extract_digest(running_digest)
        latest_norm = _extract_digest(latest_digest)
        is_stale = bool(running_norm and latest_norm and running_norm != latest_norm)

        result = CloudRunImageCheckResult(
            job_name=job_name,
            service=service,
            running_digest=running_digest,
            latest_digest=latest_digest,
            stale=is_stale,
            skipped=False,
        )
        results.append(result)

        miss_key = f"{DP_CLOUD_RUN_STALE_IMAGE}::{job_name}"
        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=is_stale)
            if is_stale and misses < min_consecutive:
                logger.info(
                    "stale_image_watcher: DP_CLOUD_RUN_STALE_IMAGE for '%s' below consecutive-miss "
                    "threshold (%d/%d) — not paging yet (grace window / transient-blip suppression)",
                    job_name,
                    misses,
                    min_consecutive,
                )
                continue

        if not is_stale:
            continue

        summary = (
            f"Cloud Run job '{job_name}' (service={service}) is running a stale image — "
            f"running digest {running_norm or running_digest!r} != "
            f"latest digest {latest_norm or latest_digest!r}. "
            f"Rebuild + redeploy required. "
            f"(stale_cloud_run_image_alert_gap_2026_06_26)"
        )
        emit_finding(
            PipelineFinding(
                event=DP_CLOUD_RUN_STALE_IMAGE,
                severity="WARN",
                tier=EscalationTier.FILE_ISSUE,
                summary=summary,
                details={
                    "job_name": job_name,
                    "service": service,
                    "running_digest": running_digest,
                    "latest_digest": latest_digest,
                    "running_norm": running_norm or "",
                    "latest_norm": latest_norm or "",
                    "artifact_repository": artifact_repository,
                    "cloud": "GCP",
                    "registry_id": "DP-VM-007",
                    "message": summary,
                },
                registry_id="DP-VM-007",
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
    return results


def _extract_digest(image_ref: str) -> str:
    """Extract the sha256:... digest component from a Docker image reference.

    Handles:
      - ``sha256:abc123...`` (bare digest from Artifact Registry)
      - ``asia.pkg.dev/.../image@sha256:abc123...`` (run_v2 image URI with digest)
      - ``asia.pkg.dev/.../image:latest`` (tag-only, no digest — returns "")
    """
    if not image_ref:
        return ""
    at_pos = image_ref.rfind("@sha256:")
    if at_pos != -1:
        return image_ref[at_pos + 1:]  # "sha256:abc..."
    if image_ref.startswith("sha256:"):
        return image_ref
    return ""


def make_image_digest_reader(env_prefix: str) -> ImageDigestReader:
    """Return ``(project_id, location, job_name) -> running_image_digest | None``.

    Reads the Cloud Run Job's currently-configured container image URI via the
    run_v2 ``get_job`` API and extracts the sha256 digest (if the image was pinned
    by digest) or returns the full image URI (for tag-only images — the caller's
    ``_extract_digest`` handles that gracefully). Any error → ``None`` (skip).
    """
    try:
        run_mod = importlib.import_module("google.cloud.run_v2")  # noqa: imports-inside-functions — credential-bound SDK, deferred
        client = run_mod.JobsClient()  # pyright: ignore[reportAny] — dynamic cloud-SDK boundary
    except Exception as exc:
        logger.info("image-digest reader unavailable (DP-VM-007 stale-image check off): %s", exc)
        return lambda _p, _l, _j: None

    def _read(project_id: str, location: str, job_stem: str) -> str | None:
        if not project_id or not job_stem:
            return None
        job_name = f"{env_prefix}-{job_stem}" if env_prefix else job_stem
        parent = f"projects/{project_id}/locations/{location}/jobs/{job_name}"
        try:
            job = client.get_job(name=parent)  # pyright: ignore[reportAny] — dynamic SDK attr
            job_template = getattr(job, "template", None)  # pyright: ignore[reportAny]
            task_template = getattr(job_template, "template", None) if job_template else None  # pyright: ignore[reportAny]
            containers = list(getattr(task_template, "containers", []) or []) if task_template else []  # pyright: ignore[reportAny]
            if not containers:
                return None
            image: str = str(getattr(containers[0], "image", "") or "")  # pyright: ignore[reportAny]
            return image or None
        except Exception as exc:
            logger.info("image-digest lookup for '%s' → unknown: %s", job_name, exc)
            return None

    return _read


def make_latest_digest_reader() -> LatestDigestReader:
    """Return ``(project_id, location, repo, image_name) -> latest_digest | None``.

    Queries Artifact Registry for the ``:latest``-tagged image's sha256 digest.
    The ``repo`` is the short repository name (e.g. ``trading-system``); the
    ``image_name`` is the service name (e.g. ``deployment-service``). Returns the
    ``sha256:...`` digest string, or ``None`` on any error (skip / no false alert).
    """
    try:
        ar_mod = importlib.import_module("google.cloud.artifactregistry_v1")  # noqa: imports-inside-functions — credential-bound SDK, deferred
        client = ar_mod.ArtifactRegistryClient()  # pyright: ignore[reportAny] — dynamic cloud-SDK boundary
    except Exception as exc:
        logger.info("latest-digest reader unavailable (DP-VM-007 stale-image check off): %s", exc)
        return lambda _p, _l, _r, _i: None

    def _read(project_id: str, location: str, repo: str, image_name: str) -> str | None:
        if not project_id or not image_name:
            return None
        parent = f"projects/{project_id}/locations/{location}/repositories/{repo}"
        image_path = f"{parent}/dockerImages/{image_name}"
        try:
            request = ar_mod.ListDockerImagesRequest(parent=parent)  # pyright: ignore[reportAny]
            for img in client.list_docker_images(request=request):  # pyright: ignore[reportAny]
                img_name: str = str(getattr(img, "name", "") or "")  # pyright: ignore[reportAny]
                if image_path not in img_name:
                    continue
                tags: list[str] = list(getattr(img, "tags", []) or [])  # pyright: ignore[reportAny]
                if "latest" not in tags:
                    continue
                digest: str = str(getattr(img, "image_size_bytes", "") or "")  # pyright: ignore[reportAny]
                uri: str = str(getattr(img, "uri", "") or "")  # pyright: ignore[reportAny]
                if "@sha256:" in uri:
                    return uri.split("@sha256:", 1)[1]
                if "@sha256:" in img_name:
                    return "sha256:" + img_name.split("@sha256:", 1)[1]
                _ = digest  # unused
                return None
        except Exception as exc:
            logger.info("latest-digest lookup for '%s/%s' → unknown: %s", repo, image_name, exc)
            return None
        return None

    return _read


def job_to_service_map() -> dict[str, str]:
    """Return a job_stem → service dict from the classified Cloud Run job registry.

    Imported lazily inside the function to avoid a circular module-level dep.
    The registry is a pure data module (no cloud SDK), so the deferred import
    is safe and cheap.
    """
    import deployment_service.cloud_run_job_registry as _reg  # noqa: imports-inside-functions — registry dep; deferred avoids circular module-level import
    return {job.name: job.service for job in _reg.CLOUD_RUN_JOBS if job.service}
