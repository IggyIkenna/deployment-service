# SCHEMA_PROVENANCE_EXEMPT: Service-internal BoM resolution model — not a cross-repo contract.
"""
Deployment bill-of-materials (BoM) resolution.

Resolves the provenance triplet stamped on every ``DeploymentRegistryEntry``
at launch so "what code is deployed in prod right now" is a queryable BoM
(PM plan ``dependency_promotion_range_pins_and_major_bump_sit_2026_06_09``
Phase 6):

- ``image_digest`` — immutable ``sha256:...`` digest of the deployed image
- ``git_commit``   — service git commit baked at build time (``$SHORT_SHA``)
- ``dep_versions`` — installed internal dep versions (UTL / UAC) plus the
  unified-trading-library base-image digest when the build passed it through
  (``ARG BASE_IMAGE_DIGEST`` — the QG STEP 5.79 FROM-digest ratchet)

Module-level imports are stdlib-only so this file stays standalone-loadable
(mirrors the direct-load pattern in ``tests/unit/test_deployment_heartbeat_cli.py``,
which bypasses ``deployment_service/__init__``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from importlib import metadata as importlib_metadata
from typing import Protocol

logger = logging.getLogger(__name__)

# Internal distributions recorded in every BoM. Extend deliberately — each
# entry becomes a key in the registry row's ``dep_versions`` on GCS.
BOM_DISTRIBUTIONS: tuple[str, ...] = (
    "unified-trading-library",
    "unified-api-contracts",
)

# ``dep_versions`` key carrying the base-image digest passthrough (not a dist).
BASE_IMAGE_DIGEST_KEY = "base_image_digest"


class BomConfigLike(Protocol):
    """Subset of ``DeploymentConfig`` needed for BoM resolution (test-friendly)."""

    git_commit: str
    image_digest: str
    base_image_digest: str


@dataclass(frozen=True)
class DeploymentBom:
    """Resolved bill-of-materials for one deployment. Empty = honestly unknown."""

    image_digest: str = ""
    git_commit: str = ""
    dep_versions: dict[str, str] = field(default_factory=dict)


def extract_image_digest(image_ref: str) -> str:
    """Return the ``sha256:...`` digest component of an image ref, or ``""``.

    A digest is only present when the caller already pinned the image by
    digest (``repo/name@sha256:...``). Tag-only refs honestly yield ``""`` —
    no cloud-interface helper exists for Artifact Registry describe, so we
    never invent a digest from a mutable tag.
    """
    _, sep, digest = image_ref.partition("@")
    if sep and digest.startswith("sha256:"):
        return digest
    return ""


def _normalize_digest(value: str) -> str:
    """Accept a bare ``sha256:...`` digest OR a digest-pinned image ref."""
    if not value:
        return ""
    if value.startswith("sha256:"):
        return value
    return extract_image_digest(value)


def installed_dep_versions() -> dict[str, str]:
    """Installed versions of the internal deps baked into this runtime.

    ``importlib.metadata`` reads the actual installed dists — cheap and honest
    (reflects what is genuinely in the image/venv, not what a manifest claims).
    """
    versions: dict[str, str] = {}
    for dist in BOM_DISTRIBUTIONS:
        try:
            versions[dist] = importlib_metadata.version(dist)
        except importlib_metadata.PackageNotFoundError:
            # Honest absence — dist not installed in this runtime (e.g. a slim
            # tooling venv). Omit the key rather than fabricate a version.
            logger.warning("BoM: distribution %s not installed — omitted from dep_versions", dist)
    return versions


def resolve_deployment_bom(config: BomConfigLike | None = None) -> DeploymentBom:
    """Resolve the BoM for the current runtime.

    ``image_digest`` / ``git_commit`` / ``base_image_digest`` ride the
    ``DeploymentConfig`` env passthrough (``IMAGE_DIGEST`` / ``SHORT_SHA`` /
    ``GIT_COMMIT`` / ``BASE_IMAGE_DIGEST`` — stamped into the image or VM env
    at build/launch time). Empty string = honestly unknown on this backend;
    a digest is never guessed from a mutable tag.
    """
    if config is None:
        # Lazy import — keeps this module stdlib-only at import time so it
        # stays standalone-loadable in unit tests.
        from deployment_service.deployment_config import DeploymentConfig

        config = DeploymentConfig()

    dep_versions = installed_dep_versions()
    base_digest = _normalize_digest(config.base_image_digest)
    if base_digest:
        dep_versions[BASE_IMAGE_DIGEST_KEY] = base_digest
    return DeploymentBom(
        image_digest=_normalize_digest(config.image_digest),
        git_commit=config.git_commit,
        dep_versions=dep_versions,
    )


__all__ = [
    "BASE_IMAGE_DIGEST_KEY",
    "BOM_DISTRIBUTIONS",
    "BomConfigLike",
    "DeploymentBom",
    "extract_image_digest",
    "installed_dep_versions",
    "resolve_deployment_bom",
]
