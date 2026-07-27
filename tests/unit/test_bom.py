"""
Unit tests for deployment_service/bom.py — deployment bill-of-materials resolution.

Loads the module directly (its module-level imports are stdlib-only) so the
tests bypass deployment_service/__init__ — same isolation rationale as
test_deployment_heartbeat_cli.py.
"""

from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
from dataclasses import dataclass
from types import ModuleType
from unittest.mock import patch


def _load_bom_module() -> ModuleType:
    path = pathlib.Path(__file__).resolve().parents[2] / "deployment_service" / "bom.py"
    module_name = "_bom_under_test"
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_bom = _load_bom_module()

_DIGEST_A = "sha256:" + "a" * 64
_DIGEST_B = "sha256:" + "b" * 64


@dataclass
class _StubConfig:
    """Minimal BomConfigLike stub — avoids constructing DeploymentConfig."""

    git_commit: str = ""
    image_digest: str = ""
    base_image_digest: str = ""


# ----- extract_image_digest -------------------------------------------------


def test_extract_image_digest_from_pinned_ref() -> None:
    ref = f"asia-northeast1-docker.pkg.dev/p/repo/svc@{_DIGEST_A}"
    assert _bom.extract_image_digest(ref) == _DIGEST_A


def test_extract_image_digest_tag_only_ref_is_empty() -> None:
    assert _bom.extract_image_digest("asia-northeast1-docker.pkg.dev/p/repo/svc:latest") == ""
    assert _bom.extract_image_digest("") == ""
    # Non-sha256 suffix after "@" must not be mistaken for a digest.
    assert _bom.extract_image_digest("svc@v1.2.3") == ""


# ----- installed_dep_versions ------------------------------------------------


def test_installed_dep_versions_reports_only_known_dists() -> None:
    versions = _bom.installed_dep_versions()
    # Never fabricates keys outside the closed BOM_DISTRIBUTIONS set, and every
    # reported version is a non-empty string (honest importlib.metadata read).
    assert set(versions) <= set(_bom.BOM_DISTRIBUTIONS)
    assert all(isinstance(v, str) and v for v in versions.values())


# ----- resolve_deployment_bom -------------------------------------------------


def test_resolve_bom_passthrough_and_digest_normalization() -> None:
    cfg = _StubConfig(
        git_commit="deadbee",
        image_digest=f"repo/svc@{_DIGEST_A}",  # digest-pinned ref → digest extracted
        base_image_digest=_DIGEST_B,  # bare digest → kept as-is
    )
    bom = _bom.resolve_deployment_bom(cfg)
    assert bom.git_commit == "deadbee"
    assert bom.image_digest == _DIGEST_A
    assert bom.dep_versions[_bom.BASE_IMAGE_DIGEST_KEY] == _DIGEST_B


def test_resolve_bom_tag_only_image_yields_empty_digest() -> None:
    cfg = _StubConfig(image_digest="repo/svc:latest")
    bom = _bom.resolve_deployment_bom(cfg)
    assert bom.image_digest == ""  # honest unknown — never guessed from a tag


def test_resolve_bom_unknowns_stay_empty() -> None:
    bom = _bom.resolve_deployment_bom(_StubConfig())
    assert bom.image_digest == ""
    assert bom.git_commit == ""
    assert _bom.BASE_IMAGE_DIGEST_KEY not in bom.dep_versions
    # Installed internal dists are still honestly reported.
    assert set(bom.dep_versions) <= set(_bom.BOM_DISTRIBUTIONS)


def test_resolve_bom_reads_git_commit_from_env_via_deployment_config() -> None:
    """Phase 3c (artifact_pipeline_observability plan): `setup-data-pipeline-vm.sh` now `export
    GIT_COMMIT=<sha>` at VM launch — this is the Python half of that fix, covering the REAL
    `DeploymentConfig` (not the `_StubConfig` double above) so the env-var alias resolution itself is
    exercised, not just `resolve_deployment_bom`'s own passthrough logic (already covered above).
    `GIT_COMMIT` is the first `AliasChoices` entry on `DeploymentConfig.git_commit` — no code change
    was needed for this to work; this test pins that it doesn't silently regress.
    """
    from deployment_service.deployment_config import DeploymentConfig

    with patch.dict(os.environ, {"GIT_COMMIT": "4b3aad7181cb782c1ea41677fa1e720765aad88f"}):
        config = DeploymentConfig()
        assert config.git_commit == "4b3aad7181cb782c1ea41677fa1e720765aad88f"
        bom = _bom.resolve_deployment_bom(config)
        assert bom.git_commit == "4b3aad7181cb782c1ea41677fa1e720765aad88f"
