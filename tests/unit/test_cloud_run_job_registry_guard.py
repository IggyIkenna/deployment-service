"""Guard test for the deployment-classification spine (Phase 0).

Asserts:

(a) every prefix in ``vm_zombie_watchdog.VM_PREFIX_TO_BUCKET`` classifies via
    ``classify_deployment_target`` without raising ``UnclassifiedDeploymentError``;
(b) every ``terraform/gcp/*_scheduler.tf`` file's Cloud Run job-name STEM appears
    in ``CLOUD_RUN_JOBS`` — the canonical "added a Cloud Run job, forgot to
    classify it" guard;
(c) the 3 paper-launcher prefixes classify to the PAPER umbrella;
(d) a manifest-consolidator job classifies to the BATCH umbrella.

The .tf parse strips terraform template fragments (``${local.env_prefix}-``,
``${local.deployment_env_short}``, ``-${each.key}``, ``${...}``, trailing
``-cron``) to a stable stem, then checks that some registered job name CONTAINS
that stem (per-asset_group ``for_each`` jobs register one entry per AG, so a
stem like ``manifest-consolidator`` is a substring of ``manifest-consolidator-cefi``).
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest

# Ensure resolve_bucket_name (called at vm_zombie_watchdog module load) has an env
# tier + project to resolve against. Reads the UAC-packaged cloud-providers.yaml —
# no network. Set + path-insert BEFORE importing the watchdog.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS_VM = _REPO_ROOT / "scripts" / "vm"
_TERRAFORM_GCP = _REPO_ROOT / "terraform" / "gcp"

os.environ.setdefault("DEPLOYMENT_ENV", "prod")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("PROJECT_ID", "test-project")
if str(_SCRIPTS_VM) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_VM))

import vm_zombie_watchdog  # noqa: E402 — load after env/path setup above.
from unified_api_contracts import DeploymentKind, DeploymentUmbrella

from deployment_service.cloud_run_job_registry import CLOUD_RUN_JOBS
from deployment_service.deployment_classification import (
    PAPER_PREFIXES,
    UnclassifiedDeploymentError,
    classify_deployment_target,
)


# ---------------------------------------------------------------------------
# (a) every VM prefix classifies without raising.
# ---------------------------------------------------------------------------
def test_every_vm_prefix_classifies() -> None:
    """Every VM_PREFIX_TO_BUCKET prefix → classify_deployment_target, no raise."""
    failures: list[str] = []
    for prefix, spec in vm_zombie_watchdog.VM_PREFIX_TO_BUCKET.items():
        lifecycle = spec.lifecycle_class.value if spec is not None else None
        # Heartbeat-only (None spec) entries have no lifecycle_class to derive an
        # umbrella from — they are reserved/one-off prefixes; classify only the
        # typed (VmPrefixSpec) entries, which is what the surface tracks.
        if spec is None:
            continue
        try:
            target = classify_deployment_target(prefix, lifecycle_class=lifecycle)
        except UnclassifiedDeploymentError as exc:  # pragma: no cover - failure path
            failures.append(f"{prefix!r}: {exc}")
            continue
        assert target.umbrella in DeploymentUmbrella
        assert target.kind is DeploymentKind.VM
    assert not failures, "Unclassified VM prefixes:\n" + "\n".join(failures)


# ---------------------------------------------------------------------------
# (b) every scheduler-tf job stem appears in CLOUD_RUN_JOBS.
# ---------------------------------------------------------------------------
_JOB_NAME_RE = re.compile(r'name\s*=\s*"([^"]*(?:\$\{[^}]*\})?[^"]*)"')


def _stem_from_template(raw: str) -> str:
    """Reduce a terraform job-name template to a stable, env/AG-agnostic stem."""
    stem = raw
    stem = stem.replace("${local.env_prefix}-", "")
    stem = re.sub(r"-?\$\{local\.deployment_env_short\}", "", stem)
    stem = stem.replace("-${each.key}", "")
    stem = re.sub(r"\$\{[^}]*\}", "", stem)
    stem = stem.removesuffix("-cron")
    return stem.strip("-")


def _tf_job_stems(tf_text: str) -> set[str]:
    """Extract candidate Cloud Run job-name stems from one scheduler tf's text.

    Returns stems for ``module "*_job"`` / ``resource google_cloud_run_v2_job``
    ``name =`` lines + ``job_name =`` lines + the cron ``jobs/<name>:run`` URIs +
    the OAuth-body inner job names — i.e. every place a Cloud Run job name is
    spelled. Non-job names (env-var names, bucket names, scheduler-only crons
    with no backing job) are tolerated: the assertion only requires that EACH tf
    contributes AT LEAST ONE stem that the registry covers.
    """
    stems: set[str] = set()
    for raw in _JOB_NAME_RE.findall(tf_text):
        stem = _stem_from_template(raw)
        if stem:
            stems.add(stem)
    # job_name = "..." (t1_batch + cf-audit body).
    for raw in re.findall(r'job_name\s*=\s*"([^"]+)"', tf_text):
        stem = _stem_from_template(raw)
        if stem:
            stems.add(stem)
    # cron target URIs: .../jobs/<name>:run
    for raw in re.findall(r"/jobs/([^:\"]+):run", tf_text):
        stem = _stem_from_template(raw)
        if stem:
            stems.add(stem)
    return stems


def _registered_names() -> set[str]:
    return {t.name for t in CLOUD_RUN_JOBS}


def test_every_scheduler_tf_job_is_registered() -> None:
    """Each *_scheduler.tf contributes ≥1 job stem covered by CLOUD_RUN_JOBS."""
    registered = _registered_names()
    tf_files = sorted(_TERRAFORM_GCP.glob("*_scheduler.tf"))
    assert tf_files, "no *_scheduler.tf files found — wrong path?"

    uncovered: list[str] = []
    for tf in tf_files:
        stems = _tf_job_stems(tf.read_text())
        if not stems:
            continue  # scheduler-only file with no job-name spelled (tolerated)
        # A tf is covered if ANY of its stems is a substring of a registered name
        # (per-AG for_each jobs register `manifest-consolidator-cefi` etc., so the
        # `manifest-consolidator` stem matches by substring).
        covered = any(any(stem in name or name in stem for name in registered) for stem in stems)
        if not covered:
            uncovered.append(f"{tf.name}: stems={sorted(stems)}")

    assert not uncovered, (
        "Scheduler tf(s) with NO registered Cloud Run job — add a CLOUD_RUN_JOBS entry:\n" + "\n".join(uncovered)
    )


def test_guard_detects_an_unregistered_scheduler_tf(tmp_path: Path) -> None:
    """The guard MUST fail when a scheduler tf names a job absent from the registry.

    Synthesises a tf with a never-registered job name and asserts the
    substring-coverage check would reject it — proves the guard is not vacuous.
    """
    bogus = 'name = "${local.env_prefix}-totally-unregistered-phantom-job"\n'
    stems = _tf_job_stems(bogus)
    assert stems == {"totally-unregistered-phantom-job"}
    registered = _registered_names()
    covered = any(any(stem in name or name in stem for name in registered) for stem in stems)
    assert not covered, "guard is vacuous — a phantom job name was wrongly considered covered"


# ---------------------------------------------------------------------------
# (c) paper prefixes → PAPER.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("prefix", list(PAPER_PREFIXES))
def test_paper_prefixes_classify_paper(prefix: str) -> None:
    """defi-paper- / funding-ensemble-paper- / strategy-paper- → PAPER umbrella."""
    # Pass a non-paper lifecycle to prove the prefix override wins, not the lifecycle.
    target = classify_deployment_target(f"{prefix}archetype-20260101", lifecycle_class="LONG_LIVED_LIVE")
    assert target.umbrella is DeploymentUmbrella.PAPER


def test_paper_prefix_specs_carry_umbrella_override() -> None:
    """The 3 paper prefixes in VM_PREFIX_TO_BUCKET carry the PAPER umbrella override."""
    for prefix in PAPER_PREFIXES:
        spec = vm_zombie_watchdog.VM_PREFIX_TO_BUCKET[prefix]
        assert spec is not None, f"{prefix} missing from VM_PREFIX_TO_BUCKET"
        assert spec.umbrella is DeploymentUmbrella.PAPER, f"{prefix} missing PAPER umbrella override"


# ---------------------------------------------------------------------------
# (d) a consolidator job → BATCH.
# ---------------------------------------------------------------------------
def test_consolidator_job_is_batch() -> None:
    """A manifest-consolidator Cloud Run job classifies to BATCH."""
    consolidators = [t for t in CLOUD_RUN_JOBS if "manifest-consolidator" in t.name]
    assert consolidators, "no manifest-consolidator job registered"
    for t in consolidators:
        assert t.umbrella is DeploymentUmbrella.BATCH
        assert t.kind is DeploymentKind.CLOUD_RUN_JOB


def test_no_unclassified_umbrella_in_registry() -> None:
    """Every registered Cloud Run job has a valid umbrella + GCP cloud + JOB kind."""
    for t in CLOUD_RUN_JOBS:
        assert t.umbrella in DeploymentUmbrella
        assert t.kind is DeploymentKind.CLOUD_RUN_JOB
        assert t.cloud.value == "GCP"


def test_unknown_lifecycle_class_raises() -> None:
    """A non-paper name with an unknown / None lifecycle_class raises (no silent default)."""
    with pytest.raises(UnclassifiedDeploymentError):
        classify_deployment_target("cefi-mr-20260101", lifecycle_class=None)
    with pytest.raises(UnclassifiedDeploymentError):
        classify_deployment_target("cefi-mr-20260101", lifecycle_class="NOT_A_REAL_CLASS")
