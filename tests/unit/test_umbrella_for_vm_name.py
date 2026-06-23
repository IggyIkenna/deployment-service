"""Unit tests for ``umbrella_for_vm_name`` — the emitter-side umbrella resolver.

The deployment emitters (VM-side ``deployment_heartbeat`` DEPLOYMENT_* events + the
exit-code / heartbeat-stall fleet monitors' DP_VM_* findings) call this to STAMP the
deployment umbrella on each alert payload so the alerting-service router can split
LIVE → ``#uts-live-alerts`` vs BATCH → ``#data-pipeline-alerts`` (operator 2026-06-23).

block-network: pure function over an in-memory prefix registry; no I/O.
"""

from __future__ import annotations

import pytest
from unified_api_contracts import DeploymentUmbrella, LifecycleClass, VmPrefixSpec

from deployment_service.deployment_classification import (
    UnclassifiedDeploymentError,
    umbrella_for_vm_name,
)

pytestmark = pytest.mark.unit


_REGISTRY: dict[str, VmPrefixSpec | None] = {
    "cefi-aster-": VmPrefixSpec(bucket="tick-cefi", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "prediction-live-": VmPrefixSpec(bucket="tick-pred", lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    # paper prefix carries an explicit umbrella override on the spec.
    "defi-paper-": VmPrefixSpec(
        bucket=None,
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
        umbrella=DeploymentUmbrella.PAPER,
    ),
    # a longer, more-specific prefix that must win over a shorter generic one.
    "cefi-": VmPrefixSpec(bucket="tick-cefi", lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
}


class TestUmbrellaForVmName:
    def test_batch_lifecycle_resolves_batch(self) -> None:
        got = umbrella_for_vm_name("cefi-aster-2024-20260623-113700", _REGISTRY)
        assert got is DeploymentUmbrella.BATCH

    def test_long_lived_live_resolves_live(self) -> None:
        got = umbrella_for_vm_name("prediction-live-polymarket-1", _REGISTRY)
        assert got is DeploymentUmbrella.LIVE

    def test_paper_spec_override_wins(self) -> None:
        got = umbrella_for_vm_name("defi-paper-funding-1", _REGISTRY)
        assert got is DeploymentUmbrella.PAPER

    def test_longest_prefix_match_wins(self) -> None:
        # "cefi-aster-" (more specific) must beat the generic "cefi-".
        got = umbrella_for_vm_name("cefi-aster-x", _REGISTRY)
        assert got is DeploymentUmbrella.BATCH

    def test_unregistered_prefix_raises(self) -> None:
        with pytest.raises(UnclassifiedDeploymentError):
            umbrella_for_vm_name("totally-unknown-vm-1", _REGISTRY)

    def test_empty_vm_name_raises(self) -> None:
        with pytest.raises(UnclassifiedDeploymentError):
            umbrella_for_vm_name("", _REGISTRY)
