"""Unit tests for DP-VM-012 Cloud Run Service liveness watcher + registry guard.

Credential-free: Cloud Run SDK I/O is injected via the ``CloudRunServiceReader``
callable. ``emit_finding`` is called with ``dry_run=True`` so no Slack/GCS
side-effects occur.

Coverage:
  (a) registry guard — non-empty names, no duplicates, min_scale >= 0, valid locations
  (b) healthy service → no finding emitted
  (c) terminal condition FAILED → finding emitted (terminal_condition_failed)
  (d) OOM signature in condition messages → finding emitted with OOM classification
  (e) below min_scale (ready_instances=0, min_scale=2) → finding emitted
  (f) service not found (found=False) → skipped, no finding
  (g) API error (snapshot=None) → skipped, no finding
  (h) MissTracker gating: first miss below threshold → no finding; second miss
      crosses threshold → finding emitted
  (i) service recovers after a miss → MissTracker resets, no finding on next sweep
"""

from __future__ import annotations

from deployment_service.cloud_run_service_registry import (
    CLOUD_RUN_SERVICES,
    CloudRunServiceTarget,
)
from deployment_service.data_pipeline_monitors._miss_tracker import MissTracker
from deployment_service.data_pipeline_monitors.cloud_run_service_liveness_watcher import (
    CloudRunServiceReader,
    ServiceHealthSnapshot,
    _has_oom_signature,
    _is_terminal_failed,
    _miss_key,
    check_cloud_run_service_liveness,
)

# ---------------------------------------------------------------------------
# Minimal in-memory StorageClient stub — just enough for MissTracker.
# ---------------------------------------------------------------------------

class _FakeStorage:
    """Minimal stub satisfying the StorageClient interface used by MissTracker."""

    def __init__(self) -> None:
        self._blobs: dict[tuple[str, str], bytes] = {}

    def blob_exists(self, bucket: str, path: str) -> bool:
        return (bucket, path) in self._blobs

    def download_bytes(self, bucket: str, path: str) -> bytes:
        return self._blobs[(bucket, path)]

    def get_blob_metadata(self, bucket: str, path: str):  # noqa: ANN201
        return None

    def upload_bytes(
        self,
        bucket: str,
        path: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> None:
        self._blobs[(bucket, path)] = data


def _fresh_tracker() -> MissTracker:
    return MissTracker.load(storage_client=_FakeStorage(), log_bucket="test-log-bucket")


# ---------------------------------------------------------------------------
# Shared helpers: snapshot factories.
# ---------------------------------------------------------------------------

def _healthy_snapshot(name: str) -> ServiceHealthSnapshot:
    return ServiceHealthSnapshot(
        name=name,
        terminal_condition_state="CONDITION_SUCCEEDED",
        condition_messages="",
        ready_instance_count=1,
        found=True,
    )


def _failed_snapshot(name: str, reason: str = "ContainerExited") -> ServiceHealthSnapshot:
    return ServiceHealthSnapshot(
        name=name,
        terminal_condition_state="CONDITION_FAILED",
        condition_messages=f"{reason} container exited with non-zero status",
        ready_instance_count=None,
        found=True,
    )


def _oom_snapshot(name: str) -> ServiceHealthSnapshot:
    return ServiceHealthSnapshot(
        name=name,
        terminal_condition_state="CONDITION_FAILED",
        condition_messages="oomkilled signal: 9 out of memory",
        ready_instance_count=None,
        found=True,
    )


def _not_found_snapshot(name: str) -> ServiceHealthSnapshot:
    return ServiceHealthSnapshot(
        name=name,
        terminal_condition_state=None,
        condition_messages="",
        ready_instance_count=None,
        found=False,
    )


def _make_reader(
    snapshots: dict[str, ServiceHealthSnapshot | None],
) -> CloudRunServiceReader:
    """Return a CloudRunServiceReader that returns the given snapshot dict."""

    def _reader(
        project_id: str,
        env_prefix: str,
        targets: object,
    ) -> dict[str, ServiceHealthSnapshot | None]:
        return dict(snapshots)

    return _reader  # type: ignore[return-value]


def _run(
    targets: list[CloudRunServiceTarget],
    snapshots: dict[str, ServiceHealthSnapshot | None],
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = 2,
) -> dict[str, dict[str, object]]:
    return check_cloud_run_service_liveness(
        targets=targets,
        service_reader=_make_reader(snapshots),  # type: ignore[arg-type]
        project_id="test-project",
        env_prefix="",
        pm_repo_path=None,
        dry_run=True,
        miss_tracker=miss_tracker,
        min_consecutive=min_consecutive,
    )


# ---------------------------------------------------------------------------
# (a) Registry guard.
# ---------------------------------------------------------------------------

def test_registry_has_entries() -> None:
    """CLOUD_RUN_SERVICES must contain at least one entry."""
    assert len(CLOUD_RUN_SERVICES) >= 1


def test_registry_names_non_empty() -> None:
    """Every registry entry must have a non-empty name."""
    for target in CLOUD_RUN_SERVICES:
        assert target.name.strip(), f"empty name in registry entry: {target!r}"


def test_registry_no_duplicate_names() -> None:
    """Registry must not contain duplicate service names."""
    names = [t.name for t in CLOUD_RUN_SERVICES]
    assert len(names) == len(set(names)), f"duplicate names: {names}"


def test_registry_min_scale_non_negative() -> None:
    """Every min_scale must be >= 0 (0 = scale-to-zero allowed)."""
    for target in CLOUD_RUN_SERVICES:
        assert target.min_scale >= 0, f"{target.name}: min_scale={target.min_scale} < 0"


def test_registry_location_non_empty() -> None:
    """Every registry entry must have a non-empty location string."""
    for target in CLOUD_RUN_SERVICES:
        assert target.location.strip(), f"{target.name}: empty location"


def test_registry_contains_known_audit_services() -> None:
    """The three audit-identified blind-spot services must be present."""
    names = {t.name for t in CLOUD_RUN_SERVICES}
    for required in (
        "market-data-query-service",
        "central-market-data-tardis-loader",
        "uts-prod-data-status-rollup-svc",
    ):
        assert required in names, f"audit service missing from registry: {required}"


def test_central_tardis_loader_min_scale_is_2() -> None:
    """central-market-data-tardis-loader must register min_scale=2 (audit finding)."""
    matches = [t for t in CLOUD_RUN_SERVICES if t.name == "central-market-data-tardis-loader"]
    assert matches, "central-market-data-tardis-loader not in registry"
    assert matches[0].min_scale == 2, f"expected min_scale=2, got {matches[0].min_scale}"


# ---------------------------------------------------------------------------
# (b) Helpers: _is_terminal_failed.
# ---------------------------------------------------------------------------

def test_is_terminal_failed_succeeded_is_healthy() -> None:
    assert not _is_terminal_failed("CONDITION_SUCCEEDED")


def test_is_terminal_failed_any_capitalisation_succeeded() -> None:
    assert not _is_terminal_failed("condition_succeeded")


def test_is_terminal_failed_failed_state() -> None:
    assert _is_terminal_failed("CONDITION_FAILED")


def test_is_terminal_failed_unknown_state() -> None:
    assert _is_terminal_failed("CONDITION_UNKNOWN")


def test_is_terminal_failed_pending_state() -> None:
    assert _is_terminal_failed("CONDITION_PENDING")


def test_is_terminal_failed_none_returns_false() -> None:
    """None terminal_condition_state → skip (fail toward no false alert)."""
    assert not _is_terminal_failed(None)


# ---------------------------------------------------------------------------
# (c) Helpers: _has_oom_signature.
# ---------------------------------------------------------------------------

def test_has_oom_signature_signal_9() -> None:
    assert _has_oom_signature("container killed signal: 9")


def test_has_oom_signature_exit_code_137() -> None:
    assert _has_oom_signature("exit_code=137 container stopped")


def test_has_oom_signature_out_of_memory() -> None:
    assert _has_oom_signature("out of memory error encountered")


def test_has_oom_signature_oomkilled() -> None:
    assert _has_oom_signature("oomkilled")


def test_has_oom_signature_no_match() -> None:
    assert _has_oom_signature("container exited with status 1") == ""


def test_has_oom_signature_empty() -> None:
    assert _has_oom_signature("") == ""


# ---------------------------------------------------------------------------
# (b) check_cloud_run_service_liveness: healthy service → no finding.
# ---------------------------------------------------------------------------

def test_healthy_service_no_finding() -> None:
    """A service with CONDITION_SUCCEEDED emits no finding."""
    target = CloudRunServiceTarget(name="svc-a", description="svc-a desc")
    result = _run([target], {"svc-a": _healthy_snapshot("svc-a")})
    assert result["svc-a"] == {}


def test_multiple_healthy_services_no_finding() -> None:
    """Multiple healthy services produce empty findings for each."""
    targets = [
        CloudRunServiceTarget(name="svc-1", description=""),
        CloudRunServiceTarget(name="svc-2", description=""),
    ]
    snapshots = {
        "svc-1": _healthy_snapshot("svc-1"),
        "svc-2": _healthy_snapshot("svc-2"),
    }
    result = _run(targets, snapshots)
    assert result["svc-1"] == {}
    assert result["svc-2"] == {}


# ---------------------------------------------------------------------------
# (c) Terminal condition FAILED → finding emitted.
# ---------------------------------------------------------------------------

def test_terminal_failed_emits_finding() -> None:
    """CONDITION_FAILED emits a non-empty finding (no miss_tracker → fires immediately)."""
    target = CloudRunServiceTarget(name="crash-svc", description="crash desc")
    result = _run([target], {"crash-svc": _failed_snapshot("crash-svc")}, min_consecutive=1)
    details = result["crash-svc"]
    assert details, "expected a finding for CONDITION_FAILED"
    assert details["service_name"] == "crash-svc"
    assert details["terminal_condition_state"] == "CONDITION_FAILED"
    assert details["failure_reason"] == "terminal_condition_failed"
    assert not details["oom"]


# ---------------------------------------------------------------------------
# (d) OOM signature → OOM classification.
# ---------------------------------------------------------------------------

def test_oom_signature_classifies_as_oom() -> None:
    """OOM signature in condition messages → failure_reason='OOM'."""
    target = CloudRunServiceTarget(name="oom-svc", description="oom desc")
    result = _run([target], {"oom-svc": _oom_snapshot("oom-svc")}, min_consecutive=1)
    details = result["oom-svc"]
    assert details["oom"] is True
    assert details["failure_reason"] == "OOM"
    assert details["oom_signature"], "oom_signature must be non-empty when OOM detected"


# ---------------------------------------------------------------------------
# (e) Below min_scale → finding emitted.
# ---------------------------------------------------------------------------

def test_below_min_scale_emits_finding() -> None:
    """ready_instance_count < min_scale emits a finding when terminal is healthy."""
    target = CloudRunServiceTarget(name="scale-svc", description="scale desc", min_scale=2)
    snap = ServiceHealthSnapshot(
        name="scale-svc",
        terminal_condition_state="CONDITION_SUCCEEDED",
        condition_messages="",
        ready_instance_count=0,
        found=True,
    )
    result = _run([target], {"scale-svc": snap}, min_consecutive=1)
    details = result["scale-svc"]
    assert details, "expected finding for below-min-scale"
    assert details["min_scale"] == 2
    assert details["ready_instance_count"] == 0


def test_at_min_scale_no_finding() -> None:
    """ready_instance_count == min_scale → healthy, no finding."""
    target = CloudRunServiceTarget(name="scale-ok-svc", description="", min_scale=1)
    snap = ServiceHealthSnapshot(
        name="scale-ok-svc",
        terminal_condition_state="CONDITION_SUCCEEDED",
        condition_messages="",
        ready_instance_count=1,
        found=True,
    )
    result = _run([target], {"scale-ok-svc": snap})
    assert result["scale-ok-svc"] == {}


# ---------------------------------------------------------------------------
# (f) Not-found service → skip (no finding).
# ---------------------------------------------------------------------------

def test_not_found_service_skipped() -> None:
    """found=False (service doesn't exist in project/region) → skip, no finding."""
    target = CloudRunServiceTarget(name="ghost-svc", description="")
    result = _run([target], {"ghost-svc": _not_found_snapshot("ghost-svc")})
    assert result["ghost-svc"] == {}


# ---------------------------------------------------------------------------
# (g) API error (None snapshot) → skip (no finding).
# ---------------------------------------------------------------------------

def test_api_error_skipped() -> None:
    """None snapshot (API error) → skip, no false alert."""
    target = CloudRunServiceTarget(name="err-svc", description="")
    result = _run([target], {"err-svc": None})
    assert result["err-svc"] == {}


def test_missing_from_reader_result_skipped() -> None:
    """Service absent from reader result dict → treated as API error, skip."""
    target = CloudRunServiceTarget(name="absent-svc", description="")
    result = _run([target], {})  # absent key → snapshots.get() = None
    assert result["absent-svc"] == {}


# ---------------------------------------------------------------------------
# (h) MissTracker gating — two consecutive misses before paging.
# ---------------------------------------------------------------------------

def test_miss_tracker_first_miss_below_threshold_no_finding() -> None:
    """First sweep miss below consecutive threshold → no finding emitted."""
    tracker = _fresh_tracker()
    target = CloudRunServiceTarget(name="gated-svc", description="")
    result = _run(
        [target],
        {"gated-svc": _failed_snapshot("gated-svc")},
        miss_tracker=tracker,
        min_consecutive=2,
    )
    assert result["gated-svc"] == {}, "should not page on first miss"


def test_miss_tracker_second_miss_crosses_threshold() -> None:
    """Second consecutive sweep miss crosses threshold (=2) → finding emitted."""
    tracker = _fresh_tracker()
    target = CloudRunServiceTarget(name="gated-svc", description="")
    # First sweep — below threshold.
    _run([target], {"gated-svc": _failed_snapshot("gated-svc")}, miss_tracker=tracker, min_consecutive=2)
    # Second sweep — crosses threshold.
    result = _run([target], {"gated-svc": _failed_snapshot("gated-svc")}, miss_tracker=tracker, min_consecutive=2)
    details = result["gated-svc"]
    assert details, "should page on second consecutive miss"
    assert details["service_name"] == "gated-svc"


def test_miss_tracker_threshold_1_fires_immediately() -> None:
    """min_consecutive=1 → finding fires on the very first miss."""
    tracker = _fresh_tracker()
    target = CloudRunServiceTarget(name="fast-svc", description="")
    result = _run(
        [target],
        {"fast-svc": _failed_snapshot("fast-svc")},
        miss_tracker=tracker,
        min_consecutive=1,
    )
    assert result["fast-svc"], "min_consecutive=1 should fire immediately"


# ---------------------------------------------------------------------------
# (i) Service recovers — MissTracker resets, no finding on next sweep.
# ---------------------------------------------------------------------------

def test_miss_tracker_resets_on_recovery() -> None:
    """A miss followed by a healthy sweep resets the counter; no finding on next miss."""
    tracker = _fresh_tracker()
    target = CloudRunServiceTarget(name="recover-svc", description="")

    # Sweep 1: miss.
    _run([target], {"recover-svc": _failed_snapshot("recover-svc")}, miss_tracker=tracker, min_consecutive=2)

    # Sweep 2: healthy → counter resets.
    _run([target], {"recover-svc": _healthy_snapshot("recover-svc")}, miss_tracker=tracker)

    # Sweep 3: miss again — counter starts fresh, below threshold again.
    result = _run([target], {"recover-svc": _failed_snapshot("recover-svc")}, miss_tracker=tracker, min_consecutive=2)
    assert result["recover-svc"] == {}, "counter should have reset on recovery"


# ---------------------------------------------------------------------------
# (j) Mixed batch: one healthy + one failing.
# ---------------------------------------------------------------------------

def test_mixed_batch_only_failing_emits_finding() -> None:
    """In a mixed batch only the failing service emits a finding."""
    t_ok = CloudRunServiceTarget(name="ok-svc", description="")
    t_fail = CloudRunServiceTarget(name="fail-svc", description="")
    snapshots = {
        "ok-svc": _healthy_snapshot("ok-svc"),
        "fail-svc": _failed_snapshot("fail-svc"),
    }
    result = _run([t_ok, t_fail], snapshots, min_consecutive=1)
    assert result["ok-svc"] == {}
    assert result["fail-svc"], "fail-svc should emit a finding"


# ---------------------------------------------------------------------------
# (k) Miss key uniqueness — different services get distinct miss keys.
# ---------------------------------------------------------------------------

def test_miss_keys_are_unique_per_service() -> None:
    """Each CloudRunServiceTarget in the registry has a unique miss key."""
    keys = [_miss_key(t) for t in CLOUD_RUN_SERVICES]
    assert len(keys) == len(set(keys)), f"non-unique miss keys: {keys}"
