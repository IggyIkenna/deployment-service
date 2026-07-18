"""Unit tests for the post-mortem heartbeat-sidecar reliability check (DP-VM-008).

Credential-free + block-network safe: GCS I/O is injected via the shared
``FakeStorage`` test double. Covers the 2026-07-18 recurrence
(``zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md`` § Incident 2):
a killed VM's heartbeat blob showing a write only 16s before the delete call —
GENUINELY_STALE when the blob age at kill time clears the threshold,
RELIABILITY_GAP_SUSPECTED when it doesn't, UNKNOWN_NO_BLOB when the VM never
wrote one at all.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from deployment_service.data_pipeline_monitors import _gcs
from deployment_service.data_pipeline_monitors import heartbeat_sidecar_reliability as reliability
from tests.unit.test_data_pipeline_monitors import LOG_BUCKET, FakeStorage

_VM = "af-backfill-20260717-151237"


def _heartbeat_blob(vm: str) -> str:
    return _gcs.HEARTBEAT_BLOB.format(vm=vm)


# ── classify_heartbeat_reliability (pure) ───────────────────────────────────
def test_classify_genuinely_stale_at_or_above_threshold():
    result = reliability.classify_heartbeat_reliability(
        _VM, heartbeat_age_at_kill_min=15.0, stall_threshold_min=10.0, kill_time="2026-07-18T09:18:00+00:00"
    )
    assert result.verdict is reliability.HeartbeatReliabilityVerdict.GENUINELY_STALE
    assert result.heartbeat_age_at_kill_min == 15.0


def test_classify_exactly_at_threshold_is_genuinely_stale():
    # >= threshold, not strictly >, matches the watchdog's own `hb_age > threshold` kill gate boundary.
    result = reliability.classify_heartbeat_reliability(
        _VM, heartbeat_age_at_kill_min=10.0, stall_threshold_min=10.0, kill_time="2026-07-18T09:18:00+00:00"
    )
    assert result.verdict is reliability.HeartbeatReliabilityVerdict.GENUINELY_STALE


def test_classify_reliability_gap_suspected_below_threshold():
    # The 2026-07-18 incident shape: blob written only 16s (0.27min) before the kill,
    # far under the af-backfill- 10.0min threshold.
    result = reliability.classify_heartbeat_reliability(
        _VM, heartbeat_age_at_kill_min=0.27, stall_threshold_min=10.0, kill_time="2026-07-18T09:18:00+00:00"
    )
    assert result.verdict is reliability.HeartbeatReliabilityVerdict.RELIABILITY_GAP_SUSPECTED


def test_classify_unknown_no_blob():
    result = reliability.classify_heartbeat_reliability(
        _VM, heartbeat_age_at_kill_min=None, stall_threshold_min=10.0, kill_time="2026-07-18T09:18:00+00:00"
    )
    assert result.verdict is reliability.HeartbeatReliabilityVerdict.UNKNOWN_NO_BLOB


# ── heartbeat_age_at_kill (GCS read) ────────────────────────────────────────
def test_heartbeat_age_at_kill_computes_gap_from_content_epoch():
    kill_dt = datetime(2026, 7, 18, 9, 18, 0, tzinfo=UTC)
    write_dt = kill_dt - timedelta(seconds=16)
    write_epoch = int(write_dt.timestamp())
    storage = FakeStorage({(LOG_BUCKET, _heartbeat_blob(_VM)): (f"{write_epoch}\n-1\nrunning".encode(), None)})
    age = reliability.heartbeat_age_at_kill(storage, LOG_BUCKET, _VM, kill_dt.isoformat())
    assert age is not None
    assert abs(age - (16.0 / 60.0)) < 1e-6


def test_heartbeat_age_at_kill_none_when_blob_missing():
    age = reliability.heartbeat_age_at_kill(FakeStorage({}), LOG_BUCKET, _VM, "2026-07-18T09:18:00+00:00")
    assert age is None


def test_heartbeat_age_at_kill_none_on_unparseable_kill_time():
    storage = FakeStorage({(LOG_BUCKET, _heartbeat_blob(_VM)): (b"1700000000\n-1\nrunning", None)})
    age = reliability.heartbeat_age_at_kill(storage, LOG_BUCKET, _VM, "not-a-timestamp")
    assert age is None


def test_heartbeat_age_at_kill_treats_naive_kill_time_as_utc():
    kill_dt = datetime(2026, 7, 18, 9, 18, 0)  # naive
    write_epoch = int(datetime(2026, 7, 18, 9, 17, 44, tzinfo=UTC).timestamp())
    storage = FakeStorage({(LOG_BUCKET, _heartbeat_blob(_VM)): (f"{write_epoch}\n-1\nrunning".encode(), None)})
    age = reliability.heartbeat_age_at_kill(storage, LOG_BUCKET, _VM, kill_dt.isoformat())
    assert age is not None
    assert abs(age - (16.0 / 60.0)) < 1e-6


# ── audit_killed_vm / audit_killed_vms (integration of the two above) ──────
def test_audit_killed_vm_end_to_end_reliability_gap():
    kill_iso = "2026-07-18T09:18:00+00:00"
    write_epoch = int(datetime(2026, 7, 18, 9, 17, 44, tzinfo=UTC).timestamp())
    storage = FakeStorage({(LOG_BUCKET, _heartbeat_blob(_VM)): (f"{write_epoch}\n-1\nrunning".encode(), None)})
    result = reliability.audit_killed_vm(storage, LOG_BUCKET, _VM, kill_iso, stall_threshold_min=10.0)
    assert result.verdict is reliability.HeartbeatReliabilityVerdict.RELIABILITY_GAP_SUSPECTED
    assert result.vm_name == _VM
    assert result.kill_time == kill_iso


def test_audit_killed_vm_end_to_end_genuinely_stale():
    kill_dt = datetime(2026, 7, 18, 9, 18, 0, tzinfo=UTC)
    write_epoch = int((kill_dt - timedelta(minutes=25)).timestamp())
    storage = FakeStorage({(LOG_BUCKET, _heartbeat_blob(_VM)): (f"{write_epoch}\n-1\nrunning".encode(), None)})
    result = reliability.audit_killed_vm(storage, LOG_BUCKET, _VM, kill_dt.isoformat(), stall_threshold_min=10.0)
    assert result.verdict is reliability.HeartbeatReliabilityVerdict.GENUINELY_STALE


def test_audit_killed_vms_batch_uses_per_vm_threshold():
    kill_dt = datetime(2026, 7, 18, 9, 18, 0, tzinfo=UTC)
    stale_vm = "cefi-hyperliquid-2023-20260623-113700"
    gap_vm = _VM
    storage = FakeStorage(
        {
            # stale_vm: no write in the last 25 min -> genuinely stale under a 10min threshold.
            (LOG_BUCKET, _heartbeat_blob(stale_vm)): (
                f"{int((kill_dt - timedelta(minutes=25)).timestamp())}\n-1\nrunning".encode(),
                None,
            ),
            # gap_vm: written 16s before kill -> suspicious under the same 10min threshold.
            (LOG_BUCKET, _heartbeat_blob(gap_vm)): (
                f"{int((kill_dt - timedelta(seconds=16)).timestamp())}\n-1\nrunning".encode(),
                None,
            ),
        }
    )
    results = reliability.audit_killed_vms(
        storage,
        LOG_BUCKET,
        [(stale_vm, kill_dt.isoformat()), (gap_vm, kill_dt.isoformat())],
        stall_threshold_min_for_vm=lambda _vm: 10.0,
    )
    by_vm = {r.vm_name: r.verdict for r in results}
    assert by_vm[stale_vm] is reliability.HeartbeatReliabilityVerdict.GENUINELY_STALE
    assert by_vm[gap_vm] is reliability.HeartbeatReliabilityVerdict.RELIABILITY_GAP_SUSPECTED
