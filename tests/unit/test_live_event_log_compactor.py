"""Unit tests for deployment_service/jobs/live_event_log_compactor.py.

Covers ``_extract_rows`` — the JSON->rows parsing boundary that replaced the old
(incorrect) assumption that warm-sink objects are already Parquet. The Pub/Sub
``cloud_storage_config`` on every ``warm_sink.tf`` subscription writes the raw
message bytes verbatim (no ``parquet_config``/``avro_config``), so every warm
object is a JSON ``CanonicalPersistEnvelope`` despite its ``.parquet`` suffix.
"""

from __future__ import annotations

import json

from unified_api_contracts.events.persist import CanonicalPersistEnvelope, RetentionClass

from deployment_service.jobs.live_event_log_compactor import _extract_rows

_BASE_ENVELOPE_FIELDS = {
    "asset_group": "prediction",
    "data_type": "book_snapshot_5",
    "period_start": "2026-07-30T00:00:00Z",
    "period_end": "2026-07-30T00:01:00Z",
    "source": "MTDS",
    "available_at": "2026-07-30T00:01:00Z",
    "retention_class": RetentionClass.REPRODUCIBLE,
    "correlation_id": "corr-1",
    "vm_name": "vm-1",
}


def _envelope_bytes(**overrides: object) -> bytes:
    envelope = CanonicalPersistEnvelope(**{**_BASE_ENVELOPE_FIELDS, **overrides})
    return envelope.model_dump_json().encode("utf-8")


def test_extract_rows_list_payload() -> None:
    raw = _envelope_bytes(payload_inline=json.dumps([{"venue": "kalshi", "price": 1}, {"venue": "kalshi", "price": 2}]))
    rows = _extract_rows("blob-1", raw)
    assert rows == [{"venue": "kalshi", "price": 1}, {"venue": "kalshi", "price": 2}]


def test_extract_rows_single_dict_payload() -> None:
    raw = _envelope_bytes(payload_inline=json.dumps({"venue": "kalshi", "price": 1}))
    rows = _extract_rows("blob-2", raw)
    assert rows == [{"venue": "kalshi", "price": 1}]


def test_extract_rows_payload_pointer_skips() -> None:
    raw = _envelope_bytes(payload_inline=None, payload_pointer="gs://bucket/path.json")
    rows = _extract_rows("blob-3", raw)
    assert rows == []


def test_extract_rows_invalid_json_payload_skips() -> None:
    raw = _envelope_bytes(payload_inline="not json")
    rows = _extract_rows("blob-4", raw)
    assert rows == []


def test_extract_rows_malformed_envelope_skips() -> None:
    rows = _extract_rows("blob-5", b'{"schema_version":"1"}')
    assert rows == []


def test_extract_rows_non_envelope_bytes_skips() -> None:
    rows = _extract_rows("blob-6", b"PAR1garbagePAR1")
    assert rows == []
