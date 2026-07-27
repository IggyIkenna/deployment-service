"""Unit tests for launch-mdps-backfill-vm.sh's venue/data_type label computation.

MDPS-parity followup (worker_session_teardown_kills_long_running_pipeline_check_2026_07_27.md):
market-data-processing-service's pipeline_e2e_check.py duplicate-in-flight VM guard matches on
labels.category/labels.venue/labels.data_type, so this launcher must stamp venue/data_type
labels whenever the caller passes a SINGLE (non-space-separated) --venues/--data-types value —
the driver's per-shard launch pattern — and omit them for the multi-value narrow-scope-filter
form (a label can't unambiguously represent a list).

Extracts the real ``extra_labels`` computation lines from the shipped script (never a
hand-duplicated copy) and evaluates them in a bash subshell with FILTER_VENUES/
FILTER_DATA_TYPES set — no ``gcloud`` call is ever reached (the script's real
``gcloud compute instances create`` call is a live cloud action and must never run in a test).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

_LAUNCHER_PATH = Path(__file__).resolve().parents[2] / "scripts" / "vm" / "launch-mdps-backfill-vm.sh"

_SNIPPET_START = 'local extra_labels=""'
_SNIPPET_END = 'gcloud compute instances create "$vm_name" \\'


def _extract_label_snippet() -> str:
    lines = _LAUNCHER_PATH.read_text().splitlines()
    start = next(i for i, line in enumerate(lines) if _SNIPPET_START in line)
    end = next(i for i, line in enumerate(lines) if _SNIPPET_END in line)
    assert end > start, "label snippet anchors not found in expected order"
    return "\n".join(lines[start:end])


def _eval_extra_labels(filter_venues: str, filter_data_types: str) -> str:
    snippet = _extract_label_snippet()
    script = f'''
FILTER_VENUES="{filter_venues}"
FILTER_DATA_TYPES="{filter_data_types}"
{snippet}
echo "$extra_labels"
'''
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def test_single_venue_and_data_type_both_labeled() -> None:
    assert _eval_extra_labels("BINANCE-FUTURES", "trades") == ",venue=binance-futures,data_type=trades"


def test_multi_value_venues_omits_venue_label() -> None:
    assert _eval_extra_labels("BINANCE-FUTURES BYBIT", "trades") == ",data_type=trades"


def test_multi_value_data_types_omits_data_type_label() -> None:
    assert _eval_extra_labels("BINANCE-FUTURES", "trades ohlcv_1m") == ",venue=binance-futures"


def test_empty_filters_produce_no_extra_labels() -> None:
    assert _eval_extra_labels("", "") == ""


def test_snippet_anchors_still_present() -> None:
    """If a future edit removes/renames the anchors, fail loudly instead of silently
    testing stale extracted content."""
    snippet = _extract_label_snippet()
    assert "FILTER_VENUES" in snippet
    assert "FILTER_DATA_TYPES" in snippet
