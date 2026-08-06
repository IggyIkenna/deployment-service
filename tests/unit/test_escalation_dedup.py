"""Unit tests for the escalation-dispatch dedup (Option A, operator-confirmed
2026-08-06 — ``dp_escalation_worker_dispatch_no_open_issue_check_2026_07_29.md``).

Covers both layers:
  - ``escalation_dedup.py`` in isolation (frontmatter parsing, tuple matching,
    checkpoint comparison, surgical checkpoint upsert, Progress Log append);
  - the ``escalation.route_finding`` wiring (a dedup skip suppresses the
    fast-spawn ``repository_dispatch`` but never the DP_* event itself; a
    genuinely-new-activity finding, or one with no matching OPEN issue doc,
    dispatches normally).

Credential-free: ``log_event`` and ``_dispatch_to_orchestrator`` are
monkeypatched, no real GitHub API call / network access.
"""

from __future__ import annotations

from pathlib import Path

from deployment_service.data_pipeline_monitors import escalation, escalation_dedup
from deployment_service.data_pipeline_monitors.escalation import (
    EscalationTier,
    PipelineFinding,
)

_DP_FETCH_009_TAGS = "tags: [data-correctness, attempted-failed, cefi, dp-fetch-009]"


def _issue_doc_text(
    *,
    status: str = "open",
    asset_group: str = "cefi",
    data_type: str = "derivative_ticker",
    tags: str = _DP_FETCH_009_TAGS,
    checkpoint_block: str = "",
    extra_frontmatter: str = "",
) -> str:
    """A minimal-but-realistic issue doc matching the real corpus's
    convention: no structured ``data_type:`` field — the tuple lives in free
    text (title/summary/source all carry ``asset_group=<ag> data_type=<dt>``,
    mirroring every doc observed in
    ``dp_escalation_worker_dispatch_no_open_issue_check_2026_07_29.md``)."""
    return f"""---
doc_type: issue
title: "DP_RUN_MOSTLY_EMPTY (DP-FETCH-009) CRITICAL for {asset_group} {data_type}"
summary: >-
  Escalation worker investigated asset_group={asset_group} data_type={data_type}.
status: {status}
asset_group: [{asset_group}]
{tags}
{checkpoint_block}{extra_frontmatter}source:
  "POST /api/escalate wall_type=data_pipeline_failure, asset_group={asset_group} data_type={data_type}"
last_updated: 2026-08-01
---

# DP_RUN_MOSTLY_EMPTY for {asset_group}/{data_type}

## Progress Log

- **2026-07-28:** initial diagnosis + fix.
"""


def _finding(
    *,
    asset_group_name: str = "cefi",
    data_type: str = "derivative_ticker",
    max_attempted_at: str = "2026-07-29T09:03:12Z",
    registry_id: str = "DP-FETCH-009",
) -> PipelineFinding:
    return PipelineFinding(
        event="DP_RUN_MOSTLY_EMPTY",
        severity="CRITICAL",
        tier=EscalationTier.PAGE_OPERATOR,
        summary=f"high attempted_failed batch — asset_group={asset_group_name} data_type={data_type}",
        details={
            "asset_group": f"{asset_group_name}::{data_type}",
            "asset_group_name": asset_group_name,
            "data_type": data_type,
            "captured": 1000,
            "attempted_failed": 200,
            "max_attempted_at": max_attempted_at,
        },
        registry_id=registry_id,
    )


# ── escalation_dedup.py — module-level unit tests ───────────────────────────


def test_parse_frontmatter_returns_none_without_a_block():
    assert escalation_dedup.parse_frontmatter("# just a heading\nno frontmatter here") is None


def test_parse_frontmatter_parses_a_valid_block():
    fm = escalation_dedup.parse_frontmatter(_issue_doc_text())
    assert fm is not None
    assert fm["status"] == "open"
    assert fm["asset_group"] == ["cefi"]


def test_find_open_issue_for_tuple_matches_asset_group_data_type_and_registry(tmp_path: Path):
    issues_dir = tmp_path / "plans" / "active" / "issues"
    issues_dir.mkdir(parents=True)
    (issues_dir / "cefi_derivative_ticker_2026_07_28.md").write_text(_issue_doc_text(), encoding="utf-8")

    match = escalation_dedup.find_open_issue_for_tuple(
        tmp_path, asset_group="cefi", data_type="derivative_ticker", registry_id="DP-FETCH-009"
    )
    assert match is not None
    path, fm = match
    assert path.name == "cefi_derivative_ticker_2026_07_28.md"
    assert fm["status"] == "open"


def test_find_open_issue_ignores_closed_docs(tmp_path: Path):
    issues_dir = tmp_path / "plans" / "active" / "issues"
    issues_dir.mkdir(parents=True)
    (issues_dir / "closed_doc.md").write_text(_issue_doc_text(status="resolved"), encoding="utf-8")

    match = escalation_dedup.find_open_issue_for_tuple(
        tmp_path, asset_group="cefi", data_type="derivative_ticker", registry_id="DP-FETCH-009"
    )
    assert match is None


def test_find_open_issue_ignores_different_data_type(tmp_path: Path):
    issues_dir = tmp_path / "plans" / "active" / "issues"
    issues_dir.mkdir(parents=True)
    (issues_dir / "book_snapshot_doc.md").write_text(_issue_doc_text(data_type="book_snapshot_5"), encoding="utf-8")

    match = escalation_dedup.find_open_issue_for_tuple(
        tmp_path, asset_group="cefi", data_type="derivative_ticker", registry_id="DP-FETCH-009"
    )
    assert match is None


def test_find_open_issue_ignores_different_asset_group(tmp_path: Path):
    issues_dir = tmp_path / "plans" / "active" / "issues"
    issues_dir.mkdir(parents=True)
    (issues_dir / "defi_doc.md").write_text(_issue_doc_text(asset_group="defi"), encoding="utf-8")

    match = escalation_dedup.find_open_issue_for_tuple(
        tmp_path, asset_group="cefi", data_type="derivative_ticker", registry_id="DP-FETCH-009"
    )
    assert match is None


def test_checkpoint_has_new_activity_true_when_no_checkpoint():
    assert escalation_dedup.checkpoint_has_new_activity({}, max_attempted_at="2026-07-29T09:03:12Z") is True


def test_checkpoint_has_new_activity_false_when_unchanged():
    fm = {
        "dp_escalation_checkpoint": {"max_attempted_at": "2026-07-29T09:03:12Z", "checked_at": "2026-07-30T00:00:00Z"}
    }
    assert escalation_dedup.checkpoint_has_new_activity(fm, max_attempted_at="2026-07-29T09:03:12Z") is False


def test_checkpoint_has_new_activity_true_when_newer():
    fm = {
        "dp_escalation_checkpoint": {"max_attempted_at": "2026-07-29T09:03:12Z", "checked_at": "2026-07-30T00:00:00Z"}
    }
    assert escalation_dedup.checkpoint_has_new_activity(fm, max_attempted_at="2026-07-30T12:00:00Z") is True


def test_checkpoint_has_new_activity_true_when_max_attempted_at_missing():
    fm = {"dp_escalation_checkpoint": {"max_attempted_at": "2026-07-29T09:03:12Z"}}
    assert escalation_dedup.checkpoint_has_new_activity(fm, max_attempted_at="") is True


def test_upsert_checkpoint_bootstraps_and_preserves_other_frontmatter(tmp_path: Path):
    path = tmp_path / "doc.md"
    path.write_text(_issue_doc_text(), encoding="utf-8")

    assert escalation_dedup.upsert_checkpoint(path, max_attempted_at="2026-07-29T09:03:12Z") is True
    text = path.read_text(encoding="utf-8")
    fm = escalation_dedup.parse_frontmatter(text)
    assert fm is not None
    assert fm["dp_escalation_checkpoint"]["max_attempted_at"] == "2026-07-29T09:03:12Z"
    assert fm["status"] == "open"  # untouched
    assert fm["asset_group"] == ["cefi"]  # untouched
    assert "# DP_RUN_MOSTLY_EMPTY for cefi/derivative_ticker" in text  # body untouched
    # Exactly the original two `---` frontmatter delimiters — no stray block
    # glued onto the closing fence.
    assert text.split("---", 2)[0] == ""


def test_upsert_checkpoint_replaces_an_existing_block_in_place(tmp_path: Path):
    path = tmp_path / "doc.md"
    checkpoint_block = (
        'dp_escalation_checkpoint:\n  max_attempted_at: "2026-07-29T09:03:12Z"\n  checked_at: "2026-07-30T00:00:00Z"\n'
    )
    path.write_text(_issue_doc_text(checkpoint_block=checkpoint_block), encoding="utf-8")

    assert escalation_dedup.upsert_checkpoint(path, max_attempted_at="2026-07-31T00:00:00Z") is True
    fm = escalation_dedup.parse_frontmatter(path.read_text(encoding="utf-8"))
    assert fm is not None
    assert fm["dp_escalation_checkpoint"]["max_attempted_at"] == "2026-07-31T00:00:00Z"
    # Only ONE checkpoint block survives (no duplicate key from a bad regex match).
    assert path.read_text(encoding="utf-8").count("dp_escalation_checkpoint:") == 1


def test_upsert_checkpoint_when_block_is_the_last_frontmatter_field_keeps_valid_yaml(tmp_path: Path):
    """Regression: the checkpoint regex's end-of-string anchor must not glue the
    new block onto the closing `---` when the checkpoint is the LAST field —
    re-upserting twice in a row (bootstrap, then advance) must stay parseable."""
    path = tmp_path / "doc.md"
    path.write_text(_issue_doc_text(), encoding="utf-8")

    assert escalation_dedup.upsert_checkpoint(path, max_attempted_at="2026-07-29T09:03:12Z") is True
    assert escalation_dedup.upsert_checkpoint(path, max_attempted_at="2026-07-30T00:00:00Z") is True
    text = path.read_text(encoding="utf-8")
    fm = escalation_dedup.parse_frontmatter(text)
    assert fm is not None
    assert fm["dp_escalation_checkpoint"]["max_attempted_at"] == "2026-07-30T00:00:00Z"
    assert fm["status"] == "open"
    assert "# DP_RUN_MOSTLY_EMPTY for cefi/derivative_ticker" in text


def test_append_progress_log_entry_appends_after_existing_entries(tmp_path: Path):
    path = tmp_path / "doc.md"
    path.write_text(_issue_doc_text(), encoding="utf-8")

    assert escalation_dedup.append_progress_log_entry(path, "dedup-skipped, nothing new") is True
    text = path.read_text(encoding="utf-8")
    assert "initial diagnosis + fix." in text  # existing entry preserved
    assert "dedup-skipped, nothing new" in text
    assert text.index("initial diagnosis + fix.") < text.index("dedup-skipped, nothing new")


def test_check_dispatch_dedup_no_matching_doc_returns_none(tmp_path: Path):
    (tmp_path / "plans" / "active" / "issues").mkdir(parents=True)
    result = escalation_dedup.check_dispatch_dedup(
        pm_root=tmp_path,
        asset_group="cefi",
        data_type="derivative_ticker",
        registry_id="DP-FETCH-009",
        max_attempted_at="2026-07-29T09:03:12Z",
        event="DP_RUN_MOSTLY_EMPTY",
    )
    assert result is None


# ── escalation.route_finding wiring ──────────────────────────────────────────


def _patch_route_finding(monkeypatch) -> tuple[list[tuple[str, str, dict]], list[dict]]:
    emitted: list[tuple[str, str, dict]] = []
    dispatched: list[dict] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity, details or {})),
    )

    def _fake_dispatch(finding, _issue_path):  # noqa: ANN001
        dispatched.append(dict(finding.details))
        return {"dispatched": True, "reason": "http_204"}

    monkeypatch.setattr(escalation, "_dispatch_to_orchestrator", _fake_dispatch)
    return emitted, dispatched


def test_route_finding_skips_dispatch_when_open_issue_has_no_new_activity(tmp_path: Path, monkeypatch):
    emitted, dispatched = _patch_route_finding(monkeypatch)
    pm = tmp_path / "unified-trading-pm"
    issues_dir = pm / "plans" / "active" / "issues"
    issues_dir.mkdir(parents=True)
    doc = issues_dir / "cefi_derivative_ticker_2026_07_28.md"
    doc.write_text(_issue_doc_text(), encoding="utf-8")
    # Bootstrap a checkpoint matching the finding's max_attempted_at exactly —
    # simulates a PRIOR dispatch (skip or full) having already verified this
    # exact reading.
    escalation_dedup.upsert_checkpoint(doc, max_attempted_at="2026-07-29T09:03:12Z")

    result = escalation.route_finding(_finding(max_attempted_at="2026-07-29T09:03:12Z"), pm_repo_path=str(pm))

    assert dispatched == []  # the fast-spawn worker was NEVER invoked
    assert result["dispatch"]["dispatched"] is False
    assert result["dispatch"]["reason"] == "dedup_open_issue_no_new_activity"
    assert result["escalation_dedup"]["skipped"] is True
    # The DP_* event is still ALWAYS emitted — only the extra dispatch is gated.
    assert any(e[0] == "DP_RUN_MOSTLY_EMPTY" for e in emitted)
    assert emitted[0][2]["escalation_dedup_skipped"] is True
    # A lightweight verification note landed on the doc (no full re-diagnosis).
    text = doc.read_text(encoding="utf-8")
    assert "DEDUP SKIP" in text


def test_route_finding_dispatches_normally_with_no_matching_issue_doc(tmp_path: Path, monkeypatch):
    emitted, dispatched = _patch_route_finding(monkeypatch)
    pm = tmp_path / "unified-trading-pm"
    (pm / "plans" / "active" / "issues").mkdir(parents=True)

    result = escalation.route_finding(_finding(), pm_repo_path=str(pm))

    assert len(dispatched) == 1  # normal full dispatch — nothing to dedup against
    assert result["dispatch"]["dispatched"] is True
    assert result.get("escalation_dedup") is None
    assert any(e[0] == "DP_RUN_MOSTLY_EMPTY" for e in emitted)


def test_route_finding_dispatches_when_genuinely_new_activity_since_checkpoint(tmp_path: Path, monkeypatch):
    """The doc's own 2026-07-30 agt-40f31f finding: a moved `max_attempted_at`
    must NOT be silently dedup-skipped — a full worker dispatch still fires so
    a live session can rule out (or confirm) a real regression."""
    _, dispatched = _patch_route_finding(monkeypatch)
    pm = tmp_path / "unified-trading-pm"
    issues_dir = pm / "plans" / "active" / "issues"
    issues_dir.mkdir(parents=True)
    doc = issues_dir / "cefi_derivative_ticker_2026_07_28.md"
    doc.write_text(_issue_doc_text(), encoding="utf-8")
    escalation_dedup.upsert_checkpoint(doc, max_attempted_at="2026-07-29T09:03:12Z")

    result = escalation.route_finding(_finding(max_attempted_at="2026-07-30T12:00:00Z"), pm_repo_path=str(pm))

    assert len(dispatched) == 1  # genuinely new activity → full dispatch still fires
    assert result["dispatch"]["dispatched"] is True
    assert result["escalation_dedup"]["skipped"] is False
    # The checkpoint advances to the new reading so a LATER re-fire at this
    # SAME (now-current) value can dedup-skip.
    fm = escalation_dedup.parse_frontmatter(doc.read_text(encoding="utf-8"))
    assert fm is not None
    assert fm["dp_escalation_checkpoint"]["max_attempted_at"] == "2026-07-30T12:00:00Z"


def test_route_finding_dedup_inapplicable_for_findings_without_the_tuple_shape(tmp_path: Path, monkeypatch):
    """A VM-lifecycle finding (no asset_group_name/data_type details) is never
    dedup-eligible — it must dispatch exactly as before this change."""
    _, dispatched = _patch_route_finding(monkeypatch)
    pm = tmp_path / "unified-trading-pm"
    (pm / "plans" / "active" / "issues").mkdir(parents=True)
    finding = PipelineFinding(
        event="DP_VM_STALL",
        severity="CRITICAL",
        tier=EscalationTier.PAGE_OPERATOR,
        summary="vm stalled",
        details={"vm_name": "uts-prod-cefi-bf-x", "relaunch_launcher": "launch-cefi-bf.sh", "asset_group": "cefi"},
        registry_id="DP-VM-003",
    )

    result = escalation.route_finding(finding, pm_repo_path=str(pm))

    assert len(dispatched) == 1
    assert result.get("escalation_dedup") is None
