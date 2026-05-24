"""Shared scaffolding for the 10 Layer-0 deterministic recovery scripts.

Every script subclasses ``Layer0Script`` and implements only:

- ``ACTION_TYPE`` class attribute (from UAC ``ActionType``)
- ``add_action_args(parser)`` — script-specific scope CLI args
- ``compute_scope_key(args) -> str`` — stable key for the repair-loop detector
- ``execute_action(args) -> tuple[ActionStatus, dict[str, ...]]`` — does the
  real work; returns final status + post_action_state.
- ``dry_run_plan(args) -> dict[str, ...]`` — returns what the action WOULD do.

The base class handles: arg parsing, AgentActionEmitter wiring, RepeatedRepairLoopDetector
check, AgentActionEvent emission (STARTED + SUCCEEDED|FAILED|BLOCKED_BY_LOOP_DETECTOR),
provenance tagging, runbook_id lookup, config_hash + code_version stamping.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import tempfile
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import ClassVar

from unified_api_contracts.incident import (
    ActionProvenance,
    ActionStatus,
    ActionType,
)
from unified_trading_library import (
    AgentActionEmitter,
    RecoveryScriptRegistry,
    RepeatedRepairLoopDetector,
)

_logger = logging.getLogger("recovery.layer0")

# Singleton loop detector for the lifetime of a single process invocation.
# (Across processes, the alerting-service gateway maintains the durable
# counter; this in-process detector is the cheap fast-path check.)
_LOCAL_LOOP_DETECTOR = RepeatedRepairLoopDetector(window_seconds=900, threshold=3)


def _git_sha(repo_path: str) -> str:
    """Capture short git SHA of a repo for config_hash + code_version stamping.
    Returns ``unknown`` if git is unavailable (e.g. running inside a VM image
    without git installed)."""
    try:
        result = subprocess.run(
            ["git", "-C", repo_path, "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return "unknown"


class Layer0Script(ABC):
    """Base class for the 10 Layer-0 deterministic recovery scripts."""

    ACTION_TYPE: ClassVar[ActionType]
    """Must be set by subclass. Looked up against RecoveryScriptRegistry."""

    def __init__(self) -> None:
        if not hasattr(self, "ACTION_TYPE"):
            raise NotImplementedError("Subclass must set ACTION_TYPE")
        self.registry_entry = RecoveryScriptRegistry.lookup(self.ACTION_TYPE)
        self.emitter = AgentActionEmitter(
            local_jsonl_dir=os.environ.get(
                "RECOVERY_LOCAL_JSONL_DIR",
                str(Path(tempfile.gettempdir()) / "uts_recovery_events"),
            )
        )

    # ── Subclasses override these ─────────────────────────────────────────

    @abstractmethod
    def add_action_args(self, parser: argparse.ArgumentParser) -> None:
        """Add script-specific scope CLI args (e.g. ``--service``)."""

    @abstractmethod
    def compute_scope_key(self, args: argparse.Namespace) -> str:
        """Return a stable scope key for the repair-loop detector.

        e.g. ``f"service:{args.service}"`` or ``f"venue:{args.venue}"``.
        """

    @abstractmethod
    def execute_action(
        self, args: argparse.Namespace
    ) -> tuple[ActionStatus, dict[str, str | bool | int | float | None]]:
        """Do the real work. Return ``(status, post_action_state_dict)``.

        Status MUST be one of:
        - ``ActionStatus.SUCCEEDED`` — happy path
        - ``ActionStatus.FAILED`` — action attempted but failed
          (Layer-1.5 LLM may retry)

        On exception, the base class catches + emits FAILED + re-raises.
        """

    @abstractmethod
    def dry_run_plan(self, args: argparse.Namespace) -> dict[str, str]:
        """Return a human-readable description of what would happen.

        Used both by ``--dry-run`` mode AND by the Layer-1.5 LLM wrapper to
        preview the action before invocation."""

    # ── Base-class machinery (do not override) ────────────────────────────

    def _build_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description=(
                f"Layer-0 recovery action: {self.ACTION_TYPE.value}\nRunbook: {self.registry_entry.runbook_id}"
            )
        )
        parser.add_argument("--reason", required=True, help="Free-text reason for the action.")
        parser.add_argument(
            "--incident-key",
            required=False,
            default=None,
            help="Parent IncidentEnvelope.incident_key. If None, a fresh key is minted.",
        )
        parser.add_argument(
            "--provenance",
            choices=[p.value for p in ActionProvenance],
            default=ActionProvenance.AUTOMATIC.value,
            help="Closed-set provenance; defaults to AUTOMATIC.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print the action plan; do NOT execute.",
        )
        parser.add_argument(
            "--agent-id",
            default="agent-recovery-controller",
            help="Agent identifier for the AgentActionEvent.",
        )
        self.add_action_args(parser)
        return parser

    def run(self, argv: list[str] | None = None) -> int:
        """CLI entrypoint. Returns POSIX exit code."""
        parser = self._build_parser()
        args = parser.parse_args(argv)

        incident_key = args.incident_key or f"adhoc-{uuid.uuid4()}"
        scope_key = self.compute_scope_key(args)

        # Common metadata for the event.
        workspace_root = os.environ.get("WORKSPACE_ROOT", str(Path.cwd().parent))
        config_hash = _git_sha(workspace_root)
        code_version = _git_sha(str(Path(__file__).parent.parent.parent))
        scope_dict: dict[str, str] = self._extract_scope_dict(args)

        # ── Dry-run short-circuit ─────────────────────────────────────────
        if args.dry_run:
            plan = self.dry_run_plan(args)
            _logger.info(
                "DRY-RUN %s scope=%s plan=%s",
                self.ACTION_TYPE.value,
                scope_dict,
                plan,
            )
            event = AgentActionEmitter.build_event(
                event_id=f"evt-{uuid.uuid4()}",
                parent_incident_key=incident_key,
                agent_id=args.agent_id,
                action_type=self.ACTION_TYPE,
                action_status=ActionStatus.DRY_RUN,
                provenance=ActionProvenance(args.provenance),
                runbook_id=self.registry_entry.runbook_id,
                config_hash=config_hash,
                code_version=code_version,
                scope=scope_dict,
                post_action_state=plan,
            )
            self.emitter.emit(event)
            print(f"DRY-RUN OK: {plan}", file=sys.stderr)
            return 0

        # ── Repair-loop check ─────────────────────────────────────────────
        loop = _LOCAL_LOOP_DETECTOR.check(self.ACTION_TYPE, scope_key)
        if loop is not None:
            _logger.error(
                "Repair loop detected: action=%s scope=%s occurrences=%d/%ds",
                loop.action_type.value,
                loop.scope_key,
                loop.occurrences_in_window,
                loop.window_seconds,
            )
            event = AgentActionEmitter.build_event(
                event_id=f"evt-{uuid.uuid4()}",
                parent_incident_key=incident_key,
                agent_id=args.agent_id,
                action_type=self.ACTION_TYPE,
                action_status=ActionStatus.BLOCKED_BY_LOOP_DETECTOR,
                provenance=ActionProvenance(args.provenance),
                runbook_id=self.registry_entry.runbook_id,
                config_hash=config_hash,
                code_version=code_version,
                scope=scope_dict,
                failure_reason=(
                    f"RepeatedRepairLoopDetector: {loop.occurrences_in_window} "
                    f"identical actions within {loop.window_seconds}s"
                ),
            )
            self.emitter.emit(event)
            print(f"BLOCKED-BY-LOOP-DETECTOR: {loop}", file=sys.stderr)
            return 3  # distinct exit code for loop-detector bail

        # ── STARTED event ─────────────────────────────────────────────────
        event_id_started = f"evt-{uuid.uuid4()}"
        self.emitter.emit(
            AgentActionEmitter.build_event(
                event_id=event_id_started,
                parent_incident_key=incident_key,
                agent_id=args.agent_id,
                action_type=self.ACTION_TYPE,
                action_status=ActionStatus.STARTED,
                provenance=ActionProvenance(args.provenance),
                runbook_id=self.registry_entry.runbook_id,
                config_hash=config_hash,
                code_version=code_version,
                scope=scope_dict,
                pre_action_state={"reason": args.reason},
            )
        )

        # ── Real action ───────────────────────────────────────────────────
        try:
            status, post_action_state = self.execute_action(args)
            failure_reason = None
        except Exception as exc:  # noqa: BLE001 — capture & emit FAILED
            status = ActionStatus.FAILED
            post_action_state = {}
            failure_reason = repr(exc)
            _logger.exception("Layer-0 %s FAILED", self.ACTION_TYPE.value)

        # ── Final event ───────────────────────────────────────────────────
        self.emitter.emit(
            AgentActionEmitter.build_event(
                event_id=f"evt-{uuid.uuid4()}",
                parent_incident_key=incident_key,
                agent_id=args.agent_id,
                action_type=self.ACTION_TYPE,
                action_status=status,
                provenance=ActionProvenance(args.provenance),
                runbook_id=self.registry_entry.runbook_id,
                config_hash=config_hash,
                code_version=code_version,
                scope=scope_dict,
                post_action_state=post_action_state,
                failure_reason=failure_reason,
            )
        )

        if status == ActionStatus.FAILED:
            return 1
        return 0

    def _extract_scope_dict(self, args: argparse.Namespace) -> dict[str, str]:
        """Build the scope dict from required CLI args defined by the registry entry."""
        scope: dict[str, str] = {}
        for arg_name in self.registry_entry.scope_required:
            val = getattr(args, arg_name, None)
            if val is not None:
                scope[arg_name] = str(val)
        return scope
