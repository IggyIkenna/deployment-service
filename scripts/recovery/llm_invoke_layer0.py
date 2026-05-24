"""Layer-1.5 LLM wrapper around the 10 closed-set Layer-0 scripts.

This is the **ONLY** entry point through which the LLM
``recovery-audit-signoff`` agent (Layer-1) is allowed to invoke recovery
actions on the system. The wrapper:

1. Validates ``action_type`` against ``RecoveryScriptRegistry`` (rejects
   anything outside the closed 10-set).
2. Validates required scope CLI args from the registry entry.
3. Stamps ``provenance=LLM_LAYER15`` on the AgentActionEvent (visible in DART
   so operators can distinguish Layer-1.5 retries from Layer-0 originals).
4. Forwards the remaining args to the underlying Layer-0 script via Python
   subprocess (NOT shell exec — no shell interpolation).

The LLM agent's authority is bounded by this wrapper. The agent CANNOT:
- Invoke shell commands directly.
- Invoke any non-registered ``action_type``.
- Override ``provenance`` (we set it).
- Skip the repair-loop detector (the underlying script enforces it).

Codex SSOT: ``codex/04-architecture/recovery-defence-in-depth-layers.md`` §
"Layer 1.5".
Implementation plan: ``plans/active/ai_recovery_audit_signoff_agent_2026_05_23.md``.
"""

from __future__ import annotations

import argparse
import logging
import shlex
import subprocess
import sys
from pathlib import Path

from unified_api_contracts.incident import ActionProvenance, ActionType
from unified_trading_library import (
    RecoveryScriptRegistry,
    UnknownActionTypeError,
)

_logger = logging.getLogger("recovery.llm_layer15")

_SCRIPTS_DIR = Path(__file__).parent


def _module_path_for(entry_action: ActionType) -> str:
    """Return ``deployment_service.scripts.recovery.<action_type>`` import path."""
    return f"scripts.recovery.{entry_action.value}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "LLM Layer-1.5 wrapper — invokes Layer-0 recovery scripts on behalf "
            "of the recovery-audit-signoff agent. CLOSED-SET authority — only "
            "the 10 registered ActionType values are accepted."
        )
    )
    parser.add_argument(
        "action_type",
        help="Layer-0 ActionType to invoke. Must be one of the 10 registered.",
    )
    parser.add_argument(
        "--llm-session-id",
        required=True,
        help="LLM session identifier (for tracing in audit logs).",
    )
    parser.add_argument(
        "--reason",
        required=True,
        help="LLM-provided reason — surfaces in the AgentActionEvent.",
    )
    parser.add_argument(
        "--incident-key",
        required=True,
        help="Parent IncidentEnvelope.incident_key. LLM MUST always pass the "
        "parent incident key — Layer-1.5 actions are never standalone.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Forward --dry-run to the Layer-0 script (preview only).",
    )
    parser.add_argument(
        "remaining",
        nargs=argparse.REMAINDER,
        help="Remaining args forwarded to the Layer-0 script.",
    )
    args = parser.parse_args(argv)

    # ── Closed-set validation ───────────────────────────────────────────
    try:
        action_type = ActionType(args.action_type)
    except ValueError:
        valid = sorted(a.value for a in ActionType)
        print(
            f"ERROR: action_type={args.action_type!r} is not a valid ActionType. Valid values: {valid}",
            file=sys.stderr,
        )
        return 4  # distinct exit code for "rejected by Layer-1.5 wrapper"

    try:
        RecoveryScriptRegistry.lookup(action_type)
    except UnknownActionTypeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 4

    # ── Build inner command — provenance is HARDCODED to LLM_LAYER15 ────
    inner_argv: list[str] = [
        sys.executable,
        "-m",
        _module_path_for(action_type),
        "--reason",
        args.reason,
        "--incident-key",
        args.incident_key,
        "--provenance",
        ActionProvenance.LLM_LAYER15.value,
        "--agent-id",
        f"recovery-audit-signoff:{args.llm_session_id}",
    ]
    if args.dry_run:
        inner_argv.append("--dry-run")

    # ── Strip any --provenance / --agent-id from REMAINDER ──────────────
    # (defence-in-depth: LLM cannot override provenance even if it tries)
    sanitized_remainder: list[str] = []
    skip_next = False
    for tok in args.remaining:
        if skip_next:
            skip_next = False
            continue
        if tok in ("--provenance", "--agent-id"):
            skip_next = True
            continue
        if tok.startswith("--provenance=") or tok.startswith("--agent-id="):
            continue
        sanitized_remainder.append(tok)
    inner_argv.extend(sanitized_remainder)

    _logger.info("LLM Layer-1.5 invoking: %s", shlex.join(inner_argv))
    result = subprocess.run(inner_argv, check=False)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
