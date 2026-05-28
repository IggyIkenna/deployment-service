"""Layer-0 ``enter_safe_mode`` — strategy-service safe-mode entry.

Runbook: RB-RISK-004.
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from typing import ClassVar

from unified_api_contracts.incident import ActionStatus, ActionType

from ._common import Layer0Script


class EnterSafeMode(Layer0Script):
    ACTION_TYPE: ClassVar[ActionType] = ActionType.ENTER_SAFE_MODE

    def add_action_args(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--strategy", required=True, help="strategy_id.")
        parser.add_argument(
            "--strategy-service-url",
            default=os.environ.get(
                "STRATEGY_SERVICE_URL", "http://strategy-service.internal:8080"
            ),
        )

    def compute_scope_key(self, args: argparse.Namespace) -> str:
        return f"safe_mode:strategy:{args.strategy}"

    def dry_run_plan(self, args: argparse.Namespace) -> dict[str, str]:
        return {
            "action": "strategy_safe_mode_enter",
            "strategy": args.strategy,
            "effect": (
                "Per-strategy policy: pause new orders, cancel-or-retain existing "
                "orders, confirm positions and hedges. Human-resume required."
            ),
        }

    def execute_action(
        self, args: argparse.Namespace
    ) -> tuple[ActionStatus, dict[str, str | bool | int | float | None]]:
        url = f"{args.strategy_service_url}/admin/strategy/{args.strategy}/safe-mode"
        body = json.dumps({"reason": "Layer-0 enter_safe_mode.py"}).encode("utf-8")
        req = urllib.request.Request(
            url, data=body, method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return ActionStatus.SUCCEEDED, {
                    "http_status": resp.status,
                    "response_tail": resp.read().decode("utf-8")[-300:],
                }
        except urllib.error.HTTPError as e:
            return ActionStatus.FAILED, {
                "http_status": e.code,
                "stderr": e.read().decode("utf-8", errors="replace")[:500],
            }
        except (urllib.error.URLError, TimeoutError) as e:
            return ActionStatus.FAILED, {"error": repr(e)[:300]}


if __name__ == "__main__":
    import sys
    sys.exit(EnterSafeMode().run())
