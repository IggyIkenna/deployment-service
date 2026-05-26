"""Layer-0 ``cancel_open_orders`` — execution-service cancel-all-orders.

NOT idempotent (orders disappear from venue once cancelled). Runbook: RB-RECON-002.
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from typing import ClassVar

from unified_api_contracts.incident import ActionStatus, ActionType

from ._common import Layer0Script


class CancelOpenOrders(Layer0Script):
    ACTION_TYPE: ClassVar[ActionType] = ActionType.CANCEL_OPEN_ORDERS

    def add_action_args(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--venue", required=True, help="Venue scope (required).")
        parser.add_argument("--strategy", default=None, help="Optional strategy scope filter.")
        parser.add_argument("--symbol", default=None, help="Optional symbol scope filter.")
        parser.add_argument(
            "--execution-service-url",
            default=os.environ.get(
                "EXECUTION_SERVICE_URL", "http://execution-service.internal:8080"
            ),
        )

    def compute_scope_key(self, args: argparse.Namespace) -> str:
        parts = [f"venue:{args.venue}"]
        if args.strategy:
            parts.append(f"strategy:{args.strategy}")
        if args.symbol:
            parts.append(f"symbol:{args.symbol}")
        return "|".join(parts)

    def dry_run_plan(self, args: argparse.Namespace) -> dict[str, str]:
        return {
            "action": "execution_service_cancel_all",
            "venue": args.venue,
            "strategy": args.strategy or "*",
            "symbol": args.symbol or "*",
            "effect": "Pull open orders from venue REST; cancel each individually",
        }

    def execute_action(
        self, args: argparse.Namespace
    ) -> tuple[ActionStatus, dict[str, str | bool | int | float | None]]:
        url = f"{args.execution_service_url}/admin/orders/cancel-all"
        body = json.dumps({
            "venue": args.venue,
            "strategy": args.strategy,
            "symbol": args.symbol,
            "reason": "Layer-0 cancel_open_orders.py",
        }).encode("utf-8")
        req = urllib.request.Request(
            url, data=body, method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = resp.read().decode("utf-8")
                return ActionStatus.SUCCEEDED, {
                    "http_status": resp.status,
                    "response_tail": payload[-300:],
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
    sys.exit(CancelOpenOrders().run())
