# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Layer-0 ``disable_venue`` — circuit-breaker force-open for a venue.

Runbook: RB-CONN-001. Existing positions are NOT touched — use
cancel_open_orders first if needed.
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from typing import ClassVar

from unified_api_contracts.incident import ActionStatus, ActionType

from ._common import Layer0Script


class DisableVenue(Layer0Script):
    ACTION_TYPE: ClassVar[ActionType] = ActionType.DISABLE_VENUE

    def add_action_args(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--venue", required=True, help="Venue to force-disable.")
        parser.add_argument("--strategy", default=None, help="Scope to strategy (optional).")
        parser.add_argument(
            "--alerting-service-url",
            default=os.environ.get(
                "ALERTING_SERVICE_URL", "http://alerting-service.internal:8080"
            ),
        )

    def compute_scope_key(self, args: argparse.Namespace) -> str:
        if args.strategy:
            return f"venue:{args.venue}|strategy:{args.strategy}"
        return f"venue:{args.venue}"

    def dry_run_plan(self, args: argparse.Namespace) -> dict[str, str]:
        return {
            "action": "circuit_breaker_force_open",
            "venue": args.venue,
            "strategy": args.strategy or "*",
            "effect": "All new orders to venue rejected; positions untouched",
        }

    def execute_action(
        self, args: argparse.Namespace
    ) -> tuple[ActionStatus, dict[str, str | bool | int | float | None]]:
        url = f"{args.alerting_service_url}/admin/circuit-breaker/force-open"
        body = json.dumps({
            "venue": args.venue,
            "strategy": args.strategy,
            "reason": "Layer-0 disable_venue.py",
        }).encode("utf-8")
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
    sys.exit(DisableVenue().run())
