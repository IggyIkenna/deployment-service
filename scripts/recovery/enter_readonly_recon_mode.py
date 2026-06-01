"""Layer-0 ``enter_readonly_recon_mode`` — service reads but rejects writes.

Used when a service has degraded persistence / reconciliation state and must
keep serving reads to consumers (e.g. DART operator views) without risking
writes. Runbook: RB-CONN-004.
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from typing import ClassVar

from unified_api_contracts.incident import ActionStatus, ActionType

from ._common import Layer0Script


class EnterReadonlyReconMode(Layer0Script):
    ACTION_TYPE: ClassVar[ActionType] = ActionType.ENTER_READONLY_RECON_MODE

    def add_action_args(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--service", required=True, help="Service to flip to readonly.")
        parser.add_argument(
            "--service-url-template",
            default=os.environ.get(
                "RECOVERY_SERVICE_URL_TEMPLATE",
                "http://{service}.internal:8080",
            ),
            help=(
                "URL template — ``{service}`` is replaced with --service. "
                "Override per-environment in env var RECOVERY_SERVICE_URL_TEMPLATE."
            ),
        )

    def compute_scope_key(self, args: argparse.Namespace) -> str:
        return f"readonly_recon:service:{args.service}"

    def dry_run_plan(self, args: argparse.Namespace) -> dict[str, str]:
        return {
            "action": "service_readonly_mode_enter",
            "service": args.service,
            "effect": (
                "Reads continue; writes return 503. Caller must hit "
                "/admin/mode/readonly endpoint with reason."
            ),
        }

    def execute_action(
        self, args: argparse.Namespace
    ) -> tuple[ActionStatus, dict[str, str | bool | int | float | None]]:
        url = f"{args.service_url_template.format(service=args.service)}/admin/mode/readonly"
        body = json.dumps({"reason": "Layer-0 enter_readonly_recon_mode.py"}).encode("utf-8")
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
    sys.exit(EnterReadonlyReconMode().run())
