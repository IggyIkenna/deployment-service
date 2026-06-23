# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Layer-0 ``restart_service`` — Cloud Run revision flip OR GCE systemctl restart.

Runbook: RB-INFRA-001.
"""

from __future__ import annotations

import argparse
import os
import subprocess
from typing import ClassVar

from unified_api_contracts.incident import ActionStatus, ActionType

from ._common import Layer0Script


class RestartService(Layer0Script):
    ACTION_TYPE: ClassVar[ActionType] = ActionType.RESTART_SERVICE

    def add_action_args(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--service", required=True,
            help="Service name (Cloud Run service name OR systemd unit name)."
        )
        parser.add_argument(
            "--mode", choices=["cloud_run", "systemd"], default="cloud_run",
            help="Deployment topology of the service."
        )
        parser.add_argument(
            "--region", default=os.environ.get("GCP_REGION", "asia-northeast1"),
            help="GCP region for cloud_run mode."
        )

    def compute_scope_key(self, args: argparse.Namespace) -> str:
        return f"service:{args.service}"

    def dry_run_plan(self, args: argparse.Namespace) -> dict[str, str]:
        if args.mode == "cloud_run":
            return {
                "action": "gcloud_run_services_update",
                "service": args.service,
                "region": args.region,
                "effect": "no-op redeploy → triggers new revision",
            }
        return {
            "action": "systemctl_restart",
            "unit": args.service,
            "effect": "service process restart",
        }

    def execute_action(
        self, args: argparse.Namespace
    ) -> tuple[ActionStatus, dict[str, str | bool | int | float | None]]:
        if args.mode == "cloud_run":
            cmd = [
                "gcloud", "run", "services", "update", args.service,
                "--region", args.region, "--no-traffic",
                "--update-labels", f"recovery-restart={int.from_bytes(os.urandom(2), 'big')}",
            ]
        else:
            cmd = ["sudo", "systemctl", "restart", args.service]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            return ActionStatus.FAILED, {"stderr": result.stderr[:500]}
        return ActionStatus.SUCCEEDED, {"stdout_tail": result.stdout[-300:]}


if __name__ == "__main__":
    import sys
    sys.exit(RestartService().run())
