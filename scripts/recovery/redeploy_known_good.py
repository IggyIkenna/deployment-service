# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Layer-0 ``redeploy_known_good`` — flip Cloud Run traffic to a previous revision.

Runbook: RB-DEPLOY-001.
"""

from __future__ import annotations

import argparse
import os
import subprocess
from typing import ClassVar

from unified_api_contracts.incident import ActionStatus, ActionType

from ._common import Layer0Script


class RedeployKnownGood(Layer0Script):
    ACTION_TYPE: ClassVar[ActionType] = ActionType.REDEPLOY_KNOWN_GOOD

    def add_action_args(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--service", required=True, help="Cloud Run service name.")
        parser.add_argument(
            "--to-revision",
            required=True,
            dest="to_revision",
            help="Target Cloud Run revision to receive 100%% of traffic.",
        )
        parser.add_argument(
            "--region", default=os.environ.get("GCP_REGION", "asia-northeast1")
        )

    def compute_scope_key(self, args: argparse.Namespace) -> str:
        return f"service:{args.service}"

    def dry_run_plan(self, args: argparse.Namespace) -> dict[str, str]:
        return {
            "action": "cloud_run_traffic_flip",
            "service": args.service,
            "to_revision": args.to_revision,
            "region": args.region,
            "effect": "100% traffic → target revision; previous revision retained",
        }

    def execute_action(
        self, args: argparse.Namespace
    ) -> tuple[ActionStatus, dict[str, str | bool | int | float | None]]:
        cmd = [
            "gcloud", "run", "services", "update-traffic", args.service,
            "--region", args.region,
            "--to-revisions", f"{args.to_revision}=100",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            return ActionStatus.FAILED, {"stderr": result.stderr[:500]}
        return ActionStatus.SUCCEEDED, {
            "to_revision": args.to_revision,
            "stdout_tail": result.stdout[-300:],
        }


if __name__ == "__main__":
    import sys
    sys.exit(RedeployKnownGood().run())
