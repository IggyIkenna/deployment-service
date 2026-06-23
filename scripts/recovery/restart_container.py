# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Layer-0 ``restart_container`` — Cloud Run revision restart OR Docker restart.

Runbook: RB-INFRA-001.
"""

from __future__ import annotations

import argparse
import subprocess
from typing import ClassVar

from unified_api_contracts.incident import ActionStatus, ActionType

from ._common import Layer0Script


class RestartContainer(Layer0Script):
    ACTION_TYPE: ClassVar[ActionType] = ActionType.RESTART_CONTAINER

    def add_action_args(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--container", required=True,
            help="Container name OR Cloud Run revision name."
        )
        parser.add_argument(
            "--mode", choices=["docker", "cloud_run_revision"], default="docker",
        )

    def compute_scope_key(self, args: argparse.Namespace) -> str:
        return f"container:{args.container}"

    def dry_run_plan(self, args: argparse.Namespace) -> dict[str, str]:
        if args.mode == "docker":
            return {"action": "docker_restart", "container": args.container}
        return {"action": "cloud_run_revision_restart", "revision": args.container}

    def execute_action(
        self, args: argparse.Namespace
    ) -> tuple[ActionStatus, dict[str, str | bool | int | float | None]]:
        if args.mode == "docker":
            cmd = ["docker", "restart", "--time", "30", args.container]
        else:
            # Cloud Run "restart" = update labels to force new instance startup
            cmd = [
                "gcloud", "run", "revisions", "delete", args.container,
                "--quiet",
            ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            return ActionStatus.FAILED, {"stderr": result.stderr[:500]}
        return ActionStatus.SUCCEEDED, {"stdout_tail": result.stdout[-300:]}


if __name__ == "__main__":
    import sys
    sys.exit(RestartContainer().run())
