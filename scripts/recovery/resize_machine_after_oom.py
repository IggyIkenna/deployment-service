# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Layer-0 ``resize_machine_after_oom`` — GCE instance machine-type resize.

Runbook: RB-INFRA-001.
"""

from __future__ import annotations

import argparse
import os
import subprocess
from typing import ClassVar

from unified_api_contracts.incident import ActionStatus, ActionType

from ._common import Layer0Script


class ResizeMachineAfterOom(Layer0Script):
    ACTION_TYPE: ClassVar[ActionType] = ActionType.RESIZE_MACHINE_AFTER_OOM

    def add_action_args(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--vm", required=True, help="GCE instance name.")
        parser.add_argument(
            "--new-machine-type", required=True, dest="new_machine_type",
            help="e.g. e2-standard-4 → e2-standard-8.",
        )
        parser.add_argument(
            "--zone", default=os.environ.get("GCP_ZONE", "asia-northeast1-c")
        )

    def compute_scope_key(self, args: argparse.Namespace) -> str:
        return f"vm:{args.vm}"

    def dry_run_plan(self, args: argparse.Namespace) -> dict[str, str]:
        return {
            "action": "gcloud_compute_instances_set_machine_type",
            "vm": args.vm,
            "new_machine_type": args.new_machine_type,
            "zone": args.zone,
            "effect": "VM stopped → machine-type changed → VM started",
        }

    def execute_action(
        self, args: argparse.Namespace
    ) -> tuple[ActionStatus, dict[str, str | bool | int | float | None]]:
        # Stop, resize, start. GCE requires the instance to be stopped.
        steps = [
            ["gcloud", "compute", "instances", "stop", args.vm, "--zone", args.zone],
            [
                "gcloud", "compute", "instances", "set-machine-type", args.vm,
                "--zone", args.zone, "--machine-type", args.new_machine_type,
            ],
            ["gcloud", "compute", "instances", "start", args.vm, "--zone", args.zone],
        ]
        for cmd in steps:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if r.returncode != 0:
                return ActionStatus.FAILED, {
                    "failed_step": " ".join(cmd[:3]),
                    "stderr": r.stderr[:500],
                }
        return ActionStatus.SUCCEEDED, {
            "new_machine_type": args.new_machine_type,
        }


if __name__ == "__main__":
    import sys
    sys.exit(ResizeMachineAfterOom().run())
