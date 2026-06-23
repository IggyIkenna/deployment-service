#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Generate mock deployment data for local dev / CI mock mode.

Creates deterministic mock VM records, shard configs, deployment history,
and health check results. Output goes to the local dev cache (or GCS in dev)
via SeedWriter — the same pattern every other service uses.

deployment-api loads this via MockStateStore.seed() in mock_state.py.

Usage:
    python scripts/seed_mock_data.py --scenario normal --seed 42 --env local
    python scripts/seed_mock_data.py --scenario heavy --seed 42
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
from datetime import UTC, datetime, timedelta

from unified_api_contracts.internal.modes import MockScenario
from unified_api_contracts.internal.testing.scenario_config import ScenarioConfig
from unified_trading_library import get_seed_writer

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

SERVICES = [
    "instruments-service",
    "market-tick-data-service",
    "features-delta-one-service",
    "execution-service",
    "strategy-service",
    "ml-service",
    "alerting-service",
]

REGIONS = ["us-central1", "asia-northeast1", "europe-west1"]
MACHINE_TYPES = ["e2-standard-4", "e2-standard-8", "n2-standard-16"]
VM_STATES = ["STAGING", "RUNNING", "STOPPING", "TERMINATED", "FAILED"]
SHARD_STATUSES = ["pending", "running", "succeeded", "failed", "cancelled"]
DEPLOYMENT_STATUSES = [
    "pending",
    "running",
    "completed",
    "completed_with_errors",
    "failed",
]


def _generate_deployments(rng: random.Random, count: int) -> list[dict[str, object]]:
    """Generate mock deployment history with state machine transitions."""
    deployments: list[dict[str, object]] = []
    base_time = datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC)
    for i in range(count):
        started = base_time + timedelta(days=i, hours=rng.randint(0, 23))
        status = rng.choice(DEPLOYMENT_STATUSES)
        duration = rng.randint(60, 7200)
        service = rng.choice(SERVICES)
        shard_count = rng.randint(2, 8)

        deployment: dict[str, object] = {
            "id": f"deploy-{i + 1:03d}",
            "deployment_id": f"deploy-{i + 1:03d}",
            "service": service,
            "status": status,
            "region": rng.choice(REGIONS),
            "shard_count": shard_count,
            "started_at": started.isoformat(),
            "completed_at": (started + timedelta(seconds=duration)).isoformat(),
            "duration_seconds": duration,
            "triggered_by": rng.choice(["scheduled", "manual", "ci"]),
            "version": f"0.{rng.randint(1, 9)}.{rng.randint(0, 20)}",
        }
        if status in ("failed", "completed_with_errors"):
            deployment["error_summary"] = rng.choice(
                [
                    "Health gate timeout after 300s",
                    "Quota exhaustion: CPU limit reached",
                    f"OOM in shard {rng.randint(0, shard_count - 1)} of {shard_count}",
                    "Cross-region failover triggered",
                ]
            )
        deployments.append(deployment)
    return deployments


def _generate_shards(rng: random.Random, deployments: list[dict[str, object]]) -> list[dict[str, object]]:
    """Generate shard records for each deployment."""
    shards: list[dict[str, object]] = []
    for dep in deployments:
        dep_id = str(dep["deployment_id"])
        shard_count = int(dep.get("shard_count", 4))
        for s in range(shard_count):
            status = rng.choice(SHARD_STATUSES)
            shard: dict[str, object] = {
                "id": f"{dep_id}-shard-{s}",
                "deployment_id": dep_id,
                "shard_index": s,
                "shard_count": shard_count,
                "status": status,
                "service": dep["service"],
                "region": dep["region"],
                "started_at": dep["started_at"],
                "duration_seconds": rng.randint(30, 3600) if status != "pending" else 0,
            }
            if status == "failed":
                shard["failure_reason"] = rng.choice(["OOM", "timeout", "crash", "health_gate_timeout"])
            shards.append(shard)
    return shards


def _generate_vms(rng: random.Random, deployments: list[dict[str, object]]) -> list[dict[str, object]]:
    """Generate mock VM records linked to deployments."""
    vms: list[dict[str, object]] = []
    for i, dep in enumerate(deployments):
        shard_count = int(dep.get("shard_count", 4))
        for s in range(shard_count):
            state = rng.choice(VM_STATES)
            vm: dict[str, object] = {
                "id": f"vm-{i:04d}-{s}",
                "vm_id": f"vm-{i:04d}-{s}",
                "name": f"deploy-{dep['service']}-{i:03d}-{s}",
                "deployment_id": dep["deployment_id"],
                "shard_index": s,
                "region": str(dep["region"]),
                "machine_type": rng.choice(MACHINE_TYPES),
                "state": state,
                "created_at": dep["started_at"],
            }
            if state == "FAILED":
                vm["failure_reason"] = rng.choice(["OOM", "timeout", "crash", "quota_exceeded"])
            vms.append(vm)
    return vms


def _generate_services(rng: random.Random) -> list[dict[str, object]]:
    """Generate mock service records for the deployment dashboard."""
    services: list[dict[str, object]] = []
    for svc in SERVICES:
        services.append(
            {
                "id": svc,
                "name": svc,
                "status": rng.choice(["healthy", "healthy", "degraded"]),
                "version": f"0.{rng.randint(1, 9)}.{rng.randint(0, 30)}",
                "region": rng.choice(REGIONS),
                "shard_count": rng.randint(1, 6),
                "last_deployed_at": (datetime(2026, 3, 1, tzinfo=UTC) + timedelta(days=rng.randint(0, 20))).isoformat(),
            }
        )
    return services


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate mock deployment data")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--scenario", default="normal")
    parser.add_argument("--env", default="local", choices=["local", "dev"])
    parser.add_argument("--output-dir", default="")
    args = parser.parse_args()

    scenario_enum = MockScenario(args.scenario)
    cfg = ScenarioConfig.load(scenario_enum)
    effective_seed = cfg.seed
    rng = random.Random(effective_seed)

    scale = {"normal": 1, "heavy": 3, "stress": 5, "light": 1}.get(args.scenario, 1)
    deployment_count = 20 * scale

    log.info(
        "Generating deployment mock data: scenario=%s seed=%d deployments=%d",
        args.scenario,
        effective_seed,
        deployment_count,
    )

    deployments = _generate_deployments(rng, deployment_count)
    shards = _generate_shards(rng, deployments)
    vms = _generate_vms(rng, deployments)
    services = _generate_services(rng)

    writer = get_seed_writer(args.env, "deployment-service", args.output_dir or None)

    writer.write_json(deployments, "deployments.json")
    writer.write_json(shards, "shards.json")
    writer.write_json(vms, "vms.json")
    writer.write_json(services, "services.json")

    manifest: dict[str, object] = {
        "scenario": args.scenario,
        "seed": effective_seed,
        "counts": {
            "deployments": len(deployments),
            "shards": len(shards),
            "vms": len(vms),
            "services": len(services),
        },
        "generator": "deployment-service/scripts/seed_mock_data.py",
    }
    writer.write_json(manifest, "seed_manifest.json")

    log.info(
        "Done: %d deployments, %d shards, %d VMs, %d services",
        len(deployments),
        len(shards),
        len(vms),
        len(services),
    )


if __name__ == "__main__":
    sys.exit(main() or 0)
