#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""AWS EC2 liveness watchdog — AWS twin of vm_zombie_watchdog.py.

Polls every backfill-class EC2 instance in ap-northeast-1 and terminates any
that match a zombie signature:
  1. Has been RUNNING for >= MIN_AGE_MINUTES
  2. AND either:
     a. Heartbeat blob (s3://.../vm-heartbeat/<vm>.txt) is missing OR mtime
        > HEARTBEAT_STALE_MINUTES old
     b. OR no manifest shard write in > SHARD_STALE_MINUTES (for prefix-registered VMs)

AWS analogue to vm_zombie_watchdog.py (which watches GCE VMs).

Differences from GCP version:
  - Uses boto3.client("ec2") instead of google.cloud.compute_v1
  - Heartbeat blob: s3://unified-trading-deployment-scripts-{account}/vm-heartbeat/{name}.txt
  - VM discovery: ec2.describe_instances(Filters=[purpose=uts-backfill])
  - Termination: ec2.terminate_instances (vs gcloud delete)
  - IAM dependency: needs sts:AssumeRole or ec2:DescribeInstances + s3:GetObject
    BLOCKED-AWS-PERMISSIONS pending 1.B IAM role creation.

VM_PREFIX_TO_BUCKET mirrors the GCP registry but with AWS S3 bucket names.
VMs not in the registry are still watched via heartbeat-only (same catch-all
as GCP version).

Daemon opt-out: tag tier=daemon on long-lived VMs (sports-scheduler, watchdog itself).

SSOT: deployment-service/scripts/vm/vm_zombie_watchdog_aws.py
Plan: api_keys_wallets_accounts_readiness_2026_05_10.md Phase 1.G
"""

from __future__ import annotations

import argparse
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import boto3  # noqa: TID251 (EC2 instance mgmt; no cloud-agnostic compute wrapper; rule targets storage)
from botocore.exceptions import ClientError
from unified_api_contracts import VmPrefixSpec
from unified_api_contracts.canonical.crosscutting import LifecycleClass
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AWS_REGION = "ap-northeast-1"
AWS_ACCOUNT_ID = "427895769566"
HEARTBEAT_BUCKET = f"unified-trading-deployment-scripts-{AWS_ACCOUNT_ID}"

MIN_AGE_MINUTES = 20
HEARTBEAT_STALE_MINUTES = 15
SHARD_STALE_MINUTES = 45
POLL_INTERVAL_SECONDS = 300  # 5-minute poll loop — same cadence as GCP watchdog


def _aws_bucket(kind: str, asset_group: str | None = None) -> str:
    """Resolve an AWS S3 shard-check bucket name from the SSOT yaml.

    Mirrors vm_zombie_watchdog.py's _b() but cloud="aws".
    """
    return resolve_bucket_name(cloud="aws", kind=kind, asset_group=asset_group)


# Pre-computed shard-bucket names (yaml SSOT via resolve_bucket_name).
_TICK_CEFI: str = _aws_bucket("market-data", "cefi")
_TICK_DEFI: str = _aws_bucket("market-data", "defi")
_TICK_TRADFI: str = _aws_bucket("market-data", "tradfi")
_TICK_SPORTS: str = _aws_bucket("market-data", "sports")
_TICK_PRED: str = _aws_bucket("market-data-tick-prediction")
_INSTR_CEFI: str = _aws_bucket("instruments-store", "cefi")
_INSTR_DEFI: str = _aws_bucket("instruments-store", "defi")
_INSTR_TRADFI: str = _aws_bucket("instruments-store", "tradfi")
_INSTR_SPORTS: str = _aws_bucket("instruments-store", "sports")
_FEAT_SPORTS: str = _aws_bucket("features-sports")

# VM prefix → VmPrefixSpec: manifest-shard S3 bucket + lifecycle_class.
# Mirrors vm_zombie_watchdog.py VM_PREFIX_TO_BUCKET (same prefixes, AWS bucket names).
# VmPrefixSpec(bucket=None) = heartbeat-only.
VM_PREFIX_TO_BUCKET: dict[str, VmPrefixSpec | None] = {
    # ------------------------------------------------------------------
    # CeFi MTDS backfill
    # ------------------------------------------------------------------
    "mtds-backfill-cefi-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-backfill-defi-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-backfill-tradfi-": VmPrefixSpec(bucket=_TICK_TRADFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-backfill-sports-": VmPrefixSpec(bucket=_TICK_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-backfill-prediction-": VmPrefixSpec(bucket=_TICK_PRED, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-perp-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-lend-idx-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-lst-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-dex-pools-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-eigenlayer-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-gas-fees-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-gas-fleet-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-liq-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-vault-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-sol-drift-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-sol-gas-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-pyth-arch-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-pyth-lst-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-sports-odds-": VmPrefixSpec(bucket=_TICK_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mtds-pred-": VmPrefixSpec(bucket=_TICK_PRED, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "tradfi-backfill-": VmPrefixSpec(bucket=_TICK_TRADFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "defi-backfill-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "jito-sol-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "marinade-sol-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # ------------------------------------------------------------------
    # Instruments service
    # ------------------------------------------------------------------
    "instr-backfill-cefi-": VmPrefixSpec(bucket=_INSTR_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "instr-backfill-defi-": VmPrefixSpec(bucket=_INSTR_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "instr-backfill-tradfi-": VmPrefixSpec(bucket=_INSTR_TRADFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "instr-backfill-sports-": VmPrefixSpec(bucket=_INSTR_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "instr-backfill-pred-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "instr-smoke-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "eu-enum-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "eu-v2-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "sports-instr-ref-": VmPrefixSpec(bucket=_INSTR_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "manifest-cons-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "manifest-recon-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "manifest-apply-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "defi-phantom-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "sports-mfst-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # ------------------------------------------------------------------
    # CeFi/TradFi sharded backfill (launch-cefi-sharded-backfill-aws.sh)
    # VM names: cefi-sharded-{venue}-{year}-{group}-{YYYYMMDD}
    #           tradfi-sharded-{instrument}-{year}-{group}-{YYYYMMDD}
    # ------------------------------------------------------------------
    "cefi-sharded-": VmPrefixSpec(bucket=_TICK_CEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "tradfi-sharded-": VmPrefixSpec(bucket=_TICK_TRADFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # ------------------------------------------------------------------
    # Features service
    # ------------------------------------------------------------------
    "features-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "feats-backfill-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "feats-onchain-": VmPrefixSpec(bucket=_TICK_DEFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "feats-sports-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "feats-sports-par-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "feats-sfi-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "feats-sfi-prog-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "feats-live-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.SCHEDULED_RECURRING),
    "sports-ent-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "sports-full-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "sports-sched-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "api-fb-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "footystats-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "fill-player-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "fix-recov-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "fix-audit-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "transfermkt-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "understat-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "openmeteo-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "pred-feats-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "pred-pipe-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # ------------------------------------------------------------------
    # Strategy / Execution
    # ------------------------------------------------------------------
    "strat-live-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "strat-paper-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "strat-test-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "strat-grid-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "defi-backtest-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "defi-paper-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "defi-recurse-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "exec-alpha-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "scenario-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "aave-val-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "amm-val-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # ------------------------------------------------------------------
    # ML
    # ------------------------------------------------------------------
    "ml-train-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "synth-bench-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    # ------------------------------------------------------------------
    # Infrastructure / ops (heartbeat-only — no shard writes)
    # ------------------------------------------------------------------
    "gov-backfill-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "qg-snap-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "canon-migr-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "canon-smoke-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cefi-migr-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "gcs-migr-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "bl-recon-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.SCHEDULED_RECURRING),
    "blank-recon-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "bucket-rsync-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "cross-rescan-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "honest-cov-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "measure-hcov-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "dashboard-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "dr-drill-cron-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.SCHEDULED_RECURRING),
    "dr-cutover-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "client-rep-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "wallet-treas-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "tradfi-sess-": VmPrefixSpec(bucket=_TICK_TRADFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "tradfi-stamp-": VmPrefixSpec(bucket=_TICK_TRADFI, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mdps-backfill-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "mdps-sports-": VmPrefixSpec(bucket=_FEAT_SPORTS, lifecycle_class=LifecycleClass.EPHEMERAL_BATCH),
    "aws-zombie-watchdog-": None,  # the watchdog itself — daemon opt-out via purpose tag
    # Orchestrator epic VM fleet (LONG_LIVED_LIVE — permanent worker nodes)
    "agent-orch-vm-defi-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-cefi-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-tradfi-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-sports-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-prediction-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-ml-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-trading-core-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-operator-ops-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-cross-cutting-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-orchestrator-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
}

# Daemon-opt-out purposes (never reaped by catch-all sweep)
DAEMON_PURPOSES = frozenset({"vm-zombie-watchdog-aws", "sports-scheduler"})


@dataclass
class Ec2Instance:
    instance_id: str
    name: str
    state: str
    launch_time: datetime
    purpose: str | None
    tier: str | None


def _get_tag(tags: list[dict[str, str]], key: str) -> str | None:
    for t in tags or []:
        if t.get("Key") == key:
            return t.get("Value")
    return None


def _list_running_instances(ec2: boto3.client) -> list[Ec2Instance]:
    """List all running/pending EC2 instances tagged purpose=uts-backfill."""
    paginator = ec2.get_paginator("describe_instances")
    instances: list[Ec2Instance] = []
    for page in paginator.paginate(
        Filters=[
            {"Name": "instance-state-name", "Values": ["running", "pending"]},
        ]
    ):
        for reservation in page["Reservations"]:
            for inst in reservation["Instances"]:
                tags = inst.get("Tags", [])
                purpose = _get_tag(tags, "purpose")
                tier = _get_tag(tags, "tier")
                name = _get_tag(tags, "Name") or inst["InstanceId"]
                instances.append(
                    Ec2Instance(
                        instance_id=inst["InstanceId"],
                        name=name,
                        state=inst["State"]["Name"],
                        launch_time=inst["LaunchTime"].replace(tzinfo=UTC),
                        purpose=purpose,
                        tier=tier,
                    )
                )
    return instances


def _heartbeat_stale(s3: boto3.client, vm_name: str, stale_minutes: int) -> bool:
    """Return True if the heartbeat blob is missing or older than stale_minutes."""
    key = f"vm-heartbeat/{vm_name}.txt"
    try:
        obj = s3.head_object(Bucket=HEARTBEAT_BUCKET, Key=key)
        last_modified: datetime = obj["LastModified"].replace(tzinfo=UTC)
        age = datetime.now(UTC) - last_modified
        return age > timedelta(minutes=stale_minutes)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return True
        raise


def _shard_stale(s3: boto3.client, bucket: str, vm_name: str, stale_minutes: int) -> bool:
    """Return True if the per-VM manifest shard is missing or older than stale_minutes."""
    key = f"_index/per_vm/{vm_name}.parquet"
    try:
        obj = s3.head_object(Bucket=bucket, Key=key)
        last_modified: datetime = obj["LastModified"].replace(tzinfo=UTC)
        age = datetime.now(UTC) - last_modified
        return age > timedelta(minutes=stale_minutes)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return True
        raise


def _prefix_spec(name: str) -> VmPrefixSpec | None | bool:
    """Return the VmPrefixSpec for the longest matching prefix, or False if not registered."""
    best_len = 0
    best: VmPrefixSpec | None | bool = False
    for prefix, spec in VM_PREFIX_TO_BUCKET.items():
        if name.startswith(prefix) and len(prefix) > best_len:
            best_len = len(prefix)
            best = spec
    return best


def _is_zombie(
    ec2: boto3.client,
    s3: boto3.client,
    inst: Ec2Instance,
    min_age: int,
    heartbeat_stale: int,
    shard_stale: int,
) -> tuple[bool, str]:
    """Return (is_zombie, reason) for the given instance."""
    age = datetime.now(UTC) - inst.launch_time
    if age < timedelta(minutes=min_age):
        return False, f"too young ({int(age.total_seconds() / 60)}min < {min_age}min)"

    # Daemon opt-out
    if inst.tier == "daemon" or inst.purpose in DAEMON_PURPOSES:
        return False, "daemon (opt-out)"

    spec = _prefix_spec(inst.name)

    # Registered prefix with VmPrefixSpec(bucket=...)
    if spec is not False and spec is not None:
        hb_stale = _heartbeat_stale(s3, inst.name, heartbeat_stale)
        sh_stale = _shard_stale(s3, spec.bucket, inst.name, shard_stale)
        if hb_stale and sh_stale:
            return True, f"heartbeat+shard both stale > {heartbeat_stale}/{shard_stale}min"
        if hb_stale:
            return True, f"heartbeat stale > {heartbeat_stale}min"
        return False, "ok (heartbeat fresh)"

    # Registered prefix with bucket=None → heartbeat-only
    if spec is not False and spec is None:
        hb_stale = _heartbeat_stale(s3, inst.name, heartbeat_stale)
        if hb_stale:
            return True, f"heartbeat stale > {heartbeat_stale}min (heartbeat-only)"
        return False, "ok (heartbeat fresh)"

    # spec is False → catch-all: check heartbeat only
    hb_stale = _heartbeat_stale(s3, inst.name, heartbeat_stale)
    if hb_stale:
        return True, f"catch-all: heartbeat stale > {heartbeat_stale}min"
    return False, "ok (catch-all heartbeat)"


def _check_and_reap(
    ec2: boto3.client,
    s3: boto3.client,
    inst: Ec2Instance,
    dry_run: bool,
    min_age: int,
    heartbeat_stale: int,
    shard_stale: int,
) -> str:
    is_zombie, reason = _is_zombie(ec2, s3, inst, min_age, heartbeat_stale, shard_stale)
    if not is_zombie:
        return f"[ALIVE] {inst.name} ({inst.instance_id}): {reason}"

    action = "WOULD TERMINATE" if dry_run else "TERMINATING"
    logger.warning("%s zombie: %s (%s) — %s", action, inst.name, inst.instance_id, reason)

    if not dry_run:
        try:
            ec2.terminate_instances(InstanceIds=[inst.instance_id])
        except ClientError as exc:
            return f"[ERROR] {inst.name} ({inst.instance_id}): terminate failed — {exc}"

    return f"[{'DRY-RUN' if dry_run else 'REAPED'}] {inst.name} ({inst.instance_id}): {reason}"


def run_sweep(
    *,
    region: str,
    dry_run: bool,
    min_age: int,
    heartbeat_stale: int,
    shard_stale: int,
    workers: int,
) -> int:
    """Run one zombie sweep. Returns number of VMs reaped (or would reap in dry-run)."""
    ec2 = boto3.client("ec2", region_name=region)
    s3 = boto3.client("s3", region_name=region)

    instances = _list_running_instances(ec2)
    logger.info("Found %d running/pending EC2 instances", len(instances))

    reaped = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_check_and_reap, ec2, s3, inst, dry_run, min_age, heartbeat_stale, shard_stale): inst
            for inst in instances
        }
        for fut in as_completed(futures):
            result = fut.result()
            if "[REAPED]" in result or ("[DRY-RUN] " in result and "zombie" in result.lower()):
                reaped += 1
            logger.info(result)

    return reaped


def main() -> None:
    parser = argparse.ArgumentParser(description="AWS EC2 zombie watchdog")
    parser.add_argument("--region", default=AWS_REGION)
    parser.add_argument("--dry-run", action="store_true", help="Report zombies but do not terminate")
    parser.add_argument("--once", action="store_true", help="Run one sweep and exit (default: poll loop)")
    parser.add_argument("--min-age", type=int, default=MIN_AGE_MINUTES)
    parser.add_argument("--heartbeat-stale", type=int, default=HEARTBEAT_STALE_MINUTES)
    parser.add_argument("--shard-stale", type=int, default=SHARD_STALE_MINUTES)
    parser.add_argument("--poll-interval", type=int, default=POLL_INTERVAL_SECONDS)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()

    if args.dry_run:
        logger.info("DRY-RUN mode — no instances will be terminated")

    while True:
        try:
            reaped = run_sweep(
                region=args.region,
                dry_run=args.dry_run,
                min_age=args.min_age,
                heartbeat_stale=args.heartbeat_stale,
                shard_stale=args.shard_stale,
                workers=args.workers,
            )
            logger.info("Sweep complete — reaped=%d", reaped)
        except Exception as exc:
            logger.error("Sweep error: %s", exc, exc_info=True)

        if args.once:
            break

        logger.info("Next sweep in %ds", args.poll_interval)
        time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()


# Per Runbook Execution-Owner SSOT HARD RULE
# owner: deployment-service maintainer
# cadence: long-lived daemon VM (launch-ec2-vm.sh --task aws-zombie-watchdog)
# verifier: python vm_zombie_watchdog_aws.py --dry-run --once (exit 0 = no errors)
# last_executed: NEVER (pending 1.B IAM role creation + AWS_SECURITY_GROUP_IDS/AWS_SUBNET_ID config)
