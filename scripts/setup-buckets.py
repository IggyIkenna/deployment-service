#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""
Setup Storage Buckets for Unified Trading System

This script provisions all required storage buckets (GCS or S3) for the Unified
Trading System.

Rewritten 2026-07-14 (`bucket_estate_consolidation_to_sub100_2026_07_13.md`
Deferred #8) onto the canonical resolver:

* Service/data bucket names now come from the UTL SSOT resolver
  (``unified_trading_library.resolve_bucket_name``) against
  ``configs/cloud-providers.yaml`` — this script enumerates every
  ``<cloud>.storage`` kind x asset_group exactly like
  ``terraform/gcp/canonical_buckets.tf``'s ``for_each`` derivation (same
  excluded-kinds set: ``audit-records`` / ``manual-audit`` are retention-locked,
  hand-managed compliance buckets). The pre-canonical ``dependencies.yaml``
  ``bucket_template`` scheme (``{category_lower}`` placeholders the old local
  resolver never substituted) is NOT used here any more — ``dependencies.yaml``
  is untouched because ``deployment_service.dependencies.DependencyLoader``
  (a genuinely separate consumer) still reads those fields for its own
  dependency-check machinery.
* Tiers are ``prd`` + ``test`` only (dev/stg retired per the 2026-07-13
  ruling). The old ``-test-`` infix string hack (``get_test_bucket_name``) is
  gone — the test tier is just ``deployment_env="test"`` resolution, the same
  mechanism as prod.
* ``configs/bucket_config.yaml`` is now ONLY the registry of genuine
  hand-managed infra buckets (terraform-state, deployment-orchestration,
  build-metadata, databento-batch-registry, backtest-results,
  client-reporting-data, events, unified-deployment-state, ...) — every data
  bucket that duplicated the canonical cloud-providers.yaml matrix
  (eigenlayer-rewards, ml-configs-store, features-volatility-defi + their
  -test twins) was stripped, along with the now-dead ``aws_bucket_mappings``
  section (superseded by the AWS side of cloud-providers.yaml, which mirrors
  GCP 1:1 per Phase-1 symmetry).
* Lifecycle/settings: canonical data buckets get the same creation defaults as
  ``terraform/gcp/canonical_buckets.tf`` (STANDARD storage class, uniform
  bucket-level access, no versioning, STANDARD->COLDLINE @ 60 days). Infra
  buckets keep whatever settings ``bucket_config.yaml``'s ``bucket_settings``
  section declares (unchanged from before this rewrite).

Features:
- Idempotent: skips existing buckets
- Multi-cloud: supports both GCP (GCS) and AWS (S3)
- Bootstraps a NEW project/account: --project-id substitutes into every
  resolved template (the UTL resolver reads ${GCP_PROJECT_ID}/${AWS_ACCOUNT_ID}
  from the process env, so this script sets that env var from --project-id
  before resolving — the same bridge setup-dev-project.sh already relies on).

Prerequisites:
- GCP: gcloud auth login (or service account credentials)
- AWS: aws configure (or IAM role)

Usage:
    # GCP - create all prd-tier buckets
    python setup-buckets.py --cloud gcp

    # GCP - create prd + test tier buckets
    python setup-buckets.py --cloud gcp --include-test

    # GCP - only test-tier buckets (never touches prod; infra buckets skipped)
    python setup-buckets.py --cloud gcp --test-only

    # GCP - buckets for a specific service (best-effort kind-name filter)
    python setup-buckets.py --cloud gcp --service instruments-service --include-test

    # GCP - dry run (show what would be created)
    python setup-buckets.py --cloud gcp --include-test --dry-run

    # AWS - create all buckets including test tier
    python setup-buckets.py --cloud aws --include-test

    # List all required buckets without creating
    python setup-buckets.py --cloud gcp --include-test --list-only

    # Bootstrap a brand-new GCP project
    python setup-buckets.py --cloud gcp --include-test --project-id my-new-project
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Final, Literal, cast

sys.path.insert(0, str(Path(__file__).parent))
import yaml
from _common import get_aws_account_id, get_aws_region, get_gcs_region, get_project_id
from unified_trading_library import AssetGroup, resolve_bucket_name

# BucketNamingError isn't re-exported at the unified_trading_library top level (only
# resolve_bucket_name/AssetGroup are) — narrow, justified exemption from the top-level-only
# import rule, same mechanism check-import-patterns.py documents for this exact case.
from unified_trading_library.cloud_interface.bucket_naming import BucketNamingError  # noqa: qg-deep-import

logger = logging.getLogger(__name__)
# Global configuration holder
bucket_config = None

# Canonical tiers per the 2026-07-13 bucket-estate-consolidation operator ruling —
# dev/stg retired, prd + test only. Mirrors terraform/gcp/canonical_buckets.tf's
# `canonical_tiers` local exactly.
_CANONICAL_TIERS: Final[tuple[str, ...]] = ("prd", "test")

# Kinds intentionally excluded from the canonical enumeration — same set as
# terraform/gcp/canonical_buckets.tf's `canonical_excluded_kinds`: retention-locked,
# hand-managed compliance buckets a generic loop cannot express safely (locked
# GCS Object Retention / S3 Object Lock policies would force a
# prevent_destroy-blocked replacement).
_EXCLUDED_CANONICAL_KINDS: Final[frozenset[str]] = frozenset({"audit-records", "manual-audit"})


def get_config_dir() -> Path:
    """Get the configs directory path."""
    script_dir = Path(__file__).parent.parent
    return script_dir / "configs"


def load_yaml(path: Path) -> dict:
    """Load a YAML file."""
    with open(path) as f:
        return yaml.safe_load(f)


def load_bucket_config(config_dir: Path) -> dict:
    """Load bucket configuration from bucket_config.yaml."""
    global bucket_config
    if bucket_config is None:
        config_path = config_dir / "bucket_config.yaml"
        if not config_path.exists():
            raise FileNotFoundError(f"Bucket config not found: {config_path}")
        bucket_config = load_yaml(config_path)
    return bucket_config


def validate_config(config: dict) -> None:
    """Validate bucket configuration schema.

    Only the sections this script actually reads are required.
    ``bucket_config.yaml`` also carries ``shared_bucket_services`` /
    ``service_categories`` sections this script no longer consumes (the
    old dependencies.yaml-driven per-service resolver is gone) — those stay in
    the file because ``system-integration-tests/tests/conftest.py`` still
    reads them for its own (separate) bucket enumeration.
    """
    required_sections = ["defaults", "infrastructure_buckets", "bucket_settings"]

    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required config section: {section}")

    if "gcp" not in config["defaults"] or "aws" not in config["defaults"]:
        raise ValueError("Missing gcp/aws sections in defaults")

    for cloud in ["gcp", "aws"]:
        if cloud not in config["bucket_settings"]:
            raise ValueError(f"Missing {cloud} bucket settings")


def get_default_region(cloud: str) -> str:
    """Return the default region for `cloud` without requiring a resolvable project/account ID.

    Split out of :func:`get_default_values` (2026-07-14 rewrite) — the
    ``--project-id``-supplied-but-no-``--region`` path in ``main()`` only ever
    needed the region half; calling the combined function there forced a
    ``GCP_PROJECT_ID``/``AWS_ACCOUNT_ID`` env lookup it had no other use for,
    so bootstrapping a brand-new project via ``--project-id`` alone (no env
    var also exported) raised even though a region default was available.
    """
    config = load_bucket_config(get_config_dir())
    default_region = config["defaults"][cloud]["region"]
    return get_gcs_region(default_region) if cloud == "gcp" else get_aws_region(default_region)


def get_default_values(cloud: str) -> dict:
    """Get default values (project/account ID + region) for a cloud provider."""
    region = get_default_region(cloud)

    if cloud == "gcp":
        return {"project_id": get_project_id(), "region": region}
    else:  # aws
        account_id = get_aws_account_id()
        if not account_id:
            try:
                result = subprocess.run(
                    ["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if result.returncode == 0:
                    account_id = result.stdout.strip()
            except (OSError, ValueError, RuntimeError) as e:
                logger.warning("Unexpected error during get default values: %s", e, exc_info=True)
        if not account_id:
            raise ValueError(
                "AWS_ACCOUNT_ID is required when using AWS. "
                "Set AWS_ACCOUNT_ID or ensure 'aws sts get-caller-identity' succeeds."
            )
        return {"account_id": account_id, "region": region}


def _kind_matches_service_filter(kind: str, service_filter: str) -> bool:
    """Best-effort ``--service`` narrowing over canonical kind names.

    Kinds are yaml-key granular (e.g. ``instruments-store``, ``market-data``)
    and are not 1:1 with service names (e.g. ``instruments-service``,
    ``market-tick-data-service``) — this is a convenience filter, not an exact
    join. Matches on the service name's first hyphen-stem, e.g.
    ``--service instruments-service`` -> stem ``instruments`` -> matches
    ``instruments-store`` / ``instruments-store-prediction``.
    """
    stem = service_filter.removesuffix("-service").split("-")[0].lower()
    return stem in kind.lower()


def _resolve_canonical_bucket(cloud: str, kind: str, asset_group: str | None, tier: str) -> str | None:
    """Resolve one (kind, asset_group, tier) triple via the UTL SSOT resolver.

    Returns ``None`` (with a logged warning) on any resolution failure instead
    of aborting the whole enumeration — one unresolvable kind should not stop
    every other bucket from being listed/created.
    """
    try:
        return resolve_bucket_name(
            cloud=cast('Literal["gcp", "aws"]', cloud),
            kind=kind,
            asset_group=cast("AssetGroup | None", asset_group),
            deployment_env=tier,
        )
    except BucketNamingError as e:
        logger.warning("Skipping unresolvable bucket kind=%r asset_group=%r tier=%r: %s", kind, asset_group, tier, e)
        return None


def get_canonical_data_buckets(
    config_dir: Path,
    cloud: str,
    tiers: tuple[str, ...],
    service_filter: str | None = None,
) -> list[dict]:
    """Enumerate every canonical service/data bucket for `cloud`.

    Mirrors ``terraform/gcp/canonical_buckets.tf``'s derivation: iterate every
    kind under ``cloud-providers.yaml``'s ``<cloud>.storage`` (excluding the
    retention-locked/hand-managed kinds in
    :data:`_EXCLUDED_CANONICAL_KINDS`), flatten per-asset_group dict kinds
    into one entry per asset_group, and resolve each (kind, asset_group, tier)
    triple through the UTL resolver. An env-less template (no
    ``${DEPLOYMENT_ENV_SHORT}`` token — e.g. the Group-B features/ml/strategy/
    execution kinds per the 2026-05 env-split-rollback note in the yaml)
    resolves to the IDENTICAL name for every tier and collapses via `seen`;
    an env-tiered template produces distinct prd/test buckets.
    """
    cloud_config = load_yaml(config_dir / "cloud-providers.yaml")
    storage_kinds = cloud_config.get(cloud, {}).get("storage", {})

    buckets: list[dict] = []
    seen: set[str] = set()

    for kind, entry in storage_kinds.items():
        if kind in _EXCLUDED_CANONICAL_KINDS:
            continue
        if service_filter and not _kind_matches_service_filter(kind, service_filter):
            continue

        asset_groups: list[str | None]
        if isinstance(entry, str):
            asset_groups = [None]
        elif isinstance(entry, dict):
            asset_groups = [str(ag).lower() for ag in entry]
        else:
            continue

        for asset_group in asset_groups:
            for tier in tiers:
                name = _resolve_canonical_bucket(cloud, kind, asset_group, tier)
                if not name or name in seen:
                    continue
                seen.add(name)
                buckets.append(
                    {
                        "name": name,
                        "service": kind,
                        "type": "data",
                        "category": (asset_group or "ALL").upper(),
                        "is_test": tier == "test",
                        "origin": "canonical",
                    }
                )

    return sorted(buckets, key=lambda b: (b["is_test"], b["name"]))


def get_infrastructure_buckets(project_id: str, cloud: str) -> list[dict]:
    """Get genuine, hand-managed infrastructure buckets.

    Reads ONLY ``bucket_config.yaml``'s ``infrastructure_buckets`` registry
    (terraform-state, deployment-orchestration, build-metadata,
    databento-batch-registry, backtest-results, client-reporting-data, events,
    unified-deployment-state, ...) — every data bucket that duplicated the
    canonical ``cloud-providers.yaml`` matrix was stripped from that registry
    as part of the 2026-07-14 rewrite; those come from
    :func:`get_canonical_data_buckets` instead. Infra buckets never get a
    test-tier sibling (unchanged from the pre-rewrite behavior).
    """
    config = load_bucket_config(get_config_dir())
    infra_config = config["infrastructure_buckets"][cloud]

    buckets: list[dict] = []
    for bucket_def in infra_config:
        bucket_name = bucket_def["name_template"].replace("{project_id}", project_id)
        buckets.append(
            {
                "name": bucket_name,
                "service": bucket_def["service"],
                "type": bucket_def["type"],
                "category": bucket_def["category"],
                "is_test": False,
                "origin": "infra",
            }
        )

    return buckets


def bucket_exists_gcs(bucket_name: str) -> bool:
    """Check if a GCS bucket exists via UCI get_storage_client."""
    try:
        from unified_trading_library import get_storage_client

        client = get_storage_client(provider="gcp")
        list(client.list_blobs(bucket=bucket_name, prefix="", max_results=1))
        return True
    except Exception:
        # NotFound (404), Forbidden (403), or any other error = bucket doesn't exist or inaccessible
        return False


def bucket_exists_s3(bucket_name: str) -> bool:
    """Check if an S3 bucket exists via UCI get_storage_client."""
    try:
        from unified_trading_library import get_storage_client

        client = get_storage_client(provider="aws")
        list(client.list_blobs(bucket=bucket_name, prefix="", max_results=1))
        return True
    except Exception:
        # NoSuchBucket, AccessDenied, or any other error = bucket doesn't exist or inaccessible
        return False


def create_gcs_bucket(
    bucket_name: str,
    project_id: str,
    region: str,
    label_key: str,
    category: str,
    dry_run: bool = False,
    is_test: bool = False,
    origin: str = "canonical",
) -> bool:
    """Create a GCS bucket.

    ``origin="canonical"`` (the default): settings mirror
    ``terraform/gcp/canonical_buckets.tf`` exactly per the 2026-07-13 ruling —
    STANDARD storage class, uniform bucket-level access, no versioning,
    STANDARD->COLDLINE @ 60 days.
    ``origin="infra"``: settings come from ``bucket_config.yaml``'s
    ``bucket_settings.gcp`` section (unchanged from before this rewrite).
    """
    if bucket_exists_gcs(bucket_name):
        logger.info(f"  [EXISTS] gs://{bucket_name}")
        return True

    bucket_type = "TEST" if is_test else "PROD"
    if dry_run:
        logger.info(f"  [DRY-RUN] Would create gs://{bucket_name} ({bucket_type}, {origin})")
        return True

    logger.info(f"  [CREATE] gs://{bucket_name} ({bucket_type}, {origin})")

    if origin == "infra":
        config = load_bucket_config(get_config_dir())
        settings = config["bucket_settings"]["gcp"]
        storage_class = settings["storage_class"]
        uniform_access = settings["uniform_bucket_access"]
        versioning = settings["versioning"]
        lifecycle_rule = settings["lifecycle_rules"]["test" if is_test else "production"]
        lifecycle_days = lifecycle_rule["age_days"]
        lifecycle_action = lifecycle_rule["action"]
        lifecycle_storage_class = lifecycle_rule["storage_class"]
        managed_by = settings["labels"]["managed_by"]
    else:
        # Canonical data buckets — mirror terraform/gcp/canonical_buckets.tf's
        # google_storage_bucket.canonical resource (2026-07-13 ruling).
        storage_class = "STANDARD"
        uniform_access = True
        versioning = False
        lifecycle_days = 60
        lifecycle_action = "SetStorageClass"
        lifecycle_storage_class = "COLDLINE"
        managed_by = "setup-buckets-canonical"

    try:
        create_args = [
            "gsutil",
            "mb",
            "-p",
            project_id,
            "-c",
            storage_class,
            "-l",
            region,
        ]
        if uniform_access:
            create_args.extend(["-b", "on"])
        create_args.append(f"gs://{bucket_name}")

        result = subprocess.run(create_args, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            logger.info(f"    ERROR: {result.stderr}")
            return False

        if versioning:
            subprocess.run(
                ["gsutil", "versioning", "set", "on", f"gs://{bucket_name}"],
                capture_output=True,
                timeout=30,
            )

        lifecycle_config = {
            "rule": [
                {
                    "action": {
                        "type": lifecycle_action,
                        "storageClass": lifecycle_storage_class,
                    },
                    "condition": {"age": lifecycle_days},
                }
            ]
        }

        lifecycle_file = os.path.join(tempfile.gettempdir(), f"lifecycle_{bucket_name}.json")
        with open(lifecycle_file, "w") as f:
            json.dump(lifecycle_config, f)

        subprocess.run(
            ["gsutil", "lifecycle", "set", lifecycle_file, f"gs://{bucket_name}"],
            capture_output=True,
            timeout=30,
        )
        os.remove(lifecycle_file)

        labels = f"managed-by:{managed_by}"
        labels += f",kind:{label_key.replace('-', '_')}"
        if category != "ALL":
            labels += f",category:{category.lower()}"
        labels += f",environment:{'test' if is_test else 'production'}"

        subprocess.run(
            ["gsutil", "label", "ch", "-l", labels, f"gs://{bucket_name}"],
            capture_output=True,
            timeout=30,
        )

        logger.info(
            f"    OK (versioning={'on' if versioning else 'off'}, "
            f"lifecycle={lifecycle_days}d->{lifecycle_storage_class})"
        )
        return True

    except subprocess.TimeoutExpired:
        logger.info("    ERROR: Timeout creating bucket")
        return False
    except (OSError, ValueError, RuntimeError) as e:
        logger.info(f"    ERROR: {e}")
        return False


def create_s3_bucket(
    bucket_name: str,
    region: str,
    service: str,
    category: str,
    dry_run: bool = False,
    is_test: bool = False,
) -> bool:
    """
    Create an S3 bucket with settings from configuration.

    Unchanged from before this rewrite: AWS canonical/data buckets and infra
    buckets both use ``bucket_config.yaml``'s ``bucket_settings.aws`` section —
    no AWS-side terraform ruling exists yet to mirror (the 2026-07-13 ruling
    only re-derives GCP lifecycle settings).
    """
    config = load_bucket_config(get_config_dir())
    settings = config["bucket_settings"]["aws"]

    if bucket_exists_s3(bucket_name):
        logger.info(f"  [EXISTS] s3://{bucket_name}")
        return True

    bucket_type = "TEST" if is_test else "PROD"
    if dry_run:
        logger.info(f"  [DRY-RUN] Would create s3://{bucket_name} ({bucket_type})")
        return True

    logger.info(f"  [CREATE] s3://{bucket_name} ({bucket_type})")

    try:
        # Create bucket
        create_cmd = ["aws", "s3api", "create-bucket", "--bucket", bucket_name, "--region", region]

        # For regions other than us-east-1, need LocationConstraint
        if region != "us-east-1":
            create_cmd.extend(["--create-bucket-configuration", f"LocationConstraint={region}"])

        result = subprocess.run(create_cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            logger.info(f"    ERROR: {result.stderr}")
            return False

        # Enable versioning if configured
        if settings["versioning"]:
            subprocess.run(
                [
                    "aws",
                    "s3api",
                    "put-bucket-versioning",
                    "--bucket",
                    bucket_name,
                    "--versioning-configuration",
                    "Status=Enabled",
                ],
                capture_output=True,
                timeout=30,
            )

        # Enable encryption from configuration
        encryption_config = {
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": settings["encryption"]["algorithm"]}}]
        }
        subprocess.run(
            [
                "aws",
                "s3api",
                "put-bucket-encryption",
                "--bucket",
                bucket_name,
                "--server-side-encryption-configuration",
                json.dumps(encryption_config),
            ],
            capture_output=True,
            timeout=30,
        )

        # Set tags from configuration
        tags_config = settings["tags"]
        tags = [
            {"Key": "ManagedBy", "Value": tags_config["ManagedBy"]},
            {"Key": "Project", "Value": tags_config["Project"]},
            {"Key": "Service", "Value": service},
            {"Key": "Environment", "Value": "test" if is_test else "production"},
        ]
        if category != "ALL":
            tags.append({"Key": "Category", "Value": category})

        subprocess.run(
            [
                "aws",
                "s3api",
                "put-bucket-tagging",
                "--bucket",
                bucket_name,
                "--tagging",
                json.dumps({"TagSet": tags}),
            ],
            capture_output=True,
            timeout=30,
        )

        encryption_alg = settings["encryption"]["algorithm"]
        logger.info(f"    OK (versioning=on, encryption={encryption_alg})")
        return True

    except subprocess.TimeoutExpired:
        logger.info("    ERROR: Timeout creating bucket")
        return False
    except (OSError, ValueError, RuntimeError) as e:
        logger.info(f"    ERROR: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Setup storage buckets for Unified Trading System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--cloud", choices=["gcp", "aws"], required=True, help="Cloud provider")
    parser.add_argument(
        "--service",
        help="Best-effort filter: only kinds whose name contains this service's hyphen-stem "
        "(e.g. 'instruments-service' matches 'instruments-store*'). Never filters infra buckets.",
    )
    parser.add_argument("--project-id", help="GCP project ID or AWS account ID")
    parser.add_argument("--region", help="Region for bucket creation")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be created without creating",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="List all required buckets without creating",
    )
    parser.add_argument("--config-dir", type=Path, help="Path to configs directory")
    parser.add_argument(
        "--include-test",
        action="store_true",
        help="Include test-tier buckets alongside prd-tier buckets",
    )
    parser.add_argument(
        "--test-only",
        action="store_true",
        help="Only create test-tier buckets (skip prd-tier; infra buckets are always skipped in this mode)",
    )

    args = parser.parse_args()

    # Tiers: prd + test only (dev/stg retired 2026-07-13 ruling) — driven purely by
    # --include-test/--test-only, matching terraform/gcp/canonical_buckets.tf's
    # fixed `canonical_tiers`. There is no more --env flag: it only ever selected
    # among the now-retired dev/stg/prod axis and no live caller passed it.
    if args.test_only:
        tiers: tuple[str, ...] = ("test",)
    elif args.include_test:
        tiers = _CANONICAL_TIERS
    else:
        tiers = ("prd",)

    # Get config directory
    config_dir = args.config_dir or get_config_dir()
    if not config_dir.exists():
        logger.info(f"ERROR: Config directory not found: {config_dir}")
        sys.exit(1)

    # Load and validate bucket configuration
    try:
        bucket_cfg = load_bucket_config(config_dir)
        validate_config(bucket_cfg)
    except (FileNotFoundError, ValueError) as e:
        logger.info(f"ERROR: Configuration error: {e}")
        sys.exit(1)

    # Get project/account ID and region from configuration or args
    if args.project_id:
        project_id = args.project_id
        region = args.region or get_default_region(args.cloud)
    else:
        defaults = get_default_values(args.cloud)
        project_id = defaults.get("project_id") or defaults.get("account_id")
        region = args.region or defaults.get("region")

        if not project_id:
            if args.cloud == "aws":
                logger.info("ERROR: AWS_ACCOUNT_ID not set and could not determine from AWS CLI")
                logger.info("Run: aws sts get-caller-identity")
            else:
                logger.info("ERROR: GCP_PROJECT_ID not set")
            sys.exit(1)

    # Bridge --project-id into the process env so the UTL resolver's
    # ${GCP_PROJECT_ID}/${AWS_ACCOUNT_ID} substitution picks it up — the resolver
    # is a pure function of (cloud, kind, asset_group) + the process env by
    # design (see unified_trading_library.cloud_interface.bucket_naming), so
    # this is the sanctioned way to target a brand-new project/account without
    # requiring the caller to also export the env var (setup-dev-project.sh
    # already does both belt-and-braces).
    if args.cloud == "gcp":
        os.environ["GCP_PROJECT_ID"] = project_id  # config-bootstrap: bucket-naming resolver bridge
    else:
        os.environ["AWS_ACCOUNT_ID"] = project_id  # config-bootstrap: bucket-naming resolver bridge

    # Get all required buckets
    canonical_buckets = get_canonical_data_buckets(
        config_dir,
        args.cloud,
        tiers,
        service_filter=args.service,
    )
    # Infra buckets never get a test-tier sibling; --test-only skips them entirely
    # (unchanged from before this rewrite — provision-test-buckets.sh relies on
    # this to stay scoped to test-tier data buckets only).
    infra_buckets = [] if args.test_only else get_infrastructure_buckets(project_id, args.cloud)
    buckets = canonical_buckets + infra_buckets

    if not buckets:
        logger.info("No buckets to create.")
        sys.exit(0)

    # Count production vs test buckets
    prod_buckets = [b for b in buckets if not b.get("is_test", False)]
    test_buckets = [b for b in buckets if b.get("is_test", False)]

    # Print header
    logger.info("")
    logger.info("=" * 70)
    logger.info("Unified Trading System - Bucket Setup")
    logger.info("=" * 70)
    logger.info(f"Cloud Provider: {args.cloud.upper()}")
    logger.info(f"Project/Account: {project_id}")
    logger.info(f"Region: {region}")
    logger.info(f"Total Buckets: {len(buckets)}")
    if args.include_test or args.test_only:
        logger.info(f"  - Production (prd): {len(prod_buckets)}")
        logger.info(f"  - Test: {len(test_buckets)}")
    if args.service:
        logger.info(f"Service Filter: {args.service}")
    if args.test_only:
        logger.info("Mode: TEST BUCKETS ONLY")
    elif args.include_test:
        logger.info("Mode: Production + Test Buckets")
    logger.info("=" * 70)
    logger.info("")

    # List only mode
    if args.list_only:
        if args.include_test or args.test_only:
            # Separate production and test buckets in output
            if prod_buckets and not args.test_only:
                logger.info("Production buckets:")
                logger.info("")
                for bucket in prod_buckets:
                    prefix = "gs://" if args.cloud == "gcp" else "s3://"
                    logger.info(f"  {prefix}{bucket['name']}")
                    logger.info(
                        f"      Kind: {bucket['service']}, Type: {bucket['type']}, Category: {bucket['category']}"
                    )
                logger.info("")

            if test_buckets:
                logger.info("Test buckets (for quality gates and CI/CD):")
                logger.info("")
                for bucket in test_buckets:
                    prefix = "gs://" if args.cloud == "gcp" else "s3://"
                    logger.info(f"  {prefix}{bucket['name']}")
                    logger.info(f"      Kind: {bucket['service']}, Category: {bucket['category']}")
                logger.info("")
        else:
            logger.info("Required buckets:")
            logger.info("")
            for bucket in buckets:
                prefix = "gs://" if args.cloud == "gcp" else "s3://"
                logger.info(f"  {prefix}{bucket['name']}")
                logger.info(f"      Kind: {bucket['service']}, Type: {bucket['type']}, Category: {bucket['category']}")
            logger.info("")

        # Print env var suggestions for services
        if args.include_test or args.test_only:
            logger.info("=" * 70)
            logger.info("Environment Variables for Test Buckets")
            logger.info("=" * 70)
            logger.info("Add these to your .env files or CI/CD configuration:")
            logger.info("")
            for bucket in test_buckets:
                # Generate env var name from kind name
                kind_name = bucket["service"].replace("-", "_").upper()
                category = bucket["category"]
                if category != "ALL":
                    env_var = f"{kind_name}_GCS_BUCKET_{category}_TEST"
                else:
                    env_var = f"{kind_name}_GCS_BUCKET_TEST"
                logger.info(f"  {env_var}={bucket['name']}")
            logger.info("")

        sys.exit(0)

    # Check prerequisites
    if args.cloud == "gcp":
        # Check gcloud/gsutil
        try:
            subprocess.run(["gsutil", "version"], capture_output=True, timeout=10)
        except (OSError, ValueError, RuntimeError):
            sys.exit(1)
    else:
        # Check aws cli
        try:
            subprocess.run(["aws", "--version"], capture_output=True, timeout=10)
        except (OSError, ValueError, RuntimeError):
            logger.info("ERROR: aws CLI not found. Install AWS CLI.")
            sys.exit(1)

    # Create buckets
    created = 0
    skipped = 0
    failed = 0

    for bucket in buckets:
        is_test = bucket.get("is_test", False)

        if args.cloud == "gcp":
            success = create_gcs_bucket(
                bucket["name"],
                project_id,
                region,
                bucket["service"],
                bucket["category"],
                args.dry_run,
                is_test=is_test,
                origin=bucket.get("origin", "canonical"),
            )
        else:
            success = create_s3_bucket(
                bucket["name"],
                region,
                bucket["service"],
                bucket["category"],
                args.dry_run,
                is_test=is_test,
            )

        if success:
            if bucket_exists_gcs(bucket["name"]) if args.cloud == "gcp" else bucket_exists_s3(bucket["name"]):
                skipped += 1
            else:
                created += 1
        else:
            failed += 1

    # Print summary
    logger.info("")
    logger.info("=" * 70)
    logger.info("Summary")
    logger.info("=" * 70)
    if args.dry_run:
        logger.info(f"  Would create: {len(buckets)} buckets")
    else:
        logger.info(f"  Created: {created}")
        logger.info(f"  Already existed: {skipped}")
        logger.info(f"  Failed: {failed}")
    logger.info("")

    if failed > 0:
        logger.info("Some buckets failed to create. Check permissions and try again.")
        sys.exit(1)
    else:
        logger.info("All buckets ready!")


if __name__ == "__main__":
    main()
