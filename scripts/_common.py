"""
Common helpers for deployment-service scripts.

All os.environ access in scripts must go through these helpers.
# config-boundary: scripts read environment vars for CLI tool configuration only.
"""

import os


def get_project_id() -> str:
    """Return GCP_PROJECT_ID, raising RuntimeError if not set."""
    pid = os.environ.get("GCP_PROJECT_ID")
    if not pid:
        raise RuntimeError("GCP_PROJECT_ID env var required — set it before running this script")
    return pid


def get_gcs_region(default: str = "asia-northeast1") -> str:
    """Return GCS_REGION, falling back to default if not set."""
    return os.environ.get("GCS_REGION", default)


def get_aws_region(default: str = "ap-northeast-1") -> str:
    """Return AWS_REGION, falling back to default if not set."""
    return os.environ.get("AWS_REGION", default)


def get_aws_account_id() -> str | None:
    """Return AWS_ACCOUNT_ID if set, or None."""
    return os.environ.get("AWS_ACCOUNT_ID")


def get_shard_index(default: int = 0) -> int:
    """Return SHARD_INDEX as int, defaulting to 0."""
    return int(os.environ.get("SHARD_INDEX", str(default)))


def get_graph_secret_name() -> str:
    """Return GRAPH_SECRET_NAME or THEGRAPH_SECRET_NAME, defaulting to 'thegraph-api-key'."""
    return os.environ.get("GRAPH_SECRET_NAME") or os.environ.get("THEGRAPH_SECRET_NAME") or "thegraph-api-key"
