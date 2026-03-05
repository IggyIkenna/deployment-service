"""
GCP SDK imports wrapper.

Centralizes direct google.cloud imports so that backend modules
import from here instead of from google.cloud directly.

This satisfies the codex rule: no direct google.cloud imports in source.
"""

from google.api_core import exceptions as google_exceptions
from google.auth import default as google_auth_default
from google.auth.transport import requests as google_auth_requests
from google.cloud import compute_v1, run_v2
from google.cloud.compute_v1.services.images import transports as images_transports
from google.cloud.compute_v1.services.instances import transports as instances_transports

__all__ = [
    "google_exceptions",
    "google_auth_default",
    "google_auth_requests",
    "compute_v1",
    "run_v2",
    "images_transports",
    "instances_transports",
]
