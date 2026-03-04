"""
Pytest configuration and fixtures for deployment-service tests.
"""

import os
import sys

import pytest

# Add the service package to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set test defaults for CI/unit tests
for key, default in [
    ("GCP_PROJECT_ID", "test-project"),
]:
    if not (os.getenv(key) or "").strip():
        os.environ[key] = default
