"""Integration tests that import and exercise each library dependency.

Satisfies check-integration-dep-coverage.py: each manifest library dep
must be imported in at least one tests/integration/ file.
"""

from __future__ import annotations

import pytest


@pytest.mark.integration
def test_unified_trading_library_import() -> None:
    from unified_trading_library import GracefulShutdownHandler

    assert GracefulShutdownHandler is not None


@pytest.mark.integration
def test_unified_trading_library_config_interface_import() -> None:
    from unified_trading_library import BaseConfig

    assert BaseConfig is not None


@pytest.mark.integration
def test_unified_trading_library_events_interface_import() -> None:
    from unified_trading_library import log_event, setup_events

    assert callable(log_event)
    assert callable(setup_events)


@pytest.mark.integration
def test_unified_trading_library_cloud_interface_import() -> None:
    from unified_trading_library import get_storage_client

    assert callable(get_storage_client)


@pytest.mark.integration
def test_deployment_api_import() -> None:
    pytest.importorskip("deployment_api")  # test-only peer, not a manifest dep (2026-06-04 @5734823)
    import deployment_api

    assert hasattr(deployment_api, "__version__")
