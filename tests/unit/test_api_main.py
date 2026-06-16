"""Tests for deployment_service.api.main — covers create_app + freshness helpers."""

from __future__ import annotations

import importlib
from datetime import date
from unittest.mock import patch

import pytest
from fastapi import FastAPI


class TestApiMainModule:
    """Cover module-level create_app + helper functions."""

    def test_create_app_returns_fastapi(self):
        from deployment_service.api.main import create_app

        app = create_app()
        assert isinstance(app, FastAPI)

    def test_create_app_registers_health_route(self):
        from deployment_service.api.main import create_app

        app = create_app()
        # FastAPI >= 0.115 wraps included routers in _IncludedRouter (no .path);
        # use the OpenAPI paths dict which reflects the fully-resolved route set.
        paths = set(app.openapi().get("paths", {}).keys())
        assert "/health" in paths or any("/health" in p for p in paths)

    def test_module_level_app_is_fastapi(self):
        from deployment_service.api import main as main_mod

        assert isinstance(main_mod.app, FastAPI)

    def test_data_freshness_when_no_date(self):
        """_data_freshness returns stale=True when no date set."""
        import deployment_service.api.main as main_mod

        main_mod._last_processed_date = None  # reset
        result = main_mod._data_freshness()
        assert result["stale"] is True
        assert result["last_processed_date"] is None

    def test_data_freshness_when_date_set(self):
        """_data_freshness returns stale=False with a date."""
        import deployment_service.api.main as main_mod

        test_date = date(2026, 5, 26)
        main_mod._last_processed_date = test_date
        result = main_mod._data_freshness()
        assert result["stale"] is False
        assert result["last_processed_date"] == "2026-05-26"
        main_mod._last_processed_date = None  # cleanup

    def test_set_last_processed_date(self):
        """set_last_processed_date mutates module-level state."""
        import deployment_service.api.main as main_mod
        from deployment_service.api.main import set_last_processed_date

        test_date = date(2026, 5, 1)
        set_last_processed_date(test_date)
        assert main_mod._last_processed_date == test_date
        main_mod._last_processed_date = None  # cleanup
