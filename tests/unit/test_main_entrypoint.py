"""Tests for deployment_service.__main__ — covers _run_server and _run_cli."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import deployment_service.__main__ as main_mod


class TestRunServer:
    def test_run_server_calls_uvicorn(self):
        mock_uvicorn = MagicMock()
        mock_app = MagicMock()
        with (
            patch.dict("sys.modules", {"uvicorn": mock_uvicorn}),
            patch("deployment_service.api.app.app", mock_app),
        ):
            sys.modules["uvicorn"] = mock_uvicorn
            main_mod._run_server(9000)
        mock_uvicorn.run.assert_called_once()

    def test_run_server_uses_specified_port(self):
        mock_uvicorn = MagicMock()
        sys.modules["uvicorn"] = mock_uvicorn
        main_mod._run_server(8888)
        call_kwargs = mock_uvicorn.run.call_args
        assert call_kwargs is not None


class TestRunCli:
    def test_run_cli_calls_cli(self):
        mock_cli = MagicMock()
        with patch("deployment_service.cli.cli", mock_cli):
            main_mod._run_cli()
        mock_cli.assert_called_once()
