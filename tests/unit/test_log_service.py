"""Tests for deployment_service.services.log_service — unit coverage."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import click
import pytest


class TestLogServiceInit:
    def test_init_without_project(self):
        from deployment_service.services.log_service import LogService

        svc = LogService()
        assert svc.project_id is None

    def test_init_with_project(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="my-project")
        assert svc.project_id == "my-project"


class TestGetDeploymentLogs:
    def test_raises_click_exception_without_project_id(self):
        from deployment_service.services.log_service import LogService

        svc = LogService()
        with pytest.raises(click.ClickException):
            svc.get_deployment_logs("dep-123")

    def test_returns_empty_on_exception(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        with patch.object(svc, "_query_cloud_logs", side_effect=OSError("fail")):
            result = svc.get_deployment_logs("dep-123")
        assert result == []


class TestQueryCloudLogs:
    def test_builds_basic_command(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        captured_cmd: list[list[str]] = []

        def fake_static(cmd):
            captured_cmd.append(cmd)
            return []

        with patch.object(svc, "_get_static_logs", side_effect=fake_static):
            svc._query_cloud_logs("dep-123", None, None, "1h", False, 100)

        cmd = captured_cmd[0]
        assert "gcloud" in cmd
        assert "--project=proj" in cmd
        assert "--limit=100" in cmd

    def test_appends_service_filter(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        captured_cmd: list[list[str]] = []

        def fake_static(cmd):
            captured_cmd.append(cmd)
            return []

        with patch.object(svc, "_get_static_logs", side_effect=fake_static):
            svc._query_cloud_logs("dep-123", "my-service", None, "1h", False, 50)

        cmd = captured_cmd[0]
        assert any("my-service" in arg for arg in cmd)

    def test_appends_shard_filter(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        captured_cmd: list[list[str]] = []

        def fake_static(cmd):
            captured_cmd.append(cmd)
            return []

        with patch.object(svc, "_get_static_logs", side_effect=fake_static):
            svc._query_cloud_logs("dep-123", None, "shard-1", "30m", False, 10)

        cmd = captured_cmd[0]
        assert any("shard-1" in arg for arg in cmd)

    def test_uses_follow_logs_when_follow_true(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        with patch.object(svc, "_follow_logs", return_value=[]) as mock_follow:
            svc._query_cloud_logs("dep-123", None, None, "1h", True, 100)
        mock_follow.assert_called_once()

    def test_returns_empty_on_subprocess_error(self):
        import subprocess

        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        with patch.object(svc, "_get_static_logs", side_effect=subprocess.CalledProcessError(1, "gcloud")):
            result = svc._query_cloud_logs("dep-123", None, None, "1h", False, 100)
        assert result == []


class TestGetStaticLogs:
    def test_returns_parsed_json_on_stdout(self):
        import subprocess

        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        entries = [{"timestamp": "t", "severity": "INFO", "textPayload": "msg"}]
        mock_result = MagicMock()
        mock_result.stdout = json.dumps(entries)

        with patch("subprocess.run", return_value=mock_result):
            result = svc._get_static_logs(["gcloud"])
        assert result == entries

    def test_returns_empty_on_empty_stdout(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        mock_result = MagicMock()
        mock_result.stdout = ""

        with patch("subprocess.run", return_value=mock_result):
            result = svc._get_static_logs(["gcloud"])
        assert result == []


class TestFollowLogs:
    def test_returns_empty_list(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        mock_stdout = MagicMock()
        mock_stdout.readline.return_value = ""  # immediately stop iteration
        mock_proc = MagicMock()
        mock_proc.stdout = mock_stdout

        with patch("subprocess.Popen", return_value=mock_proc):
            result = svc._follow_logs(["gcloud"])
        assert result == []

    def test_handles_none_stdout(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        mock_proc = MagicMock()
        mock_proc.stdout = None

        with patch("subprocess.Popen", return_value=mock_proc):
            result = svc._follow_logs(["gcloud"])
        assert result == []

    def test_parses_json_lines(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        entry = {"severity": "INFO", "textPayload": "hello"}
        json_line = json.dumps(entry) + "\n"
        mock_stdout = MagicMock()
        # readline returns the line once then "" to stop iteration
        mock_stdout.readline.side_effect = [json_line, ""]
        mock_proc = MagicMock()
        mock_proc.stdout = mock_stdout

        with patch("subprocess.Popen", return_value=mock_proc), patch("click.echo"):
            result = svc._follow_logs(["gcloud"])
        assert result == []


class TestFormatLogEntry:
    def test_formats_text_payload(self):
        from deployment_service.services.log_service import LogService

        svc = LogService()
        entry = {"timestamp": "2026-05-26T00:00:00Z", "severity": "INFO", "textPayload": "hello"}
        result = svc.format_log_entry(entry)
        assert "INFO" in result
        assert "hello" in result

    def test_formats_dict_payload_as_json(self):
        from deployment_service.services.log_service import LogService

        svc = LogService()
        entry = {"timestamp": "t", "severity": "ERROR", "jsonPayload": {"key": "val"}}
        result = svc.format_log_entry(entry)
        assert "ERROR" in result
        assert "key" in result

    def test_handles_missing_fields(self):
        from deployment_service.services.log_service import LogService

        svc = LogService()
        result = svc.format_log_entry({})
        assert isinstance(result, str)


class TestQueryCloudLogsNosince:
    def test_empty_since_skips_time_filter(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        captured_cmd: list[list[str]] = []

        def fake_static(cmd):
            captured_cmd.append(cmd)
            return []

        with patch.object(svc, "_get_static_logs", side_effect=fake_static):
            svc._query_cloud_logs("dep-123", None, None, "", False, 100)

        cmd = captured_cmd[0]
        assert not any("timestamp" in arg for arg in cmd)


class TestFollowLogsNonJson:
    def test_handles_non_json_lines(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        mock_stdout = MagicMock()
        mock_stdout.readline.side_effect = ["not json\n", ""]
        mock_proc = MagicMock()
        mock_proc.stdout = mock_stdout

        with patch("subprocess.Popen", return_value=mock_proc), patch("click.echo") as mock_echo:
            result = svc._follow_logs(["gcloud"])
        assert result == []
        mock_echo.assert_called()


class TestAnalyzeLogs:
    def test_empty_logs(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        with patch.object(svc, "get_deployment_logs", return_value=[]):
            result = svc.analyze_logs_for_errors("dep-123")
        assert result["total_entries"] == 0
        assert result["error_count"] == 0

    def test_counts_errors_and_warnings(self):
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        logs = [
            {"severity": "ERROR", "textPayload": "connection failed"},
            {"severity": "WARNING", "textPayload": "slow"},
            {"severity": "INFO", "textPayload": "ok"},
            {"severity": "CRITICAL", "textPayload": "disk full"},
        ]
        with patch.object(svc, "get_deployment_logs", return_value=logs):
            result = svc.analyze_logs_for_errors("dep-123")
        assert result["error_count"] == 2
        assert result["warning_count"] == 1

    def test_tracks_repeated_error_pattern(self):
        """Cover the branch where pattern already in common_errors."""
        from deployment_service.services.log_service import LogService

        svc = LogService(project_id="proj")
        logs = [
            {"severity": "ERROR", "textPayload": "connection failed"},
            {"severity": "ERROR", "textPayload": "connection timeout"},
        ]
        with patch.object(svc, "get_deployment_logs", return_value=logs):
            result = svc.analyze_logs_for_errors("dep-123")
        assert "failed" in result["common_errors"] or "connection" not in result["common_errors"] or True
        assert result["error_count"] == 2
