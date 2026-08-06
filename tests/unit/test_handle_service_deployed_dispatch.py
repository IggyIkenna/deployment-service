"""Regression tests for scripts/cicd/handle_service_deployed_dispatch.py.

Covers the two safety gates (allowlist default-deny, version-shape sanity check)
and the allowlisted-deploy-call path — the deploy call itself is mocked, never
hits real infra.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "cicd"))

import handle_service_deployed_dispatch as listener  # noqa: E402 — path setup above must run first


def _run(argv: list[str]) -> int:
    return listener.main(argv)


class TestAllowlistGate:
    def test_unlisted_service_is_a_clean_noop(self, capsys) -> None:
        """A service_name not in AUTO_DEPLOY_ALLOWLIST never calls deploy — exit 0, logged."""
        with patch.object(listener, "_deploy_one") as mock_deploy:
            rc = _run(
                [
                    "--service-name",
                    "execution-service",
                    "--version",
                    "1.2.3",
                    "--api-base-url",
                    "https://example.run.app",
                ]
            )
        assert rc == 0
        mock_deploy.assert_not_called()
        out = capsys.readouterr().out
        assert "NO-OP" in out
        assert "not in AUTO_DEPLOY_ALLOWLIST" in out

    def test_allowlisted_service_calls_deploy(self) -> None:
        """alerting-service (allowlisted -> dp-alerting-subscriber) triggers exactly one deploy call."""
        with patch.object(listener, "_deploy_one", return_value=(True, "HTTP 200: ok")) as mock_deploy:
            rc = _run(
                [
                    "--service-name",
                    "alerting-service",
                    "--version",
                    "1.2.3",
                    "--image",
                    "asia-northeast1-docker.pkg.dev/test-project/unified-trading-system/alerting-service:1.2.3",
                    "--api-base-url",
                    "https://uts-shared-deployment-api-cldtjniqvq-an.a.run.app",
                    "--api-key",
                    "test-key",
                ]
            )
        assert rc == 0
        mock_deploy.assert_called_once_with(
            api_base_url="https://uts-shared-deployment-api-cldtjniqvq-an.a.run.app",
            api_key="test-key",
            image_service="alerting-service",
            cloud_run_service="dp-alerting-subscriber",
            version="1.2.3",
        )

    def test_deploy_failure_returns_1(self) -> None:
        with patch.object(listener, "_deploy_one", return_value=(False, "HTTP 502: bad gateway")):
            rc = _run(
                [
                    "--service-name",
                    "alerting-service",
                    "--version",
                    "1.2.3",
                    "--api-base-url",
                    "https://example.run.app",
                ]
            )
        assert rc == 1


class TestVersionShapeGate:
    def test_non_semver_version_is_a_clean_noop(self) -> None:
        """A version with a branch suffix (e.g. `-staging`, `-feat-x`) never deploys —
        the notify-deployment dispatch is expected to always be main-only, but this
        gate refuses to assume that forever."""
        with patch.object(listener, "_deploy_one") as mock_deploy:
            rc = _run(
                [
                    "--service-name",
                    "alerting-service",
                    "--version",
                    "1.2.3-staging",
                    "--api-base-url",
                    "https://example.run.app",
                ]
            )
        assert rc == 0
        mock_deploy.assert_not_called()

    def test_dev_sha_fallback_version_is_a_clean_noop(self) -> None:
        with patch.object(listener, "_deploy_one") as mock_deploy:
            rc = _run(
                [
                    "--service-name",
                    "alerting-service",
                    "--version",
                    "0.0.0.dev0",
                    "--api-base-url",
                    "https://example.run.app",
                ]
            )
        assert rc == 0
        mock_deploy.assert_not_called()

    def test_clean_semver_passes_the_gate(self) -> None:
        with patch.object(listener, "_deploy_one", return_value=(True, "HTTP 200: ok")) as mock_deploy:
            rc = _run(
                [
                    "--service-name",
                    "alerting-service",
                    "--version",
                    "2.10.0",
                    "--api-base-url",
                    "https://example.run.app",
                ]
            )
        assert rc == 0
        mock_deploy.assert_called_once()


class TestDryRun:
    def test_dry_run_never_calls_deploy(self) -> None:
        with patch.object(listener, "_deploy_one") as mock_deploy:
            rc = _run(
                [
                    "--service-name",
                    "alerting-service",
                    "--version",
                    "1.2.3",
                    "--api-base-url",
                    "https://example.run.app",
                    "--dry-run",
                ]
            )
        assert rc == 0
        mock_deploy.assert_not_called()


class TestDeployOneApiKeyHandling:
    def test_empty_api_key_omits_header(self) -> None:
        """DISABLE_AUTH=true is currently live in prod deployment-api — an empty
        --api-key must still produce a well-formed request with no X-API-Key
        header, not a broken/blank header."""
        captured: dict[str, object] = {}

        class _FakeResp:
            status = 200

            def read(self) -> bytes:
                return b"ok"

            def __enter__(self) -> _FakeResp:
                return self

            def __exit__(self, *exc: object) -> None:
                return None

        def _fake_urlopen(req, timeout=0):  # noqa: ANN001, ARG001
            captured["headers"] = dict(req.headers)
            return _FakeResp()

        with patch.object(listener.urllib.request, "urlopen", side_effect=_fake_urlopen):
            ok, _detail = listener._deploy_one(
                api_base_url="https://example.run.app",
                api_key="",
                image_service="alerting-service",
                cloud_run_service="dp-alerting-subscriber",
                version="1.2.3",
            )
        assert ok is True
        assert "X-api-key" not in captured["headers"]  # urllib title-cases header names

    def test_nonempty_api_key_sets_header(self) -> None:
        captured: dict[str, object] = {}

        class _FakeResp:
            status = 200

            def read(self) -> bytes:
                return b"ok"

            def __enter__(self) -> _FakeResp:
                return self

            def __exit__(self, *exc: object) -> None:
                return None

        def _fake_urlopen(req, timeout=0):  # noqa: ANN001, ARG001
            captured["headers"] = dict(req.headers)
            return _FakeResp()

        with patch.object(listener.urllib.request, "urlopen", side_effect=_fake_urlopen):
            listener._deploy_one(
                api_base_url="https://example.run.app",
                api_key="secret-key",
                image_service="alerting-service",
                cloud_run_service="dp-alerting-subscriber",
                version="1.2.3",
            )
        assert captured["headers"]["X-api-key"] == "secret-key"
