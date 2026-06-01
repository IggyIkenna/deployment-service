"""Unit tests for deployment_service/config_reloaders.py.

Covers:
  - start_domain_config_reloaders() happy-path: two reloaders started
  - start_domain_config_reloaders() with empty config_store_bucket: no-op
  - stop_domain_config_reloaders(): stops both, clears globals
  - _on_instruments_reload() callback log + log_event call
  - _on_venues_reload() callback log + log_event call
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_service_config(bucket: str = "my-config-bucket", project: str | None = "proj-123") -> object:
    """Minimal DeploymentConfig stub."""
    cfg = MagicMock()
    cfg.config_store_bucket = bucket
    cfg.gcp_project_id = project
    return cfg


def _make_domain_config(subscription_list: list[str], enabled_venues: list[str]) -> object:
    cfg = MagicMock()
    cfg.subscription_list = subscription_list
    cfg.enabled_venues = enabled_venues
    return cfg


# ---------------------------------------------------------------------------
# start_domain_config_reloaders
# ---------------------------------------------------------------------------


class TestStartDomainConfigReloaders:
    def test_empty_bucket_is_noop(self) -> None:
        """If config_store_bucket is falsy, no reloaders are created."""
        from deployment_service import config_reloaders

        svc_cfg = _make_service_config(bucket="")
        with patch("deployment_service.config_reloaders.DomainConfigReloader") as mock_cls:
            config_reloaders.start_domain_config_reloaders(svc_cfg)  # type: ignore[arg-type]
            mock_cls.assert_not_called()

    def test_happy_path_creates_two_reloaders(self) -> None:
        """Both instrument + venue reloaders are instantiated and started."""
        from deployment_service import config_reloaders

        svc_cfg = _make_service_config()
        mock_reloader = MagicMock()

        with patch("deployment_service.config_reloaders.DomainConfigReloader", return_value=mock_reloader) as mock_cls:
            config_reloaders.start_domain_config_reloaders(svc_cfg)  # type: ignore[arg-type]

        assert mock_cls.call_count == 2
        # on_reload + start_watching called per reloader
        assert mock_reloader.on_reload.call_count == 2
        assert mock_reloader.start_watching.call_count == 2

    def test_reloaders_initialised_with_correct_bucket(self) -> None:
        from deployment_service import config_reloaders

        svc_cfg = _make_service_config(bucket="special-bucket", project="proj-xyz")
        mock_reloader = MagicMock()

        with patch("deployment_service.config_reloaders.DomainConfigReloader", return_value=mock_reloader) as mock_cls:
            config_reloaders.start_domain_config_reloaders(svc_cfg)  # type: ignore[arg-type]

        calls = mock_cls.call_args_list
        buckets = {c.kwargs.get("config_bucket") for c in calls}
        assert "special-bucket" in buckets
        projects = {c.kwargs.get("project_id") for c in calls}
        assert "proj-xyz" in projects


# ---------------------------------------------------------------------------
# stop_domain_config_reloaders
# ---------------------------------------------------------------------------


class TestStopDomainConfigReloaders:
    def test_stop_calls_stop_watching_on_both(self) -> None:
        """stop_domain_config_reloaders stops both reloaders and clears globals."""
        from deployment_service import config_reloaders

        mock_reloader = MagicMock()
        svc_cfg = _make_service_config()

        with patch("deployment_service.config_reloaders.DomainConfigReloader", return_value=mock_reloader):
            config_reloaders.start_domain_config_reloaders(svc_cfg)  # type: ignore[arg-type]

        config_reloaders.stop_domain_config_reloaders()
        assert mock_reloader.stop_watching.call_count == 2
        # Globals cleared
        assert config_reloaders._instrument_reloader is None
        assert config_reloaders._venue_reloader is None

    def test_stop_without_start_is_safe(self) -> None:
        """stop_domain_config_reloaders is idempotent even when nothing was started."""
        from deployment_service import config_reloaders

        # Force globals to None
        config_reloaders._instrument_reloader = None
        config_reloaders._venue_reloader = None
        config_reloaders.stop_domain_config_reloaders()  # Should not raise


# ---------------------------------------------------------------------------
# _on_instruments_reload / _on_venues_reload callbacks
# ---------------------------------------------------------------------------


class TestReloadCallbacks:
    def test_on_instruments_reload_calls_log_event(self) -> None:
        from deployment_service.config_reloaders import _on_instruments_reload

        instruments_config = _make_domain_config(
            subscription_list=["BTC-USDT", "ETH-USDT"],
            enabled_venues=["binance", "okx"],
        )
        with patch("deployment_service.config_reloaders.log_event") as mock_log:
            _on_instruments_reload(instruments_config)  # type: ignore[arg-type]

        mock_log.assert_called_once()
        call_kwargs = mock_log.call_args
        assert call_kwargs[0][0] == "CONFIG_CHANGED"
        details = call_kwargs[1]["details"]
        assert details["domain"] == "instruments"
        assert details["instruments_count"] == 2
        assert details["venues_count"] == 2

    def test_on_venues_reload_calls_log_event(self) -> None:
        from deployment_service.config_reloaders import _on_venues_reload

        venues_config = _make_domain_config(subscription_list=[], enabled_venues=["binance"])
        with patch("deployment_service.config_reloaders.log_event") as mock_log:
            _on_venues_reload(venues_config)  # type: ignore[arg-type]

        mock_log.assert_called_once()
        details = mock_log.call_args[1]["details"]
        assert details["domain"] == "venues"
        assert details["enabled_venues_count"] == 1
