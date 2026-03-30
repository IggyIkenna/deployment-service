"""Service-to-service authentication — delegates to shared UCI middleware."""

from unified_trading_library import create_s2s_auth_dependency

verify_service_token = create_s2s_auth_dependency("deployment-service")
