"""Service-to-service authentication — delegates to shared UCI middleware."""

from unified_cloud_interface import create_s2s_auth_dependency

verify_service_token = create_s2s_auth_dependency("deployment-service")
