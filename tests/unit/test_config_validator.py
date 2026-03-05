"""Tests for ConfigValidator and ValidationUtils."""

from unittest.mock import patch

import pytest

from deployment_service.config.config_validator import ConfigurationError, ConfigValidator, ValidationUtils


class TestValidationUtils:
    """Test ValidationUtils functionality."""

    def test_get_required_with_value(self):
        """Test get_required with valid value."""
        config = {"key": "value"}
        result = ValidationUtils.get_required(config, "key")
        assert result == "value"

    def test_get_required_missing_key(self):
        """Test get_required raises for missing key."""
        config = {}
        with pytest.raises(ConfigurationError) as exc:
            ValidationUtils.get_required(config, "missing_key")
        assert "Configuration 'missing_key' is required but not found" in str(exc.value)

    def test_get_required_empty_string(self):
        """Test get_required raises for empty string."""
        config = {"key": ""}
        with pytest.raises(ConfigurationError) as exc:
            ValidationUtils.get_required(config, "key")
        assert "Configuration 'key' cannot be empty" in str(exc.value)

    def test_get_required_none_value(self):
        """Test get_required raises for None value."""
        config = {"key": None}
        with pytest.raises(ConfigurationError) as exc:
            ValidationUtils.get_required(config, "key")
        assert "Configuration 'key' cannot be None" in str(exc.value)

    def test_get_required_env_with_value(self):
        """Test get_required_env with valid environment variable."""
        with patch.dict("os.environ", {"TEST_VAR": "test_value"}):
            result = ValidationUtils.get_required_env("TEST_VAR")
            assert result == "test_value"

    def test_get_required_env_missing(self):
        """Test get_required_env raises for missing variable."""
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ConfigurationError) as exc:
                ValidationUtils.get_required_env("MISSING_VAR")
            assert "Environment variable 'MISSING_VAR' is required but not set" in str(exc.value)

    def test_get_required_env_empty(self):
        """Test get_required_env raises for empty variable."""
        with patch.dict("os.environ", {"EMPTY_VAR": ""}):
            with pytest.raises(ConfigurationError) as exc:
                ValidationUtils.get_required_env("EMPTY_VAR")
            assert "Environment variable 'EMPTY_VAR' cannot be empty" in str(exc.value)

    def test_get_with_default(self):
        """Test get_with_default returns value when present."""
        config = {"key": "value"}
        result = ValidationUtils.get_with_default(config, "key", "default")
        assert result == "value"

    def test_get_with_default_missing(self):
        """Test get_with_default returns default when missing."""
        config = {}
        result = ValidationUtils.get_with_default(config, "missing", "default")
        assert result == "default"

    def test_get_with_default_empty_string(self):
        """Test get_with_default returns default for empty string."""
        config = {"key": ""}
        result = ValidationUtils.get_with_default(config, "key", "default")
        assert result == "default"

    def test_get_with_default_none(self):
        """Test get_with_default returns default for None."""
        config = {"key": None}
        result = ValidationUtils.get_with_default(config, "key", "default")
        assert result == "default"

    def test_get_env_with_default(self):
        """Test get_env_with_default returns value when set."""
        with patch.dict("os.environ", {"TEST_VAR": "test_value"}):
            result = ValidationUtils.get_env_with_default("TEST_VAR", "default")
            assert result == "test_value"

    def test_get_env_with_default_missing(self):
        """Test get_env_with_default returns default when missing."""
        with patch.dict("os.environ", {}, clear=True):
            result = ValidationUtils.get_env_with_default("MISSING", "default")
            assert result == "default"

    def test_get_env_with_default_empty(self):
        """Test get_env_with_default returns default for empty."""
        with patch.dict("os.environ", {"EMPTY": ""}):
            result = ValidationUtils.get_env_with_default("EMPTY", "default")
            assert result == "default"


class TestConfigValidator:
    """Test ConfigValidator functionality."""

    @pytest.fixture
    def validator(self):
        """Create a ConfigValidator instance."""
        return ConfigValidator()

    def test_validate_sharding_valid(self, validator):
        """Test validate_sharding with valid config."""
        config = {"dimensions": ["date", "venue"], "max_shards": 100, "min_shard_size": 1}
        # Should not raise
        validator.validate_sharding(config)
        assert True, "Validation passed without raising"

    def test_validate_sharding_missing_dimensions(self, validator):
        """Test validate_sharding raises for missing dimensions."""
        config = {"max_shards": 100}
        with pytest.raises(ConfigurationError) as exc:
            validator.validate_sharding(config)
        assert "dimensions" in str(exc.value)

    def test_validate_sharding_invalid_max_shards(self, validator):
        """Test validate_sharding with invalid max_shards."""
        config = {"dimensions": ["date"], "max_shards": -1}
        with pytest.raises(ConfigurationError):
            validator.validate_sharding(config)

    def test_validate_sharding_invalid_dimensions_type(self, validator):
        """Test validate_sharding with non-list dimensions."""
        config = {
            "dimensions": "date",  # Should be list
            "max_shards": 100,
        }
        with pytest.raises(ConfigurationError):
            validator.validate_sharding(config)

    def test_validate_deployment_valid(self, validator):
        """Test validate_deployment with valid config."""
        config = {"service": "test-service", "region": "us-central1", "backend": "cloud-run"}
        # Should not raise
        validator.validate_deployment(config)
        assert True, "Validation passed without raising"

    def test_validate_deployment_missing_service(self, validator):
        """Test validate_deployment raises for missing service."""
        config = {"region": "us-central1"}
        with pytest.raises(ConfigurationError) as exc:
            validator.validate_deployment(config)
        assert "service" in str(exc.value)

    def test_validate_deployment_invalid_backend(self, validator):
        """Test validate_deployment with invalid backend."""
        config = {"service": "test-service", "backend": "invalid-backend"}
        with pytest.raises(ConfigurationError):
            validator.validate_deployment(config)

    def test_validate_bucket_valid(self, validator):
        """Test validate_bucket with valid config."""
        config = {"name": "test-bucket", "region": "us-central1"}
        # Should not raise
        validator.validate_bucket(config)
        assert True, "Validation passed without raising"

    def test_validate_bucket_missing_name(self, validator):
        """Test validate_bucket raises for missing name."""
        config = {"region": "us-central1"}
        with pytest.raises(ConfigurationError) as exc:
            validator.validate_bucket(config)
        assert "name" in str(exc.value)

    def test_validate_bucket_invalid_name(self, validator):
        """Test validate_bucket with invalid bucket name."""
        config = {
            "name": "Invalid_Bucket_Name!",  # Invalid characters
            "region": "us-central1",
        }
        with pytest.raises(ConfigurationError):
            validator.validate_bucket(config)

    def test_validate_docker_valid(self, validator):
        """Test validate_docker with valid config."""
        config = {"image": "gcr.io/project/image:latest", "registry": "gcr.io"}
        # Should not raise
        validator.validate_docker(config)
        assert True, "Validation passed without raising"

    def test_validate_docker_missing_image(self, validator):
        """Test validate_docker raises for missing image."""
        config = {"registry": "gcr.io"}
        with pytest.raises(ConfigurationError) as exc:
            validator.validate_docker(config)
        assert "image" in str(exc.value)

    def test_validate_docker_invalid_image_format(self, validator):
        """Test validate_docker with invalid image format."""
        config = {
            "image": "not-a-valid-image"  # Missing registry/tag
        }
        with pytest.raises(ConfigurationError):
            validator.validate_docker(config)

    def test_validate_comprehensive(self, validator):
        """Test comprehensive validation of complex config."""
        config = {
            "deployment": {"service": "test-service", "region": "us-central1", "backend": "cloud-run"},
            "sharding": {"dimensions": ["date", "venue"], "max_shards": 100},
            "bucket": {"name": "test-bucket", "region": "us-central1"},
            "docker": {"image": "gcr.io/project/image:latest"},
        }

        # Validate each section
        validator.validate_deployment(config["deployment"])
        validator.validate_sharding(config["sharding"])
        validator.validate_bucket(config["bucket"])
        validator.validate_docker(config["docker"])
        assert True, "Validation passed without raising"


class TestConfigurationError:
    """Test ConfigurationError exception."""

    def test_configuration_error_message(self):
        """Test ConfigurationError preserves message."""
        error = ConfigurationError("Test error message")
        assert str(error) == "Test error message"

    def test_configuration_error_inheritance(self):
        """Test ConfigurationError inherits from Exception."""
        error = ConfigurationError("Test")
        assert isinstance(error, Exception)
