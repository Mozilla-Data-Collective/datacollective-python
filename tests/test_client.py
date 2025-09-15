import os
import tempfile
from unittest.mock import patch

import pytest
import requests

from datacollective import DataCollective


class TestDataCollective:
    """Test suite for DataCollective client."""

    def test_init_with_api_key_parameter(self):
        """Test initialization with API key passed as parameter."""
        client = DataCollective(api_key="test-api-key-123")
        assert client.api_key == "test-api-key-123"
        assert client.api_url == "https://datacollective.mozillafoundation.org/api/"

    def test_init_with_env_variable(self):
        """Test initialization with API key from environment variable."""
        with patch.dict(os.environ, {"MDC_API_KEY": "env-api-key-456"}):
            client = DataCollective()
            assert client.api_key == "env-api-key-456"

    def test_init_missing_api_key_raises_error(self):
        """Test that missing API key raises ValueError."""
        with patch.dict(os.environ, {}, clear=True):
            with patch(
                "datacollective.client.load_dotenv"
            ):  # Mock to prevent loading from file
                with pytest.raises(ValueError) as exc_info:
                    DataCollective()
                assert "API key missing" in str(exc_info.value)

    def test_custom_api_url_from_env(self):
        """Test custom API URL from environment variable."""
        with patch.dict(
            os.environ,
            {"MDC_API_KEY": "test-key", "MDC_API_URL": "https://custom.api.url"},
        ):
            client = DataCollective()
            assert client.api_url == "https://custom.api.url/"

    def test_default_api_url_when_env_not_set(self):
        """Test default API URL is used when env variable not set."""
        with patch.dict(os.environ, {"MDC_API_KEY": "test-key"}):
            # Ensure MDC_API_URL is not set
            os.environ.pop("MDC_API_URL", None)
            client = DataCollective()
            assert client.api_url == "https://datacollective.mozillafoundation.org/api/"

    def test_environment_parameter_loads_correct_env_file(self):
        """Test that different environment parameter loads correct .env file."""
        # Create temporary .env files
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create .env.development file
            dev_env_file = os.path.join(tmpdir, ".env.development")
            with open(dev_env_file, "w") as f:
                f.write("MDC_API_KEY=dev-key-789\n")
                f.write("MDC_API_URL=https://dev.api.url\n")

            # Change to temp directory and create client
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                client = DataCollective(environment="development")
                assert client.api_key == "dev-key-789"
                assert client.api_url == "https://dev.api.url/"
            finally:
                os.chdir(original_cwd)

    def test_production_environment_loads_default_env(self):
        """Test that production environment loads default .env file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create default .env file
            env_file = os.path.join(tmpdir, ".env")
            with open(env_file, "w") as f:
                f.write("MDC_API_KEY=prod-key-999\n")

            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                client = DataCollective(environment="production")
                assert client.api_key == "prod-key-999"
            finally:
                os.chdir(original_cwd)

    def test_parameter_overrides_env_variable(self):
        """Test that parameter takes precedence over environment variable."""
        with patch.dict(os.environ, {"MDC_API_KEY": "env-key"}):
            client = DataCollective(api_key="param-key")
            assert client.api_key == "param-key"

    @patch("datacollective.client.requests.post")
    def test_get_dataset_handles_http_error(self, mock_post):
        """Test that get_dataset handles HTTP errors properly."""
        # Mock a 403 Forbidden response
        mock_response = mock_post.return_value
        mock_response.status_code = 403

        # Create a proper HTTPError with response
        http_error = requests.exceptions.HTTPError("403 Client Error: Forbidden")
        http_error.response = mock_response
        mock_response.raise_for_status.side_effect = http_error

        client = DataCollective(api_key="test-key")
        result = client.get_dataset("test-dataset")

        assert result is None
        mock_post.assert_called_once()


class TestDataCollectiveWithMocking:
    """Tests using mocking for isolation."""

    @patch("datacollective.client.load_dotenv")
    def test_load_dotenv_called_for_development(self, mock_load_dotenv):
        """Test that load_dotenv is called with correct path for development."""
        with patch.dict(os.environ, {"MDC_API_KEY": "test-key"}):
            with patch("os.path.exists", return_value=True):
                DataCollective(environment="development")
                mock_load_dotenv.assert_called_once_with(dotenv_path=".env.development")

    @patch("datacollective.client.load_dotenv")
    def test_load_dotenv_fallback_when_env_file_missing(self, mock_load_dotenv):
        """Test that load_dotenv falls back to default when env file doesn't exist."""
        with patch.dict(os.environ, {"MDC_API_KEY": "test-key"}):
            with patch("os.path.exists", return_value=False):
                DataCollective(environment="staging")
                mock_load_dotenv.assert_called_once_with()


# Fixtures for shared test data
@pytest.fixture
def api_key():
    """Fixture providing a test API key."""
    return "test-api-key-fixture"


@pytest.fixture
def client(api_key):
    """Fixture providing a DataCollective client."""
    return DataCollective(api_key=api_key)


class TestDataCollectiveWithFixtures:
    """Tests using fixtures for common setup."""

    def test_client_fixture_has_api_key(self, client, api_key):
        """Test that fixture-provided client has correct API key."""
        assert client.api_key == api_key

    def test_client_fixture_has_default_url(self, client):
        """Test that fixture-provided client has default URL."""
        assert client.api_url == "https://datacollective.mozillafoundation.org/api/"
