import os

import pytest


@pytest.fixture(autouse=True)
def clean_environment():
    """Automatically clean environment variables before each test."""
    # Store original environment
    original_env = os.environ.copy()

    # Clear MDC-related variables
    for key in ["MDC_API_KEY", "MDC_API_URL", "ENVIRONMENT"]:
        os.environ.pop(key, None)

    yield

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_env_file(tmp_path):
    """Create a temporary directory with mock .env files."""
    # Create temporary .env files
    env_file = tmp_path / ".env"
    env_file.write_text(
        "MDC_API_KEY=prod-test-key\nMDC_API_URL=https://prod.test.url\n"
    )

    dev_env_file = tmp_path / ".env.development"
    dev_env_file.write_text(
        "MDC_API_KEY=dev-test-key\nMDC_API_URL=https://dev.test.url\n"
    )

    return tmp_path
