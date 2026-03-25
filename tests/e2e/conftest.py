import os
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from tests.e2e.helpers import (
    DEFAULT_LIVE_DATASET_ID,
    EXAMPLE_DATASET_ARCHIVE_PATH,
    LIVE_TEST_SKIP_REASON,
    DEFAULT_LIVE_DATASET_SLUG,
)


@pytest.fixture(scope="session")
def live_api_settings() -> tuple[str | None, str | None]:
    api_key = os.getenv("MDC_TEST_API_KEY")
    api_url = os.getenv("MDC_TEST_API_URL")
    if not (api_key and api_url):
        pytest.skip(LIVE_TEST_SKIP_REASON)
    return api_key, api_url


@pytest.fixture
def live_api_env(
    monkeypatch: MonkeyPatch,
    live_api_settings: tuple[str, str],
) -> None:
    api_key, api_url = live_api_settings
    monkeypatch.setenv("MDC_API_KEY", api_key)
    monkeypatch.setenv("MDC_API_URL", api_url)


@pytest.fixture
def live_download_dir(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    live_api_env: None,
) -> Path:
    monkeypatch.setenv("MDC_DOWNLOAD_PATH", str(tmp_path))
    return tmp_path


@pytest.fixture(scope="session")
def dataset_id() -> str:
    return DEFAULT_LIVE_DATASET_ID


@pytest.fixture(scope="session")
def dataset_slug() -> str:
    return DEFAULT_LIVE_DATASET_SLUG


@pytest.fixture(scope="session")
def example_dataset_archive_path() -> Path:
    if not EXAMPLE_DATASET_ARCHIVE_PATH.exists():
        pytest.fail(
            f"Missing shared E2E archive fixture: {EXAMPLE_DATASET_ARCHIVE_PATH}"
        )
    return EXAMPLE_DATASET_ARCHIVE_PATH
