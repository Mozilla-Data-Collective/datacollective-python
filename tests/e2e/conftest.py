import os
import warnings
from pathlib import Path
from typing import NoReturn, Iterator

import pytest

from requests import ConnectionError as RequestsConnectionError
from requests import HTTPError
from requests import Timeout

from datacollective.errors import RateLimitError
from datacollective.models import DatasetSubmission, License, Task, Visibility
from datacollective.submissions import delete_submission


LIVE_TEST_SKIP_REASON = (
    "Set MDC_TEST_API_KEY and MDC_TEST_API_URL to run live API tests."
)


def skip_if_rate_limited(exc: Exception) -> NoReturn:
    """Skip live E2E tests when the backend or network is unavailable."""
    if isinstance(exc, RateLimitError):
        pytest.skip("Skipped due to API rate limiting (HTTP 429)")
    if isinstance(exc, HTTPError) and getattr(exc, "response", None):
        if getattr(exc.response, "status_code", None) == 429:
            pytest.skip("Skipped due to API rate limiting (HTTP 429)")
    if isinstance(exc, (RequestsConnectionError, Timeout)):
        pytest.skip("Skipped due to live API connectivity issues")
    raise exc


@pytest.fixture(scope="session")
def live_api_settings() -> tuple[str | None, str | None]:
    api_key = os.getenv("MDC_TEST_API_KEY")
    api_url = os.getenv("MDC_TEST_API_URL")
    if not (api_key and api_url):
        pytest.skip(LIVE_TEST_SKIP_REASON)
    return api_key, api_url


@pytest.fixture
def live_api_env(monkeypatch, live_api_settings: tuple[str, str]) -> None:
    api_key, api_url = live_api_settings
    monkeypatch.setenv("MDC_API_KEY", api_key)
    monkeypatch.setenv("MDC_API_URL", api_url)


@pytest.fixture
def live_download_dir(tmp_path: Path, monkeypatch, live_api_env: None) -> Path:
    monkeypatch.setenv("MDC_DOWNLOAD_PATH", str(tmp_path))
    return tmp_path


@pytest.fixture
def submissions_to_delete(live_api_env: None) -> Iterator[list[str]]:
    """Submission IDs a test created, deleted from dev once the test finishes."""
    submission_ids: list[str] = []
    yield submission_ids
    for submission_id in submission_ids:
        try:
            delete_submission(submission_id)
        except RateLimitError:
            warnings.warn(
                f"Rate limited: test submission {submission_id} was not deleted",
                stacklevel=1,
            )


@pytest.fixture(scope="session")
def dev_dataset_id() -> str:
    return "cmiq2s3q5000fo207k9g6g7ou"


@pytest.fixture(scope="session")
def approved_dataset_submission_id() -> str:
    return "cmmxk22ls0003mf07vou27n72"


@pytest.fixture(scope="session")
def example_dataset_archive_path() -> Path:
    archive_path = Path(__file__).resolve().parents[2] / "docs/example_dataset.tar.gz"
    if not archive_path.exists():
        pytest.fail(f"Missing shared E2E archive fixture: {archive_path}")
    return archive_path


def sample_dataset_submission(name: str) -> DatasetSubmission:
    return DatasetSubmission(
        name=name,
        longDescription="End-to-end test submission created by the Python SDK test suite.",
        shortDescription="SDK live upload test",
        locale="en-US",
        task=Task.ASR,
        format="TSV",
        licenseAbbreviation=License.CC_BY_4_0,
        other="Synthetic test metadata for validating real upload flows.",
        restrictions="No production usage.",
        forbiddenUsage="Do not treat this as a real dataset.",
        additionalConditions="Used exclusively for automated SDK validation.",
        pointOfContactFullName="SDK Test Contact",
        pointOfContactEmail="sdk-tests@example.com",
        fundedByFullName="SDK Test Funder",
        fundedByEmail="sdk-tests@example.com",
        legalContactFullName="SDK Legal Contact",
        legalContactEmail="sdk-tests@example.com",
        createdByFullName="SDK Test Creator",
        createdByEmail="sdk-tests@example.com",
        intendedUsage="Exercise the upload and submission lifecycle in live tests.",
        ethicalReviewProcess="No review required for synthetic test data.",
        showContactInfo=False,
        visibility=Visibility.PUBLIC,
        exclusivityOptOut=True,
        agreeToSubmit=True,
    )
