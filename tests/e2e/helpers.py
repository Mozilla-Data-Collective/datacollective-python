from pathlib import Path
from typing import NoReturn

import pytest
from requests import HTTPError

from datacollective.errors import RateLimitError
from datacollective.models import DatasetSubmission, License, Task

LIVE_TEST_SKIP_REASON = (
    "Set MDC_TEST_API_KEY and MDC_TEST_API_URL to run live API tests."
)
DEFAULT_LIVE_DATASET_ID = "cmiq2s3q5000fo207k9g6g7ou"
DEFAULT_LIVE_DATASET_SLUG = "dataset-for-api-python-sdk-tests-do-not-6529c8c3"
EXAMPLE_DATASET_ARCHIVE_PATH = (
    Path(__file__).resolve().parents[2] / "docs/example_dataset.tar.gz"
)


def skip_if_rate_limited(exc: Exception) -> NoReturn:
    """Skip live E2E tests when the backend rejects us with HTTP 429."""
    if isinstance(exc, RateLimitError):
        pytest.skip("Skipped due to API rate limiting (HTTP 429)")
    if isinstance(exc, HTTPError) and getattr(exc, "response", None):
        if getattr(exc.response, "status_code", None) == 429:
            pytest.skip("Skipped due to API rate limiting (HTTP 429)")
    raise exc


def build_full_submission(name: str) -> DatasetSubmission:
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
        exclusivityOptOut=True,
        agreeToSubmit=True,
    )
