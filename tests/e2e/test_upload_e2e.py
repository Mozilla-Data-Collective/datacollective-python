from datetime import datetime
import os
from pathlib import Path
from typing import NoReturn

import pytest
from requests import HTTPError

from datacollective.errors import RateLimitError
from datacollective.models import DatasetSubmission, License, Task
from datacollective.submissions import create_submission_with_upload

MDC_TEST_API_KEY = os.getenv("MDC_TEST_API_KEY")
MDC_TEST_API_URL = os.getenv("MDC_TEST_API_URL")

pytestmark = pytest.mark.skipif(
    not (MDC_TEST_API_KEY and MDC_TEST_API_URL),
    reason="Set MDC_TEST_API_KEY and MDC_TEST_API_URL to run live API tests.",
)


def _skip_if_rate_limited(exc: Exception) -> NoReturn:
    if isinstance(exc, RateLimitError):
        pytest.skip("Skipped due to API rate limiting (HTTP 429)")
    if isinstance(exc, HTTPError) and getattr(exc, "response", None):
        if getattr(exc.response, "status_code", None) == 429:
            pytest.skip("Skipped due to API rate limiting (HTTP 429)")
    raise exc


def _archive_path() -> Path:
    return Path(__file__).resolve().parents[2] / "docs/example_dataset.tar.gz"


def _full_submission(name: str) -> DatasetSubmission:
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


def test_create_submission_with_upload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("MDC_API_KEY", MDC_TEST_API_KEY)
    monkeypatch.setenv("MDC_API_URL", MDC_TEST_API_URL)

    name = f"python-sdk-e2e-{datetime.now().strftime('%H:%M - %d/%m/%Y')}"
    archive_path = _archive_path()
    state_path = tmp_path / "predefined-license-upload-state.json"
    submission = _full_submission(name=name)

    try:
        response = create_submission_with_upload(
            file_path=str(archive_path),
            submission=submission,
            state_path=str(state_path),
            verbose=True,
        )
    except Exception as exc:  # noqa: BLE001
        _skip_if_rate_limited(exc)
    else:
        submission_payload = response.get("submission", {})

        assert isinstance(response, dict)
        assert isinstance(submission_payload, dict)
        assert submission_payload.get("id")
        assert submission_payload.get("fileUploadId")
        assert not state_path.exists(), (
            "Upload state should be cleaned up after success"
        )
