import os
from pathlib import Path

import pandas as pd
import pytest
from _pytest.monkeypatch import MonkeyPatch
from requests import HTTPError

from datacollective import get_dataset_details, load_dataset

MDC_TEST_API_KEY = os.getenv("MDC_TEST_API_KEY")
MDC_TEST_API_URL = os.getenv("MDC_TEST_API_URL")

pytestmark = pytest.mark.skipif(
    not (MDC_TEST_API_KEY and MDC_TEST_API_URL),
    reason="Set MDC_API_KEY and MDC_TEST_API_URL to run live API tests.",
)


def _skip_if_rate_limited(exc: Exception) -> None:
    """
    Since our backend implements strict rate limiting
    there is a chance that our e2e tests might hit it,
    so we skip the tests when backend returns HTTP 429 (rate limit)."""
    if isinstance(exc, HTTPError) and getattr(exc, "response", None):
        if getattr(exc.response, "status_code", None) == 429:
            pytest.skip("Skipped due to API rate limiting (HTTP 429)")
    raise exc


def test_get_dataset_details_live_api(
    monkeypatch: MonkeyPatch,
    dataset_id: str = "cmiq2s3q5000fo207k9g6g7ou",
) -> None:
    """NOTE: This test calls a live MDC API endpoint (dev)."""
    monkeypatch.setenv("MDC_API_KEY", MDC_TEST_API_KEY)
    monkeypatch.setenv("MDC_API_URL", MDC_TEST_API_URL)

    try:
        details = get_dataset_details(dataset_id)
    except Exception as exc:
        _skip_if_rate_limited(exc)

    assert isinstance(details, dict)
    assert details.get("id") == dataset_id
    assert isinstance(details.get("name"), str) and details["name"].strip()


def test_load_dataset_live_api(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    dataset_id: str = "cmiq2s3q5000fo207k9g6g7ou",
) -> None:
    """NOTE: This test calls a live MDC API endpoint (dev)."""

    monkeypatch.setenv("MDC_API_KEY", MDC_TEST_API_KEY)
    monkeypatch.setenv("MDC_DOWNLOAD_PATH", str(tmp_path))
    monkeypatch.setenv("MDC_API_URL", MDC_TEST_API_URL)

    try:
        df = load_dataset(
            dataset_id,
            download_directory=str(tmp_path),
            show_progress=False,
            overwrite_existing=True,
        )
    except Exception as exc:  # noqa: BLE001
        _skip_if_rate_limited(exc)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert len(df.columns) > 0
