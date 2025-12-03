import os
from pathlib import Path

import pandas as pd
import pytest
from _pytest.monkeypatch import MonkeyPatch

from datacollective import get_dataset_details, load_dataset

MDC_TEST_API_KEY = os.getenv("MDC_TEST_API_KEY")
MDC_TEST_API_URL = os.getenv("MDC_TEST_API_URL")

pytestmark = pytest.mark.skipif(
    not (MDC_TEST_API_KEY and MDC_TEST_API_URL),
    reason="Set MDC_API_KEY and MDC_TEST_API_URL to run live API tests.",
)


def test_get_dataset_details_live_api(
    monkeypatch: MonkeyPatch,
    dataset_id: str = "cmiq2s3q5000fo207k9g6g7ou",
) -> None:
    """NOTE: This test calls a live MDC API endpoint (dev)."""
    monkeypatch.setenv("MDC_API_KEY", MDC_TEST_API_KEY)
    monkeypatch.setenv("MDC_API_URL", MDC_TEST_API_URL)

    details = get_dataset_details(dataset_id)

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

    df = load_dataset(
        dataset_id,
        download_directory=str(tmp_path),
        show_progress=False,
        overwrite_existing=True,
    )

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert len(df.columns) > 0
