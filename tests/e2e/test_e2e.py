import os
from pathlib import Path

import pandas as pd
import pytest
from _pytest.monkeypatch import MonkeyPatch
from requests import HTTPError

from datacollective import get_dataset_details, load_dataset, save_dataset_to_disk
from datacollective.api_utils import _prepare_download_headers, api_request
from datacollective.download import (
    get_download_plan,
    write_checksum_file,
)

MDC_TEST_API_KEY = os.getenv("MDC_TEST_API_KEY")
MDC_TEST_API_URL = os.getenv("MDC_TEST_API_URL")

pytestmark = pytest.mark.skipif(
    not (MDC_TEST_API_KEY and MDC_TEST_API_URL),
    reason="Set MDC_TEST_API_KEY and MDC_TEST_API_URL to run live API tests.",
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


def test_resume_download(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    dataset_id: str = "cmiq2s3q5000fo207k9g6g7ou",
) -> None:
    """
    Verify that download resumes after an interruption.
    This test:
    1. Starts a real download
    2. Interrupts it after downloading some bytes (simulating a network failure)
    3. Resumes the download and verifies it completes successfully
    NOTE: This test calls a live MDC API endpoint.
    """
    monkeypatch.setenv("MDC_API_KEY", MDC_TEST_API_KEY)
    monkeypatch.setenv("MDC_API_URL", MDC_TEST_API_URL)

    try:
        # Get download plan to know the expected checksum and file paths
        plan = get_download_plan(dataset_id, str(tmp_path))

        # Write the checksum file (as save_dataset_to_disk would do before downloading)
        if plan.checksum:
            write_checksum_file(plan.checksum_filepath, plan.checksum)

        # Start a real download but interrupt it after downloading some bytes
        # We'll download only a portion by manually streaming and stopping early

        headers, _ = _prepare_download_headers(plan.tmp_filepath, None)

        with api_request(
            "GET",
            plan.download_url,
            stream=True,
            timeout=(10, 30),
            headers=headers,
        ) as response:
            bytes_to_download = min(
                1024 * 10, plan.size_bytes // 4
            )  # Download ~10KB or 25%
            downloaded = 0

            with open(plan.tmp_filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    downloaded += len(chunk)
                    if downloaded >= bytes_to_download:
                        break  # Simulate interruption

        # Verify partial download state
        assert plan.tmp_filepath.exists(), (
            "Part file should exist after partial download"
        )
        assert plan.checksum_filepath.exists(), "Checksum file should exist"
        partial_size = plan.tmp_filepath.stat().st_size
        assert partial_size > 0, "Part file should have some content"
        assert partial_size < plan.size_bytes, (
            "Part file should be smaller than full file"
        )

        # Now resume the download using save_dataset_to_disk
        result_path = save_dataset_to_disk(
            dataset_id,
            download_directory=str(tmp_path),
            show_progress=False,
        )

        # Verify resume message was printed
        captured = capsys.readouterr()
        assert "Resuming previously interrupted download" in captured.out

    except Exception as exc:
        _skip_if_rate_limited(exc)

    # After successful download:
    assert result_path.exists(), "Final file should exist"
    assert result_path.stat().st_size == plan.size_bytes, (
        "Final file should be complete"
    )
    # Cleanup files should be gone
    assert not plan.checksum_filepath.exists(), (
        "Checksum file should be removed after completion"
    )
    assert not plan.tmp_filepath.exists(), (
        "Part file should be removed after completion"
    )
