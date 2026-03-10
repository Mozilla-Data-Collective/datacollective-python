import logging
from pathlib import Path

import pytest

from datacollective import get_dataset_details, save_dataset_to_disk
from datacollective.api_utils import (
    _prepare_download_headers,
    send_api_request,
)
from datacollective.download import (
    get_download_plan,
    write_checksum_file,
)
from tests.e2e.helpers import skip_if_rate_limited


def test_get_dataset_details_live_api(
    live_api_env: None,
    dataset_id: str,
) -> None:
    """NOTE: This test calls a live MDC API endpoint (dev)."""

    details = None
    try:
        details = get_dataset_details(dataset_id)
    except Exception as exc:
        skip_if_rate_limited(exc)

    assert details is not None
    assert isinstance(details, dict)
    assert details.get("id") == dataset_id
    dataset_name = details.get("name")
    assert isinstance(dataset_name, str)
    assert dataset_name.strip()


def test_resume_download(
    tmp_path: Path,
    live_api_env: None,
    caplog: pytest.LogCaptureFixture,
    dataset_id: str,
) -> None:
    """
    Verify that download resumes after an interruption.
    This test:
    1. Starts a real download
    2. Interrupts it after downloading some bytes (simulating a network failure)
    3. Resumes the download and verifies it completes successfully
    NOTE: This test calls a live MDC API endpoint.
    """

    plan = None
    result_path = None
    try:
        # Get download plan to know the expected checksum and file paths
        plan = get_download_plan(dataset_id, str(tmp_path))

        # Write the checksum file (as save_dataset_to_disk would do before downloading)
        if plan.checksum:
            write_checksum_file(plan.checksum_filepath, plan.checksum)

        # Start a real download but interrupt it after downloading some bytes
        # We'll download only a portion by manually streaming and stopping early

        headers, _ = _prepare_download_headers(plan.tmp_filepath, None)

        with send_api_request(
            "GET",
            plan.download_url,
            stream=True,
            timeout=(10, 30),
            extra_headers=headers,
            include_auth_headers=False,
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
        with caplog.at_level(logging.INFO, logger="datacollective.download"):
            result_path = save_dataset_to_disk(
                dataset_id,
                download_directory=str(tmp_path),
                show_progress=False,
            )

        # Verify resume message was logged
        assert "Resuming previously interrupted download" in caplog.text

    except Exception as exc:
        skip_if_rate_limited(exc)

    assert plan is not None
    assert result_path is not None
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
