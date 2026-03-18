import logging
import tarfile
from pathlib import Path
from typing import Any

import pytest
import requests
from datacollective.errors import DownloadError

import datacollective.download as download_module
from datacollective import download_dataset, get_dataset_details
from tests.e2e.conftest import skip_if_rate_limited


def test_get_dataset_details_live_api(
    live_api_env: None,
    dev_dataset_id: str,
) -> None:
    """NOTE: This test calls a live MDC API endpoint (dev)."""
    details = None

    try:
        details = get_dataset_details(dev_dataset_id)
    except Exception as exc:
        skip_if_rate_limited(exc)

    assert details is not None
    assert isinstance(details, dict)
    assert details.get("id") == dev_dataset_id
    dataset_name = details.get("name")
    dataset_id = details.get("id")
    dataset_slug = details.get("slug")
    # verify they are non-empty strings
    assert isinstance(dataset_name, str)
    assert isinstance(dataset_id, str)
    assert isinstance(dataset_slug, str)
    assert dataset_name.strip()
    assert dataset_id.strip()
    assert dataset_slug.strip()


def test_download_dataset_live_api(
    live_download_dir: Path,
    dev_dataset_id: str,
) -> None:
    """NOTE: This test calls a live MDC API endpoint (dev)."""
    archive_path = None
    extract_dir = live_download_dir / "extracted"

    try:
        archive_path = download_dataset(
            dev_dataset_id,
            download_directory=str(live_download_dir),
            show_progress=False,
            overwrite_existing=True,
        )
    except Exception as exc:
        skip_if_rate_limited(exc)

    assert archive_path is not None
    assert archive_path.exists()
    assert archive_path.suffix == ".gz"
    assert archive_path.stat().st_size > 0
    assert tarfile.is_tarfile(archive_path)

    # Make sure downloaded file can be extracted successfully and its not empty
    extract_dir.mkdir()
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
        archive.extractall(extract_dir)

    assert members
    assert any(member.isfile() for member in members)

    extracted_files = [path for path in extract_dir.rglob("*") if path.is_file()]
    assert extracted_files
    assert any(path.stat().st_size > 0 for path in extracted_files)


@pytest.fixture
def interrupt_first_download(monkeypatch: pytest.MonkeyPatch) -> dict[str, bool]:
    interrupted = {"value": False}
    real_send_api_request = download_module._send_api_request

    def flaky_send_api_request(*args: Any, **kwargs: Any) -> Any:
        method = args[0] if args else kwargs.get("method")
        response = real_send_api_request(*args, **kwargs)

        if (
            str(method).upper() == "GET"
            and kwargs.get("stream")
            and not interrupted["value"]
        ):
            real_iter_content = response.iter_content

            def interrupted_iter_content(chunk_size: int):
                for chunk in real_iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    yield chunk[: max(1, len(chunk) // 2)]
                    interrupted["value"] = True
                    raise requests.ConnectionError("Simulated interruption")

            response.iter_content = interrupted_iter_content  # type: ignore

        return response

    monkeypatch.setattr(download_module, "send_api_request", flaky_send_api_request)
    return interrupted


def test_resume_download(
    live_download_dir: Path,
    caplog: pytest.LogCaptureFixture,
    interrupt_first_download: dict[str, bool],
    dev_dataset_id: str,
) -> None:
    """
    Verify that download resumes after an interruption.
    This test calls `download_dataset()` twice:
    1. The first live download is interrupted after writing the first partial chunk.
    2. The second live download resumes from the saved `.part` and `.checksum` files.
    NOTE: This test calls a live MDC API endpoint.
    """
    result = None

    try:
        with pytest.raises(DownloadError):
            download_dataset(
                dev_dataset_id,
                download_directory=str(live_download_dir),
                show_progress=False,
                overwrite_existing=True,
            )

        part_files = list(live_download_dir.glob("*.part"))
        assert len(part_files) == 1
        assert part_files[0].stat().st_size > 0

        with caplog.at_level(logging.INFO, logger="datacollective.download"):
            result = download_dataset(
                dev_dataset_id,
                download_directory=str(live_download_dir),
                show_progress=False,
            )
    except Exception as exc:
        skip_if_rate_limited(exc)

    assert interrupt_first_download["value"]
    assert result is not None
    assert result.exists()
    assert "Resuming previously interrupted download" in caplog.text
