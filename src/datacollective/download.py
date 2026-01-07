from fox_progress_bar import ProgressBar
from dataclasses import dataclass
import os

from pathlib import Path
from typing import Any
from datacollective.api_utils import (
    HTTP_TIMEOUT,
    ENV_DOWNLOAD_PATH,
    _extract_checksum_from_api_response,
    _get_api_url,
    _prepare_download_headers,
    api_request,
)

from datacollective.errors import DownloadError


@dataclass
class DownloadPlan:
    download_url: str
    filename: str
    target_filepath: Path
    tmp_filepath: Path
    size_bytes: int
    checksum: (
        str | None
    )  # Some datasets sadly will not have checksums yet - we should close this up when they all are guaranteed to


def _get_download_plan(dataset_id: str, download_directory: str | None) -> DownloadPlan:
    """
    Create a download plan object for the given dataset ID that includes:
    - a valid download URL created by the API for this download session
    - the filename for the dataset archive
    - the final target filepath on disk where the archive will be saved
    - a temporary path for atomic download
    - the size of the dataset in bytes
    - the checksum of the dataset
    Args:
        dataset_id: The dataset ID (as shown in MDC platform).
        download_directory: Directory where to save the downloaded dataset.
            If None or empty, falls back to env MDC_DOWNLOAD_PATH or default.
    Returns:
        A DownloadPlan object with download details.
    Raises:
        ValueError: If dataset_id is empty.
        FileNotFoundError: If the dataset does not exist (404).
        PermissionError: If access is denied (403) or download directory is not writable.
        RuntimeError: If rate limit is exceeded (429) or unexpected response format.
        requests.HTTPError: For other non-2xx responses.
    """
    if not dataset_id or not dataset_id.strip():
        raise ValueError("`dataset_id` must be a non-empty string")

    base_dir = _resolve_download_dir(download_directory)

    # Create a download session to get `downloadUrl` and `filename`
    session_url = f"{_get_api_url()}/datasets/{dataset_id}/download"
    resp = api_request("POST", session_url)
    payload: dict[str, Any] = dict(resp.json())

    download_url = payload.get("downloadUrl")
    filename = payload.get("filename")
    size_bytes = payload.get("sizeBytes")
    checksum = payload.get("checksum")
    if not download_url or not filename or not size_bytes:
        raise RuntimeError(f"Unexpected response format: {payload}")

    target_filepath = base_dir / filename

    # Stream download to a temporary file for atomicity
    tmp_filepath = target_filepath.with_suffix(target_filepath.suffix + ".part")

    return DownloadPlan(
        download_url=download_url,
        filename=filename,
        target_filepath=target_filepath,
        tmp_filepath=tmp_filepath,
        size_bytes=int(size_bytes),
        checksum=checksum,
    )


def _execute_download_plan(
    download_plan: DownloadPlan,
    resume_download_checksum: str | None,
    show_progress: bool,
) -> None:
    """
    Execute the download plan, downloading the dataset to the temporary path.
    Args:
        download_plan: The DownloadPlan object with download details.
        resume_download_checksum: Provide the checksum to resume a previously interrupted download.
        show_progress: Whether to show a progress bar during download.
    Raises:
        DownloadError: If the download fails or is interrupted.
    """

    headers, downloaded_bytes = _prepare_download_headers(
        download_plan.tmp_filepath, resume_download_checksum
    )

    progress_bar = None
    if show_progress:
        progress_bar = ProgressBar(download_plan.size_bytes)
        progress_bar.update(downloaded_bytes)
        progress_bar._display()
    try:
        with api_request(
            "GET",
            download_plan.download_url,
            stream=True,
            timeout=HTTP_TIMEOUT,
            headers=headers,
        ) as response:
            if download_plan.checksum:
                # Only validate checksum if we have one to check against
                checksum = _extract_checksum_from_api_response(response)
                if checksum != download_plan.checksum:
                    raise ValueError(
                        f"Checksum from server ({checksum}) does not match expected checksum for dataset ({download_plan.checksum})."
                    )

            print(f"Downloading dataset: {download_plan.filename}")

            with open(download_plan.tmp_filepath, "ab") as f:
                for chunk in response.iter_content(chunk_size=1 << 16):
                    if not chunk:
                        continue
                    f.write(chunk)
                    downloaded_bytes = len(chunk)
                    if progress_bar:
                        progress_bar.update(downloaded_bytes)

            if progress_bar:
                progress_bar.finish()
    except (Exception, KeyboardInterrupt) as e:
        raise DownloadError(downloaded_bytes, download_plan.checksum) from e


def _resolve_download_dir(download_directory: str | None) -> Path:
    """
    Resolve and ensure the download directory exists and is writable.

    Args:
        download_directory (str | None): User-specified download directory.
            If None or empty, falls back to env MDC_DOWNLOAD_PATH or default.

    Returns:
        The resolved Path object for the download directory.
    """
    if download_directory and download_directory.strip():
        base = download_directory
    else:
        base = os.getenv(ENV_DOWNLOAD_PATH, "~/.mozdata/datasets")
    p = Path(os.path.expanduser(base))
    p.mkdir(parents=True, exist_ok=True)
    if not os.access(p, os.W_OK):
        raise PermissionError(f"Directory `{str(p)}` is not writable")
    return p
