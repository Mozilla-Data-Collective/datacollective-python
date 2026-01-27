import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fox_progress_bar import ProgressBar

from datacollective.api_utils import (
    ENV_DOWNLOAD_PATH,
    HTTP_TIMEOUT,
    _get_api_url,
    _prepare_download_headers,
    send_api_request,
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
    )  # We allow None if the API does not return a checksum, but that's generally unexpected.
    checksum_filepath: Path


def get_download_plan(dataset_id: str, download_directory: str | None) -> DownloadPlan:
    """
    Send a POST request to the API to receive the download session details for a dataset.

    Args:
        dataset_id: The dataset ID (as shown in MDC platform).
        download_directory: Directory where to save the downloaded dataset.
            If None or empty, falls back to env MDC_DOWNLOAD_PATH or default.
    Returns:
        a DownloadPlan containing:
        - a download session URL created by the API
        - the filename for the dataset archive defined by the API
        - the final target filepath on disk where the archive will be saved
        - a temporary path for atomic download
        - the size of the dataset archive in bytes
        - the checksum of the dataset
    Raises:
        ValueError: If dataset_id is empty.
        FileNotFoundError: If the dataset does not exist (404).
        PermissionError: If access is denied (403) or download directory is not writable.
        RuntimeError: If rate limit is exceeded (429) or unexpected response format.
        requests.HTTPError: For other non-2xx responses.
    """
    if not dataset_id or not dataset_id.strip():
        raise ValueError("`dataset_id` must be a non-empty string")

    base_dir = resolve_download_dir(download_directory)

    # Create a download session to get `downloadUrl` and `filename`
    session_url = f"{_get_api_url()}/datasets/{dataset_id}/download"
    resp = send_api_request(method="POST", url=session_url)

    payload: dict[str, Any] = dict(resp.json())
    download_url = payload.get("downloadUrl")
    filename = payload.get("filename")
    size_bytes = payload.get("sizeBytes")
    checksum = payload.get("checksum")

    if not download_url or not filename or not size_bytes:
        raise RuntimeError(f"Unexpected response format: {payload}")

    target_filepath = base_dir / filename

    # Stream download to a temporary file for atomicity
    tmp_filepath = target_filepath.with_name(target_filepath.name + ".part")

    checksum_filepath = _get_checksum_filepath(target_filepath)

    return DownloadPlan(
        download_url=download_url,
        filename=filename,
        target_filepath=target_filepath,
        tmp_filepath=tmp_filepath,
        size_bytes=int(size_bytes),
        checksum=checksum,
        checksum_filepath=checksum_filepath,
    )


def determine_resume_state(download_plan: DownloadPlan) -> str | None:
    """
    Determine whether to resume a download based on existing files.

    Returns:
        resume_checksum: The checksum to use for resumption, or None if starting fresh.

    Cases handled:
        Case 1: .checksum and .part exist, checksum matches -> resume download.
        Case 2: .checksum and .part exist, checksum does NOT match -> start fresh.
        Case 3: .part exists but no .checksum -> start fresh (cannot safely resume).
        Case 4: .checksum exists but no .part -> start fresh (orphaned checksum).
        Case 5: Neither .checksum nor .part exist -> start fresh.
    """
    tmp_filepath = download_plan.tmp_filepath

    # Check existence of .part and .checksum files
    part_exists = tmp_filepath.exists()
    checksum_file_exists = download_plan.checksum_filepath.exists()
    stored_checksum = (
        _read_checksum_file(download_plan.checksum_filepath)
        if checksum_file_exists
        else None
    )

    # Case 1: Both .part and .checksum exist
    if part_exists and checksum_file_exists and stored_checksum:
        if stored_checksum == download_plan.checksum:
            # Checksum matches -> resume download
            print("Resuming previously interrupted download...")
            return stored_checksum
        else:
            # Case 2: Checksum does not match, i.e. dataset was updated -> start fresh
            print(
                "Dataset has been updated since the previous download attempt. "
                "Starting fresh download..."
            )
            cleanup_partial_download(tmp_filepath, download_plan.checksum_filepath)
            return None

    # Case 3: .part exists but no .checksum: cannot safely resume -> start fresh
    if part_exists and not checksum_file_exists:
        print(
            "Partial download found without checksum file. Starting fresh download..."
        )
        cleanup_partial_download(tmp_filepath, download_plan.checksum_filepath)
        return None

    # Case 4: .checksum exists but no .part -> start fresh
    if checksum_file_exists and not part_exists:
        cleanup_partial_download(tmp_filepath, download_plan.checksum_filepath)
        return None

    # Case 5: Neither .checksum nor .part exist -> start fresh
    return None


def execute_download_plan(
    download_plan: DownloadPlan,
    resume_download_checksum: str | None,
    show_progress: bool,
) -> None:
    """
    Execute the download plan, downloading the dataset to a temporary path.
    Args:
        download_plan: The DownloadPlan object with download details.
        resume_download_checksum: Provide the checksum to resume a previously interrupted download.
        show_progress: Whether to show a progress bar during download.
    Raises:
        DownloadError: If the download fails or is interrupted.
    """

    headers, downloaded_bytes_so_far = _prepare_download_headers(
        download_plan.tmp_filepath, resume_download_checksum
    )

    progress_bar = None
    session_downloaded_bytes = 0
    total_downloaded_bytes = downloaded_bytes_so_far
    print(f"Downloading dataset: {download_plan.filename}")
    if show_progress:
        progress_bar = ProgressBar(download_plan.size_bytes)
        progress_bar.update(downloaded_bytes_so_far)
        progress_bar._display()
    try:
        with send_api_request(
            method="GET",
            url=download_plan.download_url,
            stream=True,
            timeout=HTTP_TIMEOUT,
            extra_headers=headers,
            include_auth_headers=False,  # Download URL is pre-signed, no auth needed
        ) as response:
            with open(download_plan.tmp_filepath, "ab") as f:
                # Iterate over response in 64KB chunks to avoid using too much memory
                for chunk in response.iter_content(chunk_size=1 << 16):
                    if not chunk:
                        continue
                    f.write(chunk)
                    downloaded_bytes_so_far = len(chunk)
                    session_downloaded_bytes += downloaded_bytes_so_far
                    total_downloaded_bytes += downloaded_bytes_so_far
                    if progress_bar:
                        progress_bar.update(downloaded_bytes_so_far)

            if progress_bar:
                progress_bar.finish()
    except (Exception, KeyboardInterrupt) as e:
        raise DownloadError(
            session_bytes=session_downloaded_bytes,
            total_downloaded_bytes=total_downloaded_bytes,
            total_archive_bytes=download_plan.size_bytes,
            checksum=download_plan.checksum,
        ) from e


def resolve_download_dir(download_directory: str | None) -> Path:
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


def _get_checksum_filepath(target_filepath: Path) -> Path:
    """Return the path to the .checksum file for a given target file."""
    return target_filepath.with_suffix(target_filepath.suffix + ".checksum")


def write_checksum_file(checksum_filepath: Path, checksum: str) -> None:
    """Write the checksum to the .checksum file."""
    checksum_filepath.write_text(checksum)


def _read_checksum_file(checksum_filepath: Path) -> str | None:
    """Read the checksum from the .checksum file, or None if it doesn't exist."""
    if not checksum_filepath.exists():
        return None
    return checksum_filepath.read_text().strip()


def cleanup_partial_download(tmp_filepath: Path, checksum_filepath: Path) -> None:
    """Remove partial download files (.part and .checksum)."""
    if tmp_filepath.exists():
        tmp_filepath.unlink()
    if checksum_filepath.exists():
        checksum_filepath.unlink()
