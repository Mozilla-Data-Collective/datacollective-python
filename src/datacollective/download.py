import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fox_progress_bar import ProgressBar

from datacollective.api_utils import (
    ENV_DOWNLOAD_PATH,
    HTTP_TIMEOUT,
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


def get_download_plan(dataset_id: str, download_directory: str | None) -> DownloadPlan:
    """
    Create a download plan object for the given dataset ID that includes:
    - a download session URL created by the API
    - the filename for the dataset archive defined by the API
    - the final target filepath on disk where the archive will be saved
    - a temporary path for atomic download
    - the size of the dataset archive in bytes
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

    base_dir = resolve_download_dir(download_directory)

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
    tmp_filepath = target_filepath.with_name(target_filepath.name + ".part")

    return DownloadPlan(
        download_url=download_url,
        filename=filename,
        target_filepath=target_filepath,
        tmp_filepath=tmp_filepath,
        size_bytes=int(size_bytes),
        checksum=checksum,
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
    checksum_filepath = get_checksum_filepath(download_plan.target_filepath)
    tmp_filepath = download_plan.tmp_filepath

    part_exists = tmp_filepath.exists()
    checksum_file_exists = checksum_filepath.exists()
    stored_checksum = (
        _read_checksum_file(checksum_filepath) if checksum_file_exists else None
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
            cleanup_partial_download(tmp_filepath, checksum_filepath)
            return None

    # Case 3: .part exists but no .checksum: cannot safely resume -> start fresh
    if part_exists and not checksum_file_exists:
        print(
            "Partial download found without checksum file. Starting fresh download..."
        )
        cleanup_partial_download(tmp_filepath, checksum_filepath)
        return None

    # Case 4: .checksum exists but no .part -> start fresh
    if checksum_file_exists and not part_exists:
        cleanup_partial_download(tmp_filepath, checksum_filepath)
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


def get_checksum_filepath(target_filepath: Path) -> Path:
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
