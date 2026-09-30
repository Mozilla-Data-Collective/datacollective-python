import os
from pathlib import Path
from typing import Any

from fox_progress_bar import ProgressBar
from pydantic import Field

from datacollective.api_utils import (
    ENV_DOWNLOAD_PATH,
    HTTP_TIMEOUT,
    _get_api_url,
    _send_api_request,
)
from datacollective.errors import DownloadError
from datacollective.logging_utils import get_logger
from datacollective.models import NonEmptyStrModel

logger = get_logger(__name__)

DOWNLOAD_SOURCE_SAVE = "download_dataset"
DOWNLOAD_SOURCE_LOAD = "load_dataset"


class DownloadPlan(NonEmptyStrModel):
    download_url: str
    target_filepath: Path
    tmp_filepath: Path
    size_bytes: int = Field(..., gt=0)
    checksum: (
        str | None
    )  # We allow None if the API does not return a checksum, but that's generally unexpected.
    checksum_filepath: Path


def _download_dataset(
    dataset_id: str,
    archive_filename: str,
    download_directory: str | None,
    show_progress: bool,
    overwrite_existing: bool,
    download_source: str | None = None,
) -> Path:
    """
    Download a dataset archive from MDC to a local directory.

    Flow:
    1. Check if the dataset archive already exists in the download directory.
        If it does and overwrite_existing is False, skip download.
    2. Call the MDC API to get a download session URL and other necessary details.
    3. If overwrite_existing is True, clean up any existing files.
    4. Determine whether to resume download based on existing .checksum and .part files.
        If not resuming, write a new .checksum file.
    5. Execute the download plan, downloading to a temporary file.
    6. Once download is complete, rename the temporary file to the target filename and remove the .checksum file.

    Args:
        dataset_id: The unique dataset ID.
        archive_filename: The archive filename.
        download_directory: The directory to download the dataset archive to.
        show_progress: Show a progress bar when downloading the dataset.
        overwrite_existing: If True, overwrite any existing dataset archive in the download directory.
        download_source: Optional context appended to the User-Agent for download analytics.
    Returns:
        The full path to the downloaded dataset archive.
    """
    # Check for already fully downloaded cached version of the dataset archive
    base_dir = _resolve_download_dir(download_directory)
    target_filepath = base_dir / archive_filename
    if target_filepath.exists() and not overwrite_existing:
        logger.info(
            f"Skipping download. Dataset archive already exists at `{target_filepath}`"
        )
        return target_filepath

    # Call MDC API to start download session
    download_plan = _get_download_plan(
        dataset_id=dataset_id,
        target_filepath=target_filepath,
        download_source=download_source,
    )

    # If overwriting, clean up any existing complete or partial download files
    if overwrite_existing:
        logger.info(
            f"Overwriting existing file. Cleaning up any existing files at `{target_filepath}`"
        )
        _cleanup_partial_download(download_plan)
        target_filepath.unlink(missing_ok=True)

    resume_checksum = _determine_resume_state(download_plan)

    # Write checksum file before starting download (for potential resume later)
    if download_plan.checksum and not resume_checksum:
        download_plan.checksum_filepath.write_text(download_plan.checksum)

    _execute_download_plan(
        download_plan=download_plan,
        resume_download_checksum=resume_checksum,
        show_progress=show_progress,
        download_source=download_source,
    )

    # Download complete. Rename temp file to target and remove checksum file
    download_plan.tmp_filepath.replace(target_filepath)
    download_plan.checksum_filepath.unlink(missing_ok=True)

    logger.info(f"Saved dataset to `{target_filepath}`")
    return target_filepath


def _get_download_plan(
    dataset_id: str,
    target_filepath: Path,
    download_source: str | None = None,
) -> DownloadPlan:
    """
    Send a POST request to the API to receive the download session details for a dataset.

    Args:
        dataset_id: The dataset ID (as shown in MDC platform).
        target_filepath: Resolved, absolute full file path to save the downloaded dataset.
        download_source: Optional context appended to the User-Agent for download analytics.

    Returns:
        a DownloadPlan object

    Raises:
        FileNotFoundError: If the dataset does not exist (404).
        PermissionError: If access is denied (403).
        RuntimeError: If rate limit is exceeded (429) or unexpected response format.
        requests.HTTPError: For other non-2xx responses.
    """
    session_url = f"{_get_api_url()}/datasets/{dataset_id}/download"
    resp = _send_api_request(
        method="POST", url=session_url, source_function=download_source
    )

    payload: dict[str, Any] = resp.json()
    download_url = payload.get("downloadUrl")
    size_bytes = payload.get("sizeBytes")
    checksum = payload.get("checksum")

    if not download_url or not size_bytes:
        raise RuntimeError(f"Unexpected response format: {payload}")

    # Stream download to a temporary file for atomicity
    tmp_filepath = target_filepath.with_name(target_filepath.name + ".part")

    checksum_filepath = _get_checksum_filepath(target_filepath)

    size_bytes = int(str(size_bytes))
    download_plan = DownloadPlan(
        download_url=str(download_url),
        target_filepath=target_filepath,
        tmp_filepath=tmp_filepath,
        size_bytes=size_bytes,
        checksum=checksum,
        checksum_filepath=checksum_filepath,
    )
    logger.debug(
        f"Download plan: full path={target_filepath}, size={size_bytes} bytes, checksum={checksum}",
    )
    return download_plan


def _determine_resume_state(download_plan: DownloadPlan) -> str | None:
    """
    Decide whether a previous partial download can be resumed.

    A download is resumed only when both the .part and .checksum files exist
    and the stored checksum matches the current one. In every other case the
    leftover .part / .checksum files are removed and the download starts fresh.

    Args:
        download_plan: The DownloadPlan object with download details.

    Returns:
        The checksum to use for resumption, or None if starting fresh.
    """
    part_exists = download_plan.tmp_filepath.exists()
    checksum_filepath = download_plan.checksum_filepath
    stored_checksum = (
        checksum_filepath.read_text().strip() if checksum_filepath.exists() else None
    )

    if part_exists and stored_checksum:
        if stored_checksum == download_plan.checksum:
            logger.info("Resuming previously interrupted download...")
            return stored_checksum
        logger.info(
            "Dataset has been updated since the previous download attempt. "
            "Starting fresh download..."
        )
    elif part_exists:
        logger.warning(
            "Partial download found without checksum file. Starting fresh download..."
        )

    _cleanup_partial_download(download_plan)
    return None


def _execute_download_plan(
    download_plan: DownloadPlan,
    resume_download_checksum: str | None,
    show_progress: bool,
    download_source: str | None = None,
) -> None:
    """
    Execute the download plan, downloading the dataset to a temporary path.

    Args:
        download_plan: The DownloadPlan object with download details.
        resume_download_checksum: Provide the checksum to resume a previously interrupted download.
        show_progress: Whether to show a progress bar during download.
        download_source: Optional context appended to the User-Agent for download analytics.

    Raises:
        DownloadError: If the download fails or is interrupted.
    """
    headers: dict[str, str] = {}
    previously_downloaded_bytes = 0
    if resume_download_checksum and download_plan.tmp_filepath.exists():
        previously_downloaded_bytes = download_plan.tmp_filepath.stat().st_size
        headers["Range"] = f"bytes={previously_downloaded_bytes}-"

    progress_bar = None
    session_downloaded_bytes = 0
    logger.info(f"Downloading dataset: {download_plan.target_filepath}")
    if show_progress:
        progress_bar = ProgressBar(download_plan.size_bytes)
        progress_bar.update(previously_downloaded_bytes)
        progress_bar._display()
    try:
        with _send_api_request(
            method="GET",
            url=download_plan.download_url,
            stream=True,
            timeout=HTTP_TIMEOUT,
            extra_headers=headers,
            include_auth_headers=False,  # Download URL is pre-signed, no auth needed
            source_function=download_source,
        ) as response:
            with open(download_plan.tmp_filepath, "ab") as f:
                # Iterate over response in 64KB chunks to avoid using too much memory
                for chunk in response.iter_content(chunk_size=1 << 16):
                    if not chunk:
                        continue
                    f.write(chunk)
                    session_downloaded_bytes += len(chunk)
                    if progress_bar:
                        progress_bar.update(len(chunk))

            if progress_bar:
                progress_bar.finish()
    except (Exception, KeyboardInterrupt) as e:
        raise DownloadError(
            session_bytes=session_downloaded_bytes,
            total_downloaded_bytes=previously_downloaded_bytes
            + session_downloaded_bytes,
            total_archive_bytes=download_plan.size_bytes,
            checksum=download_plan.checksum,
        ) from e


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
    p = Path(base).expanduser()
    p.mkdir(parents=True, exist_ok=True)
    if not os.access(p, os.W_OK):
        raise PermissionError(f"Directory `{p}` is not writable")
    logger.debug(f"Download directory set: {p}")
    return p


def _get_checksum_filepath(target_filepath: Path) -> Path:
    """Return the path to the .checksum file for a given target file."""
    return target_filepath.with_suffix(target_filepath.suffix + ".checksum")


def _cleanup_partial_download(download_plan: DownloadPlan) -> None:
    """Remove partial download files (.part and .checksum)."""
    download_plan.tmp_filepath.unlink(missing_ok=True)
    download_plan.checksum_filepath.unlink(missing_ok=True)
