from __future__ import annotations

import shutil
import tarfile
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd

from datacollective.api_utils import (
    _get_api_url,
    send_api_request,
)
from datacollective.dataset_loading_scripts.registry import (
    load_dataset_from_name_as_dataframe,
)
from datacollective.download import (
    cleanup_partial_download,
    determine_resume_state,
    execute_download_plan,
    get_download_plan,
    resolve_download_dir,
    write_checksum_file,
)


def get_dataset_details(dataset_id: str) -> dict[str, Any]:
    """
    Return dataset details from the MDC API as a dictionary.

    Args:
        dataset_id: The dataset ID (as shown in MDC platform).
    Returns:
        A dict with dataset details as returned by the API.
    Raises:
        ValueError: If dataset_id is empty.
        FileNotFoundError: If the dataset does not exist (404).
        PermissionError: If access is denied (403).
        RuntimeError: If rate limit is exceeded (429).
        requests.HTTPError: For other non-2xx responses.
    """
    if not dataset_id or not dataset_id.strip():
        raise ValueError("`dataset_id` must be a non-empty string")

    url = f"{_get_api_url()}/datasets/{dataset_id}"
    resp = send_api_request(method="GET", url=url)
    return dict(resp.json())


def save_dataset_to_disk(
    dataset_id: str,
    download_directory: str | None = None,
    show_progress: bool = True,
    overwrite_existing: bool = False,
) -> Path:
    """
    Download the dataset archive to a local directory and return the archive path.
    Skips download if the target file already exists (unless `overwrite_existing=True`).

    Automatically resumes interrupted downloads if a matching .checksum file exists from a
    previous attempt.

    Args:
        dataset_id: The dataset ID (as shown in MDC platform).
        download_directory: Directory where to save the downloaded archive file.
            If None or empty, falls back to env MDC_DOWNLOAD_PATH or default.
        show_progress: Whether to show a progress bar during download.
        overwrite_existing: Whether to overwrite the existing archive file.
    Returns:
        Path to the downloaded dataset archive.
    Raises:
        ValueError: If dataset_id is empty.
        FileNotFoundError: If the dataset does not exist (404).
        PermissionError: If access is denied (403) or download directory is not writable.
        RuntimeError: If rate limit is exceeded (429) or unexpected response format.
        requests.HTTPError: For other non-2xx responses.
    """
    download_plan = get_download_plan(dataset_id, download_directory)

    # Case 1: Skip download if complete dataset archive already exists
    if download_plan.target_filepath.exists() and not overwrite_existing:
        print(
            f"File already exists. "
            f"Skipping download: `{str(download_plan.target_filepath)}`"
        )
        return Path(download_plan.target_filepath)

    # If overwriting, clean up any existing complete or partial download files
    if overwrite_existing:
        cleanup_partial_download(
            download_plan.tmp_filepath, download_plan.checksum_filepath
        )
        if download_plan.target_filepath.exists():
            download_plan.target_filepath.unlink()

    # Determine whether to resume download based on existing .checksum and .part files
    resume_checksum = determine_resume_state(download_plan)

    # Write checksum file before starting download (for potential resume later)
    if download_plan.checksum and not resume_checksum:
        write_checksum_file(download_plan.checksum_filepath, download_plan.checksum)

    execute_download_plan(download_plan, resume_checksum, show_progress)

    # Download complete. Rename temp file to target and remove checksum file
    download_plan.tmp_filepath.replace(download_plan.target_filepath)
    if download_plan.checksum_filepath.exists():
        download_plan.checksum_filepath.unlink()

    print(f"Saved dataset to `{str(download_plan.target_filepath)}`")
    return Path(download_plan.target_filepath)


def load_dataset(
    dataset_id: str,
    download_directory: str | None = None,
    show_progress: bool = True,
    overwrite_existing: bool = False,
    overwrite_extracted: bool = False,
) -> pd.DataFrame:
    """
    Download (if needed), extract (if not already extracted), and load the dataset into a pandas DataFrame.

    If the dataset archive already exists in the download directory, it will not be re-downloaded
    unless `overwrite_existing=True`.

    If there is a directory with the same name as the archive file without the suffix extension, we assume
    it has already been extracted, and it will not be re-extracted unless `overwrite_extracted=True`.

    Uses dataset `details['name']` to check in registry.py for dataset-specific loading logic.

    Automatically resumes interrupted downloads if a .checksum file exists from a
    previous attempt.

    Args:
        dataset_id: The dataset ID (as shown in MDC platform).
        download_directory: Directory where to save the downloaded archive file.
            If None or empty, falls back to env MDC_DOWNLOAD_PATH or default.
        show_progress: Whether to show a progress bar during download.
        overwrite_existing: Whether to overwrite existing archive.
        overwrite_extracted: Whether to overwrite existing extracted files by re-extracting the archive file.
            Only makes sense when overwrite_existing is False.
            Will check in the download directory for existing extracted files with the default naming of the folder.
    Returns:
        A pandas DataFrame with the loaded dataset.
    Raises:
        ValueError: If dataset_id is empty.
        FileNotFoundError: If the dataset does not exist (404).
        PermissionError: If access is denied (403) or download directory is not writable.
        RuntimeError: If rate limit is exceeded (429) or unexpected response format.
        requests.HTTPError: For other non-2xx responses.
    """
    archive_path = save_dataset_to_disk(
        dataset_id=dataset_id,
        download_directory=download_directory,
        show_progress=show_progress,
        overwrite_existing=overwrite_existing,
    )
    base_dir = resolve_download_dir(download_directory)
    extract_dir = _extract_archive(
        archive_path=archive_path,
        dest_dir=base_dir,
        overwrite_extracted=overwrite_extracted,
    )

    details = get_dataset_details(dataset_id)
    dataset_name = str(details.get("name", "")).lower()

    return load_dataset_from_name_as_dataframe(dataset_name, extract_dir)


def _extract_archive(
    archive_path: Path, dest_dir: Path, overwrite_extracted: bool
) -> Path:
    """
    Extract the given archive (.tar.gz, .zip) into `dest_dir`. If the extracted
    directory already exists (check if the default extracted folder exists) and overwrite_extracted is False,
    skip extraction.

    Args:
        archive_path: Path to the archive file.
        dest_dir: Directory where to extract the contents.
        overwrite_extracted: Whether to overwrite existing extracted files.
    Returns:
        Path to the extracted root directory.
    Raises:
        ValueError: If the archive type is unsupported.
    """
    extract_root = _strip_archive_suffix(archive_path)
    # Extract into a dedicated directory under `dest_dir` using stripped name
    target = dest_dir / extract_root.name
    if target.exists():
        if not overwrite_extracted:
            print(
                f"Extracted directory already exists. "
                f"Skipping extraction: `{str(target)}`"
            )
            return target

        print(f"Overwriting existing extracted directory: `{str(target)}`")
        shutil.rmtree(target)

    target.mkdir(parents=True, exist_ok=True)

    if archive_path.suffix == ".zip":
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(target)
    elif archive_path.name.endswith(".tar.gz") or archive_path.suffix == ".tgz":
        with tarfile.open(archive_path, "r:gz") as tf:
            tf.extractall(target)
    else:
        raise ValueError(
            f"Unsupported archive type for `{archive_path.name}`. Expected .tar.gz, .tgz, or .zip."
        )
    return target


def _strip_archive_suffix(path: Path) -> Path:
    """
    Strip known archive suffixes from the filename.
    Args:
        path: Path to the archive file.
    Returns:
        Path with the archive suffix removed.
    """
    name = path.name
    if name.endswith(".tar.gz"):
        return path.with_name(name[: -len(".tar.gz")])
    if name.endswith(".tgz"):
        return path.with_name(name[: -len(".tgz")])
    if name.endswith(".zip"):
        return path.with_name(name[: -len(".zip")])
    # Unknown; drop one suffix if present
    return path.with_suffix("")
