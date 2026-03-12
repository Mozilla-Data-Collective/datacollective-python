import logging
import warnings
from pathlib import Path
from typing import Any

import pandas as pd

from datacollective.api_utils import (
    _get_api_url,
    _send_api_request,
)
from datacollective.archive_utils import _extract_archive
from datacollective.download import (
    DOWNLOAD_SOURCE_SAVE,
    DOWNLOAD_SOURCE_LOAD,
    _get_download_plan,
    _resolve_download_dir,
    _resolve_and_execute_download_plan,
)
from datacollective.schema_loaders.cache_schema import _resolve_schema
from datacollective.schema_loaders.registry import _load_dataset_from_schema
from datacollective.schema import _get_dataset_schema

logger = logging.getLogger(__name__)


def get_dataset_details(dataset_id: str) -> dict[str, Any]:
    """
    Return dataset details from the MDC API as a dictionary.

    Args:
        dataset_id: The dataset ID (as shown in MDC platform) or slug.

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
    resp = _send_api_request(method="GET", url=url)
    return dict(resp.json())


def download_dataset(
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

    Note: Previously called `save_dataset_to_disk`, which remains available as a
    deprecated alias for backward compatibility.

    Args:
        dataset_id: The dataset ID (as shown in MDC platform) or slug.
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
    _id = resolve_dataset_id(dataset_id)
    download_plan = _get_download_plan(
        _id,
        download_directory,
        download_source=DOWNLOAD_SOURCE_SAVE,
    )
    return _resolve_and_execute_download_plan(
        download_plan=download_plan,
        show_progress=show_progress,
        overwrite_existing=overwrite_existing,
        download_source=DOWNLOAD_SOURCE_SAVE,
    )


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

    Uses the dataset schema to determine task-specific loading logic.

    Automatically resumes interrupted downloads if a .checksum file exists from a
    previous attempt.

    Args:
        dataset_id: The dataset ID (as shown in MDC platform) or slug.
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
        ValueError: If dataset_id is empty or schema is unsupported.
        FileNotFoundError: If the dataset does not exist (404).
        PermissionError: If access is denied (403) or download directory is not writable.
        RuntimeError: If rate limit is exceeded (429) or unexpected response format.
        requests.HTTPError: For other non-2xx responses.
    """
    _id = resolve_dataset_id(dataset_id)
    schema = _get_dataset_schema(_id)
    if schema is None:
        raise RuntimeError(
            f"Dataset '{_id}' exists but is not supported by load_dataset yet. "
            f"You can download the raw archive with: download_dataset('{_id}'). "
            f"If you are the data owner consider submitting a schema for your dataset via the registry: https://mozilla-data-collective.github.io/dataset-schema-registry/"
        )

    download_plan = _get_download_plan(
        _id,
        download_directory,
        download_source=DOWNLOAD_SOURCE_LOAD,
    )
    archive_checksum = download_plan.checksum

    archive_path = _resolve_and_execute_download_plan(
        download_plan=download_plan,
        show_progress=show_progress,
        overwrite_existing=overwrite_existing,
        download_source=DOWNLOAD_SOURCE_LOAD,
    )
    base_dir = _resolve_download_dir(download_directory)
    extract_dir = _extract_archive(
        archive_path=archive_path,
        dest_dir=base_dir,
        overwrite_extracted=overwrite_extracted,
    )

    schema = _resolve_schema(_id, extract_dir, archive_checksum)
    return _load_dataset_from_schema(schema, extract_dir)


def resolve_dataset_id(dataset_id: str) -> str:
    """
    Resolves a dataset ID or slug to its canonical MDC ID.

    Args:
        dataset_id: The dataset ID (as shown in MDC platform) or slug.

    Returns:
        The canonical dataset ID.

    Raises:
        RuntimeError: If the dataset does not exist.
    """
    try:
        dataset_details = get_dataset_details(dataset_id)
        return dataset_details.get("id", "")
    except FileNotFoundError:
        raise RuntimeError(
            f"Dataset '{dataset_id}' does not exist in MDC or the ID is mistyped."
        )


def save_dataset_to_disk(
    dataset_id: str,
    download_directory: str | None = None,
    show_progress: bool = True,
    overwrite_existing: bool = False,
) -> Path:
    """
    Deprecated alias for `download_dataset`.

    Use `download_dataset` instead. This name is kept for backward compatibility.
    """
    warnings.warn(
        "`save_dataset_to_disk` is deprecated and will be removed in a future "
        "release. Use `download_dataset` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return download_dataset(
        dataset_id=dataset_id,
        download_directory=download_directory,
        show_progress=show_progress,
        overwrite_existing=overwrite_existing,
    )
