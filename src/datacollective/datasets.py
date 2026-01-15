from __future__ import annotations

import os
import tarfile
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
from fox_progress_bar import ProgressBar
import requests

from datacollective.api_utils import (
    ENV_DOWNLOAD_PATH,
    HTTP_TIMEOUT,
    _get_api_url,
    api_request,
)
from datacollective.dataset_loading_scripts.registry import (
    load_dataset_from_name_as_dataframe,
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
    resp = api_request("GET", url)
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
    Args:
        dataset_id: The dataset ID (as shown in MDC platform).
        download_directory: Directory where to save the downloaded dataset.
            If None or empty, falls back to env MDC_DOWNLOAD_PATH or default.
        show_progress: Whether to show a progress bar during download.
        overwrite_existing: Whether to overwrite existing files.
    Returns:
        Path to the downloaded dataset archive.
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
    if not download_url or not filename:
        raise RuntimeError(f"Unexpected response format: {payload}")

    target_path = base_dir / filename
    if target_path.exists() and not overwrite_existing:
        print(f"File already exists. Skipping download: `{str(target_path)}`")
        return Path(target_path)

    # Stream download to a temporary file for atomicity
    tmp_path = target_path.with_suffix(target_path.suffix + ".part")


    with requests.request(
        method="GET",
        url=download_url,
        timeout=HTTP_TIMEOUT,
    ) as r:
        total = int(r.headers.get("content-length", "0"))

        if show_progress:
            print(f"Downloading dataset: {filename}")
            progress_bar = ProgressBar(total)
            # Show initial progress bar with fox at the start
            progress_bar._display()
        else:
            print(f"Downloading dataset: {filename}")

        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 16):
                if not chunk:
                    continue
                f.write(chunk)
                if show_progress:
                    progress_bar.update(len(chunk))

    if show_progress:
        progress_bar.finish()

    tmp_path.replace(target_path)
    print(f"Saved dataset to `{str(target_path)}`")
    return Path(target_path)


def load_dataset(
    dataset_id: str,
    download_directory: str | None = None,
    show_progress: bool = True,
    overwrite_existing: bool = False,
) -> pd.DataFrame:
    """
    Download (if needed), extract, and load the dataset into a pandas DataFrame.
    Uses dataset `details['name']` to check in registry.py for dataset-specific loading logic.
    Args:
        dataset_id: The dataset ID (as shown in MDC platform).
        download_directory: Directory where to save the downloaded dataset.
            If None or empty, falls back to env MDC_DOWNLOAD_PATH or default.
        show_progress: Whether to show a progress bar during download.
        overwrite_existing: Whether to overwrite existing files.
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
    base_dir = _resolve_download_dir(download_directory)
    extract_dir = _extract_archive(archive_path, base_dir)

    details = get_dataset_details(dataset_id)
    dataset_name = str(details.get("name", "")).lower()

    return load_dataset_from_name_as_dataframe(dataset_name, extract_dir)


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


def _extract_archive(archive_path: Path, dest_dir: Path) -> Path:
    """
    Extract the given archive (.tar.gz, .tgz, .zip) into `dest_dir`.
    Args:
        archive_path: Path to the archive file.
        dest_dir: Directory where to extract the contents.
    Returns:
        Path to the extracted root directory.
    Raises:
        ValueError: If the archive type is unsupported.
    """
    extract_root = _strip_archive_suffix(archive_path)
    # Extract into a dedicated directory under `dest_dir` using stripped name
    target = dest_dir / extract_root.name
    if target.exists():
        # Keep it simple and ensure fresh state
        import shutil

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
