from __future__ import annotations

import os
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from fox_progress_bar import ProgressBar

from datacollective.api_utils import (
    ENV_DOWNLOAD_PATH,
    HTTP_TIMEOUT,
    _extract_checksum_from_api_response,
    _get_api_url,
    _prepare_download_headers,
    api_request,
)
from datacollective.dataset_loading_scripts.registry import (
    load_dataset_from_name_as_dataframe,
)
from datacollective.errors import DownloadError


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


@dataclass
class DownloadPlan:
    download_url: str
    filename: str
    target_path: Path
    tmp_path: Path
    size_bytes: int
    checksum: str | None


def _get_download_plan(dataset_id: str, download_directory: str | None) -> DownloadPlan:
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

    target_path = base_dir / filename

    # Stream download to a temporary file for atomicity
    tmp_path = target_path.with_suffix(target_path.suffix + ".part")

    return DownloadPlan(
        download_url=download_url,
        filename=filename,
        target_path=target_path,
        tmp_path=tmp_path,
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
        download_plan.tmp_path, resume_download_checksum
    )

    progress_bar = None
    if show_progress:
        progress_bar = ProgressBar(download_plan.size_bytes)
        progress_bar.update(downloaded_bytes)
        progress_bar._display()
    try:
        with requests.get(
            download_plan.download_url,
            stream=True,
            timeout=HTTP_TIMEOUT,
            headers=headers,
        ) as response:
            response.raise_for_status()
            # Validate checksum if both expected checksum and response checksum exist.
            if download_plan.checksum:
                response_checksum = _extract_checksum_from_api_response(response)
                if response_checksum != download_plan.checksum:
                    raise ValueError(
                        f"Checksum from server ({response_checksum}) does not match expected checksum for dataset ({download_plan.checksum})."
                    )

            print(f"Downloading dataset: {download_plan.filename}")

            with open(download_plan.tmp_path, "ab") as f:
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


def save_dataset_to_disk(
    dataset_id: str,
    download_directory: str | None = None,
    show_progress: bool = True,
    overwrite_existing: bool = False,
    resume_download_checksum: str | None = None,
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
        resume_download_checksum: Provide the checksum to resume a previously interrupted download.
    Returns:
        Path to the downloaded dataset archive.
    Raises:
        ValueError: If dataset_id is empty.
        FileNotFoundError: If the dataset does not exist (404).
        PermissionError: If access is denied (403) or download directory is not writable.
        RuntimeError: If rate limit is exceeded (429) or unexpected response format.
        requests.HTTPError: For other non-2xx responses.
    """
    download_plan = _get_download_plan(dataset_id, download_directory)
    if resume_download_checksum and resume_download_checksum != download_plan.checksum:
        raise ValueError(
            "Cannot resume download, checksum does not match. "
            "This is likely because the dataset has been updated "
            "since the previous download attempt."
        )

    # Skip download if file already exists
    if download_plan.target_path.exists() and not overwrite_existing:
        print(
            f"File already exists. "
            f"Skipping download: `{str(download_plan.target_path)}`"
        )
        return Path(download_plan.target_path)

    _execute_download_plan(download_plan, resume_download_checksum, show_progress)

    download_plan.tmp_path.replace(download_plan.target_path)
    print(f"Saved dataset to `{str(download_plan.target_path)}`")
    return Path(download_plan.target_path)


def load_dataset(
    dataset_id: str,
    download_directory: str | None = None,
    show_progress: bool = True,
    overwrite_existing: bool = False,
    resume_download_checksum: str | None = None,
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
        resume_download_checksum: Provide the checksum to resume a previously interrupted download.
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
        resume_download_checksum=resume_download_checksum,
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
