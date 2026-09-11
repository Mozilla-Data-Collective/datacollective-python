from __future__ import annotations

import warnings
from pathlib import Path
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd

from datacollective.api_utils import (
    _get_api_url,
    _send_api_request,
)
from datacollective.models import (
    DatasetDetails,
    DatasetFilters,
    DatasetList,
    License,
    Task,
    _normalize_filter_values,
    _require_archive_filename,
    _validate_option,
)
from datacollective.archive_utils import _extract_archive
from datacollective.download import (
    DOWNLOAD_SOURCE_SAVE,
    _download_dataset,
    DOWNLOAD_SOURCE_LOAD,
)
from datacollective.hf_utils import _convert_to_hf, _require_datasets
from datacollective.logging_utils import (
    _enable_logging,
    get_logger,
)
from datacollective.schema_loaders.cache_schema import _resolve_schema
from datacollective.schema_loaders.registry import _load_dataset_from_schema
from datacollective.schema import _get_dataset_schema

if TYPE_CHECKING:
    from datasets import Dataset, DatasetDict

RETURN_FORMATS = ("pandas", "hf")
SORT_OPTIONS = ("relevance", "newest", "size")
SORT_DIRECTIONS = ("asc", "desc")
UPLOAD_DATE_OPTIONS = ("today", "thisWeek", "thisMonth", "thisYear")
PRICING_OPTIONS = ("compensated", "free")
MAX_PAGE_SIZE = 100

logger = get_logger(__name__)


def get_dataset_details(dataset_id: str) -> DatasetDetails:
    """
    Return dataset details from the MDC API.

    This is a public endpoint: no API key (`MDC_API_KEY`) is required and none is sent.

    Args:
        dataset_id: The dataset ID (as shown in MDC platform) or slug.

    Returns:
        A DatasetDetails model with the dataset details as returned by the API.

    Raises:
        ValueError: If dataset_id is empty.
        FileNotFoundError: If the dataset does not exist (404).
        RuntimeError: If rate limit is exceeded (429).
        requests.HTTPError: For other non-2xx responses.
        pydantic.ValidationError: If the API response is missing the `id` field.
    """
    if not dataset_id or not dataset_id.strip():
        raise ValueError("`dataset_id` must be a non-empty string")

    url = f"{_get_api_url()}/datasets/{dataset_id}"
    resp = _send_api_request(method="GET", url=url, include_auth_headers=False)
    return DatasetDetails.model_validate(resp.json())


def download_dataset(
    dataset_id: str,
    download_directory: str | None = None,
    show_progress: bool = True,
    overwrite_existing: bool = False,
    enable_logging: bool = False,
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
        enable_logging: Whether to enable SDK logging to console and a local log file.

    Returns:
        Path to the downloaded dataset archive.

    Raises:
        ValueError: If dataset_id is empty.
        FileNotFoundError: If the dataset does not exist (404).
        PermissionError: If access is denied (403) or download directory is not writable.
        RuntimeError: If rate limit is exceeded (429) or unexpected response format.
        requests.HTTPError: For other non-2xx responses.
    """
    _enable_logging(enable_logging)
    logger.info(f"Downloading dataset {dataset_id}")

    dataset_details = get_dataset_details(dataset_id)

    archive_path = _download_dataset(
        dataset_id=dataset_details.id,
        archive_filename=_require_archive_filename(dataset_details),
        download_directory=download_directory,
        show_progress=show_progress,
        overwrite_existing=overwrite_existing,
        download_source=DOWNLOAD_SOURCE_SAVE,
    )
    return archive_path


# Added these two overload typing declarations in order to accurately type check
# the return type of the function (DataFrame or Dataset | DatasetDict) depending
# on the return_format value defined, otherwise type checkers would complaint since
# the HF package is an optional dependency.
@overload
def load_dataset(
    dataset_id: str,
    download_directory: str | None = None,
    show_progress: bool = True,
    overwrite_existing: bool = False,
    overwrite_extracted: bool = False,
    enable_logging: bool = False,
    return_format: Literal["pandas"] = "pandas",
) -> pd.DataFrame: ...


@overload
def load_dataset(
    dataset_id: str,
    download_directory: str | None = None,
    show_progress: bool = True,
    overwrite_existing: bool = False,
    overwrite_extracted: bool = False,
    enable_logging: bool = False,
    *,
    return_format: Literal["hf"],
) -> Dataset | DatasetDict: ...


def load_dataset(
    dataset_id: str,
    download_directory: str | None = None,
    show_progress: bool = True,
    overwrite_existing: bool = False,
    overwrite_extracted: bool = False,
    enable_logging: bool = False,
    return_format: Literal["pandas", "hf"] = "pandas",
) -> pd.DataFrame | Dataset | DatasetDict:
    """
    Download (if needed), extract (if not already extracted), and load the dataset into memory.

    By default, the dataset is returned as a pandas DataFrame. Pass `return_format="hf"`
    to get a HuggingFace `datasets` object instead (requires the optional dependency datacollective[hf]).

    If the dataset archive already exists in the download directory, it will not be re-downloaded
    unless `overwrite_existing=True`.

    If there is a directory with the same name as the archive file without the suffix extension, we assume
    it has already been extracted, and it will not be re-extracted unless `overwrite_extracted=True`.

    Uses the dataset schema to determine the loading strategy.

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
        enable_logging: Whether to enable SDK logging to console and a local log file.
        return_format: Format of the returned object. `"pandas"` (default) returns a
            pandas DataFrame. `"hf"` returns a HuggingFace `Dataset`, or a `DatasetDict`
            keyed by split name for datasets with multiple splits.
    Returns:
        A pandas DataFrame with the loaded dataset, or a HuggingFace `Dataset` /
        `DatasetDict` when `return_format="hf"`.

    Raises:
        ValueError: If dataset_id is empty, schema is unsupported, or `return_format`
            is invalid.
        MissingDependencyError: If `return_format="hf"` and the HuggingFace `datasets`
            library is not installed.
        FileNotFoundError: If the dataset does not exist (404).
        PermissionError: If access is denied (403) or download directory is not writable.
        RuntimeError: If rate limit is exceeded (429) or unexpected response format.
        requests.HTTPError: For other non-2xx responses.
    """
    if return_format not in RETURN_FORMATS:
        raise ValueError(
            f"Invalid return_format '{return_format}'. "
            f"Supported formats: {', '.join(RETURN_FORMATS)}"
        )
    if return_format == "hf":
        # Raise error here if the optional dependency is missing before any download
        _require_datasets()

    _enable_logging(enable_logging)
    logger.info(f"Loading dataset {dataset_id}")

    dataset_details = get_dataset_details(dataset_id)
    archive_filename = _require_archive_filename(dataset_details)
    _id = dataset_details.id
    archive_checksum = dataset_details.checksum or None

    # try to fetch schema from registry
    schema = _get_dataset_schema(_id)
    if schema is None:
        raise RuntimeError(
            f"Dataset '{_id}' exists but is not supported by load_dataset yet. "
            f"You can download the raw archive with: download_dataset('{_id}'). "
            f"If you are the data owner consider submitting a schema for your dataset via"
            f" the registry: https://mozilla-data-collective.github.io/dataset-schema-registry/"
        )

    archive_path = _download_dataset(
        dataset_id=_id,
        archive_filename=archive_filename,
        download_directory=download_directory,
        show_progress=show_progress,
        overwrite_existing=overwrite_existing,
        download_source=DOWNLOAD_SOURCE_LOAD,
    )

    base_dir = archive_path.parent
    extract_dir = _extract_archive(
        archive_path=archive_path,
        dest_dir=base_dir,
        overwrite_extracted=overwrite_extracted,
    )

    schema = _resolve_schema(_id, extract_dir, archive_checksum)
    df = _load_dataset_from_schema(schema, extract_dir)

    if return_format == "hf":
        return _convert_to_hf(df, schema)
    return df


def list_datasets(
    query: str | None = None,
    *,
    results_per_page: int | None = None,
    page_number: int | None = None,
    task: Task | str | Sequence[Task | str] | None = None,
    locale: str | Sequence[str] | None = None,
    license_abbr: License | str | Sequence[License | str] | None = None,
    file_format: str | Sequence[str] | None = None,
    sort: Literal["relevance", "newest", "size"] | None = None,
    sort_direction: Literal["asc", "desc"] | None = None,
    upload_date: Literal["today", "thisWeek", "thisMonth", "thisYear"] | None = None,
    has_sample: bool | None = None,
    pricing: Literal["compensated", "free"] | None = None,
) -> DatasetList:
    """
    List or search the public dataset catalog of the MDC platform.

    This is a public endpoint: no API key (`MDC_API_KEY`) is required.
    Results are paginated; use `page_number` together with the returned `total` to walk
    through the whole catalog. The values accepted by the `task`, `locale`,
    `license_abbr` and `file_format` filters can be discovered with `list_dataset_filters`.
    Filtering on a value absent from those lists matches nothing rather than failing.

    Args:
        query: Free-text search string (e.g. `swahili speech`). Matches all datasets when omitted.
        results_per_page: Results per page, between 1 and 100. The platform defaults to 24.
        page_number: 1-based page number. Defaults to the first page.
        task: ML task(s) to keep, either a `Task` enum value or its string value (e.g. `ASR`).
            Pass a list to match any of several tasks.
        locale: Language/locale code(s) to keep (e.g. `en`, `de-DE`). Pass a list to match any.
        license_abbr: License abbreviation(s) to keep, either a `License` enum value or a string
            (e.g. `CC0-1.0`). Pass a list to match any.
        file_format: File format(s) to keep (e.g. `WAV`, `TSV`). Pass a list to match any.
        sort: Result ordering: `relevance`, `newest` or `size`.
        sort_direction: `asc` or `desc`, applied to `sort`.
        upload_date: Only datasets published within the given window:
            `today`, `thisWeek`, `thisMonth` or `thisYear`.
        has_sample: When True, only datasets that provide a sample file.
        pricing: free or compensated datasets

    Returns:
        A DatasetList with the matching `items` for the requested page and the
        `total` number of matches across all pages.

    Raises:
        ValueError: If an argument is out of range or not one of the accepted options.
        RuntimeError: If rate limit is exceeded (429).
        requests.HTTPError: For other non-2xx responses (e.g. 400 for a filter value the API rejects).
        pydantic.ValidationError: If the API response does not match the expected shape.
    """
    if query is not None and not query.strip():
        raise ValueError("`query` must be a non-empty string when provided")
    if results_per_page is not None and not 1 <= results_per_page <= MAX_PAGE_SIZE:
        raise ValueError(
            f"`results_per_page` must be between 1 and {MAX_PAGE_SIZE}, got {results_per_page}"
        )
    if page_number is not None and page_number < 1:
        raise ValueError(
            f"`page_number` must be a positive (1-based) integer, got {page_number}"
        )
    _validate_option("sort", sort, SORT_OPTIONS)
    _validate_option("sort_direction", sort_direction, SORT_DIRECTIONS)
    _validate_option("upload_date", upload_date, UPLOAD_DATE_OPTIONS)
    _validate_option("pricing", pricing, PRICING_OPTIONS)

    params: dict[str, Any] = {
        "q": query.strip() if query is not None else None,
        "limit": results_per_page,
        "page": page_number,
        "task": _normalize_filter_values("task", task),
        "locale": _normalize_filter_values("locale", locale),
        "license": _normalize_filter_values("license", license_abbr),
        "format": _normalize_filter_values("format", file_format),
        "sort": sort,
        "sortDirection": sort_direction,
        "uploadDate": upload_date,
        # The API treats any truthy string as "has a sample"; omitting it means "no filter".
        "sample": "true" if has_sample else None,
        "pricing": pricing,
    }
    params = {key: value for key, value in params.items() if value is not None}

    url = f"{_get_api_url()}/datasets"
    resp = _send_api_request(
        method="GET", url=url, params=params, include_auth_headers=False
    )
    return DatasetList.model_validate(resp.json())


def list_dataset_filters() -> DatasetFilters:
    """
    Return the filter values that `list_datasets` currently accepts.

    This is a public endpoint: no API key (`MDC_API_KEY`) is required.

    Every value except a task is drawn from the published catalog, so it appears
    only while some dataset carries it; the task vocabulary is fixed.

    Returns:
        A DatasetFilters model with the available `tasks`, `locales`, `licenses`
        and `formats`.

    Raises:
        RuntimeError: If rate limit is exceeded (429).
        requests.HTTPError: For other non-2xx responses.
        pydantic.ValidationError: If the API response does not match the expected shape.
    """
    url = f"{_get_api_url()}/datasets/filters"
    resp = _send_api_request(method="GET", url=url, include_auth_headers=False)
    return DatasetFilters.model_validate(resp.json())


def save_dataset_to_disk(
    dataset_id: str,
    download_directory: str | None = None,
    show_progress: bool = True,
    overwrite_existing: bool = False,
    enable_logging: bool = False,
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
        enable_logging=enable_logging,
    )
