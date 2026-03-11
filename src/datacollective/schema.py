from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import urllib.error
import urllib.request
import yaml

from pydantic import BaseModel, ConfigDict, Field

from datacollective.api_utils import SCHEMA_REGISTRY_RAW_BASE_URL

logger = logging.getLogger(__name__)


class ColumnMapping(BaseModel):
    """
    A single column mapping entry inside a schema.

    Used by index-based tasks to describe how columns in the
    index file map to logical fields and their data types.
    """

    model_config = ConfigDict(frozen=True)

    source_column: str | int = Field(
        description="column name (str) or positional index (int) for headerless files"
    )
    dtype: str = "string"
    optional: bool = False


class ContentMapping(BaseModel):
    """
    Describes how file contents / metadata map to DataFrame columns.

    Used by glob-based tasks (e.g. LM) to specify how to extract text and metadata
    from files found via glob patterns.  For example, the text content might come
    from the file contents, while metadata (e.g. language code) might come from
    the file name or parent directory.
    """

    model_config = ConfigDict(frozen=True)

    text: str | None = Field(default=None, description='e.g. "file_content"')
    meta_source: str | None = Field(default=None, description='e.g. "file_name"')


class DatasetSchema(BaseModel):
    """
    Task-agnostic representation of a dataset schema, as defined by a ``schema.yaml`` file.

    Every schema **must** have ``dataset_id`` and ``task``.  The remaining
    fields depend on the task type and the ``root_strategy``
    (``"index"`` vs ``"glob"``).

    New task types only need to populate the fields they care about;
    the loader registered for that task will decide which fields are
    required at load time.
    """

    model_config = ConfigDict(frozen=False)

    dataset_id: str = Field(
        description="Unique identifier for the dataset in the registry"
    )
    task: str = Field(
        description="A task as defined in the MDC Platform e.g. ASR, TTS etc"
    )

    # --- Index-based strategy (ASR / TTS) ---
    format: str | None = Field(default=None, description='e.g. "csv", "tsv", "pipe"')
    index_file: str | None = Field(default=None, description='e.g. "train.csv"')
    base_audio_path: str | None = Field(default=None, description='e.g. "clips/"')
    columns: dict[str, ColumnMapping] = Field(
        default_factory=dict, description="Mapping of index columns to logical fields"
    )
    separator: str | None = Field(
        default=None, description='explicit separator override (e.g. "|")'
    )
    has_header: bool = Field(
        default=True, description="whether the index file has a header row"
    )
    encoding: str = Field(
        default="utf-8", description='file encoding (e.g. "utf-8-sig" for BOM)'
    )

    # --- Glob-based strategy (LM, paired-file TTS) ---
    root_strategy: str | None = Field(
        default=None, description='"glob" | "paired_glob" | "multi_split"'
    )
    file_pattern: str | None = Field(default=None, description='e.g. "**/*.txt"')
    audio_extension: str | None = Field(
        default=None, description='for paired-file TTS: e.g. ".webm"'
    )
    content_mapping: ContentMapping | None = Field(
        default=None, description="Mapping for glob-based content extraction"
    )

    # --- Multi-split strategy (e.g. Common Voice) ---
    splits: list[str] | None = Field(
        default=None, description='split names to load, e.g. ["train", "dev", "test"]'
    )
    splits_file_pattern: str | None = Field(
        default=None, description='glob pattern for split files, e.g. "**/*.tsv"'
    )

    # --- Schema versioning ---
    checksum: str | None = Field(
        default=None, description="archive checksum for cache validation"
    )

    # --- Catch-all for future / unknown keys ---
    extra: dict[str, Any] = Field(
        default_factory=dict, description="Catch-all for future / unknown keys"
    )

    def to_yaml_dict(self) -> dict[str, Any]:
        """
        Serialise the schema to a plain dict suitable for YAML output.

        Excludes fields that are at their default values so that the
        generated ``schema.yaml`` stays compact and readable.  The
        ``extra`` dict is merged into the top level.
        """
        data = self.model_dump(exclude_defaults=True, exclude={"extra"})
        # Merge extra keys into the top level
        if self.extra:
            data.update(self.extra)
        return data


def _get_dataset_schema(dataset_id: str) -> DatasetSchema | None:
    """
    Download and return the schema.yaml content for *dataset_id*.

    Args:
        dataset_id: The registry dataset ID (the folder name under /registry/).

    Returns:
        A fully-populated `DatasetSchema` for the given dataset, or ``None`` if
        the dataset is not found in the registry (HTTP 404).
    Raises:
        RuntimeError
            For any other network / HTTP error.
    """

    url = f"{SCHEMA_REGISTRY_RAW_BASE_URL}/main/registry/{dataset_id}/schema.yaml"

    try:
        with urllib.request.urlopen(url) as response:
            raw = response.read().decode("utf-8")
        return _parse_schema(raw)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise RuntimeError(f"HTTP {exc.code} while fetching {url}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Network error while fetching {url}: {exc.reason}") from exc


def _parse_schema(raw: str | dict[str, Any] | Path) -> DatasetSchema:
    """
    Parse a schema from a YAML string, a dict, or a file path.

    Args:
        raw: YAML string, already-parsed dict, or ``Path`` to a YAML file.

    Returns:
        A fully-populated `DatasetSchema`.

    Raises:
        ValueError: If required fields are missing or the input cannot be parsed.
    """
    if isinstance(raw, Path):
        raw = raw.read_text(encoding="utf-8")
    if isinstance(raw, str):
        raw = yaml.safe_load(raw)
    if not isinstance(raw, dict):
        raise ValueError(f"Expected a dict after YAML parsing, got {type(raw)}")

    data: dict[str, Any] = raw

    dataset_id = data.get("dataset_id")
    task = data.get("task")
    if not dataset_id or not task:
        raise ValueError("schema.yaml must contain 'dataset_id' and 'task'")

    # Columns (index-based)
    columns: dict[str, ColumnMapping] = {}
    raw_columns = data.get("columns", {})
    if isinstance(raw_columns, dict):
        for col_name, col_def in raw_columns.items():
            if not isinstance(col_def, dict):
                continue
            columns[col_name] = ColumnMapping(
                source_column=col_def["source_column"],  # str or int
                dtype=col_def.get("dtype", "string"),
                optional=col_def.get("optional", False),
            )

    # Content mapping (glob-based)
    content_mapping: ContentMapping | None = None
    raw_cm = data.get("content_mapping")
    if isinstance(raw_cm, dict):
        content_mapping = ContentMapping(
            text=raw_cm.get("text"),
            meta_source=raw_cm.get("meta_source"),
        )

    # Recognised top-level keys
    known_keys = {
        "dataset_id",
        "task",
        "format",
        "index_file",
        "base_audio_path",
        "columns",
        "separator",
        "has_header",
        "encoding",
        "root_strategy",
        "file_pattern",
        "audio_extension",
        "content_mapping",
        "splits",
        "splits_file_pattern",
        "checksum",
    }
    extra = {k: v for k, v in data.items() if k not in known_keys}

    return DatasetSchema(
        dataset_id=str(dataset_id),
        task=str(task).upper(),
        format=data.get("format"),
        index_file=data.get("index_file"),
        base_audio_path=data.get("base_audio_path"),
        columns=columns,
        separator=data.get("separator"),
        has_header=data.get("has_header", True),
        encoding=data.get("encoding", "utf-8"),
        root_strategy=data.get("root_strategy"),
        file_pattern=data.get("file_pattern"),
        audio_extension=data.get("audio_extension"),
        content_mapping=content_mapping,
        splits=data.get("splits"),
        splits_file_pattern=data.get("splits_file_pattern"),
        checksum=data.get("checksum"),
        extra=extra,
    )
