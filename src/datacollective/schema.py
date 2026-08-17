from __future__ import annotations

import urllib.error
import urllib.request
import warnings
from difflib import get_close_matches
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

import yaml

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from datacollective.api_utils import SCHEMA_REGISTRY_RAW_BASE_URL
from datacollective.errors import SchemaValidationWarning
from datacollective.logging_utils import get_logger

logger = get_logger(__name__)


class Strategy(StrEnum):
    """Loading strategies recognised by schema loaders.

    The values are the valid ``root_strategy`` schema field entries; the
    registry maps each member to its loader class.
    """

    INDEX = "index"
    MULTI_SPLIT = "multi_split"
    MULTI_SECTIONS = "multi_sections"
    PAIRED_GLOB = "paired_glob"
    GLOB = "glob"


class ColumnMapping(BaseModel):
    """
    A single column mapping entry inside a schema.

    Used by index-based tasks to describe how columns in the
    index file map to logical fields and their data types.

    Unknown keys and unknown ``dtype`` values are rejected at parse time.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_column: str | int = Field(
        description="column name (str) or positional index (int) for headerless files"
    )
    dtype: Literal[
        "string", "file_path", "file_content", "category", "int", "float"
    ] = "string"
    optional: bool = False
    path_match_strategy: Literal["direct", "exact", "contains"] = "direct"
    file_extension: str | None = Field(
        default=None,
        description=(
            'optional extension used when resolving file_path columns (e.g. ".wav")'
        ),
    )
    path_template: str | None = Field(
        default=None,
        description=(
            "optional template used to construct file_path values from one or more "
            "metadata columns. Supports relative paths and ${value}, e.g. "
            '"${Speaker ID}_khm_${Sentence ID}.wav" or "${Split}/${value}.wav"'
        ),
    )


class DatasetSchema(BaseModel):
    """
    Task-agnostic representation of a dataset schema, as defined by a ``schema.yaml`` file.

    Every schema **must** have ``dataset_id`` and, to be loadable, a
    ``root_strategy``.  The remaining fields depend on the strategy; the
    loader registered for that strategy decides which fields are required
    at load time.

    ``task`` is optional.  When set to a task with a known contract (e.g. ASR,
    TTS), the loaded DataFrame is validated to contain the task's required
    logical columns.
    """

    model_config = ConfigDict(frozen=False)

    dataset_id: str = Field(
        description="Unique identifier for the dataset in the registry"
    )
    task: str | None = Field(
        default=None,
        description=(
            "Optional task as defined in the MDC Platform e.g. ASR, TTS etc. "
            "When set to a task with a known contract, the loaded dataset is "
            "validated against it."
        ),
    )

    # --- Index-based strategy (ASR / TTS) ---
    format: str | None = Field(
        default=None,
        description=(
            'optional format hint (e.g. "csv", "tsv", "pipe"); '
            "inferred from the index file when omitted"
        ),
    )
    index_file: str | None = Field(default=None, description='e.g. "train.csv"')
    base_audio_path: str | list[str] | None = Field(
        default=None,
        description=(
            'e.g. "clips/" or ["clips/", "wavs/"]". Entries may also use '
            'metadata placeholders such as "${Split}/clips/".'
        ),
    )
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
    strict: bool = Field(
        default=False,
        description=(
            "Disable archive heuristics for deterministic loading: "
            "'index_file' must exist at its literal path relative to the "
            "dataset root (no recursive search) and source column names "
            "must match exactly (no fuzzy matching)."
        ),
    )

    # --- Loading strategy ---
    root_strategy: str | None = Field(
        default=None,
        description=(
            'Loading strategy; required to load a dataset: "index" | "glob" | '
            '"paired_glob" | "multi_split" | "multi_sections"'
        ),
    )
    file_pattern: str | None = Field(default=None, description='e.g. "**/*.txt"')
    audio_extension: str | None = Field(
        default=None, description='for paired-file TTS: e.g. ".webm"'
    )
    record_path: str | None = Field(
        default=None,
        description=(
            "for JSON sidecar files: top-level key holding the list of records "
            '(e.g. "transcriptions"); one DataFrame row per record'
        ),
    )

    # --- Multi-split strategy (e.g. Common Voice) ---
    splits: list[str] | None = Field(
        default=None, description='split names to load, e.g. ["train", "dev", "test"]'
    )
    splits_file_pattern: str | None = Field(
        default=None, description='glob pattern for split files, e.g. "**/*.tsv"'
    )
    # --- Multi-section strategy
    sections: list[str] | None = None
    section_root: str | None = None

    # --- Inner archive extraction ---
    extract_files: list[str] | None = Field(
        default=None,
        description=(
            "List of archive paths (relative to dataset root) that must be "
            "extracted before loading, e.g. ['Train.tar.gz', 'Dev.tar.gz']"
        ),
    )

    # --- Schema versioning ---
    checksum: str | None = Field(
        default=None, description="archive checksum for cache validation"
    )

    # --- Catch-all for future / unknown keys ---
    extra: dict[str, Any] = Field(
        default_factory=dict, description="Catch-all for future / unknown keys"
    )

    @model_validator(mode="before")
    @classmethod
    def _route_unknown_keys(cls, data: Any) -> Any:
        """Route unknown top-level keys into ``extra``, warning about each.

        Near-miss typos get a "did you mean" hint. The keys are preserved
        under ``extra`` (not dropped) so that schemas written for newer SDK
        versions keep round-tripping.
        """
        if not isinstance(data, dict):
            return data
        if not data.get("dataset_id"):
            raise ValueError("schema.yaml must contain 'dataset_id'")

        known = set(cls.model_fields)
        unknown = [key for key in data if key not in known]
        if not unknown:
            return data

        for key in unknown:
            suggestion = get_close_matches(key, known - {"extra"}, n=1)
            message = f"Unknown schema key '{key}'"
            if suggestion:
                message += f" — did you mean '{suggestion[0]}'?"
            message += " The key is ignored by this SDK version (kept under 'extra')."
            warnings.warn(message, SchemaValidationWarning, stacklevel=2)

        cleaned = {key: value for key, value in data.items() if key in known}
        cleaned["extra"] = dict(data.get("extra") or {}) | {
            key: data[key] for key in unknown
        }
        return cleaned

    @field_validator("task", mode="before")
    @classmethod
    def _normalize_task(cls, value: Any) -> str | None:
        return str(value).upper() if value else None

    @field_validator("root_strategy", mode="before")
    @classmethod
    def _validate_root_strategy(cls, value: Any) -> Any:
        if value is None:
            return value
        try:
            Strategy(value)
        except ValueError:
            supported = ", ".join(member.value for member in Strategy)
            raise ValueError(
                f"Unknown root_strategy '{value}'. Supported strategies: {supported}"
            ) from None
        return value

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

    Validation is delegated entirely to the `DatasetSchema` model: unknown
    top-level keys are routed into ``extra`` with a `SchemaValidationWarning`,
    while unknown ``root_strategy`` values, unknown column ``dtype`` values,
    and unknown keys inside a column mapping raise at parse time.

    Args:
        raw: YAML string, already-parsed dict, or ``Path`` to a YAML file.

    Returns:
        A fully-populated `DatasetSchema`.

    Raises:
        ValueError: If required fields are missing or the input cannot be
            parsed (`pydantic.ValidationError` is a ``ValueError``).
    """
    if isinstance(raw, Path):
        raw = raw.read_text(encoding="utf-8")
    if isinstance(raw, str):
        raw = yaml.safe_load(raw)
    if not isinstance(raw, dict):
        raise ValueError(f"Expected a dict after YAML parsing, got {type(raw)}")

    return DatasetSchema.model_validate(raw)
