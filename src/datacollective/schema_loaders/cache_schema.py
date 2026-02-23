import logging
from pathlib import Path
from typing import Any
import yaml

from datacollective.api_utils import get_dataset_schema
from datacollective.schema import DatasetSchema, parse_schema

logger = logging.getLogger(__name__)


def _resolve_schema(dataset_id: str, extract_dir: Path) -> DatasetSchema:
    """
    Return the dataset schema, using a locally cached ``schema.yaml`` when the
    checksum in the cached file matches the remote checksum, avoiding an
    unnecessary API call to re-download the schema.

    The function first checks whether a ``schema.yaml`` already exists in
    *extract_dir*.  If that file contains a ``checksum`` field, the remote
    schema is fetched and its checksum is compared.  When the two match the
    local copy is returned directly.  Otherwise, the freshly downloaded schema
    is saved to disk for next time.

    Args:
        dataset_id: The dataset ID.
        extract_dir: Path to the extracted dataset directory.

    Returns:
        A :class:`DatasetSchema` instance.
    """
    schema_path = extract_dir / "schema.yaml"

    # Try to load a cached schema first
    cached_schema = _load_cached_schema(schema_path)

    if cached_schema is not None and cached_schema.checksum is not None:
        # Fetch the remote schema and compare checksums
        remote_schema = get_dataset_schema(dataset_id)
        if (
            remote_schema.checksum is not None
            and remote_schema.checksum == cached_schema.checksum
        ):
            logger.info(
                "Schema checksum matches cached copy – skipping schema download."
            )
            return cached_schema
        # Checksum mismatch → use the freshly fetched remote schema and update cache
        _save_schema_to_disk(remote_schema, schema_path)
        return remote_schema

    # No usable cache → fetch from API and persist locally
    remote_schema = get_dataset_schema(dataset_id)
    _save_schema_to_disk(remote_schema, schema_path)
    return remote_schema


def _load_cached_schema(schema_path: Path) -> DatasetSchema | None:
    """
    Attempt to load and parse a ``schema.yaml`` from *schema_path*.

    Returns:
        A :class:`DatasetSchema` if the file exists and is valid, otherwise ``None``.
    """
    if not schema_path.is_file():
        return None
    try:
        return parse_schema(schema_path)
    except Exception:
        logger.debug(f"Failed to parse cached schema at {schema_path}", exc_info=True)
        return None


def _save_schema_to_disk(schema: DatasetSchema, schema_path: Path) -> None:
    """
    Persist the schema to *schema_path* so that subsequent loads can skip
    the API call when the checksum hasn't changed.
    """

    data: dict[str, Any] = {
        "dataset_id": schema.dataset_id,
        "task": schema.task,
    }
    if schema.checksum is not None:
        data["checksum"] = schema.checksum
    if schema.format is not None:
        data["format"] = schema.format
    if schema.index_file is not None:
        data["index_file"] = schema.index_file
    if schema.base_audio_path is not None:
        data["base_audio_path"] = schema.base_audio_path
    if schema.separator is not None:
        data["separator"] = schema.separator
    if not schema.has_header:
        data["has_header"] = schema.has_header
    if schema.encoding != "utf-8":
        data["encoding"] = schema.encoding
    if schema.root_strategy is not None:
        data["root_strategy"] = schema.root_strategy
    if schema.file_pattern is not None:
        data["file_pattern"] = schema.file_pattern
    if schema.audio_extension is not None:
        data["audio_extension"] = schema.audio_extension
    if schema.columns:
        data["columns"] = {
            name: {
                "source_column": col.source_column,
                "dtype": col.dtype,
                **({"optional": col.optional} if col.optional else {}),
            }
            for name, col in schema.columns.items()
        }
    if schema.content_mapping is not None:
        cm: dict[str, Any] = {}
        if schema.content_mapping.text is not None:
            cm["text"] = schema.content_mapping.text
        if schema.content_mapping.meta_source is not None:
            cm["meta_source"] = schema.content_mapping.meta_source
        data["content_mapping"] = cm
    if schema.extra:
        data.update(schema.extra)

    try:
        schema_path.parent.mkdir(parents=True, exist_ok=True)
        schema_path.write_text(
            yaml.dump(data, default_flow_style=False, sort_keys=False),
            encoding="utf-8",
        )
        logger.debug(f"Saved schema cache to {schema_path}")
    except OSError:
        logger.debug(f"Could not write schema cache to {schema_path}", exc_info=True)
