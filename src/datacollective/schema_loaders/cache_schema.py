import logging
from pathlib import Path
import yaml

from datacollective.schema import DatasetSchema, parse_schema, get_dataset_schema

logger = logging.getLogger(__name__)


def _resolve_schema(
    dataset_id: str,
    extract_dir: Path,
    archive_checksum: str | None = None,
) -> DatasetSchema:
    """
    Return the dataset schema, using a locally cached ``schema.yaml`` when the
    archive checksum stored in the cached file matches the checksum of the
    current dataset archive, avoiding an unnecessary API call to re-download
    the schema.

    The function first checks whether a ``schema.yaml`` already exists in
    *extract_dir*.  If that file contains a ``checksum`` field **and** the
    caller supplies *archive_checksum*, the two are compared.  When they match,
    the local copy is returned directly.  Otherwise, the schema is fetched from
    the remote registry, stamped with the current *archive_checksum*, and saved
    to disk for next time.

    Args:
        dataset_id: The dataset ID.
        extract_dir: Path to the extracted dataset directory.
        archive_checksum: Checksum of the downloaded dataset archive. When
            provided, it is compared against the ``checksum`` stored in the
            cached ``schema.yaml`` to decide whether the cache is still valid.

    Returns:
        A :class:`DatasetSchema` instance.
    """
    schema_path = extract_dir / "schema.yaml"

    # Try to load a cached schema first
    cached_schema = _load_cached_schema(schema_path)

    if (
        cached_schema is not None
        and archive_checksum is not None
        and cached_schema.checksum is not None
        and cached_schema.checksum == archive_checksum
    ):
        logger.info(
            "Archive checksum matches cached schema – skipping schema download."
        )
        return cached_schema

    # Cache miss or no archive checksum available → fetch from the registry
    remote_schema = get_dataset_schema(dataset_id)
    if remote_schema is None:
        if cached_schema is not None:
            logger.info("Dataset not found in registry – using cached schema.")
            return cached_schema
        raise ValueError(
            f"Dataset '{dataset_id}' not found in the schema registry and no local schema cache exists."
        )

    # Stamp the remote schema with the archive checksum so that subsequent
    # loads can skip the remote fetch when the archive hasn't changed.
    if archive_checksum is not None:
        remote_schema.checksum = archive_checksum

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
    data = schema.to_yaml_dict()

    try:
        schema_path.parent.mkdir(parents=True, exist_ok=True)
        schema_path.write_text(
            yaml.dump(data, default_flow_style=False, sort_keys=False),
            encoding="utf-8",
        )
        logger.debug(f"Saved schema cache to {schema_path}")
    except OSError:
        logger.debug(f"Could not write schema cache to {schema_path}", exc_info=True)
