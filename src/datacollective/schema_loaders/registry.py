from __future__ import annotations

import tarfile
import zipfile
from pathlib import Path
from typing import Type

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader
from datacollective.schema_loaders.tasks.asr import ASRLoader
from datacollective.schema_loaders.tasks.oth import OTHLoader
from datacollective.schema_loaders.tasks.tts import TTSLoader

logger = get_logger(__name__)


_TASK_REGISTRY: dict[str, Type[BaseSchemaLoader]] = {
    "ASR": ASRLoader,
    "TTS": TTSLoader,
    "OTH": OTHLoader,
}


def _get_task_loader(task: str) -> Type[BaseSchemaLoader]:
    """
    Return the loader class for *task*.

    Raises:
        ValueError: If no loader is registered for the given task.
    """
    key = task.upper()
    if key not in _TASK_REGISTRY:
        supported = ", ".join(sorted(_TASK_REGISTRY))
        raise ValueError(
            f"No schema loader registered for task '{key}'. "
            f"Supported tasks: {supported}"
        )
    return _TASK_REGISTRY[key]


def _load_dataset_from_schema(schema: DatasetSchema, extract_dir: Path) -> pd.DataFrame:
    """
    Instantiate the appropriate loader for *schema.task* and return the
    loaded `~pandas.DataFrame`.

    Args:
        schema: Parsed dataset schema.
        extract_dir: Root directory where the dataset archive was extracted.

    Returns:
        A pandas DataFrame with the loaded dataset.
    """
    if schema.extract_files:
        _extract_inner_archives(schema.extract_files, extract_dir)

    loader_cls = _get_task_loader(schema.task)
    loader = loader_cls(schema=schema, extract_dir=extract_dir)
    logger.info(f"Loading dataset '{schema.dataset_id}' with {loader_cls.__name__}")
    return loader.load()


def _extract_inner_archives(extract_files: list[str], extract_dir: Path) -> None:
    """Extract inner archives listed in ``schema.extract_files``.

    Each path is resolved relative to *extract_dir* (searching recursively
    if not found at the literal path).  A marker file
    ``.<archive_name>.extracted`` is written next to the archive after
    successful extraction so the same archive is never unpacked twice.
    """
    for relative_path in extract_files:
        # Locate the archive — try literal path first, then recursive search
        archive_path = extract_dir / relative_path
        if not archive_path.is_file():
            candidates = list(extract_dir.rglob(Path(relative_path).name))
            candidates = [c for c in candidates if c.is_file()]
            if not candidates:
                raise FileNotFoundError(
                    f"Inner archive '{relative_path}' not found under '{extract_dir}'"
                )
            candidates.sort(key=lambda p: len(p.parts))
            archive_path = candidates[0]

        marker = archive_path.parent / f".{archive_path.name}.extracted"
        if marker.exists():
            logger.debug(f"Skipping already-extracted archive: {archive_path.name}")
            continue

        dest = archive_path.parent
        logger.info(f"Extracting inner archive: {archive_path.name}")

        if tarfile.is_tarfile(archive_path):
            with tarfile.open(archive_path) as tf:
                tf.extractall(path=dest, filter="data")
        elif zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path) as zf:
                zf.extractall(path=dest)
        else:
            raise ValueError(
                f"Unsupported archive format for '{archive_path.name}'. "
                "Only tar (gz/bz2/xz) and zip are supported."
            )

        marker.touch()
        logger.info(f"Extracted {archive_path.name} → {dest}")
