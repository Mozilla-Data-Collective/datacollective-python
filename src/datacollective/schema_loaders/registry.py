from __future__ import annotations

import logging
from pathlib import Path
from typing import Type

import pandas as pd

from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader
from datacollective.schema_loaders.tasks.asr import ASRLoader
from datacollective.schema_loaders.tasks.tts import TTSLoader

logger = logging.getLogger(__name__)


_TASK_REGISTRY: dict[str, Type[BaseSchemaLoader]] = {
    "ASR": ASRLoader,
    "TTS": TTSLoader,
}


def get_task_loader(task: str) -> Type[BaseSchemaLoader]:
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


def load_dataset_from_schema(schema: DatasetSchema, extract_dir: Path) -> pd.DataFrame:
    """
    Instantiate the appropriate loader for *schema.task* and return the
    loaded `~pandas.DataFrame`.

    Args:
        schema: Parsed dataset schema.
        extract_dir: Root directory where the dataset archive was extracted.

    Returns:
        A pandas DataFrame with the loaded dataset.
    """
    loader_cls = get_task_loader(schema.task)
    loader = loader_cls(schema=schema, extract_dir=extract_dir)
    logger.info(f"Loading dataset '{schema.dataset_id}' with {loader_cls.__name__}")
    return loader.load()
