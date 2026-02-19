import logging
from pathlib import Path

import pandas as pd

from datacollective.dataset_loading_scripts.common_voice import (
    _load_scripted,
    _load_spontaneous,
)

logger = logging.getLogger(__name__)


def load_dataset_from_name_as_dataframe(
    dataset_name: str, extract_dir: Path
) -> pd.DataFrame:
    """
    In order to enable loading MDC datasets as Pandas DataFrames, this function
    routes the loading process to the appropriate dataset-specific loader based on
    the dataset name. Each dataset loader is implemented in its own module under
    `datacollective.dataset_loading_scripts`.

    Args:
        dataset_name (str): The name of the dataset (lowercased).
        extract_dir (Path): The directory where the dataset has been extracted.
    Returns:
        A pandas DataFrame containing the loaded dataset.
    Raises:
        ValueError: If the dataset name is not supported for loading.
    """
    if "scripted" in dataset_name:
        logger.debug(f"Routing dataset {dataset_name} to scripted loader")
        return _load_scripted(extract_dir)
    if "spontaneous" in dataset_name:
        logger.debug(f"Routing dataset {dataset_name} to spontaneous loader")
        return _load_spontaneous(extract_dir)

    raise ValueError(
        f"Dataset name `{dataset_name}` currently not supported for loading as DataFrame."
    )
