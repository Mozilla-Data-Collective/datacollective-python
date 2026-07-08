from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING

import pandas as pd

from datacollective.errors import MissingDependencyError
from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema

if TYPE_CHECKING:
    from datasets import Dataset, DatasetDict

logger = get_logger(__name__)

#: Column added by multi-split loaders to be used to key the resulting DatasetDict.
SPLIT_COLUMN = "split"

HF_INSTALL_INSTRUCTIONS = (
    "The HuggingFace `datasets` library is required for `return_format='hf'` "
    "but is not installed. Install the optional dependency with:\n"
    '    pip install "datacollective[hf]"\n'
    "or, if you use uv:\n"
    '    uv add "datacollective[hf]"'
)


def _require_datasets() -> ModuleType:
    """Import and return the `datasets` module.

    Raises:
        MissingDependencyError: If the `datasets` library is not installed,
            with instructions on how to install it.
    """
    try:
        import datasets
    except ImportError as exc:
        raise MissingDependencyError(HF_INSTALL_INSTRUCTIONS) from exc
    return datasets


def _convert_to_hf(df: pd.DataFrame, schema: DatasetSchema) -> Dataset | DatasetDict:
    """Convert a loaded pandas DataFrame into a HuggingFace `datasets` object.

    For multi-split datasets (``schema.splits`` is set, and the loader has
    added a ``split`` column), one `Dataset` is built per split value and the
    result is a `DatasetDict` keyed by split name, with the redundant
    ``split`` column dropped. Otherwise, a single `Dataset` is returned.

    Args:
        df: The loaded dataset.
        schema: The dataset schema that produced *df*; its ``splits`` field
            decides whether the result is split into a `DatasetDict`.

    Returns:
        A `Dataset`, or a `DatasetDict` for multi-split datasets.

    Raises:
        MissingDependencyError: If the `datasets` library is not installed.
    """
    ds = _require_datasets()

    if schema.splits and SPLIT_COLUMN in df.columns:
        splits: dict[str, Dataset] = {}
        for split_name, split_df in df.groupby(SPLIT_COLUMN, observed=True):
            splits[str(split_name)] = ds.Dataset.from_pandas(
                split_df.drop(columns=[SPLIT_COLUMN]),
                split=str(split_name),
                preserve_index=False,
            )
        logger.info(f"Converted DataFrame to DatasetDict with splits: {list(splits)}")
        return ds.DatasetDict(splits)

    dataset = ds.Dataset.from_pandas(df, preserve_index=False)
    logger.info(f"Converted DataFrame to Dataset with {dataset.num_rows} rows")
    return dataset
