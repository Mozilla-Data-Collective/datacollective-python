from __future__ import annotations

from pathlib import Path

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader

logger = get_logger(__name__)


class IndexLoader(BaseSchemaLoader):
    """Load a dataset from a single delimited index file (the default strategy).

    An index file (e.g. CSV/TSV) holds one row per sample.  When the schema
    declares column mappings they are applied (renaming, dtype conversion,
    file-path resolution); otherwise the raw DataFrame is returned as-is.
    """

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)
        if not schema.index_file:
            raise ValueError("index strategy schema must specify 'index_file'")

    def load(self) -> pd.DataFrame:
        raw_df = self._load_index_file()
        if not self.schema.columns:
            # No column mapping -> return the raw dataframe as-is
            return raw_df
        return self._apply_column_mappings(raw_df)
