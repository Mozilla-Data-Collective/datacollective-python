from pathlib import Path

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader

logger = get_logger(__name__)


class OTHLoader(BaseSchemaLoader):
    """Index-based loader for tasks classified as OTH.

    Reads a single delimited index file and applies the schema's column
    mappings. Example datasets: Mozilla Common Voice Text Language Identification dataset.
    """

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)
        if not schema.index_file:
            raise ValueError("OTH schema must specify 'index_file'")

    def load(self) -> pd.DataFrame:
        raw_df = self._load_index_file()
        if not self.schema.columns:
            return raw_df
        return self._apply_column_mappings(raw_df)
