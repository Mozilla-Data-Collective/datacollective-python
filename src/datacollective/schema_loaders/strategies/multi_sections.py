from __future__ import annotations

from pathlib import Path

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader

logger = get_logger(__name__)


class MultiSectionsLoader(BaseSchemaLoader):
    """Load a dataset organised as one index file per section directory.

    Each section directory under ``section_root`` holds its own index file.
    A ``section`` column (the directory name) is added to each part, column
    mappings are applied when declared, and the parts are concatenated.
    """

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)
        if not schema.sections:
            raise ValueError(
                "multi_sections schema must specify 'sections' (list of section names)"
            )
        if not schema.section_root:
            raise ValueError("multi_sections schema must specify 'section_root'")
        if not schema.index_file:
            raise ValueError("multi_sections schema must specify 'index_file'")

    def load(self) -> pd.DataFrame:
        sections = self._resolve_sections()
        parts: list[pd.DataFrame] = []
        for section_path in sections:
            section_df = self._read_delimited_file(section_path)
            section_name = section_path.parents[0].name

            if self.schema.columns:
                section_df = self._apply_column_mappings(section_df)
            section_df["section"] = section_name
            parts.append(section_df)

        return pd.concat(parts, ignore_index=True)

    def _resolve_sections(self) -> list[Path]:
        """
        Get a list of valid sections, i.e. subdirectories that include an index file.
        """
        assert self.schema.sections is not None
        assert self.schema.index_file is not None
        assert self.schema.section_root is not None

        section_paths = []
        for section in self.schema.sections:
            section_path = (
                self.extract_dir
                / Path(self.schema.section_root)
                / Path(section)
                / self.schema.index_file
            )
            if not section_path.exists():
                raise FileNotFoundError(f"Index file '{section_path}' not found ")
            section_paths.append(section_path)

        return section_paths
