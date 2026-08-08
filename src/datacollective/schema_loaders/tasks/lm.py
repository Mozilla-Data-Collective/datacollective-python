from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader

logger = logging.getLogger(__name__)


class LMLoader(BaseSchemaLoader):
    """Load an LM dataset described by a `DatasetSchema`.

    See docs/loaders/lm.md for details on supported loading strategies and schema fields.
    """

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)
        if schema.root_strategy != "glob":
            raise ValueError(
                f"LM schema must use root_strategy='glob', got '{schema.root_strategy}'"
            )
        if not schema.file_pattern:
            raise ValueError("LM schema must specify 'file_pattern'")
        if not schema.content_mapping:
            raise ValueError("LM schema must specify 'content_mapping'")

    def load(self) -> pd.DataFrame:
        assert self.schema.file_pattern is not None
        assert self.schema.content_mapping is not None

        files = sorted(self.extract_dir.rglob(self.schema.file_pattern))

        if not files:
            raise FileNotFoundError(
                f"No files matching '{self.schema.file_pattern}' "
                f"found under '{self.extract_dir}'"
            )

        logger.debug(f"Found {len(files)} files matching '{self.schema.file_pattern}'")

        rows: list[dict[str, str]] = []
        cm = self.schema.content_mapping

        for file_path in files:
            row: dict[str, str] = {}

            if cm.text == "file_content":
                row["text"] = file_path.read_text(encoding=self.schema.encoding)
            if cm.meta_source == "file_name":
                row["meta_source"] = file_path.name

            rows.append(row)

        return pd.DataFrame(rows)
