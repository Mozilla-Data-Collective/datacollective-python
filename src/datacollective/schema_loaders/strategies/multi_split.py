from __future__ import annotations

from pathlib import Path

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader

logger = get_logger(__name__)


class MultiSplitLoader(BaseSchemaLoader):
    """Load a dataset spread across one delimited file per split.

    All split files whose stems match the ``splits`` list are read, a
    ``split`` column is added to each, column mappings are applied when
    declared, and the parts are concatenated.
    """

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)
        if not schema.splits:
            raise ValueError(
                "multi_split schema must specify 'splits' (list of split names)"
            )

    def load(self) -> pd.DataFrame:
        assert self.schema.splits is not None

        pattern = self.schema.splits_file_pattern or "**/*.tsv"
        allowed_splits = set(self.schema.splits)

        split_files: dict[str, Path] = {}
        for path in self.extract_dir.rglob(pattern):
            if path.stem in allowed_splits:
                # Prefer the shallowest match per split name
                if path.stem not in split_files or len(path.parts) < len(
                    split_files[path.stem].parts
                ):
                    split_files[path.stem] = path

        if not split_files:
            raise RuntimeError(
                f"No split files matching pattern '{pattern}' with stems in "
                f"{sorted(allowed_splits)} found under '{self.extract_dir}'"
            )

        frames: list[pd.DataFrame] = []

        for split_name, file_path in sorted(split_files.items()):
            logger.debug(f"Reading split '{split_name}' from {file_path}")
            raw_df = self._read_delimited_file(file_path)
            raw_df["split"] = split_name

            if self.schema.columns:
                mapped = self._apply_column_mappings(raw_df)
                mapped["split"] = split_name
                frames.append(mapped)
            else:
                frames.append(raw_df)

        return pd.concat(frames, ignore_index=True)
