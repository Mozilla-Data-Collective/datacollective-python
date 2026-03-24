from __future__ import annotations

from pathlib import Path

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import (
    BaseSchemaLoader,
    FORMAT_SEP,
    Strategy,
)

logger = get_logger(__name__)


class ASRLoader(BaseSchemaLoader):
    """Load an ASR dataset described by a `DatasetSchema`."""

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)
        if schema.root_strategy == Strategy.MULTI_SPLIT:
            if not schema.splits:
                raise ValueError(
                    "ASR multi_split schema must specify 'splits' (list of split names)"
                )
        else:
            if not schema.index_file:
                raise ValueError("ASR schema must specify 'index_file'")
            if not schema.format:
                raise ValueError("ASR schema must specify 'format' (csv or tsv)")
            if not schema.columns:
                raise ValueError(
                    "ASR schema must specify at least two column mappings for audio and transcription"
                )

    def load(self) -> pd.DataFrame:
        if self.schema.root_strategy == Strategy.MULTI_SPLIT:
            return self._load_multi_split()
        raw_df = self._load_index_file()
        return self._apply_column_mappings(raw_df)

    def _load_multi_split(self) -> pd.DataFrame:
        """
        Load all split TSV/CSV files whose stems match the ``splits`` list,
        add a ``split`` column to each, apply column mappings, and concatenate.
        """
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

        sep = self.schema.separator or FORMAT_SEP.get(self.schema.format or "tsv", "\t")
        frames: list[pd.DataFrame] = []

        for split_name, file_path in sorted(split_files.items()):
            logger.debug(f"Reading split '{split_name}' from {file_path}")
            raw_df = pd.read_csv(
                file_path, sep=sep, header="infer", encoding=self.schema.encoding
            )  # If the split files have headers, they will be inferred here and preserved in the raw_df columns. If not, raw_df will have default integer columns.
            raw_df["split"] = split_name

            if self.schema.columns:
                mapped = self._apply_column_mappings(raw_df)
                mapped["split"] = split_name
                frames.append(mapped)
            else:
                frames.append(raw_df)

        return pd.concat(frames, ignore_index=True)
