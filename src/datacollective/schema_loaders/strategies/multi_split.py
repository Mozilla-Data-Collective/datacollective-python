from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd

from datacollective.errors import DataLoadWarning
from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader

logger = get_logger(__name__)


class MultiSplitLoader(BaseSchemaLoader):
    """Load a dataset spread across one delimited file per split.

    All split files whose stems match the ``splits`` list are read, a
    ``split`` column is added to each, column mappings are applied when
    declared, and the parts are concatenated. A declared split without a
    file warns (raises when ``strict``); two files for one split at the same
    depth raise.
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

        candidates: dict[str, list[Path]] = {}
        for path in self.extract_dir.rglob(pattern):
            if path.stem in allowed_splits:
                candidates.setdefault(path.stem, []).append(path)

        if not candidates:
            raise RuntimeError(
                f"No split files matching pattern '{pattern}' with stems in "
                f"{sorted(allowed_splits)} found under '{self.extract_dir}'"
            )

        split_files = {
            split_name: self._pick_split_file(split_name, paths)
            for split_name, paths in candidates.items()
        }
        self._check_missing_splits(set(split_files), pattern)

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

    def _pick_split_file(self, split_name: str, paths: list[Path]) -> Path:
        """Return the shallowest file for *split_name*; equal-depth ties are
        ambiguous, as for ``index_file``."""
        paths.sort(key=lambda p: (len(p.parts), str(p)))
        ties = [p for p in paths if len(p.parts) == len(paths[0].parts)]
        if len(ties) > 1:
            raise ValueError(
                f"Ambiguous split '{split_name}': {len(ties)} matching files at "
                f"the same depth under '{self.extract_dir}': "
                f"{[str(t) for t in ties[:5]]}. Narrow 'splits_file_pattern' "
                "so it matches one file per split."
            )
        return paths[0]

    def _check_missing_splits(self, found: set[str], pattern: str) -> None:
        assert self.schema.splits is not None
        missing = [split for split in self.schema.splits if split not in found]
        if not missing:
            return
        message = (
            f"Declared splits {missing} have no file matching pattern "
            f"'{pattern}' under '{self.extract_dir}'"
        )
        if self.schema.strict:
            raise FileNotFoundError(message)
        warnings.warn(
            f"{message}; loading without them. Remove them from 'splits' or "
            "set 'strict: true' to make this an error.",
            DataLoadWarning,
            stacklevel=3,
        )
