from __future__ import annotations

from pathlib import Path

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader, Strategy

logger = get_logger(__name__)


class OTHLoader(BaseSchemaLoader):
    """Load a dataset described by a `DatasetSchema` with task ``OTH``.

    Supports the ``glob`` strategy for directory-structured datasets
    where metadata (e.g. speaker ID, language) is encoded in the path
    hierarchy rather than in an index file or text-file pairing.
    """

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)
        if schema.root_strategy == Strategy.GLOB:
            if not schema.file_pattern:
                raise ValueError("OTH glob schema must specify 'file_pattern'")
        else:
            raise ValueError(
                "OTH schema must specify either 'root_strategy: glob' with "
                "'file_pattern', or an 'index_file'"
            )

    def load(self) -> pd.DataFrame:
        return self._load_glob()

    def _load_glob(self) -> pd.DataFrame:
        """Glob for files and derive metadata from the directory hierarchy.

        When ``splits`` is set, each split name is treated as a subdirectory
        under ``extract_dir`` and a ``split`` column is added.  Otherwise
        the glob runs from ``extract_dir`` directly.

        For each matched file the loader extracts:
        - ``audio_path``: absolute path to the file
        - ``speaker_id``: grandparent directory name
        - ``language``: parent directory name
        - ``split`` (when splits are configured): source split directory
        """
        assert self.schema.file_pattern is not None

        if self.schema.splits:
            return self._load_glob_splits()

        return self._glob_directory(self.extract_dir)

    def _load_glob_splits(self) -> pd.DataFrame:
        assert self.schema.splits is not None

        frames: list[pd.DataFrame] = []
        for split_name in self.schema.splits:
            split_dir = self.extract_dir / split_name
            if not split_dir.is_dir():
                raise FileNotFoundError(
                    f"Split directory '{split_name}' not found at '{split_dir}'"
                )
            df = self._glob_directory(split_dir)
            df["split"] = split_name
            frames.append(df)

        return pd.concat(frames, ignore_index=True)

    def _glob_directory(self, root: Path) -> pd.DataFrame:
        assert self.schema.file_pattern is not None

        matched = sorted(root.rglob(self.schema.file_pattern))
        matched = [p for p in matched if not p.name.startswith("._")]

        if not matched:
            raise FileNotFoundError(
                f"No files matching '{self.schema.file_pattern}' found under '{root}'"
            )

        logger.debug(f"Found {len(matched)} files under '{root.name}'")

        rows: list[dict[str, str]] = []
        for path in matched:
            rows.append(
                {
                    "audio_path": str(path),
                    "language": path.parent.name,
                    "speaker_id": path.parent.parent.name,
                }
            )

        return pd.DataFrame(rows)
