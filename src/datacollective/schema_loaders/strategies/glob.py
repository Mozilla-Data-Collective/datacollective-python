from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader

logger = get_logger(__name__)

#: Path-derived sources usable as ``source_column`` in glob column mappings.
GLOB_SOURCES = ("path", "name", "stem", "parent", "parents[N]", "content")

_PARENTS_RE = re.compile(r"parents\[(\d+)\]")


class GlobLoader(BaseSchemaLoader):
    """Load a directory-structured dataset by globbing for files.

    Metadata (e.g. speaker ID, language) is derived from each matched file's
    path rather than from an index file or sidecar pairing.

    When the schema declares ``columns``, each mapping's ``source_column``
    names a path-derived source instead of a file column:

    - ``path``: absolute path to the matched file
    - ``name``: file name (with extension)
    - ``stem``: file name without extension
    - ``parent``: parent directory name (same as ``parents[0]``)
    - ``parents[N]``: name of the Nth ancestor directory (0 = parent);
      empty string when the path is not that deep
    - ``content``: text content of the matched file (read with
      ``schema.encoding``, stripped)

    Without ``columns`` the loader falls back to its default output:
    ``audio_path`` (absolute path), ``language`` (parent directory name) and
    ``speaker_id`` (grandparent directory name).
    """

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)
        if not schema.file_pattern:
            raise ValueError("glob schema must specify 'file_pattern'")
        for logical_name, col_map in schema.columns.items():
            if not self._is_valid_source(col_map.source_column):
                raise ValueError(
                    f"glob schema column '{logical_name}' has unsupported "
                    f"source_column {col_map.source_column!r}. Supported "
                    f"path-derived sources: {', '.join(GLOB_SOURCES)}"
                )

    def load(self) -> pd.DataFrame:
        """Glob for files and derive metadata from each matched path.

        When ``splits`` is set, each split name is treated as a subdirectory
        under ``extract_dir`` and a ``split`` column is added.  Otherwise
        the glob runs from ``extract_dir`` directly.
        """
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

        if not self.schema.columns:
            # Default output when no mapping is declared
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

        raw_df = self._build_path_metadata(matched)
        return self._apply_column_mappings(raw_df)

    def _build_path_metadata(self, files: list[Path]) -> pd.DataFrame:
        """Build a raw DataFrame with one column per referenced path source.

        Only the sources referenced by the schema's column mappings are
        materialised (so file contents are read only when requested).
        The result is fed through the regular `_apply_column_mappings`, which
        handles renaming, dtypes and optional columns.
        """
        sources = {
            str(col_map.source_column) for col_map in self.schema.columns.values()
        }
        return pd.DataFrame(
            {source: self._derive_source_values(files, source) for source in sources}
        )

    def _derive_source_values(self, files: list[Path], source: str) -> list[str]:
        if source == "path":
            return [str(path) for path in files]
        if source == "name":
            return [path.name for path in files]
        if source == "stem":
            return [path.stem for path in files]
        if source == "parent":
            return [path.parent.name for path in files]
        if source == "content":
            return [
                path.read_text(encoding=self.schema.encoding).strip() for path in files
            ]

        match = _PARENTS_RE.fullmatch(source)
        assert match is not None  # guaranteed by __init__ validation
        index = int(match.group(1))
        return [
            path.parents[index].name if index < len(path.parents) else ""
            for path in files
        ]

    def _is_valid_source(self, source: str | int) -> bool:
        if not isinstance(source, str):
            return False
        if source in ("path", "name", "stem", "parent", "content"):
            return True
        return _PARENTS_RE.fullmatch(source) is not None
