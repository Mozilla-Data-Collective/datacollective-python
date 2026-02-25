"""
Abstract base class for all task-specific schema loaders.

To add a new task type:
1. Subclass `BaseSchemaLoader`.
2. Implement `load`.
3. Register the subclass in ``registry.py``.
"""

from __future__ import annotations

import abc
import logging
from pathlib import Path

import pandas as pd

from datacollective.schema import DatasetSchema

logger = logging.getLogger(__name__)

#: Separator lookup used by index-based loaders.
FORMAT_SEP: dict[str, str] = {
    "csv": ",",
    "tsv": "\t",
    "pipe": "|",
}

STRATEGY_MULTI_SPLIT = "multi_split"
STRATEGY_PAIRED_GLOB = "paired_glob"
STRATEGY_GLOB = "glob"


class BaseSchemaLoader(abc.ABC):
    """
    Interface that every task-specific loader must implement.

    Args:
        schema (DatasetSchema): The parsed schema for the dataset.
        extract_dir (Path): The directory where the dataset files have been extracted.
    """

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        self.schema = schema
        self.extract_dir = extract_dir

    @abc.abstractmethod
    def load(self) -> pd.DataFrame:
        """Load the dataset into a pandas DataFrame according to ``self.schema``."""
        ...

    def _load_index_file(self) -> pd.DataFrame:
        """Locate the index file and read it into a raw :class:`~pandas.DataFrame`.

        Resolves the separator from ``schema.separator`` (explicit override) or
        ``schema.format`` via :data:`FORMAT_SEP`, then delegates the file
        lookup to :meth:`_resolve_index_file`.

        Used by all index-based loaders (ASR, TTS, MT, …) so that each loader
        only needs to call :meth:`_apply_column_mappings` on the result.

        Returns:
            A raw (unmapped) DataFrame exactly as read from the index file.
        """
        index_path = self._resolve_index_file()
        sep = self.schema.separator or FORMAT_SEP.get(self.schema.format or "", ",")
        header = "infer" if self.schema.has_header else None

        logger.debug(f"Reading index file: {index_path} (sep={sep!r})")
        return pd.read_csv(
            index_path, sep=sep, header=header, encoding=self.schema.encoding
        )

    def _resolve_index_file(self) -> Path:
        """Find the index file inside the extracted directory.

        The method searches recursively and returns the shallowest match.

        Used by index-based loaders.

        Raises:
            FileNotFoundError: If no matching file is found.
        """
        assert self.schema.index_file is not None
        candidates = list(self.extract_dir.rglob(self.schema.index_file))
        if not candidates:
            raise FileNotFoundError(
                f"Index file '{self.schema.index_file}' not found "
                f"under '{self.extract_dir}'"
            )
        # Prefer the shallowest match
        candidates.sort(key=lambda p: len(p.parts))
        return candidates[0]

    def _apply_column_mappings(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Select and rename columns according to the schema, applying dtype conversions.

        Used by index-based loaders.

        Raises:
            KeyError: If a required column is not found in *raw_df*.
        """
        result_cols: dict[str, pd.Series] = {}

        for logical_name, col_map in self.schema.columns.items():
            source = col_map.source_column

            if source not in raw_df.columns:
                if col_map.optional:
                    logger.debug(f"Optional column '{source}' not found — skipping.")
                    continue
                raise KeyError(
                    f"Required column '{source}' not found in index file. "
                    f"Available columns: {list(raw_df.columns)}"
                )

            series = raw_df[source]

            if col_map.dtype == "file_path":
                base = self.schema.base_audio_path or ""
                series = series.apply(
                    lambda v, _b=base: str(self.extract_dir / _b / str(v))
                )
            elif col_map.dtype == "category":
                series = series.astype("category")
            elif col_map.dtype == "int":
                series = pd.to_numeric(series, errors="coerce").astype("Int64")
            elif col_map.dtype == "float":
                series = pd.to_numeric(series, errors="coerce")
            else:
                # default: treat as string
                series = series.astype(str)

            result_cols[logical_name] = series

        return pd.DataFrame(result_cols)
