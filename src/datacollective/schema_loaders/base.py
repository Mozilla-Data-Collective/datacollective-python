from __future__ import annotations

import abc
from enum import StrEnum
from pathlib import Path

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema

logger = get_logger(__name__)

#: Separator lookup used by index-based loaders.
FORMAT_SEP: dict[str, str] = {
    "csv": ",",
    "tsv": "\t",
    "pipe": "|",
}


class Strategy(StrEnum):
    """Loading strategies recognised by schema loaders."""

    MULTI_SPLIT = "multi_split"
    MULTI_SELECTION = "multi_selection"
    PAIRED_GLOB = "paired_glob"
    GLOB = "glob"


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
        """Locate the index file and read it into a raw `~pandas.DataFrame`.

        Resolves the separator from ``schema.separator`` (explicit override) or
        ``schema.format`` via `FORMAT_SEP`, then delegates the file
        lookup to `_resolve_index_file`.

        Used by all index-based loaders (ASR, TTS, ...) so that each loader
        only needs to call `_apply_column_mappings` on the result.

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

    def _load_multi_sections(self) -> pd.DataFrame:
        """
        Parsing logic for archives with multiple directories, and each directory
        has its own index file. The section name is inferred from the parent directory of the index file.
        """
        sections = self._resolve_sections()
        sep = self.schema.separator or FORMAT_SEP.get(self.schema.format or "", ",")
        header = "infer" if self.schema.has_header else None

        df = pd.DataFrame()
        for section_path in sections:
            logger.debug(f"Reading section: {section_path} (sep={sep!r})")
            section_df = pd.read_csv(
                section_path, sep=sep, header=header, encoding=self.schema.encoding
            )
            section_df["section"] = section_path.parents[0].name
            df = pd.concat([df, section_df])

        return df

    def _resolve_sections(self) -> list:
        """
        Get a list of valid sections, i.e. subdirectories that include an index file.
        """

        assert self.schema.sections is not None
        assert self.schema.index_file is not None
        assert self.schema.section_root is not None
        sections = self.schema.sections
        section_paths = []
        for section in sections:
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
