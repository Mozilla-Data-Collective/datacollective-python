from __future__ import annotations

import abc
import csv
from enum import StrEnum
from pathlib import Path

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import ColumnMapping, DatasetSchema

logger = get_logger(__name__)

#: Separator lookup used by index-based loaders.
FORMAT_SEP: dict[str, str] = {
    "csv": ",",
    "tsv": "\t",
    "pipe": "|",
}

SUFFIX_SEP: dict[str, str] = {
    ".csv": ",",
    ".tsv": "\t",
    ".tab": "\t",
    ".psv": "|",
    ".pipe": "|",
}


class Strategy(StrEnum):
    """Loading strategies recognised by schema loaders."""

    MULTI_SPLIT = "multi_split"
    MULTI_SECTIONS = "multi_sections"
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
        self._audio_file_cache: dict[
            tuple[tuple[str, ...], str | None], list[Path]
        ] = {}

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
        return self._read_delimited_file(index_path)

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
        df = pd.DataFrame()
        for section_path in sections:
            section_df = self._read_delimited_file(section_path)
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
            resolved_source = self._resolve_source_column(raw_df, source)

            if resolved_source is None:
                if col_map.optional:
                    logger.debug(f"Optional column '{source}' not found — skipping.")
                    continue
                raise KeyError(
                    f"Required column '{source}' not found in index file. "
                    f"Available columns: {list(raw_df.columns)}"
                )

            series = raw_df[resolved_source]

            if col_map.dtype == "file_path":
                series = series.apply(
                    lambda v, _col_map=col_map: self._resolve_file_path(v, _col_map)
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

    def _read_delimited_file(self, file_path: Path) -> pd.DataFrame:
        sep = self._resolve_separator(file_path)
        header = "infer" if self.schema.has_header else None

        logger.debug(f"Reading delimited file: {file_path} (sep={sep!r})")
        df = self._read_csv(file_path, sep=sep, header=header)

        sniffed_sep = self._maybe_sniff_separator(file_path, df, sep)
        if sniffed_sep is not None and sniffed_sep != sep:
            logger.debug(
                "Retrying %s with sniffed separator %r instead of %r",
                file_path,
                sniffed_sep,
                sep,
            )
            df = self._read_csv(file_path, sep=sniffed_sep, header=header)

        return self._normalize_dataframe_columns(df)

    def _read_csv(
        self, file_path: Path, sep: str | None, header: str | None
    ) -> pd.DataFrame:
        kwargs: dict[str, object] = {
            "header": header,
            "encoding": self.schema.encoding,
            "skipinitialspace": True,
        }
        if sep is None:
            kwargs["sep"] = None
            kwargs["engine"] = "python"
        else:
            kwargs["sep"] = sep
        return pd.read_csv(file_path, **kwargs)

    def _resolve_separator(self, file_path: Path | None = None) -> str | None:
        if self.schema.separator:
            return self.schema.separator
        if self.schema.format:
            return FORMAT_SEP.get(self.schema.format.casefold())
        index_file_path = (
            Path(self.schema.index_file) if self.schema.index_file else None
        )
        for candidate in (file_path, index_file_path):
            if not candidate:
                continue
            suffix = candidate.suffix.casefold()
            if suffix in SUFFIX_SEP:
                return SUFFIX_SEP[suffix]
        return None

    def _maybe_sniff_separator(
        self, file_path: Path, raw_df: pd.DataFrame, initial_sep: str | None
    ) -> str | None:
        if self.schema.separator or len(raw_df.columns) != 1 or not self.schema.columns:
            return None

        required_sources = [
            col_map.source_column
            for col_map in self.schema.columns.values()
            if not col_map.optional
        ]
        if not required_sources:
            return None
        if all(
            self._resolve_source_column(raw_df, source) is not None
            for source in required_sources
        ):
            return None

        with file_path.open(
            "r", encoding=self.schema.encoding, errors="ignore"
        ) as handle:
            sample = handle.read(4096)

        delimiters = "".join(
            delim
            for delim in (",", "\t", "|", ";")
            if delim in sample and delim != initial_sep
        )
        if not delimiters:
            return None

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=delimiters)
        except csv.Error:
            return None

        return dialect.delimiter

    def _normalize_dataframe_columns(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        if raw_df.empty and not len(raw_df.columns):
            return raw_df

        normalized_columns: list[str | int] = []
        for column in raw_df.columns:
            if isinstance(column, str):
                normalized_columns.append(column.replace("\ufeff", "").strip())
            else:
                normalized_columns.append(column)

        result = raw_df.copy()
        result.columns = normalized_columns
        return result

    def _resolve_source_column(
        self, raw_df: pd.DataFrame, source: str | int
    ) -> str | int | None:
        if source in raw_df.columns:
            return source
        if isinstance(source, int):
            return source if source in raw_df.columns else None

        stripped_source = source.strip()
        if stripped_source in raw_df.columns:
            return stripped_source

        normalized_source = self._normalize_column_key(stripped_source)
        matches = [
            column
            for column in raw_df.columns
            if isinstance(column, str)
            and self._normalize_column_key(column) == normalized_source
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise KeyError(
                f"Column '{source}' matched multiple index columns after normalization: {matches}"
            )
        return None

    def _normalize_column_key(self, column: str) -> str:
        cleaned = column.replace("\ufeff", "").strip()
        return " ".join(cleaned.split()).casefold()

    def _resolve_file_path(self, value: object, col_map: ColumnMapping) -> str:
        if pd.isna(value):
            return str(value)

        raw_value = str(value).strip()
        if not raw_value:
            return raw_value

        direct_candidates = self._build_direct_file_candidates(
            raw_value, col_map.file_extension
        )
        for candidate in direct_candidates:
            if candidate.exists():
                return str(candidate)

        if col_map.path_match_strategy != "direct":
            matched_path = self._search_audio_file(raw_value, col_map)
            if matched_path is not None:
                return str(matched_path)
            raise FileNotFoundError(
                f"Could not resolve file_path value '{raw_value}' using "
                f"path_match_strategy='{col_map.path_match_strategy}' "
                f"under base_audio_path={self.schema.base_audio_path!r}"
            )

        if direct_candidates:
            return str(direct_candidates[0])
        return raw_value

    def _build_direct_file_candidates(
        self, raw_value: str, file_extension: str | None
    ) -> list[Path]:
        relative_candidates = [Path(raw_value)]
        normalized_extension = self._normalize_extension(file_extension)
        if normalized_extension is not None and not Path(raw_value).suffix:
            relative_candidates.append(
                Path(raw_value).with_suffix(normalized_extension)
            )

        candidates: list[Path] = []
        seen: set[str] = set()
        for relative_candidate in relative_candidates:
            if relative_candidate.is_absolute():
                path_candidates = [relative_candidate]
            else:
                path_candidates = list(
                    root / relative_candidate for root in self._get_audio_search_roots()
                )
                path_candidates.append(self.extract_dir / relative_candidate)

            for candidate in path_candidates:
                key = str(candidate)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(candidate)

        return candidates

    def _get_audio_search_roots(self) -> list[Path]:
        raw_paths = self.schema.base_audio_path
        if raw_paths is None or raw_paths == "":
            return [self.extract_dir]

        path_values = raw_paths if isinstance(raw_paths, list) else [raw_paths]
        roots: list[Path] = []
        seen: set[str] = set()
        for raw_path in path_values:
            if raw_path in (None, ""):
                root = self.extract_dir
            else:
                path = Path(raw_path)
                root = path if path.is_absolute() else self.extract_dir / path

            key = str(root)
            if key in seen:
                continue
            seen.add(key)
            roots.append(root)

        return roots or [self.extract_dir]

    def _search_audio_file(self, raw_value: str, col_map: ColumnMapping) -> Path | None:
        search_roots = self._get_audio_search_roots()
        search_files = self._get_searchable_audio_files(
            search_roots, col_map.file_extension
        )
        normalized_extension = self._normalize_extension(col_map.file_extension)
        raw_path = Path(raw_value)
        expected_name = raw_path.name
        expected_stem = raw_path.stem if raw_path.suffix else raw_path.name
        normalized_value = raw_value.casefold()

        for candidate in search_files:
            if col_map.path_match_strategy == "exact":
                if candidate.name == expected_name:
                    return candidate
                if raw_path.suffix:
                    continue
                if candidate.stem == expected_stem:
                    return candidate
                if (
                    normalized_extension is not None
                    and candidate.name == f"{expected_name}{normalized_extension}"
                ):
                    return candidate
            elif col_map.path_match_strategy == "contains":
                relative_strings = [
                    candidate.name.casefold(),
                    candidate.stem.casefold(),
                ]
                relative_strings.extend(
                    self._candidate_relative_paths(candidate, search_roots)
                )
                if any(
                    normalized_value in relative_string
                    for relative_string in relative_strings
                ):
                    return candidate

        return None

    def _candidate_relative_paths(
        self, candidate: Path, search_roots: list[Path]
    ) -> list[str]:
        relative_paths: list[str] = []
        for root in search_roots:
            try:
                relative_paths.append(candidate.relative_to(root).as_posix().casefold())
            except ValueError:
                continue
        return relative_paths

    def _get_searchable_audio_files(
        self, search_roots: list[Path], file_extension: str | None
    ) -> list[Path]:
        normalized_extension = self._normalize_extension(file_extension)
        cache_key = (tuple(str(root) for root in search_roots), normalized_extension)
        cached = self._audio_file_cache.get(cache_key)
        if cached is not None:
            return cached

        files: list[Path] = []
        for root in search_roots:
            if root.is_file():
                if self._matches_extension(root, normalized_extension):
                    files.append(root)
                continue
            if not root.exists():
                continue

            root_files = [
                path
                for path in root.rglob("*")
                if path.is_file()
                and self._matches_extension(path, normalized_extension)
            ]
            root_files.sort(
                key=lambda path: (len(path.relative_to(root).parts), str(path))
            )
            files.extend(root_files)

        self._audio_file_cache[cache_key] = files
        return files

    def _matches_extension(self, path: Path, extension: str | None) -> bool:
        if extension is None:
            return True
        return path.suffix.casefold() == extension.casefold()

    def _normalize_extension(self, extension: str | None) -> str | None:
        if extension is None or extension == "":
            return None
        return extension if extension.startswith(".") else f".{extension}"
