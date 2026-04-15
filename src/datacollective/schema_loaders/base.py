from __future__ import annotations

import abc
import csv
import re
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
        self.extract_dir = extract_dir.expanduser().resolve()
        self._resolved_index_file: Path | None = None
        self._dataset_root: Path | None = None
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
        if self._resolved_index_file is not None:
            return self._resolved_index_file

        assert self.schema.index_file is not None
        candidates = list(self.extract_dir.rglob(self.schema.index_file))
        if not candidates:
            raise FileNotFoundError(
                f"Index file '{self.schema.index_file}' not found "
                f"under '{self.extract_dir}'"
            )
        # Prefer the shallowest match
        candidates.sort(key=lambda p: len(p.parts))
        self._resolved_index_file = candidates[0]
        self._dataset_root = self._derive_dataset_root(
            self._resolved_index_file, self.schema.index_file
        )
        return self._resolved_index_file

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
                series = raw_df.apply(
                    lambda row, _col_map=col_map, _source=resolved_source: (
                        self._resolve_file_path(row[_source], _col_map, row)
                    ),
                    axis=1,
                )
            elif col_map.dtype == "file_content":
                series = raw_df.apply(
                    lambda row, _col_map=col_map, _source=resolved_source: (
                        self._load_file_content(row[_source], _col_map, row)
                    ),
                    axis=1,
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

    def _resolve_file_path(
        self, value: object, col_map: ColumnMapping, row: pd.Series | None = None
    ) -> str:
        if pd.isna(value):
            return str(value)

        source_value = str(value).strip()
        raw_value = source_value
        if row is not None and col_map.path_template:
            raw_value = self._render_path_template(
                source_value, row, col_map.path_template
            )
        if not raw_value:
            return raw_value

        direct_candidates = self._build_direct_file_candidates(
            raw_value,
            col_map.file_extension,
            row=row,
            template_value=source_value,
        )
        for candidate in direct_candidates:
            if candidate.exists():
                return str(candidate)

        if col_map.path_match_strategy != "direct":
            matched_path = self._search_audio_file(
                raw_value,
                col_map,
                row=row,
                template_value=source_value,
            )
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

    def _load_file_content(
        self, value: object, col_map: ColumnMapping, row: pd.Series | None = None
    ) -> str:
        """Resolve a file path (like ``file_path`` dtype) and return its text content."""
        resolved = self._resolve_file_path(value, col_map, row)
        path = Path(resolved)
        if path.is_file():
            return path.read_text(encoding=self.schema.encoding).strip()
        return resolved

    def _build_direct_file_candidates(
        self,
        raw_value: str,
        file_extension: str | None,
        row: pd.Series | None = None,
        template_value: str | None = None,
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
                    root / relative_candidate
                    for root in self._get_audio_search_roots(
                        row=row, template_value=template_value or raw_value
                    )
                )
                dataset_root = self._get_dataset_root()
                path_candidates.append(dataset_root / relative_candidate)
                if dataset_root != self.extract_dir:
                    path_candidates.append(self.extract_dir / relative_candidate)

            for candidate in path_candidates:
                key = str(candidate)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(candidate)

        return candidates

    def _get_audio_search_roots(
        self,
        row: pd.Series | None = None,
        template_value: str | None = None,
    ) -> list[Path]:
        raw_paths = self.schema.base_audio_path
        dataset_root = self._get_dataset_root()
        if raw_paths is None or raw_paths == "":
            return [dataset_root]

        path_values = raw_paths if isinstance(raw_paths, list) else [raw_paths]
        roots: list[Path] = []
        seen: set[str] = set()
        for raw_path in path_values:
            if raw_path in (None, ""):
                root = dataset_root
            else:
                rendered_path = raw_path
                if row is not None and "${" in raw_path:
                    rendered_path = self._render_path_template(
                        template_value or "",
                        row,
                        raw_path,
                        template_name="base_audio_path",
                    )

                if rendered_path in (None, ""):
                    root = dataset_root
                else:
                    path = Path(rendered_path)
                    root = path if path.is_absolute() else dataset_root / path

            key = str(root)
            if key in seen:
                continue
            seen.add(key)
            roots.append(root)

        return roots or [dataset_root]

    def _search_audio_file(
        self,
        raw_value: str,
        col_map: ColumnMapping,
        row: pd.Series | None = None,
        template_value: str | None = None,
    ) -> Path | None:
        search_roots = self._get_audio_search_roots(
            row=row, template_value=template_value or raw_value
        )
        search_files = self._get_searchable_audio_files(
            search_roots, col_map.file_extension
        )
        normalized_extension = self._normalize_extension(col_map.file_extension)
        raw_path = Path(raw_value)
        expected_name = raw_path.name
        expected_stem = raw_path.stem if raw_path.suffix else raw_path.name
        normalized_value = raw_value.casefold()
        normalized_relative_value = raw_path.as_posix().casefold()
        normalized_relative_with_extension = None
        if not raw_path.suffix and normalized_extension is not None:
            normalized_relative_with_extension = (
                f"{normalized_relative_value}{normalized_extension.casefold()}"
            )
        matches: list[Path] = []
        seen_matches: set[str] = set()

        for candidate in search_files:
            is_match = False
            relative_paths = self._candidate_relative_paths(candidate, search_roots)
            if col_map.path_match_strategy == "exact":
                if candidate.name == expected_name:
                    is_match = True
                elif not raw_path.suffix and candidate.stem == expected_stem:
                    is_match = True
                elif (
                    not raw_path.suffix
                    and normalized_extension is not None
                    and candidate.name == f"{expected_name}{normalized_extension}"
                ):
                    is_match = True
                elif normalized_relative_value in relative_paths:
                    is_match = True
                elif (
                    normalized_relative_with_extension is not None
                    and normalized_relative_with_extension in relative_paths
                ):
                    is_match = True
            elif col_map.path_match_strategy == "contains":
                relative_strings = [
                    candidate.name.casefold(),
                    candidate.stem.casefold(),
                ]
                relative_strings.extend(relative_paths)
                if any(
                    normalized_value in relative_string
                    for relative_string in relative_strings
                ):
                    is_match = True

            if not is_match:
                continue

            candidate_key = str(candidate)
            if candidate_key in seen_matches:
                continue
            seen_matches.add(candidate_key)
            matches.append(candidate)

        if len(matches) > 1:
            raise ValueError(
                f"Ambiguous file_path value '{raw_value}' using "
                f"path_match_strategy='{col_map.path_match_strategy}'. "
                f"Matches: {[str(match) for match in matches[:5]]}"
            )
        return matches[0] if matches else None

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
                if self._is_searchable_audio_file(root, normalized_extension):
                    files.append(root)
                continue
            if not root.exists():
                continue

            root_files = [
                path
                for path in root.rglob("*")
                if self._is_searchable_audio_file(path, normalized_extension)
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

    def _is_searchable_audio_file(self, path: Path, extension: str | None) -> bool:
        return (
            path.is_file()
            and not path.name.startswith("._")
            and self._matches_extension(path, extension)
        )

    def _normalize_extension(self, extension: str | None) -> str | None:
        if extension is None or extension == "":
            return None
        return extension if extension.startswith(".") else f".{extension}"

    def _get_dataset_root(self) -> Path:
        return self._dataset_root or self.extract_dir

    def _derive_dataset_root(
        self, resolved_path: Path, relative_path: str | None
    ) -> Path:
        if not relative_path:
            return resolved_path.parent

        relative = Path(relative_path)
        if relative.is_absolute():
            return relative.parent

        num_parts = len(relative.parts)
        if num_parts <= 1:
            return resolved_path.parent

        return resolved_path.parents[num_parts - 1]

    def _render_path_template(
        self,
        raw_value: str,
        row: pd.Series,
        template: str,
        template_name: str = "path_template",
    ) -> str:
        def replace(match: re.Match[str]) -> str:
            placeholder = match.group(1).strip()
            if placeholder == "value":
                return raw_value

            row_key = self._resolve_row_column(row, placeholder)
            if row_key is None:
                raise KeyError(
                    f"Could not render {template_name} placeholder '{placeholder}'. "
                    f"Available columns: {list(row.index)}"
                )

            cell_value = row[row_key]
            if pd.isna(cell_value):
                return ""
            return str(cell_value).strip()

        return re.sub(r"\$\{([^}]+)\}", replace, template)

    def _resolve_row_column(
        self, row: pd.Series, source: str | int
    ) -> str | int | None:
        if source in row.index:
            return source
        if isinstance(source, int):
            return source if source in row.index else None

        stripped_source = source.strip()
        if stripped_source in row.index:
            return stripped_source

        normalized_source = self._normalize_column_key(stripped_source)
        matches = [
            column
            for column in row.index
            if isinstance(column, str)
            and self._normalize_column_key(column) == normalized_source
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise KeyError(
                f"Column '{source}' matched multiple row columns after normalization: {matches}"
            )
        return None
