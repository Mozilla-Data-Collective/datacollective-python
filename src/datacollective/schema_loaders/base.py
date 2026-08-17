from __future__ import annotations

import abc
import re
from pathlib import Path
from typing import Any

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import ColumnMapping, DatasetSchema, Strategy

__all__ = ["FORMAT_SEP", "SUFFIX_SEP", "BaseSchemaLoader", "Strategy"]

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


class BaseSchemaLoader(abc.ABC):
    """
    Interface that every strategy loader must implement.

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

        Used by index-based strategies so that each loader only needs to call
        `_apply_column_mappings` on the result.

        Returns:
            A raw (unmapped) DataFrame exactly as read from the index file.
        """
        index_path = self._resolve_index_file()
        return self._read_delimited_file(index_path)

    def _resolve_index_file(self) -> Path:
        """Find the index file inside the extracted directory.

        Resolution is deterministic: the literal path relative to the dataset
        root wins when it exists. Otherwise (non-strict schemas only) the tree
        is searched recursively and the shallowest match is used; multiple
        matches at the same depth are an error rather than an arbitrary pick.

        Used by index-based loaders.

        Raises:
            FileNotFoundError: If no matching file is found (in strict mode,
                if the literal relative path does not exist).
            ValueError: If the recursive search is ambiguous.
        """
        if self._resolved_index_file is not None:
            return self._resolved_index_file

        assert self.schema.index_file is not None
        literal = self.extract_dir / self.schema.index_file
        if literal.is_file():
            resolved = literal
        elif self.schema.strict:
            raise FileNotFoundError(
                f"Index file '{self.schema.index_file}' not found at "
                f"'{literal}' (strict schema: no recursive search)"
            )
        else:
            candidates = list(self.extract_dir.rglob(self.schema.index_file))
            if not candidates:
                raise FileNotFoundError(
                    f"Index file '{self.schema.index_file}' not found "
                    f"under '{self.extract_dir}'"
                )
            # Prefer the shallowest match; equal-depth ties are ambiguous
            candidates.sort(key=lambda p: (len(p.parts), str(p)))
            min_depth = len(candidates[0].parts)
            ties = [c for c in candidates if len(c.parts) == min_depth]
            if len(ties) > 1:
                raise ValueError(
                    f"Ambiguous index_file '{self.schema.index_file}': "
                    f"{len(ties)} matches at the same depth under "
                    f"'{self.extract_dir}': {[str(t) for t in ties[:5]]}. "
                    "Set 'index_file' to an explicit path relative to the "
                    "dataset root."
                )
            resolved = candidates[0]

        self._resolved_index_file = resolved
        self._dataset_root = self._derive_dataset_root(resolved, self.schema.index_file)
        return self._resolved_index_file

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
        if self.schema.strict:
            # Strict schemas require exact column names — no fuzzy matching
            return None

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
    ) -> Any:
        if pd.isna(value):
            return value

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
    ) -> Any:
        """Resolve a file path (like ``file_path`` dtype) and return its text content."""
        if pd.isna(value):  # if missing value, skip loading
            return value

        # Remove whitespaces in the path
        raw = str(value).strip()
        parts = Path(raw).parts
        if parts:
            raw = str(Path(*[p.strip() for p in parts]))

        resolved = self._resolve_file_path(raw, col_map, row)
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
                path_candidates = [
                    root / relative_candidate
                    for root in self._get_audio_search_roots(
                        row=row, template_value=template_value or raw_value
                    )
                ]
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
        """Resolve ``base_audio_path`` into deduplicated search roots.

        Empty entries (including template renders that come out empty) fall
        back to the dataset root; relative entries are anchored at it.
        """
        raw_paths = self.schema.base_audio_path
        if not isinstance(raw_paths, list):
            raw_paths = [raw_paths] if raw_paths else []

        roots: list[Path] = []
        for raw_path in raw_paths:
            root = self._resolve_audio_root(raw_path, row, template_value)
            if root not in roots:
                roots.append(root)
        return roots or [self._get_dataset_root()]

    def _resolve_audio_root(
        self,
        raw_path: str,
        row: pd.Series | None,
        template_value: str | None,
    ) -> Path:
        dataset_root = self._get_dataset_root()
        if not raw_path:
            return dataset_root

        if row is not None and "${" in raw_path:
            raw_path = self._render_path_template(
                template_value or "",
                row,
                raw_path,
                template_name="base_audio_path",
            )
            if not raw_path:
                return dataset_root

        path = Path(raw_path)
        return path if path.is_absolute() else dataset_root / path

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

        if col_map.path_match_strategy == "exact":
            matches = self._find_exact_matches(
                raw_value, col_map.file_extension, search_files, search_roots
            )
        else:  # "contains"
            matches = self._find_contains_matches(raw_value, search_files, search_roots)

        if len(matches) > 1:
            raise ValueError(
                f"Ambiguous file_path value '{raw_value}' using "
                f"path_match_strategy='{col_map.path_match_strategy}'. "
                f"Matches: {[str(match) for match in matches[:5]]}"
            )
        return matches[0] if matches else None

    def _find_exact_matches(
        self,
        raw_value: str,
        file_extension: str | None,
        search_files: list[Path],
        search_roots: list[Path],
    ) -> list[Path]:
        """Candidates whose name — or, for extension-less values, stem or
        extension-completed name — equals the value, or whose path relative to
        a search root equals it (case-insensitive)."""
        raw_path = Path(raw_value)
        extension = self._normalize_extension(file_extension)

        expected_names = {raw_path.name}
        expected_relatives = {raw_path.as_posix().casefold()}
        match_stem = not raw_path.suffix
        if match_stem and extension is not None:
            expected_names.add(f"{raw_path.name}{extension}")
            expected_relatives.add(
                f"{raw_path.as_posix().casefold()}{extension.casefold()}"
            )

        matches: list[Path] = []
        for candidate in search_files:
            is_match = (
                candidate.name in expected_names
                or (match_stem and candidate.stem == raw_path.name)
                or not expected_relatives.isdisjoint(
                    self._candidate_relative_paths(candidate, search_roots)
                )
            )
            if is_match and candidate not in matches:
                matches.append(candidate)
        return matches

    def _find_contains_matches(
        self,
        raw_value: str,
        search_files: list[Path],
        search_roots: list[Path],
    ) -> list[Path]:
        """Candidates whose name, stem, or path relative to a search root
        contains the value as a substring (case-insensitive)."""
        needle = raw_value.casefold()

        matches: list[Path] = []
        for candidate in search_files:
            haystacks = [candidate.name.casefold(), candidate.stem.casefold()]
            haystacks.extend(self._candidate_relative_paths(candidate, search_roots))
            if (
                any(needle in haystack for haystack in haystacks)
                and candidate not in matches
            ):
                matches.append(candidate)
        return matches

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
        """List candidate files under *search_roots* (shallowest first per
        root), cached per (roots, extension) pair."""
        extension = self._normalize_extension(file_extension)
        cache_key = (tuple(str(root) for root in search_roots), extension)
        if cache_key not in self._audio_file_cache:
            self._audio_file_cache[cache_key] = [
                path
                for root in search_roots
                for path in self._list_searchable_files(root, extension)
            ]
        return self._audio_file_cache[cache_key]

    def _list_searchable_files(self, root: Path, extension: str | None) -> list[Path]:
        if root.is_file():
            return [root] if self._is_searchable_audio_file(root, extension) else []
        if not root.exists():
            return []

        files = [
            path
            for path in root.rglob("*")
            if self._is_searchable_audio_file(path, extension)
        ]
        files.sort(key=lambda path: (len(path.relative_to(root).parts), str(path)))
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
        if self.schema.strict:
            # Strict schemas require exact column names — no fuzzy matching
            return None

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
