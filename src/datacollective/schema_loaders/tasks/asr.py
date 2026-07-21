from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader, Strategy

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
        elif schema.root_strategy == Strategy.PAIRED_GLOB:
            if (schema.format or "").casefold() != "json":
                raise ValueError("ASR paired_glob schema only supports 'format: json'")
            if not schema.file_pattern:
                raise ValueError("ASR paired_glob schema must specify 'file_pattern'")
            if not schema.columns:
                raise ValueError(
                    "ASR paired_glob schema must specify at least two column mappings "
                    "for audio and transcription"
                )
        else:
            if not schema.index_file:
                raise ValueError("ASR schema must specify 'index_file'")
            if not schema.columns:
                raise ValueError(
                    "ASR schema must specify at least two column mappings for audio and transcription"
                )

    def load(self) -> pd.DataFrame:
        if self.schema.root_strategy == Strategy.MULTI_SPLIT:
            return self._load_multi_split()
        if self.schema.root_strategy == Strategy.PAIRED_GLOB:
            return self._load_paired_glob_json()
        raw_df = self._load_index_file()
        return self._apply_column_mappings(raw_df)

    def _load_paired_glob_json(self) -> pd.DataFrame:
        """
        Load an ASR dataset where each audio file is paired with a JSON sidecar
        (matched via ``file_pattern``) instead of a central index file.

        When ``record_path`` is set, the JSON key it names must hold a list of
        records (e.g. time-aligned utterances) and each record becomes one row;
        the remaining top-level keys are flattened with dot notation
        (audio.filename, ...) and repeated on
        every row of that file.  Without ``record_path`` each JSON file yields
        a single row.  Column mappings are then applied as for index files, so
        ``file_path`` columns (typically sourced from a filename field inside
        the JSON) resolve through the usual audio-path machinery.
        """
        assert self.schema.file_pattern is not None

        json_files = sorted(self.extract_dir.rglob(self.schema.file_pattern))
        json_files = [p for p in json_files if not p.name.startswith("._")]
        if not json_files:
            raise FileNotFoundError(
                f"No files matching '{self.schema.file_pattern}' "
                f"found under '{self.extract_dir}'"
            )

        logger.debug(
            f"Found {len(json_files)} JSON files matching '{self.schema.file_pattern}'"
        )

        record_path = self.schema.record_path
        frames: list[pd.DataFrame] = []
        for path in json_files:
            data = json.loads(path.read_text(encoding=self.schema.encoding))
            if record_path:
                if record_path not in data:
                    raise KeyError(
                        f"record_path '{record_path}' not found in '{path}'. "
                        f"Available keys: {list(data)}"
                    )
                frame = pd.json_normalize(data, record_path=record_path)
                meta = pd.json_normalize(
                    {key: value for key, value in data.items() if key != record_path}
                )
                for column in meta.columns:
                    frame[column] = meta[column].iloc[0]
            else:
                frame = pd.json_normalize(data)
            frames.append(frame)

        raw_df = pd.concat(frames, ignore_index=True)
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
