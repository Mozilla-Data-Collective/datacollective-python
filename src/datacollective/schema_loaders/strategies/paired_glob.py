from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader

logger = get_logger(__name__)


class PairedGlobLoader(BaseSchemaLoader):
    """Load a dataset where each audio file is paired with a sidecar file.

    Two variants exist, selected by ``schema.format``:

    - ``format: "json"``: each audio file has a JSON sidecar (matched via
      ``file_pattern``); column mappings are required and are applied to the
      normalised JSON records.
    - otherwise: each audio file has a matching text sidecar containing the
      transcription; requires ``file_pattern`` and ``audio_extension``.
    """

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)
        if not schema.file_pattern:
            raise ValueError("paired_glob schema must specify 'file_pattern'")
        if self._is_json_variant():
            if not schema.columns:
                raise ValueError(
                    "paired_glob schema with 'format: json' must specify column "
                    "mappings (e.g. for audio and transcription)"
                )
        elif not schema.audio_extension:
            raise ValueError(
                "paired_glob schema must specify 'audio_extension' "
                "(or 'format: json' for JSON sidecar files)"
            )

    def _is_json_variant(self) -> bool:
        return (self.schema.format or "").casefold() == "json"

    def load(self) -> pd.DataFrame:
        if self._is_json_variant():
            return self._load_json_sidecars()
        return self._load_text_sidecars()

    def _load_json_sidecars(self) -> pd.DataFrame:
        """
        Load a dataset where each audio file is paired with a JSON sidecar
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

    def _load_text_sidecars(self) -> pd.DataFrame:
        """
        Load a dataset where each audio file has a matching text file (e.g.
        ``.txt``) containing the transcription. The loader searches recursively
        for all text files matching the specified `file_pattern`, reads their
        contents, and pairs them with the corresponding audio files based on
        the same filename stem. The parent directory name of each text/audio
        pair is captured as a `split` column in the resulting DataFrame.
        """
        assert self.schema.file_pattern is not None
        assert self.schema.audio_extension is not None

        text_files = sorted(self.extract_dir.rglob(self.schema.file_pattern))
        if not text_files:
            raise FileNotFoundError(
                f"No files matching '{self.schema.file_pattern}' "
                f"found under '{self.extract_dir}'"
            )

        logger.debug(
            f"Found {len(text_files)} text files matching '{self.schema.file_pattern}'"
        )

        audio_ext = self.schema.audio_extension
        rows: list[dict[str, str]] = []

        for txt_path in text_files:
            audio_path = txt_path.with_suffix(audio_ext)
            if not audio_path.exists():
                logger.debug(
                    f"No matching audio file for '{txt_path.name}' — skipping."
                )
                continue

            transcription = txt_path.read_text(encoding=self.schema.encoding).strip()
            row: dict[str, str] = {
                "audio_path": str(audio_path),
                "transcription": transcription,
            }

            # Derive domain / split from parent directory name if present
            parent_name = txt_path.parent.name
            if parent_name:
                row["split"] = parent_name

            rows.append(row)

        if not rows:
            raise FileNotFoundError(
                f"No paired (text + {audio_ext}) files found under '{self.extract_dir}'"
            )

        return pd.DataFrame(rows)
