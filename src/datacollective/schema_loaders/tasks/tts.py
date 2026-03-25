from __future__ import annotations

from pathlib import Path

import pandas as pd

from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader, Strategy

logger = get_logger(__name__)


class TTSLoader(BaseSchemaLoader):
    """Load a TTS dataset described by a `DatasetSchema`.

    See docs/loaders/tts.md for details on supported loading strategies and schema fields.
    """

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)

    def load(self) -> pd.DataFrame:
        if self.schema.root_strategy == Strategy.PAIRED_GLOB:
            return self._load_paired_glob()
        elif self.schema.root_strategy == Strategy.MULTI_SECTIONS:
            return self._load_multi_sections()
        return self._load_based_on_index()

    def _load_based_on_index(self) -> pd.DataFrame:
        """
        Load a TTS dataset using the "index" strategy, where an index file (e.g. CSV) maps audio paths to transcriptions.
        """
        if not self.schema.index_file:
            raise ValueError("TTS index-based schema must specify 'index_file'")
        if not self.schema.format and not self.schema.separator:
            raise ValueError(
                "TTS index-based schema must specify 'format' or 'separator'"
            )

        raw_df = self._load_index_file()

        if not self.schema.columns:
            # No column mapping -> return the raw dataframe as-is
            return raw_df

        return self._apply_column_mappings(raw_df)

    def _load_paired_glob(self) -> pd.DataFrame:
        """
        Load a TTS dataset using the "paired_glob" strategy, where each audio file has a
        matching `.txt` file containing the transcription. The loader searches
        recursively for all text files matching the specified `file_pattern`,
        reads their contents, and pairs them with the corresponding audio files based
        on the same filename stem. The parent directory name of each text/audio pair
        is captured as a `split` column in the resulting DataFrame.
        """
        if not self.schema.file_pattern:
            raise ValueError("TTS paired_glob schema must specify 'file_pattern'")
        if not self.schema.audio_extension:
            raise ValueError("TTS paired_glob schema must specify 'audio_extension'")

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
