from __future__ import annotations

from pathlib import Path

import pytest

from datacollective.schema import ColumnMapping, DatasetSchema
from datacollective.schema_loaders.registry import _load_dataset_from_schema


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestLoadDatasetFromSchema:
    def test_dispatches_asr(self, tmp_path: Path) -> None:
        """An ASR index-based schema should be dispatched to ASRLoader and produce a DataFrame."""
        _write(
            tmp_path / "train.tsv",
            "path\tsentence\nclip1.mp3\thello\nclip2.mp3\tworld\n",
        )

        schema = DatasetSchema(
            dataset_id="test",
            task="ASR",
            format="tsv",
            index_file="train.tsv",
            columns={
                "audio_path": ColumnMapping(source_column="path", dtype="file_path"),
                "transcription": ColumnMapping(
                    source_column="sentence", dtype="string"
                ),
            },
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 2
        assert list(df.columns) == ["audio_path", "transcription"]

    def test_dispatches_tts_index(self, tmp_path: Path) -> None:
        """A TTS index-based schema should be dispatched to TTSLoader."""
        _write(tmp_path / "meta.csv", "audio,text\nc1.wav,hello\n")

        schema = DatasetSchema(
            dataset_id="test-tts",
            task="TTS",
            format="csv",
            index_file="meta.csv",
            columns={
                "audio": ColumnMapping(source_column="audio", dtype="file_path"),
                "text": ColumnMapping(source_column="text"),
            },
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 1
        assert "audio" in df.columns

    def test_dispatches_tts_paired_glob(self, tmp_path: Path) -> None:
        """A TTS paired-glob schema should be dispatched to TTSLoader."""
        d = tmp_path / "split"
        d.mkdir()
        _write(d / "001.txt", "hello")
        (d / "001.wav").write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="test-tts-pg",
            task="TTS",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".wav",
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 1
        assert "audio_path" in df.columns
        assert "transcription" in df.columns

    def test_unknown_task_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(dataset_id="ds", task="UNKNOWN_TASK")
        with pytest.raises(ValueError, match="No schema loader registered"):
            _load_dataset_from_schema(schema, tmp_path)
