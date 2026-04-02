"""
End-to-end tests for schema-based loading.

These tests verify that the complete pipeline (from a schema.yaml + real data
files on disk all the way to a pandas DataFrame) works correctly without any
mocking. They create synthetic dataset layouts on the filesystem
and call through the public API (parse_schema -> load_dataset_from_schema).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from datacollective.schema import DatasetSchema, _parse_schema
from datacollective.schema_loaders.registry import _load_dataset_from_schema


def _write(path: Path, content: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding=encoding)


def _schema_from_dict(d: dict) -> DatasetSchema:
    return _parse_schema(d)


class TestASRIndexE2E:
    """Full pipeline: TSV/CSV index -> ASRLoader -> DataFrame."""

    def test_tsv_basic(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "train.tsv",
            "path\tsentence\tclient_id\n"
            "clips/c1.mp3\thello\tspk1\n"
            "clips/c2.mp3\tworld\tspk2\n",
        )
        schema = _schema_from_dict(
            {
                "dataset_id": "asr-tsv",
                "task": "ASR",
                "format": "tsv",
                "index_file": "train.tsv",
                "base_audio_path": "audio/",
                "columns": {
                    "audio_path": {"source_column": "path", "dtype": "file_path"},
                    "transcription": {"source_column": "sentence", "dtype": "string"},
                    "speaker": {"source_column": "client_id", "dtype": "category"},
                },
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)

        assert len(df) == 2
        assert set(df.columns) == {"audio_path", "transcription", "speaker"}
        # file_path dtype resolves against extract_dir + base_audio_path
        assert df["audio_path"].iloc[0] == str(tmp_path / "audio" / "clips/c1.mp3")
        assert df["speaker"].dtype.name == "category"

    def test_csv_with_optional_column(self, tmp_path: Path) -> None:
        _write(tmp_path / "data.csv", "path,sentence\nc1.mp3,hi\n")

        schema = _schema_from_dict(
            {
                "dataset_id": "asr-csv",
                "task": "ASR",
                "format": "csv",
                "index_file": "data.csv",
                "columns": {
                    "audio": {"source_column": "path", "dtype": "file_path"},
                    "text": {"source_column": "sentence"},
                    "missing": {
                        "source_column": "nonexistent",
                        "dtype": "string",
                        "optional": True,
                    },
                },
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert "missing" not in df.columns
        assert len(df) == 1

    def test_nested_index_file(self, tmp_path: Path) -> None:
        """Index file buried in subdirectories is still found."""
        _write(
            tmp_path / "some" / "nested" / "dir" / "meta.tsv",
            "path\tsentence\nc.mp3\thi\n",
        )
        schema = _schema_from_dict(
            {
                "dataset_id": "asr-nested",
                "task": "ASR",
                "format": "tsv",
                "index_file": "meta.tsv",
                "columns": {
                    "audio": {"source_column": "path", "dtype": "file_path"},
                    "text": {"source_column": "sentence"},
                },
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 1

    def test_int_and_float_columns(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "meta.tsv",
            "path\tsentence\tage\tscore\nc.mp3\thi\t30\t0.95\nc2.mp3\tbye\tbad\t1.5\n",
        )
        schema = _schema_from_dict(
            {
                "dataset_id": "asr-types",
                "task": "ASR",
                "format": "tsv",
                "index_file": "meta.tsv",
                "columns": {
                    "audio": {"source_column": "path", "dtype": "file_path"},
                    "text": {"source_column": "sentence"},
                    "age": {"source_column": "age", "dtype": "int"},
                    "score": {"source_column": "score", "dtype": "float"},
                },
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert df["age"].iloc[0] == 30
        assert pd.isna(df["age"].iloc[1])  # "bad" → coerced to NaN
        assert df["score"].iloc[1] == pytest.approx(1.5)

    def test_file_path_search_with_multiple_roots(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "data" / "metadata.csv",
            "Sentence ID,Sentences\nclip_001,hello\n",
        )
        audio_path = tmp_path / "data" / "recipes" / "nested" / "clip_001.wav"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"\x00")

        schema = _schema_from_dict(
            {
                "dataset_id": "asr-audio-search",
                "task": "ASR",
                "index_file": "data/metadata.csv",
                "base_audio_path": ["data/recipes/", "data/giving_gift/"],
                "columns": {
                    "audio_path": {
                        "source_column": "Sentence ID",
                        "dtype": "file_path",
                        "path_match_strategy": "exact",
                        "file_extension": ".wav",
                    },
                    "transcription": {
                        "source_column": "Sentences",
                        "dtype": "string",
                    },
                },
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert df["audio_path"].iloc[0] == str(audio_path)
        assert df["transcription"].iloc[0] == "hello"


# ===========================================================================
# ASR — multi-split
# ===========================================================================


class TestASRMultiSplitE2E:
    def test_three_splits(self, tmp_path: Path) -> None:
        for name, text in [("train", "t1"), ("dev", "d1"), ("test", "e1")]:
            _write(tmp_path / f"{name}.tsv", f"path\tsentence\n{name}.mp3\t{text}\n")

        schema = _schema_from_dict(
            {
                "dataset_id": "asr-ms",
                "task": "ASR",
                "root_strategy": "multi_split",
                "splits": ["train", "dev", "test"],
                "columns": {
                    "audio": {"source_column": "path", "dtype": "file_path"},
                    "text": {"source_column": "sentence"},
                },
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 3
        assert set(df["split"]) == {"train", "dev", "test"}

    def test_subset_of_splits(self, tmp_path: Path) -> None:
        """Only listed splits are loaded; others are ignored."""
        for name in ("train", "dev", "test", "other"):
            _write(tmp_path / f"{name}.tsv", f"path\tsentence\n{name}.mp3\ttext\n")

        schema = _schema_from_dict(
            {
                "dataset_id": "asr-ms2",
                "task": "ASR",
                "root_strategy": "multi_split",
                "splits": ["train", "dev"],
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert set(df["split"]) == {"train", "dev"}

    def test_custom_file_pattern(self, tmp_path: Path) -> None:
        _write(tmp_path / "train.csv", "path,sentence\nt.mp3,hello\n")

        schema = _schema_from_dict(
            {
                "dataset_id": "asr-ms-csv",
                "task": "ASR",
                "root_strategy": "multi_split",
                "splits": ["train"],
                "splits_file_pattern": "**/*.csv",
                "format": "csv",
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 1
        assert df["split"].iloc[0] == "train"


class TestTTSIndexE2E:
    def test_pipe_delimited_headerless(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "metadata.csv",
            "clip1.wav|Hello world\nclip2.wav|Goodbye\n",
        )
        schema = _schema_from_dict(
            {
                "dataset_id": "tts-pipe",
                "task": "TTS",
                "format": "pipe",
                "separator": "|",
                "has_header": False,
                "index_file": "metadata.csv",
                "base_audio_path": "wavs/",
                "columns": {
                    "audio_path": {"source_column": 0, "dtype": "file_path"},
                    "transcription": {"source_column": 1, "dtype": "string"},
                },
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 2
        assert df["transcription"].tolist() == ["Hello world", "Goodbye"]
        assert all("wavs" in p for p in df["audio_path"])

    def test_tsv_with_header(self, tmp_path: Path) -> None:
        _write(tmp_path / "meta.tsv", "audio\ttext\nc1.wav\thi\nc2.wav\tbye\n")

        schema = _schema_from_dict(
            {
                "dataset_id": "tts-tsv",
                "task": "TTS",
                "format": "tsv",
                "index_file": "meta.tsv",
                "columns": {
                    "audio": {"source_column": "audio", "dtype": "file_path"},
                    "text": {"source_column": "text"},
                },
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 2

    def test_no_column_mappings_returns_raw(self, tmp_path: Path) -> None:
        _write(tmp_path / "meta.csv", "a,b,c\n1,2,3\n")

        schema = _schema_from_dict(
            {
                "dataset_id": "tts-raw",
                "task": "TTS",
                "format": "csv",
                "index_file": "meta.csv",
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert list(df.columns) == ["a", "b", "c"]

    def test_custom_encoding(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "meta.tsv", "audio\ttext\nc.wav\tgrüezi\n", encoding="utf-8-sig"
        )

        schema = _schema_from_dict(
            {
                "dataset_id": "tts-enc",
                "task": "TTS",
                "format": "tsv",
                "index_file": "meta.tsv",
                "encoding": "utf-8-sig",
                "columns": {
                    "audio": {"source_column": "audio", "dtype": "file_path"},
                    "text": {"source_column": "text"},
                },
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert df["text"].iloc[0] == "grüezi"


class TestTTSPairedGlobE2E:
    def _create_paired_dataset(
        self,
        root: Path,
        splits: list[str],
        n_per_split: int = 3,
        audio_ext: str = ".webm",
    ) -> None:
        for split in splits:
            d = root / split
            d.mkdir(parents=True)
            for i in range(n_per_split):
                _write(d / f"{i:04d}.txt", f"Text {split} {i}")
                (d / f"{i:04d}{audio_ext}").write_bytes(b"\x00audio")

    def test_basic(self, tmp_path: Path) -> None:
        self._create_paired_dataset(tmp_path, ["General", "Chat"])

        schema = _schema_from_dict(
            {
                "dataset_id": "tts-pg",
                "task": "TTS",
                "root_strategy": "paired_glob",
                "file_pattern": "**/*.txt",
                "audio_extension": ".webm",
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 6
        assert set(df["split"]) == {"General", "Chat"}
        assert all(p.endswith(".webm") for p in df["audio_path"])

    def test_missing_audio_skipped(self, tmp_path: Path) -> None:
        d = tmp_path / "split"
        d.mkdir()
        _write(d / "001.txt", "has audio")
        (d / "001.webm").write_bytes(b"\x00")
        _write(d / "002.txt", "no audio")  # no matching .webm

        schema = _schema_from_dict(
            {
                "dataset_id": "tts-pg-skip",
                "task": "TTS",
                "root_strategy": "paired_glob",
                "file_pattern": "**/*.txt",
                "audio_extension": ".webm",
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 1
        assert df["transcription"].iloc[0] == "has audio"

    def test_transcription_stripped(self, tmp_path: Path) -> None:
        d = tmp_path / "s"
        d.mkdir()
        _write(d / "001.txt", "  whitespace padded  \n\n")
        (d / "001.wav").write_bytes(b"\x00")

        schema = _schema_from_dict(
            {
                "dataset_id": "tts-pg-strip",
                "task": "TTS",
                "root_strategy": "paired_glob",
                "file_pattern": "**/*.txt",
                "audio_extension": ".wav",
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert df["transcription"].iloc[0] == "whitespace padded"

    def test_audio_path_absolute(self, tmp_path: Path) -> None:
        d = tmp_path / "s"
        d.mkdir()
        _write(d / "001.txt", "hi")
        (d / "001.ogg").write_bytes(b"\x00")

        schema = _schema_from_dict(
            {
                "dataset_id": "tts-pg-abs",
                "task": "TTS",
                "root_strategy": "paired_glob",
                "file_pattern": "**/*.txt",
                "audio_extension": ".ogg",
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert Path(df["audio_path"].iloc[0]).is_absolute()


class TestTTSMultiSectionsE2E:
    def _create_multi_sections_dataset(self, root: Path, sections: list[str]) -> None:
        for section in sections:
            _write(
                root / "dataset" / section / "metadata.tsv",
                f"audio\ttext\n{section.lower()}.wav\tText {section}\n",
            )

    def test_basic(self, tmp_path: Path) -> None:
        self._create_multi_sections_dataset(tmp_path, ["General", "Chat"])

        schema = _schema_from_dict(
            {
                "dataset_id": "tts-ms",
                "task": "TTS",
                "root_strategy": "multi_sections",
                "section_root": "dataset",
                "sections": ["General", "Chat"],
                "index_file": "metadata.tsv",
                "format": "tsv",
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 2
        assert set(df["section"]) == {"General", "Chat"}
        assert df["text"].tolist() == ["Text General", "Text Chat"]

    def test_ignores_unlisted_sections(self, tmp_path: Path) -> None:
        self._create_multi_sections_dataset(tmp_path, ["General", "Chat", "Other"])

        schema = _schema_from_dict(
            {
                "dataset_id": "tts-ms-subset",
                "task": "TTS",
                "root_strategy": "multi_sections",
                "section_root": "dataset",
                "sections": ["General", "Chat"],
                "index_file": "metadata.tsv",
                "format": "tsv",
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 2
        assert set(df["section"]) == {"General", "Chat"}


class TestErrorPaths:
    def test_unknown_task_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(dataset_id="ds", task="UNKNOWN_TASK")
        with pytest.raises(ValueError, match="No schema loader registered"):
            _load_dataset_from_schema(schema, tmp_path)

    def test_asr_missing_index_file_raises(self, tmp_path: Path) -> None:
        schema = _schema_from_dict(
            {
                "dataset_id": "asr-err",
                "task": "ASR",
                "format": "tsv",
                "index_file": "missing.tsv",
                "columns": {"a": {"source_column": "x"}},
            }
        )
        with pytest.raises(FileNotFoundError, match="missing.tsv"):
            _load_dataset_from_schema(schema, tmp_path)

    def test_asr_missing_required_column_raises(self, tmp_path: Path) -> None:
        _write(tmp_path / "d.tsv", "path\tsentence\nc.mp3\thi\n")
        schema = _schema_from_dict(
            {
                "dataset_id": "asr-col",
                "task": "ASR",
                "format": "tsv",
                "index_file": "d.tsv",
                "columns": {"audio": {"source_column": "nonexistent"}},
            }
        )
        with pytest.raises(KeyError, match="nonexistent"):
            _load_dataset_from_schema(schema, tmp_path)

    def test_tts_paired_glob_no_text_files_raises(self, tmp_path: Path) -> None:
        schema = _schema_from_dict(
            {
                "dataset_id": "tts-err",
                "task": "TTS",
                "root_strategy": "paired_glob",
                "file_pattern": "**/*.txt",
                "audio_extension": ".webm",
            }
        )
        with pytest.raises(FileNotFoundError):
            _load_dataset_from_schema(schema, tmp_path)

    def test_tts_index_no_format_uses_index_extension(self, tmp_path: Path) -> None:
        _write(tmp_path / "meta.csv", "a,b\n1,2\n")
        schema = _schema_from_dict(
            {
                "dataset_id": "tts-nf",
                "task": "TTS",
                "index_file": "meta.csv",
            }
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert list(df.columns) == ["a", "b"]

    def test_asr_multi_split_no_files_raises(self, tmp_path: Path) -> None:
        schema = _schema_from_dict(
            {
                "dataset_id": "asr-ms-err",
                "task": "ASR",
                "root_strategy": "multi_split",
                "splits": ["train"],
            }
        )
        with pytest.raises(RuntimeError, match="No split files"):
            _load_dataset_from_schema(schema, tmp_path)

    def test_tts_multi_sections_missing_index_file_raises(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "dataset" / "General" / "metadata.tsv",
            "audio\ttext\ngeneral.wav\tHello from General\n",
        )
        schema = _schema_from_dict(
            {
                "dataset_id": "tts-ms-err",
                "task": "TTS",
                "root_strategy": "multi_sections",
                "section_root": "dataset",
                "sections": ["General", "Chat"],
                "index_file": "metadata.tsv",
                "format": "tsv",
            }
        )
        with pytest.raises(FileNotFoundError, match="Chat"):
            _load_dataset_from_schema(schema, tmp_path)
