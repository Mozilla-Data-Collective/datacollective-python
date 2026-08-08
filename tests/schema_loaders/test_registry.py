from __future__ import annotations

from pathlib import Path

import pytest

from datacollective.errors import TaskValidationError
from datacollective.schema import ColumnMapping, DatasetSchema
from datacollective.schema_loaders.registry import _load_dataset_from_schema


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestStrategyDispatch:
    def test_default_strategy_is_index(self, tmp_path: Path) -> None:
        """Without a root_strategy the schema is loaded via the index strategy."""
        _write(
            tmp_path / "train.tsv",
            "path\tsentence\nclip1.mp3\thello\nclip2.mp3\tworld\n",
        )

        schema = DatasetSchema(
            dataset_id="test",
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

    def test_dispatches_paired_glob(self, tmp_path: Path) -> None:
        d = tmp_path / "split"
        d.mkdir()
        _write(d / "001.txt", "hello")
        (d / "001.wav").write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="test-pg",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".wav",
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 1
        assert "audio_path" in df.columns
        assert "transcription" in df.columns

    def test_dispatches_multi_split(self, tmp_path: Path) -> None:
        _write(tmp_path / "train.tsv", "path\tsentence\nc1.mp3\thello\n")
        _write(tmp_path / "dev.tsv", "path\tsentence\nc2.mp3\tworld\n")

        schema = DatasetSchema(
            dataset_id="test-ms",
            root_strategy="multi_split",
            splits=["train", "dev"],
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 2
        assert set(df["split"]) == {"train", "dev"}

    def test_dispatches_multi_sections(self, tmp_path: Path) -> None:
        for section in ("General", "Chat"):
            _write(
                tmp_path / "dataset" / section / "metadata.tsv",
                f"audio\ttext\n{section.lower()}.wav\tHello from {section}\n",
            )

        schema = DatasetSchema(
            dataset_id="test-msec",
            root_strategy="multi_sections",
            section_root="dataset",
            sections=["General", "Chat"],
            index_file="metadata.tsv",
            format="tsv",
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 2
        assert set(df["section"]) == {"General", "Chat"}

    def test_dispatches_glob(self, tmp_path: Path) -> None:
        _write(tmp_path / "spk1" / "en" / "a.wav", "")
        _write(tmp_path / "spk2" / "fr" / "b.wav", "")

        schema = DatasetSchema(
            dataset_id="test-glob",
            root_strategy="glob",
            file_pattern="**/*.wav",
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 2
        assert list(df.columns) == ["audio_path", "language", "speaker_id"]
        assert set(df["language"]) == {"en", "fr"}
        assert set(df["speaker_id"]) == {"spk1", "spk2"}

    def test_any_strategy_works_with_any_task(self, tmp_path: Path) -> None:
        """Strategies are task-agnostic: e.g. TTS + multi_split is expressible."""
        _write(tmp_path / "train.tsv", "path\tsentence\nc1.mp3\thello\n")

        schema = DatasetSchema(
            dataset_id="test-tts-ms",
            task="TTS",
            root_strategy="multi_split",
            splits=["train"],
            columns={
                "audio_path": ColumnMapping(source_column="path", dtype="file_path"),
                "transcription": ColumnMapping(source_column="sentence"),
            },
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 1
        assert {"audio_path", "transcription", "split"} <= set(df.columns)

    def test_glob_requires_file_pattern(self, tmp_path: Path) -> None:
        schema = DatasetSchema(dataset_id="test-glob", root_strategy="glob")
        with pytest.raises(ValueError, match="must specify 'file_pattern'"):
            _load_dataset_from_schema(schema, tmp_path)

    def test_index_requires_index_file(self, tmp_path: Path) -> None:
        schema = DatasetSchema(dataset_id="test-idx")
        with pytest.raises(ValueError, match="index_file"):
            _load_dataset_from_schema(schema, tmp_path)

    def test_unknown_strategy_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(dataset_id="ds", root_strategy="unknown_strategy")
        with pytest.raises(ValueError, match="Unknown root_strategy"):
            _load_dataset_from_schema(schema, tmp_path)


class TestTaskContracts:
    def test_asr_contract_satisfied(self, tmp_path: Path) -> None:
        _write(tmp_path / "train.tsv", "path\tsentence\nc1.mp3\thello\n")

        schema = DatasetSchema(
            dataset_id="test-asr",
            task="ASR",
            format="tsv",
            index_file="train.tsv",
            columns={
                "audio_path": ColumnMapping(source_column="path", dtype="file_path"),
                "transcription": ColumnMapping(source_column="sentence"),
            },
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert {"audio_path", "transcription"} <= set(df.columns)

    def test_declared_columns_violating_contract_fail_fast(
        self, tmp_path: Path
    ) -> None:
        """Misconfigured mappings are rejected before any file resolution."""
        schema = DatasetSchema(
            dataset_id="test-asr-bad",
            task="ASR",
            format="tsv",
            index_file="missing.tsv",  # never touched: validation fails first
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
            },
        )
        with pytest.raises(TaskValidationError, match="audio_path"):
            _load_dataset_from_schema(schema, tmp_path)

    def test_raw_load_violating_contract_raises_post_load(self, tmp_path: Path) -> None:
        """A columns-less index schema loads raw, then fails the task contract."""
        _write(tmp_path / "train.tsv", "path\tsentence\nc1.mp3\thello\n")

        schema = DatasetSchema(
            dataset_id="test-asr-raw",
            task="ASR",
            format="tsv",
            index_file="train.tsv",
        )
        with pytest.raises(TaskValidationError, match="ASR"):
            _load_dataset_from_schema(schema, tmp_path)

    def test_unknown_task_loads_without_validation(self, tmp_path: Path) -> None:
        _write(tmp_path / "data.tsv", "a\tb\n1\t2\n")

        schema = DatasetSchema(
            dataset_id="test-unknown",
            task="BRAND_NEW_TASK",
            format="tsv",
            index_file="data.tsv",
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert list(df.columns) == ["a", "b"]

    def test_oth_task_has_no_contract(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "data.tsv",
            "id\tsentence\tlang\n1\thello\ten\n2\tbonjour\tfr\n",
        )

        schema = DatasetSchema(
            dataset_id="test-oth",
            task="OTH",
            format="tsv",
            index_file="data.tsv",
            columns={
                "id": ColumnMapping(source_column="id", dtype="int"),
                "sentence": ColumnMapping(source_column="sentence", dtype="string"),
                "lang": ColumnMapping(source_column="lang", dtype="category"),
            },
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 2
        assert list(df.columns) == ["id", "sentence", "lang"]
        assert df["lang"].dtype.name == "category"

    def test_no_task_loads_without_validation(self, tmp_path: Path) -> None:
        _write(tmp_path / "data.csv", "a,b\n1,2\n")

        schema = DatasetSchema(
            dataset_id="test-no-task",
            format="csv",
            index_file="data.csv",
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert list(df.columns) == ["a", "b"]

    def test_glob_strategy_skips_declared_contract_check(self, tmp_path: Path) -> None:
        """Glob output is fixed (audio_path/...), so declared columns are not checked."""
        _write(tmp_path / "spk1" / "en" / "a.wav", "")

        schema = DatasetSchema(
            dataset_id="test-oth-glob",
            task="OTH",
            root_strategy="glob",
            file_pattern="**/*.wav",
        )
        df = _load_dataset_from_schema(schema, tmp_path)
        assert len(df) == 1

    def test_paired_glob_text_variant_satisfies_contract(self, tmp_path: Path) -> None:
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
        assert {"audio_path", "transcription"} <= set(df.columns)
