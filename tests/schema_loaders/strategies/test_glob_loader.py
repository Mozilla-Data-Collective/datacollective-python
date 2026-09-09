from __future__ import annotations

from pathlib import Path

import pytest

from datacollective.schema import ColumnMapping, DatasetSchema
from datacollective.schema_loaders.strategies.glob import GlobLoader


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\x00")


class TestGlobValidation:
    def test_requires_file_pattern(self, tmp_path: Path) -> None:
        schema = DatasetSchema(dataset_id="ds", root_strategy="glob")
        with pytest.raises(ValueError, match="file_pattern"):
            GlobLoader(schema, tmp_path)

    def test_unknown_source_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="glob",
            file_pattern="**/*.wav",
            columns={"lang": ColumnMapping(source_column="grandparent")},
        )
        with pytest.raises(ValueError, match="unsupported source_column"):
            GlobLoader(schema, tmp_path)

    def test_int_source_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="glob",
            file_pattern="**/*.wav",
            columns={"lang": ColumnMapping(source_column=0)},
        )
        with pytest.raises(ValueError, match="unsupported source_column"):
            GlobLoader(schema, tmp_path)


class TestGlobLoader:
    def test_derives_metadata_from_path(self, tmp_path: Path) -> None:
        _touch(tmp_path / "spk1" / "en" / "a.wav")
        _touch(tmp_path / "spk2" / "fr" / "b.wav")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="glob",
            file_pattern="**/*.wav",
        )
        df = GlobLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert list(df.columns) == ["audio_path", "language", "speaker_id"]
        assert set(df["language"]) == {"en", "fr"}
        assert set(df["speaker_id"]) == {"spk1", "spk2"}

    def test_splits_add_split_column(self, tmp_path: Path) -> None:
        _touch(tmp_path / "train" / "spk1" / "en" / "a.wav")
        _touch(tmp_path / "dev" / "spk2" / "fr" / "b.wav")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="glob",
            file_pattern="**/*.wav",
            splits=["train", "dev"],
        )
        df = GlobLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert set(df["split"]) == {"train", "dev"}

    def test_missing_split_directory_raises(self, tmp_path: Path) -> None:
        _touch(tmp_path / "train" / "spk1" / "en" / "a.wav")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="glob",
            file_pattern="**/*.wav",
            splits=["train", "dev"],
        )
        with pytest.raises(FileNotFoundError, match="dev"):
            GlobLoader(schema, tmp_path).load()

    def test_no_matching_files_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="glob",
            file_pattern="**/*.wav",
        )
        with pytest.raises(FileNotFoundError, match="No files matching"):
            GlobLoader(schema, tmp_path).load()


class TestGlobColumnMapping:
    def test_maps_path_derived_sources(self, tmp_path: Path) -> None:
        _touch(tmp_path / "spk1" / "en" / "a.wav")
        _touch(tmp_path / "spk2" / "fr" / "b.wav")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="glob",
            file_pattern="**/*.wav",
            columns={
                "audio_path": ColumnMapping(source_column="path"),
                "file_name": ColumnMapping(source_column="name"),
                "utterance_id": ColumnMapping(source_column="stem"),
                "language": ColumnMapping(source_column="parent", dtype="category"),
                "speaker_id": ColumnMapping(source_column="parents[1]"),
            },
        )
        df = GlobLoader(schema, tmp_path).load()
        assert list(df.columns) == [
            "audio_path",
            "file_name",
            "utterance_id",
            "language",
            "speaker_id",
        ]
        assert df["audio_path"].iloc[0] == str(tmp_path / "spk1" / "en" / "a.wav")
        assert df["file_name"].iloc[0] == "a.wav"
        assert df["utterance_id"].iloc[0] == "a"
        assert set(df["language"]) == {"en", "fr"}
        assert df["language"].dtype.name == "category"
        assert set(df["speaker_id"]) == {"spk1", "spk2"}

    def test_content_source_reads_file_text(self, tmp_path: Path) -> None:
        d = tmp_path / "en"
        d.mkdir()
        (d / "a.txt").write_text("  hello world \n", encoding="utf-8")
        (d / "b.txt").write_text("goodbye\n", encoding="utf-8")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="glob",
            file_pattern="**/*.txt",
            columns={
                "text": ColumnMapping(source_column="content"),
                "language": ColumnMapping(source_column="parent", dtype="category"),
            },
        )
        df = GlobLoader(schema, tmp_path).load()
        assert list(df["text"]) == ["hello world", "goodbye"]
        assert set(df["language"]) == {"en"}

    def test_parents_index_out_of_range_is_empty(self, tmp_path: Path) -> None:
        _touch(tmp_path / "a.wav")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="glob",
            file_pattern="**/*.wav",
            columns={
                "audio_path": ColumnMapping(source_column="path"),
                "deep": ColumnMapping(source_column="parents[50]"),
            },
        )
        df = GlobLoader(schema, tmp_path).load()
        assert df["deep"].iloc[0] == ""

    def test_splits_with_columns_keep_split(self, tmp_path: Path) -> None:
        _touch(tmp_path / "train" / "spk1" / "en" / "a.wav")
        _touch(tmp_path / "dev" / "spk2" / "fr" / "b.wav")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="glob",
            file_pattern="**/*.wav",
            splits=["train", "dev"],
            columns={
                "audio_path": ColumnMapping(source_column="path"),
                "language": ColumnMapping(source_column="parent"),
            },
        )
        df = GlobLoader(schema, tmp_path).load()
        assert list(df.columns) == ["audio_path", "language", "split"]
        assert set(df["split"]) == {"train", "dev"}
