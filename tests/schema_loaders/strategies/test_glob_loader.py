from __future__ import annotations

from pathlib import Path

import pytest

from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.strategies.glob import GlobLoader


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\x00")


class TestGlobValidation:
    def test_requires_file_pattern(self, tmp_path: Path) -> None:
        schema = DatasetSchema(dataset_id="ds", root_strategy="glob")
        with pytest.raises(ValueError, match="file_pattern"):
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
