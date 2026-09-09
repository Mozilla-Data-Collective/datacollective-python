from __future__ import annotations

from pathlib import Path

import pytest

from datacollective.schema import ColumnMapping, DatasetSchema
from datacollective.schema_loaders.strategies.multi_split import MultiSplitLoader


def _write_tsv(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestMultiSplitValidation:
    def test_requires_splits(self, tmp_path: Path) -> None:
        schema = DatasetSchema(dataset_id="ds", root_strategy="multi_split")
        with pytest.raises(ValueError, match="splits"):
            MultiSplitLoader(schema, tmp_path)


class TestMultiSplitLoader:
    def test_load_multiple_splits(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "train.tsv", "path\tsentence\nc1.mp3\thello\n")
        _write_tsv(tmp_path / "dev.tsv", "path\tsentence\nc2.mp3\tworld\n")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="multi_split",
            splits=["train", "dev"],
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
            },
        )
        df = MultiSplitLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert set(df["split"]) == {"train", "dev"}
        assert "audio" in df.columns
        assert "text" in df.columns

    def test_multi_split_without_columns(self, tmp_path: Path) -> None:
        """When no column mappings, raw columns + split should be returned."""
        _write_tsv(tmp_path / "train.tsv", "path\tsentence\nc1.mp3\thello\n")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="multi_split",
            splits=["train"],
        )
        df = MultiSplitLoader(schema, tmp_path).load()
        assert "split" in df.columns
        assert "path" in df.columns  # raw column name
        assert df["split"].iloc[0] == "train"

    def test_multi_split_custom_pattern(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "train.csv", "path,sentence\nc1.mp3,hello\n")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="multi_split",
            splits=["train"],
            splits_file_pattern="**/*.csv",
            format="csv",
        )
        df = MultiSplitLoader(schema, tmp_path).load()
        assert len(df) == 1

    def test_multi_split_ignores_unlisted_splits(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "train.tsv", "path\tsentence\nc1.mp3\thello\n")
        _write_tsv(tmp_path / "other.tsv", "path\tsentence\nc2.mp3\tbye\n")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="multi_split",
            splits=["train"],  # only train, not "other"
        )
        df = MultiSplitLoader(schema, tmp_path).load()
        assert len(df) == 1
        assert df["split"].iloc[0] == "train"

    def test_multi_split_no_matching_files_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="multi_split",
            splits=["nonexistent"],
        )
        with pytest.raises(RuntimeError, match="No split files"):
            MultiSplitLoader(schema, tmp_path).load()
