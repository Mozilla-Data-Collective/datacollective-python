from __future__ import annotations

import warnings
from pathlib import Path

import pytest

from datacollective.errors import DataLoadWarning
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


class TestMultiSplitQuoting:
    def test_tsv_field_starting_with_quote_keeps_all_rows(self, tmp_path: Path) -> None:
        """A '"' at the start of a TSV field must not swallow following rows."""
        _write_tsv(
            tmp_path / "train.tsv",
            "path\tsentence\n"
            'c1.mp3\t"Quoted start, no closing quote.\n'
            "c2.mp3\tswallowed?\n"
            'c3.mp3\tends with a quote" here\n'
            "c4.mp3\tlast\n",
        )
        schema = DatasetSchema(
            dataset_id="ds", root_strategy="multi_split", splits=["train"]
        )
        df = MultiSplitLoader(schema, tmp_path).load()
        assert list(df["path"]) == ["c1.mp3", "c2.mp3", "c3.mp3", "c4.mp3"]
        assert df["sentence"].iloc[0] == '"Quoted start, no closing quote.'
        assert df["sentence"].iloc[2] == 'ends with a quote" here'


class TestMultiSplitFileResolution:
    def _schema(self, **kwargs: object) -> DatasetSchema:
        return DatasetSchema(
            dataset_id="ds",
            root_strategy="multi_split",
            splits=["train", "test"],
            **kwargs,
        )

    def test_missing_declared_split_warns(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "train.tsv", "path\nc1.mp3\n")
        with pytest.warns(DataLoadWarning, match=r"\['test'\]"):
            df = MultiSplitLoader(self._schema(), tmp_path).load()
        assert set(df["split"]) == {"train"}

    def test_missing_declared_split_raises_when_strict(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "train.tsv", "path\nc1.mp3\n")
        with pytest.raises(FileNotFoundError, match=r"\['test'\]"):
            MultiSplitLoader(self._schema(strict=True), tmp_path).load()

    def test_all_declared_splits_present_does_not_warn(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "train.tsv", "path\nc1.mp3\n")
        _write_tsv(tmp_path / "test.tsv", "path\nc2.mp3\n")
        with warnings.catch_warnings():
            warnings.simplefilter("error", DataLoadWarning)
            MultiSplitLoader(self._schema(strict=True), tmp_path).load()

    def test_equal_depth_split_files_raise(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "a" / "train.tsv", "path\nc1.mp3\n")
        _write_tsv(tmp_path / "b" / "train.tsv", "path\nc2.mp3\n")
        _write_tsv(tmp_path / "test.tsv", "path\nc3.mp3\n")
        with pytest.raises(ValueError, match="Ambiguous split 'train'"):
            MultiSplitLoader(self._schema(), tmp_path).load()

    def test_shallowest_split_file_wins(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "train.tsv", "path\nshallow.mp3\n")
        _write_tsv(tmp_path / "deep" / "train.tsv", "path\ndeep.mp3\n")
        _write_tsv(tmp_path / "test.tsv", "path\nc3.mp3\n")
        df = MultiSplitLoader(self._schema(), tmp_path).load()
        assert "shallow.mp3" in set(df["path"])
        assert "deep.mp3" not in set(df["path"])
