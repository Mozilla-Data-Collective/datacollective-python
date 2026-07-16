import builtins
import sys
from pathlib import Path

import pandas as pd
import pytest
from _pytest.monkeypatch import MonkeyPatch
from datasets import Dataset, DatasetDict

from datacollective import load_dataset
from datacollective.errors import MissingDependencyError
from datacollective.hf_utils import _convert_to_hf, _require_datasets
from datacollective.schema import DatasetSchema


@pytest.fixture
def simple_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "audio": ["clips/a.wav", "clips/b.wav", "clips/c.wav"],
            "transcription": ["hello", "world", "again"],
        }
    )


@pytest.fixture
def split_df(simple_df: pd.DataFrame) -> pd.DataFrame:
    df = simple_df.copy()
    df["split"] = ["train", "train", "test"]
    return df


@pytest.fixture
def simple_schema() -> DatasetSchema:
    return DatasetSchema(dataset_id="some-dataset-id", task="ASR")


@pytest.fixture
def multi_split_schema() -> DatasetSchema:
    return DatasetSchema(
        dataset_id="some-dataset-id",
        task="ASR",
        root_strategy="multi_split",
        splits=["train", "test"],
    )


@pytest.fixture
def no_datasets(monkeypatch: MonkeyPatch) -> None:
    """Simulate the `datasets` library not being installed."""
    monkeypatch.delitem(sys.modules, "datasets", raising=False)
    original_import = builtins.__import__

    def _blocked_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "datasets" or name.startswith("datasets."):
            raise ImportError(f"No module named '{name}'")
        return original_import(name, *args, **kwargs)  # type: ignore

    monkeypatch.setattr(builtins, "__import__", _blocked_import)


def test_convert_returns_dataset(
    simple_df: pd.DataFrame, simple_schema: DatasetSchema
) -> None:
    result = _convert_to_hf(simple_df, simple_schema)

    assert isinstance(result, Dataset)
    assert result.num_rows == 3
    assert result.column_names == ["audio", "transcription"]
    assert result[0] == {"audio": "clips/a.wav", "transcription": "hello"}


def test_convert_multi_split_returns_datasetdict(
    split_df: pd.DataFrame, multi_split_schema: DatasetSchema
) -> None:
    result = _convert_to_hf(split_df, multi_split_schema)

    assert isinstance(result, DatasetDict)
    assert set(result.keys()) == {"train", "test"}
    assert result["train"].num_rows == 2
    assert result["test"].num_rows == 1
    # The redundant split column is dropped from each split
    assert result["train"].column_names == ["audio", "transcription"]
    assert result["test"][0]["transcription"] == "again"


def test_convert_split_column_without_schema_splits_stays_dataset(
    split_df: pd.DataFrame, simple_schema: DatasetSchema
) -> None:
    """A data column that happens to be named 'split' must not shred the dataset."""
    result = _convert_to_hf(split_df, simple_schema)

    assert isinstance(result, Dataset)
    assert result.num_rows == 3
    assert "split" in result.column_names


def test_convert_preserves_category_dtype(
    simple_df: pd.DataFrame, simple_schema: DatasetSchema
) -> None:
    simple_df["locale"] = pd.Series(["en", "en", "el"], dtype="category")
    result = _convert_to_hf(simple_df, simple_schema)

    assert isinstance(result, Dataset)
    assert result[2]["locale"] == "el"


def test_require_datasets_raises_friendly_error(no_datasets: None) -> None:
    with pytest.raises(MissingDependencyError, match=r"datacollective\[hf\]"):
        _require_datasets()


def test_convert_raises_without_datasets(
    no_datasets: None, simple_df: pd.DataFrame, simple_schema: DatasetSchema
) -> None:
    with pytest.raises(MissingDependencyError):
        _convert_to_hf(simple_df, simple_schema)


def test_load_dataset_rejects_invalid_return_format() -> None:
    with pytest.raises(ValueError, match="Invalid return_format"):
        load_dataset("some-dataset-id", return_format="parquet")  # type: ignore[arg-type]


def test_load_dataset_hf_fails_fast_without_datasets(
    no_datasets: None, monkeypatch: MonkeyPatch
) -> None:
    """The missing-dependency error must be raised before any API call or download."""

    def _fail(*args: object, **kwargs: object) -> None:
        raise AssertionError("API should not be called when `datasets` is missing")

    monkeypatch.setattr("datacollective.datasets.get_dataset_details", _fail)

    with pytest.raises(MissingDependencyError, match=r"datacollective\[hf\]"):
        load_dataset("some-dataset-id", return_format="hf")


def _mock_load_pipeline(
    monkeypatch: MonkeyPatch, df: pd.DataFrame, schema: DatasetSchema
) -> None:
    """Stub out the network/extract layers so load_dataset yields *df*."""
    monkeypatch.setattr(
        "datacollective.datasets.get_dataset_details",
        lambda dataset_id: {"id": dataset_id, "filename": "data.tar.gz"},
    )
    monkeypatch.setattr(
        "datacollective.datasets._get_dataset_schema", lambda _id: schema
    )
    monkeypatch.setattr(
        "datacollective.datasets._download_dataset",
        lambda **kwargs: Path("/tmp/data.tar.gz"),
    )
    monkeypatch.setattr(
        "datacollective.datasets._extract_archive", lambda **kwargs: Path("/tmp/data")
    )
    monkeypatch.setattr("datacollective.datasets._resolve_schema", lambda *args: schema)
    monkeypatch.setattr(
        "datacollective.datasets._load_dataset_from_schema",
        lambda schema, extract_dir: df,
    )


def test_load_dataset_returns_hf_datasetdict(
    monkeypatch: MonkeyPatch,
    split_df: pd.DataFrame,
    multi_split_schema: DatasetSchema,
) -> None:
    _mock_load_pipeline(monkeypatch, split_df, multi_split_schema)

    result = load_dataset("some-dataset-id", return_format="hf")

    assert isinstance(result, DatasetDict)
    assert set(result.keys()) == {"train", "test"}


def test_load_dataset_default_still_returns_dataframe(
    monkeypatch: MonkeyPatch,
    simple_df: pd.DataFrame,
    simple_schema: DatasetSchema,
) -> None:
    _mock_load_pipeline(monkeypatch, simple_df, simple_schema)

    result = load_dataset("some-dataset-id")

    assert isinstance(result, pd.DataFrame)
    assert result.equals(simple_df)
