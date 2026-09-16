import builtins
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from _pytest.monkeypatch import MonkeyPatch

from datacollective import export_dataset
from datacollective.arrow_utils import (
    DATASET_ID_METADATA_KEY,
    DTYPE_METADATA_KEY,
    SDK_VERSION_METADATA_KEY,
    TASK_METADATA_KEY,
    _convert_to_arrow,
    _relativize_file_paths,
    _require_pyarrow,
    _sanitize_stem,
    _write_parquet,
)
from datacollective.errors import MissingDependencyError
from datacollective.models import DatasetDetails
from datacollective.schema import ColumnMapping, DatasetSchema
from datacollective.schema_loaders.registry import _load_dataset_from_schema


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
    return DatasetSchema(
        dataset_id="some-dataset-id",
        task="ASR",
        columns={
            "audio": ColumnMapping(source_column="path", dtype="file_path"),
            "transcription": ColumnMapping(source_column="sentence"),
        },
    )


@pytest.fixture
def multi_split_schema(simple_schema: DatasetSchema) -> DatasetSchema:
    return DatasetSchema(
        dataset_id="some-dataset-id",
        task="ASR",
        root_strategy="multi_split",
        splits=["train", "test"],
        columns=simple_schema.columns,
    )


@pytest.fixture
def no_pyarrow(monkeypatch: MonkeyPatch) -> None:
    """Simulate the `pyarrow` library not being installed."""
    for name in [m for m in sys.modules if m == "pyarrow" or m.startswith("pyarrow.")]:
        monkeypatch.delitem(sys.modules, name, raising=False)
    original_import = builtins.__import__

    def _blocked_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "pyarrow" or name.startswith("pyarrow."):
            raise ImportError(f"No module named '{name}'")
        return original_import(name, *args, **kwargs)  # type: ignore

    monkeypatch.setattr(builtins, "__import__", _blocked_import)


def _mock_load_pipeline(
    monkeypatch: MonkeyPatch, df: pd.DataFrame, schema: DatasetSchema
) -> None:
    """Stub out the network/extract layers so the load pipeline yields *df*."""
    monkeypatch.setattr(
        "datacollective.datasets.get_dataset_details",
        lambda dataset_id: DatasetDetails(id=dataset_id, filename="data.tar.gz"),
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


# --- _convert_to_arrow -------------------------------------------------------


def test_convert_returns_table(
    simple_df: pd.DataFrame, simple_schema: DatasetSchema
) -> None:
    result = _convert_to_arrow(simple_df, simple_schema)

    assert isinstance(result, pa.Table)
    assert result.num_rows == 3
    assert result.column_names == ["audio", "transcription"]
    assert result.to_pylist()[0] == {"audio": "clips/a.wav", "transcription": "hello"}


def test_convert_multi_split_returns_dict_of_tables(
    split_df: pd.DataFrame, multi_split_schema: DatasetSchema
) -> None:
    result = _convert_to_arrow(split_df, multi_split_schema)

    assert isinstance(result, dict)
    assert set(result) == {"train", "test"}
    assert result["train"].num_rows == 2
    assert result["test"].num_rows == 1
    for table in result.values():
        assert "split" not in table.column_names


def test_convert_split_column_without_schema_splits_stays_single_table(
    split_df: pd.DataFrame, simple_schema: DatasetSchema
) -> None:
    result = _convert_to_arrow(split_df, simple_schema)

    assert isinstance(result, pa.Table)
    assert "split" in result.column_names


def test_convert_dtype_fidelity() -> None:
    df = pd.DataFrame(
        {
            "text": ["a", None, "c"],
            "count": pd.array([1, None, 3], dtype="Int64"),
            "score": [0.5, None, 1.0],
            "label": pd.Series(["x", "y", "x"]).astype("category"),
            # every sidecar missing: pandas cannot infer a type for this column
            "content": [None, None, None],
            "undeclared": [1, 2, 3],
        }
    )
    schema = DatasetSchema(
        dataset_id="dtypes",
        columns={
            "text": ColumnMapping(source_column="text"),
            "count": ColumnMapping(source_column="count", dtype="int"),
            "score": ColumnMapping(source_column="score", dtype="float"),
            "label": ColumnMapping(source_column="label", dtype="category"),
            "content": ColumnMapping(source_column="content", dtype="file_content"),
        },
    )

    table = _convert_to_arrow(df, schema)
    assert isinstance(table, pa.Table)

    assert pa.types.is_string(table.schema.field("text").type)
    assert table.schema.field("count").type == pa.int64()
    assert table.schema.field("score").type == pa.float64()
    assert pa.types.is_dictionary(table.schema.field("label").type)
    assert pa.types.is_string(table.schema.field("content").type)
    assert table.schema.field("undeclared").type == pa.int64()

    assert table.column("text").null_count == 1
    assert table.column("count").null_count == 1
    assert table.column("score").null_count == 1
    assert table.column("content").null_count == 3


def test_convert_attaches_metadata(
    simple_df: pd.DataFrame, simple_schema: DatasetSchema
) -> None:
    table = _convert_to_arrow(simple_df, simple_schema)
    assert isinstance(table, pa.Table)

    assert table.schema.field("audio").metadata[DTYPE_METADATA_KEY] == b"file_path"
    assert table.schema.field("transcription").metadata[DTYPE_METADATA_KEY] == b"string"
    assert table.schema.metadata[DATASET_ID_METADATA_KEY] == b"some-dataset-id"
    assert table.schema.metadata[TASK_METADATA_KEY] == b"ASR"
    assert SDK_VERSION_METADATA_KEY in table.schema.metadata
    # pandas round-trip metadata written by from_pandas must be preserved
    assert b"pandas" in table.schema.metadata


def test_convert_skips_task_metadata_when_unset(simple_df: pd.DataFrame) -> None:
    table = _convert_to_arrow(simple_df, DatasetSchema(dataset_id="no-task"))
    assert isinstance(table, pa.Table)
    assert TASK_METADATA_KEY not in table.schema.metadata


# --- _relativize_file_paths --------------------------------------------------


def test_relativize_rewrites_absolute_paths_under_root(
    tmp_path: Path, simple_schema: DatasetSchema
) -> None:
    root = tmp_path / "data"
    df = pd.DataFrame(
        {
            "audio": [
                str(root / "clips" / "a.wav"),
                str(root / "b.wav"),
                None,
                "unresolved.wav",
            ],
            "transcription": ["a", "b", "c", "d"],
        }
    )

    result = _relativize_file_paths(df, simple_schema, root)

    assert result["audio"].tolist() == [
        "clips/a.wav",
        "b.wav",
        None,
        "unresolved.wav",
    ]
    # non-path columns and the input frame are untouched
    assert result["transcription"].tolist() == ["a", "b", "c", "d"]
    assert df["audio"].iloc[0] == str(root / "clips" / "a.wav")


def test_relativize_keeps_paths_outside_root_and_warns(
    tmp_path: Path, simple_schema: DatasetSchema
) -> None:
    root = tmp_path / "data"
    elsewhere = str(tmp_path / "other" / "x.wav")
    df = pd.DataFrame(
        {"audio": [str(root / "a.wav"), elsewhere], "transcription": ["a", "b"]}
    )

    with pytest.warns(UserWarning, match="outside the dataset directory"):
        result = _relativize_file_paths(df, simple_schema, root)

    assert result["audio"].tolist() == ["a.wav", elsewhere]


def test_relativize_resolves_root_like_the_loader(
    tmp_path: Path, simple_schema: DatasetSchema
) -> None:
    """The loader resolves the extraction dir, so an unresolved root must still match."""
    root = tmp_path / "data"
    root.mkdir()
    link = tmp_path / "link"
    link.symlink_to(root, target_is_directory=True)
    df = pd.DataFrame({"audio": [str(root.resolve() / "a.wav")]})

    result = _relativize_file_paths(df, simple_schema, link)

    assert result["audio"].tolist() == ["a.wav"]


def test_relativize_noop_without_file_path_columns(simple_df: pd.DataFrame) -> None:
    schema = DatasetSchema(dataset_id="x", columns={})
    assert _relativize_file_paths(simple_df, schema, Path("/nowhere")) is simple_df


def test_convert_relativizes_when_root_given(
    tmp_path: Path, simple_schema: DatasetSchema
) -> None:
    df = pd.DataFrame(
        {"audio": [str(tmp_path / "clips" / "a.wav")], "transcription": ["x"]}
    )

    kept = _convert_to_arrow(df, simple_schema)
    rel = _convert_to_arrow(df, simple_schema, dataset_root=tmp_path)

    assert isinstance(kept, pa.Table) and isinstance(rel, pa.Table)
    assert kept.column("audio").to_pylist() == [str(tmp_path / "clips" / "a.wav")]
    assert rel.column("audio").to_pylist() == ["clips/a.wav"]


# --- _write_parquet ----------------------------------------------------------


def test_write_parquet_single_table(
    tmp_path: Path, simple_df: pd.DataFrame, simple_schema: DatasetSchema
) -> None:
    table = _convert_to_arrow(simple_df, simple_schema)

    result = _write_parquet(table, tmp_path / "out", file_stem="some-dataset-id")

    assert isinstance(result, Path)
    assert result == tmp_path / "out" / "some-dataset-id.parquet"
    assert result.is_file()
    back = pq.read_table(result)
    pd.testing.assert_frame_equal(back.to_pandas(), simple_df)
    assert back.schema.field("audio").metadata[DTYPE_METADATA_KEY] == b"file_path"
    assert back.schema.metadata[DATASET_ID_METADATA_KEY] == b"some-dataset-id"


def test_write_parquet_one_file_per_split(
    tmp_path: Path, split_df: pd.DataFrame, multi_split_schema: DatasetSchema
) -> None:
    tables = _convert_to_arrow(split_df, multi_split_schema)

    result = _write_parquet(tables, tmp_path, file_stem="some-dataset-id")

    assert isinstance(result, dict)
    assert result == {
        "train": tmp_path / "some-dataset-id-train.parquet",
        "test": tmp_path / "some-dataset-id-test.parquet",
    }
    assert pq.read_table(result["train"]).num_rows == 2
    assert pq.read_table(result["test"]).num_rows == 1
    assert "split" not in pq.read_table(result["train"]).column_names


def test_write_parquet_overwrites_existing(
    tmp_path: Path, simple_df: pd.DataFrame, simple_schema: DatasetSchema
) -> None:
    table = _convert_to_arrow(simple_df, simple_schema)
    _write_parquet(table, tmp_path, file_stem="ds")
    smaller = _convert_to_arrow(simple_df.head(1), simple_schema)

    path = _write_parquet(smaller, tmp_path, file_stem="ds")

    assert pq.read_table(path).num_rows == 1


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("some-dataset-id", "some-dataset-id"),
        ("group/name", "group_name"),
        ("  spaced name\t", "spaced_name"),
        ("", "dataset"),
    ],
)
def test_sanitize_stem(raw: str, expected: str) -> None:
    assert _sanitize_stem(raw) == expected


# --- missing dependency ------------------------------------------------------


def test_require_pyarrow_raises_friendly_error(no_pyarrow: None) -> None:
    with pytest.raises(MissingDependencyError, match=r"datacollective\[arrow\]"):
        _require_pyarrow()


def test_convert_raises_without_pyarrow(
    no_pyarrow: None, simple_df: pd.DataFrame, simple_schema: DatasetSchema
) -> None:
    with pytest.raises(MissingDependencyError):
        _convert_to_arrow(simple_df, simple_schema)


def test_export_dataset_fails_fast_without_pyarrow(
    no_pyarrow: None, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """The missing-dependency error must be raised before any API call or download."""

    def _fail(*args: object, **kwargs: object) -> None:
        raise AssertionError("API should not be called when `pyarrow` is missing")

    monkeypatch.setattr("datacollective.datasets.get_dataset_details", _fail)

    with pytest.raises(MissingDependencyError):
        export_dataset("some-dataset-id", tmp_path)


# --- export_dataset ----------------------------------------------------------


def test_export_dataset_rejects_invalid_format(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Invalid format"):
        export_dataset("some-dataset-id", tmp_path, format="csv")  # type: ignore


def test_export_dataset_writes_single_parquet(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    simple_df: pd.DataFrame,
    simple_schema: DatasetSchema,
) -> None:
    _mock_load_pipeline(monkeypatch, simple_df, simple_schema)

    result = export_dataset("some-dataset-id", tmp_path / "export")

    assert result == tmp_path / "export" / "some-dataset-id.parquet"
    pd.testing.assert_frame_equal(pd.read_parquet(result), simple_df)


def test_export_dataset_writes_one_parquet_per_split(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    split_df: pd.DataFrame,
    multi_split_schema: DatasetSchema,
) -> None:
    _mock_load_pipeline(monkeypatch, split_df, multi_split_schema)

    result = export_dataset("some-dataset-id", str(tmp_path))

    assert isinstance(result, dict)
    assert result == {
        "train": tmp_path / "some-dataset-id-train.parquet",
        "test": tmp_path / "some-dataset-id-test.parquet",
    }
    assert len(pd.read_parquet(result["train"])) == 2
    assert len(pd.read_parquet(result["test"])) == 1


def test_export_two_datasets_into_same_directory_do_not_collide(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    split_df: pd.DataFrame,
    multi_split_schema: DatasetSchema,
) -> None:
    _mock_load_pipeline(monkeypatch, split_df, multi_split_schema)
    first = export_dataset("some-dataset-id", tmp_path)

    other_schema = multi_split_schema.model_copy(update={"dataset_id": "other-id"})
    _mock_load_pipeline(monkeypatch, split_df.head(1), other_schema)
    second = export_dataset("other-id", tmp_path)

    assert isinstance(first, dict) and isinstance(second, dict)
    assert set(first.values()).isdisjoint(second.values())
    assert len(pd.read_parquet(first["train"])) == 2
    assert len(pd.read_parquet(second["train"])) == 1


def test_export_dataset_relativizes_file_paths_to_extract_dir(
    monkeypatch: MonkeyPatch, tmp_path: Path, simple_schema: DatasetSchema
) -> None:
    extract_dir = tmp_path / "extracted"
    df = pd.DataFrame(
        {
            "audio": [str(extract_dir / "clips" / "a.wav")],
            "transcription": ["hello"],
        }
    )
    _mock_load_pipeline(monkeypatch, df, simple_schema)
    monkeypatch.setattr(
        "datacollective.datasets._extract_archive", lambda **kwargs: extract_dir
    )

    result = export_dataset("some-dataset-id", tmp_path / "out")

    assert pd.read_parquet(result)["audio"].tolist() == ["clips/a.wav"]


def test_export_dataset_accepts_slug_and_uses_schema_dataset_id(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    simple_df: pd.DataFrame,
) -> None:
    schema = DatasetSchema(dataset_id="resolved-id")
    _mock_load_pipeline(monkeypatch, simple_df, schema)

    result = export_dataset("my-slug", tmp_path)

    assert result == tmp_path / "resolved-id.parquet"


# --- loader-level smoke test -------------------------------------------------


def test_export_from_loaded_index_dataset_writes_relative_paths(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "train.tsv").write_text(
        "path\tsentence\nclip1.mp3\thello\nclip2.mp3\tworld\n", encoding="utf-8"
    )
    (data_dir / "clip1.mp3").write_bytes(b"\x00")
    (data_dir / "clip2.mp3").write_bytes(b"\x00")

    schema = DatasetSchema(
        dataset_id="test",
        root_strategy="index",
        format="tsv",
        index_file="train.tsv",
        columns={
            "audio_path": ColumnMapping(source_column="path", dtype="file_path"),
            "transcription": ColumnMapping(source_column="sentence", dtype="string"),
        },
    )
    df = _load_dataset_from_schema(schema, data_dir)

    # the loader hands back absolute paths ...
    assert all(Path(p).is_absolute() for p in df["audio_path"])

    path = _write_parquet(
        _convert_to_arrow(df, schema, dataset_root=data_dir),
        tmp_path / "out",
        file_stem=schema.dataset_id,
    )

    # ... but the export stores them relative to the extraction directory
    back = pq.read_table(path)
    assert back.schema.field("audio_path").metadata[DTYPE_METADATA_KEY] == b"file_path"
    assert back.column("audio_path").to_pylist() == ["clip1.mp3", "clip2.mp3"]
    assert back.column("transcription").to_pylist() == ["hello", "world"]
    # and they resolve again against that directory
    assert all((data_dir / p).is_file() for p in back.column("audio_path").to_pylist())
