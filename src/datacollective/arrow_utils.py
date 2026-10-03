from __future__ import annotations

import re
import warnings
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING

import pandas as pd

from datacollective.errors import MissingDependencyError
from datacollective.hf_utils import SPLIT_COLUMN
from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema

if TYPE_CHECKING:
    import pyarrow as pa

logger = get_logger(__name__)

ARROW_INSTALL_INSTRUCTIONS = (
    "The `pyarrow` library is required for `export_dataset` "
    "but is not installed. Install the optional dependency with:\n"
    '    pip install "datacollective[arrow]"\n'
    "or, if you use uv:\n"
    '    uv add "datacollective[arrow]"'
)

#: Arrow field metadata key holding the schema ``dtype`` of a column
#: (e.g. ``file_path``), so downstream readers can tell paths from plain text.
DTYPE_METADATA_KEY = b"mdc:dtype"
DATASET_ID_METADATA_KEY = b"mdc:dataset_id"
TASK_METADATA_KEY = b"mdc:task"
SDK_VERSION_METADATA_KEY = b"mdc:sdk_version"


def _require_pyarrow() -> ModuleType:
    """Import and return the `pyarrow` module.

    Raises:
        MissingDependencyError: If `pyarrow` is not installed, with
            instructions on how to install it.
    """
    try:
        import pyarrow
    except ImportError as exc:
        raise MissingDependencyError(ARROW_INSTALL_INSTRUCTIONS) from exc
    return pyarrow


def _arrow_type_for_dtype(dtype: str) -> pa.DataType | None:
    """Arrow type to use for a schema ``dtype`` when pandas could not infer one.

    Returns ``None`` for ``category``: the dictionary type inferred by
    `pyarrow.Table.from_pandas` is kept as-is.
    """
    pa = _require_pyarrow()
    if dtype in ("string", "file_path", "file_content"):
        return pa.string()
    if dtype == "int":
        return pa.int64()
    if dtype == "float":
        return pa.float64()
    return None


def _dataframe_to_table(df: pd.DataFrame, schema: DatasetSchema) -> pa.Table:
    """Convert a loaded DataFrame to a `pyarrow.Table`, annotated with schema info.

    Columns declared in ``schema.columns`` whose Arrow type could not be
    inferred (all-missing columns come out as the ``null`` type) are cast to
    the type implied by their schema ``dtype``. Each declared column gets a
    ``mdc:dtype`` field-metadata entry, and the table carries
    ``mdc:dataset_id``, ``mdc:task`` and ``mdc:sdk_version``. The ``pandas``
    metadata written by `from_pandas` is preserved so pandas round-trips.
    """
    pa = _require_pyarrow()
    from datacollective import __version__

    table = pa.Table.from_pandas(df, preserve_index=False)

    fields = []
    for field in table.schema:
        col_map = schema.columns.get(field.name)
        if col_map is None:
            fields.append(field)
            continue
        target = _arrow_type_for_dtype(col_map.dtype)
        if pa.types.is_null(field.type) and target is not None:
            field = field.with_type(target)
        metadata = dict(field.metadata or {})
        metadata[DTYPE_METADATA_KEY] = col_map.dtype.encode()
        fields.append(field.with_metadata(metadata))

    table_metadata = dict(table.schema.metadata or {})
    table_metadata[DATASET_ID_METADATA_KEY] = schema.dataset_id.encode()
    if schema.task:
        table_metadata[TASK_METADATA_KEY] = schema.task.encode()
    table_metadata[SDK_VERSION_METADATA_KEY] = __version__.encode()

    return table.cast(pa.schema(fields, metadata=table_metadata))


def _relativize_path(value: object, root: Path) -> object:
    """Rewrite an absolute path under *root* as a POSIX path relative to it.

    Missing values, relative paths and absolute paths outside *root* are
    returned unchanged.
    """
    if not isinstance(value, str) or not value:
        return value
    path = Path(value)
    if not path.is_absolute():
        return value
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return value


def _relativize_file_paths(
    df: pd.DataFrame, schema: DatasetSchema, dataset_root: Path
) -> pd.DataFrame:
    """Return a copy of *df* whose ``file_path`` columns are relative to *dataset_root*.

    Loaders resolve ``file_path`` columns to absolute paths inside the extracted
    dataset directory. Those paths are machine-specific (they embed the download
    directory and the user's home), so for export they are rewritten relative to
    *dataset_root*, using forward slashes regardless of platform. Values that
    are not absolute paths under *dataset_root* (missing values, unresolved raw
    values, files outside the extraction directory) are left untouched; a
    warning is emitted for the last case since such a file is not portable.
    """
    root = dataset_root.expanduser().resolve()
    path_columns = [
        name
        for name, col_map in schema.columns.items()
        if col_map.dtype == "file_path" and name in df.columns
    ]
    if not path_columns:
        return df

    df = df.copy()
    for name in path_columns:
        series = df[name].map(lambda v: _relativize_path(v, root))
        outside = series.map(lambda v: isinstance(v, str) and Path(v).is_absolute())
        if outside.any():
            examples = ", ".join(repr(v) for v in series[outside].unique()[:3])
            warnings.warn(
                f"Column '{name}': {int(outside.sum())} of {len(series)} file paths "
                f"are outside the dataset directory '{root}' and were kept as "
                f"absolute paths (e.g. {examples}). The exported file is not "
                "portable for these rows.",
                UserWarning,
                stacklevel=3,
            )
        df[name] = series
    return df


def _convert_to_arrow(
    df: pd.DataFrame,
    schema: DatasetSchema,
    *,
    dataset_root: Path | None = None,
) -> pa.Table | dict[str, pa.Table]:
    """Convert a loaded pandas DataFrame into one or more `pyarrow.Table`s.

    For multi-split datasets (``schema.splits`` is set, and the loader has
    added a ``split`` column), one table is built per split value and the
    result is a dict keyed by split name, with the redundant ``split`` column
    dropped. Otherwise, a single table is returned.

    Args:
        df: The loaded dataset.
        schema: The dataset schema that produced *df*; its ``splits`` field
            decides whether the result is split into a dict.
        dataset_root: The directory the dataset archive was extracted into.
            When given, ``file_path`` columns are rewritten relative to it (see
            `_relativize_file_paths`) so the tables do not embed local absolute
            paths. When ``None``, paths are kept as they are in *df*.

    Returns:
        A `pyarrow.Table`, or a dict of tables for multi-split datasets.

    Raises:
        MissingDependencyError: If `pyarrow` is not installed.
    """
    _require_pyarrow()

    if dataset_root is not None:
        df = _relativize_file_paths(df, schema, dataset_root)

    if schema.splits and SPLIT_COLUMN in df.columns:
        tables: dict[str, pa.Table] = {}
        for split_name, split_df in df.groupby(SPLIT_COLUMN, observed=True):
            tables[str(split_name)] = _dataframe_to_table(
                split_df.drop(columns=[SPLIT_COLUMN]).reset_index(drop=True), schema
            )
        logger.info(f"Converted DataFrame to Arrow tables with splits: {list(tables)}")
        return tables

    table = _dataframe_to_table(df, schema)
    logger.info(f"Converted DataFrame to Arrow table with {table.num_rows} rows")
    return table


def _sanitize_stem(name: str) -> str:
    """Make *name* safe to use as a file stem (no separators or whitespace)."""
    cleaned = re.sub(r"[\\/\s]+", "_", name.strip())
    return cleaned or "dataset"


def _write_parquet(
    tables: pa.Table | dict[str, pa.Table],
    output_dir: Path,
    *,
    file_stem: str,
) -> Path | dict[str, Path]:
    """Write one or more tables to Parquet under *output_dir*.

    A single table is written to ``<output_dir>/<file_stem>.parquet``; a dict
    of per-split tables is written to one ``<output_dir>/<file_stem>-<split>.parquet``
    file per split, so exports of different datasets can share a directory
    without clobbering each other. *output_dir* is created if needed. Existing
    files are overwritten.

    Returns:
        The written path, or a dict of paths keyed by split name.
    """
    pa = _require_pyarrow()
    import pyarrow.parquet as pq

    output_dir = output_dir.expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)

    if isinstance(tables, pa.Table):
        path = output_dir / f"{_sanitize_stem(file_stem)}.parquet"
        pq.write_table(tables, path)
        logger.info(f"Wrote {tables.num_rows} rows to {path}")
        return path

    paths: dict[str, Path] = {}
    stem = _sanitize_stem(file_stem)
    for split_name, table in tables.items():
        path = output_dir / f"{stem}-{_sanitize_stem(split_name)}.parquet"
        pq.write_table(table, path)
        logger.info(f"Wrote split '{split_name}' ({table.num_rows} rows) to {path}")
        paths[split_name] = path
    return paths
