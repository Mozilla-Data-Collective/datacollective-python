# Extending the Schema Loaders

This guide is for **SDK contributors** and **dataset owners** who want to add
support for new task types, extend existing loading strategies, or write a
`schema.yaml` for a new dataset.

## 1. How to add a new task type

Adding support for a new task (e.g. **MT** — Machine Translation) takes three
steps.

### Step 1 — Create the loader class

Create a new file under `src/datacollective/schema_loaders/tasks/`, e.g.
`mt.py`:

```python
from __future__ import annotations

from pathlib import Path

import pandas as pd

from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader


class MTLoader(BaseSchemaLoader):
    """Load a machine-translation dataset."""

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)
        # Validate required schema fields
        if not schema.index_file:
            raise ValueError("MT schema must specify 'index_file'")
        if not schema.format:
            raise ValueError("MT schema must specify 'format'")
        if not schema.columns:
            raise ValueError("MT schema must specify 'columns'")

    def load(self) -> pd.DataFrame:
        # BaseSchemaLoader provides _load_index_file() and
        # _apply_column_mappings() for index-based loading.
        raw_df = self._load_index_file()
        return self._apply_column_mappings(raw_df)
```

`BaseSchemaLoader` provides shared helpers you can reuse:

| Method | Purpose |
|---|---|
| `_load_index_file()` | Read the index file (CSV / TSV / pipe) into a raw DataFrame. |
| `_resolve_index_file()` | Find the index file recursively and return the shallowest match. |
| `_apply_column_mappings()` | Rename and convert columns according to the `columns` schema field. |

### Step 2 — Register the loader

In `src/datacollective/schema_loaders/registry.py`, import your class and add
it to `_TASK_REGISTRY`:

```python
from datacollective.schema_loaders.tasks.mt import MTLoader

_TASK_REGISTRY: dict[str, Type[BaseSchemaLoader]] = {
    "ASR": ASRLoader,
    "TTS": TTSLoader,
    "MT":  MTLoader,      # ← new
}
```

The registry key **must** be uppercase (the SDK normalises the `task` field to
upper case during parsing).

### Step 3 — Write tests

Add a test file `tests/test_mt_loader.py` that exercises your loader with
synthetic data. Follow the existing test patterns in `tests/`.

---

## 2. How to extend or update existing tasks / strategies

### Adding a new strategy to an existing task

Strategies are identified by the `root_strategy` field in the schema. The
strategy constants are defined in
`datacollective.schema_loaders.base`:

| Constant | Value | Used by |
|---|---|---|
| `STRATEGY_MULTI_SPLIT` | `"multi_split"` | ASR |
| `STRATEGY_PAIRED_GLOB` | `"paired_glob"` | TTS |
| `STRATEGY_GLOB` | `"glob"` | *(available for future use)* |

To add a new strategy:

1. **Define a constant** in `base.py` (e.g. `STRATEGY_ALIGNED = "aligned"`).
2. **Export it** from `schema_loaders/__init__.py`.
3. **Add a branch** in the relevant loader's `load()` method:

```python
from datacollective.schema_loaders.base import STRATEGY_ALIGNED

class ASRLoader(BaseSchemaLoader):
    def load(self) -> pd.DataFrame:
        if self.schema.root_strategy == STRATEGY_ALIGNED:
            return self._load_aligned()
        if self.schema.root_strategy == STRATEGY_MULTI_SPLIT:
            return self._load_multi_split()
        return self._load_index_based()
```

4. **Add any new schema fields** to the `DatasetSchema` Pydantic model in
   `schema.py` and update the `known_keys` set in `parse_schema()`.

### Adding new fields to the schema

The `DatasetSchema` model lives in `src/datacollective/schema.py`. To add a
new field:

1. Add it to the `DatasetSchema` class with a sensible default:

```python
class DatasetSchema(BaseModel):
    # ...existing fields...
    my_new_field: str | None = None
```

2. Add the key to the `known_keys` set in `parse_schema()` so it is not
   captured in `extra`:

```python
known_keys = {
    # ...existing keys...
    "my_new_field",
}
```

3. Wire the field into the `DatasetSchema(...)` constructor call at the bottom
   of `parse_schema()`.

The `to_yaml_dict()` method uses `model_dump(exclude_defaults=True)`, so new
fields with default values will only appear in the serialised YAML when
explicitly set.

---

## 3. How to create a `schema.yaml` for a dataset

If you are a **dataset owner** and want your dataset to be loadable via
`load_dataset()`, you need to write a `schema.yaml` and submit it to the
[dataset schema registry](https://github.com/Mozilla-Data-Collective/dataset-schema-registry).

### Step-by-step

1. **Inspect your archive.** Extract it and understand the file layout:
   - Is there a CSV / TSV index file? → Use the **index-based** strategy.
   - Are there separate files per split? → Use **multi-split**.
   - Are there paired text + audio files? → Use **paired-glob**.

2. **Identify your task type.** Pick the task that matches your data (ASR,
   TTS, etc.).  Run `datacollective.schema_loaders.registry.get_task_loader("ASR")`
   to check if the task is already supported.

3. **Write the schema.**  Start from a minimal template:

```yaml
dataset_id: "your-dataset-id"   # must match the MDC dataset ID
task: "ASR"                       # or "TTS", etc.
format: "tsv"
index_file: "metadata.tsv"
columns:
  audio_path:
    source_column: "audio"
    dtype: "file_path"
  transcription:
    source_column: "text"
    dtype: "string"
```

4. **Test locally.** Place the `schema.yaml` in your extracted dataset
   directory and try loading:

```python
from pathlib import Path
from datacollective.schema import parse_schema
from datacollective.schema_loaders.registry import load_dataset_from_schema

schema = parse_schema(Path("path/to/schema.yaml"))
df = load_dataset_from_schema(schema, extract_dir=Path("path/to/extracted/"))
print(df.head())
```

5. **Submit to the registry.** Open a pull request to the
   [dataset-schema-registry](https://github.com/Mozilla-Data-Collective/dataset-schema-registry)
   repository adding your `schema.yaml` under `registry/<your-dataset-id>/schema.yaml`.

### Schema field reference

See the [Schema-Based Loading](schema_parse.md) page for the full field
reference, column mapping syntax, and complete examples.

---

## Architecture overview

### Module map

| Module | Purpose |
|---|---|
| `datacollective.schema` | Pydantic models (`DatasetSchema`, `ColumnMapping`, `ContentMapping`) and `parse_schema()` helper. |
| `datacollective.schema_loaders.base` | Abstract base class `BaseSchemaLoader`, shared helpers (`_load_index_file`, `_resolve_index_file`, `_apply_column_mappings`), format separators (`FORMAT_SEP`), and strategy constants (`STRATEGY_*`). |
| `datacollective.schema_loaders.registry` | `_TASK_REGISTRY` dict mapping task names → loader classes, plus the `load_dataset_from_schema()` entry-point. |
| `datacollective.schema_loaders.cache_schema` | Schema caching layer — `_resolve_schema()` loads a cached `schema.yaml` or fetches from the API, comparing the archive checksum. |
| `datacollective.schema_loaders.tasks.asr` | Loader for **ASR** datasets (index-based and multi-split strategies). |
| `datacollective.schema_loaders.tasks.tts` | Loader for **TTS** datasets (index-based and paired-glob strategies). |
| `datacollective.datasets` | High-level `load_dataset()` function (download → extract → schema → load). |

### Data flow

```
load_dataset("id")
  ├── save_dataset_to_disk()       → downloads archive
  ├── _extract_archive()           → extracts to local dir
  ├── _resolve_schema()            → cached or remote schema.yaml
  │     ├── parse_schema()         → DatasetSchema (Pydantic)
  │     └── _save_schema_to_disk() → caches with checksum
  └── load_dataset_from_schema()
        ├── get_task_loader()      → looks up _TASK_REGISTRY
        └── loader.load()          → returns pd.DataFrame
```

