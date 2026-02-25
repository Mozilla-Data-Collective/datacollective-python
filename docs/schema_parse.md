# Schema-Based Dataset Loading

## Overview

Every dataset on the Mozilla Data Collective platform has an
associated **`schema.yaml`** file that describes how the raw files inside the
archive should be mapped into a **pandas DataFrame** (or other "AI-ready" data formats).  The SDK reads this declarative schema and
automatically selects the respective loading strategy.

Thus, the `df = load_dataset("your-dataset-id")` function will call:

1. `save_dataset_to_disk()` — downloads the archive (with resume support).
2. `_extract_archive()` — extracts `.tar.gz` to the given directory.
3. `_resolve_schema()` — loads a cached `schema.yaml` or fetches from the API.
4. `parse_schema()` — converts YAML into `DatasetSchema` dataclass.
5. `load_dataset_from_schema()` — dispatches to the right loader (ASR / TTS / LM) into a **DataFrame**.


**Main implementation points:**
- **Declarative** — a YAML file describes the way to load the data. No Python code to write per
  dataset, user doesn't need to execute any untrusted code.
- **Extensible** — new task types (MT, dialogue, …) are added by writing one
  Python class and registering it.
- **Consistent** — every dataset loads through the same `load_dataset()` entry-point, always returning a DataFrame.



## Architecture

### Module map

| Module | Purpose |
|---|---|
| `datacollective.schema` | Dataclass hierarchy (`DatasetSchema`, `ColumnMapping`, `ContentMapping`) and `parse_schema()` helper. |
| `datacollective.schema_loaders.base` | Abstract base class `BaseSchemaLoader` that every task-specific loader must subclass. Also provides shared helpers (`_resolve_index_file`, `_apply_column_mappings`) and the `FORMAT_SEP` separator lookup. |
| `datacollective.schema_loaders.registry` | `_TASK_REGISTRY` dict mapping task names → loader classes, plus the `load_dataset_from_schema()` entry-point. |
| `datacollective.schema_loaders.cache_schema` | Schema caching layer — `_resolve_schema()` loads a cached `schema.yaml` or fetches from the API, comparing the archive checksum. |
| `datacollective.schema_loaders.tasks.asr` | Loader for **ASR** (Automatic Speech Recognition) datasets. |
| `datacollective.schema_loaders.tasks.tts` | Loader for **TTS** (Text-to-Speech) datasets — supports both index-based and paired-glob variants. |
| `datacollective.datasets` | High-level `load_dataset()` function (download → extract → schema fetch → load). |


## `schema.yaml` reference

Every schema **must** have two fields:

| Field | Type | Description |
|---|---|---|
| `dataset_id` | `str` | Unique dataset identifier on MDC. |
| `task` | `str` | Task type |

All other fields depend on the **loading strategy** used by the task.

### Loading strategies

There are two strategies, determined by the schema fields that are present:

| Strategy | When to use | Key fields |
|---|---|---|
| **Index-based** | A metadata file (CSV/TSV/pipe) lists each sample with columns you map to logical names. | `format`, `index_file`, `columns`, optionally `base_audio_path`, `separator`, `has_header`, `encoding`. |
| **Glob-based** | No single index file — the loader scans the directory tree for files matching a pattern. | `root_strategy`, `file_pattern`, and either `content_mapping` (LM) or `audio_extension` (paired-glob TTS). |


### Common optional fields

| Field | Default | Description |
|---|---|---|
| `separator` | Inferred from `format` | Explicit column separator (e.g. `"\|"`). |
| `has_header` | `true` | Whether the index file has a header row. When `false`, `source_column` must be a positional integer. |
| `encoding` | `"utf-8"` | File encoding (e.g. `"utf-8-sig"` for files with a BOM). |
| `base_audio_path` | `""` | Directory prefix prepended to `file_path` dtype columns. |

### Column mapping (`columns`)

Used by index-based loaders.  Each key is the **logical** column name that will
appear in the resulting DataFrame.
```yaml
columns:
  audio_path:
    source_column: "path"   # column name in the index file (or int for headerless)
    dtype: "file_path"      # see dtype table below
  transcription:
    source_column: "sentence"
    dtype: "string"
  speaker_id:
    source_column: "client_id"
    dtype: "category"
    optional: true           # skip silently if missing
```

**Supported dtypes:**

| dtype | Behaviour |
|---|---|
| `string` | Cast to `str`. |
| `file_path` | Resolve to an absolute path: `extract_dir / base_audio_path / value`. |
| `category` | Cast to pandas `Categorical`. |
| `int` | Numeric coercion → nullable `Int64`. |
| `float` | Numeric coercion → `float64`. |

### Content mapping (`content_mapping`)


Used by the LM glob-based loader to describe how file contents become DataFrame
columns.

```yaml
content_mapping:
  text: "file_content"     # each file's text becomes the "text" column
  meta_source: "file_name" # filename becomes the "meta_source" column
```


## Task types

Read the docstrings in the respective loader classes for detailed field requirements and examples.


## How to add a new task type

Adding support for a new task (e.g. **MT** — Machine Translation) takes three
steps:

### Step 1 — Create the loader class

Create a new file `src/datacollective/schema_loaders/mt.py`:

```python
from __future__ import annotations
from pathlib import Path
import pandas as pd
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader, FORMAT_SEP
class MTLoader(BaseSchemaLoader):
    """Load a machine-translation dataset."""
    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)
        # validate required schema fields here
        if not schema.index_file:
            raise ValueError("MT schema must specify 'index_file'")
    def load(self) -> pd.DataFrame:
        # BaseSchemaLoader provides _resolve_index_file() and
        # _apply_column_mappings() for index-based loading.
        index_path = self._resolve_index_file()
        sep = self.schema.separator or FORMAT_SEP.get(self.schema.format or "", ",")
        header = "infer" if self.schema.has_header else None
        raw_df = pd.read_csv(index_path, sep=sep, header=header,
                             encoding=self.schema.encoding)
        return self._apply_column_mappings(raw_df)
```

### Step 2 — Register it

In `src/datacollective/schema_loaders/registry.py`, import and add the class:

```python
from datacollective.schema_loaders.mt import MTLoader
_TASK_REGISTRY: dict[str, Type[BaseSchemaLoader]] = {
    "ASR": ASRLoader,
    "LM":  LMLoader,
    "TTS": TTSLoader,
    "MT":  MTLoader,      # ← new
}
```

### Step 3 — Write a `schema.yaml`
```yaml
dataset_id: "my-mt-dataset"
task: "MT"
format: "tsv"
index_file: "parallel.tsv"
columns:
  source_text:
    source_column: "src"
    dtype: "string"
  target_text:
    source_column: "tgt"
    dtype: "string"
  language_pair:
    source_column: "pair"
    dtype: "category"
```

Now the `load_dataset()` will be able to route datasets of `task: "MT"` schemas to your new loader automatically.


### Usage

From the user POV all this logic is hidden. The user only needs the dataset ID and the SDK will handle the download,
extraction, schema fetching and parsing:

```python
from datacollective import load_dataset
df = load_dataset("your-dataset-id")
print(df.head())
```

Under the hood this calls:
1. `save_dataset_to_disk()` — downloads the archive (with resume support).
2. `_extract_archive()` — extracts `.tar.gz` / `.zip`.
3. `_resolve_schema()` — loads a cached `schema.yaml` or fetches from the API (see below).
4. `parse_schema()` — converts YAML into `DatasetSchema` dataclass.
5. `load_dataset_from_schema()` — dispatches to the right loader (ASR / TTS / LM) into a **DataFrame**.


## Schema Caching

The SDK caches the `schema.yaml` inside the extracted dataset directory to
minimise redundant API calls.  The caching logic
(`datacollective.schema_loaders.cache_schema`) works as follows:

1. When `load_dataset()` runs, it obtains a download plan from the API which
   includes the **archive checksum** of the dataset.
2. If a `schema.yaml` exists locally **with a checksum**, the stored checksum
   is compared against the archive checksum.  When they match the local copy is
   returned without fetching the remote schema.
3. If the checksums differ (e.g. the dataset archive was updated), or no local
   cache exists, the remote schema is fetched from the registry.  The archive
   checksum is then written into the cached `schema.yaml` so that subsequent
   loads can skip the remote fetch.
4. If the remote registry does not have a schema for the dataset, a local
   cached copy is used as a fallback when available.


